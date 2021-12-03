use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use pegasus::api::{Iteration, Map, Reduce, Sink};
use pegasus::resource::PartitionedResource;
use pegasus::{Configuration, JobConf, ServerConf};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "Logistic Regression", about = "logistic regression")]
struct Config {
    /// The number of hop this job will search;
    #[structopt(short = "t", default_value = "100")]
    iters: u32,
    /// The path of the origin graph data ;
    #[structopt(long = "data", parse(from_os_str))]
    data_path: PathBuf,
    /// the number of partitions to partition the local graph;
    #[structopt(short = "p", default_value = "1")]
    partitions: u32,
    #[structopt(short = "s", long = "servers")]
    servers: Option<PathBuf>,
}

fn main() {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();
    let server_conf = if let Some(ref servers) = config.servers {
        let servers = std::fs::read_to_string(servers).unwrap();
        Configuration::parse(&servers).unwrap()
    } else {
        Configuration::singleton()
    };
    pegasus::startup(server_conf).unwrap();

    let mut conf = JobConf::new("LR");
    conf.set_workers(config.partitions);
    if config.servers.is_some() {
        conf.reset_servers(ServerConf::All);
    }

    let (length, samples) = load_samples(&conf, &config.data_path).unwrap();
    pegasus::wait_servers_ready(conf.servers());

    let max_iters = config.iters;
    pegasus::run_with_resources(conf, samples, || {
        let index = pegasus::get_current_worker().index;
        let init = if index == 0 { Some(init_empty(length)) } else { None };
        move |input, output| {
            input
                .input_from(init)?
                .iterate(max_iters, |start| {
                    start
                        .broadcast()
                        .map(move |delta| {
                            let mut lr = pegasus::resource::get_resource_mut::<LRData>().unwrap();
                            lr.update_params(delta);
                            Ok(lr.apply())
                        })?
                        .reduce(|| |a, b| Ok(vector_sum(a, b)))?
                        .unfold(|vec| Ok(Some(vec).into_iter()))
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure");
    pegasus::shutdown_all();
}

struct LRData {
    partition: usize,
    samples: Matrix,
    expected: Vector,
    params: Vector,
}

impl LRData {
    pub fn new(partition: usize, samples: Matrix, expected: Vector) -> Self {
        let features_len = samples[0].len();
        let params = vec![1.0; features_len];
        LRData { partition, samples, expected, params }
    }

    pub fn update_params(&mut self, delta: Vector) {
        assert_eq!(self.params.len(), delta.len());
        for i in 0..self.params.len() {
            let delta = delta[i] / 10.0;
            self.params[i] -= delta;
        }
        println!("p[{}]: coefficients: {:?}", self.partition, self.params);
    }

    pub fn apply(&self) -> Vector {
        let product = calculate_product(&self.params, &self.samples, &self.expected);
        let mut result = Vector::with_capacity(self.params.len());
        for i in 0..self.params.len() {
            let mut sum = 0.0;
            for j in 0..product.len() {
                sum += product[j] * self.samples[j][i];
            }
            result.push(sum);
        }
        println!("p[{}]: partial params delta {:?}", self.partition, result);
        result
    }
}

type Vector = Vec<f64>;
type Matrix = Vec<Vector>;

fn init_empty(length: usize) -> Vector {
    vec![0f64; length]
}

fn load_samples<A: AsRef<Path>>(
    conf: &JobConf, path: A,
) -> std::io::Result<(usize, PartitionedResource<LRData>)> {
    let spt = std::env::var("PEGASUS_CSV_SPLIT")
        .unwrap_or(",".to_owned())
        .parse::<char>()
        .unwrap();
    println!("use split '{}'", spt);
    let file = BufReader::new(std::fs::File::open(path)?);
    let mut partitions = Vec::new();
    for _ in 0..conf.workers {
        partitions.push((vec![], vec![]));
    }
    let mut features_len = 0;
    for (n, line) in file.lines().enumerate() {
        if n > 0 {
            let err_msg = format!("parse {}th line failure:", n);
            let index = n % conf.workers as usize;
            let line = line?;
            let mut split = line.split(spt).filter(|s| !s.is_empty());
            let e = split
                .next()
                .expect(&err_msg)
                .parse::<f64>()
                .expect(&err_msg);
            partitions[index].0.push(e);
            let mut features = Vec::new();
            features.push(1.0);
            for item in split {
                let n = item.parse::<f64>().expect(&err_msg) / 100.0;
                features.push(n);
            }
            if features_len == 0 {
                features_len = features.len();
            } else {
                assert_eq!(features.len(), features_len);
            }
            partitions[index].1.push(features);
        }
    }
    let mut lr_parts = Vec::new();
    for (expect, samples) in partitions {
        println!("partition {} has {} samples;", lr_parts.len(), samples.len());
        let lr = LRData::new(lr_parts.len(), samples, expect);
        lr_parts.push(lr);
    }

    if let Ok(p) = PartitionedResource::new(conf, lr_parts) {
        Ok((features_len, p))
    } else {
        unreachable!("")
    }
}

fn calculate_product(params: &Vector, samples: &Matrix, expected: &Vector) -> Vector {
    let mut r = Vec::with_capacity(params.len());
    for i in 0..samples.len() {
        assert_eq!(params.len(), samples[i].len());
        let mut sum = 0.0;
        for j in 0..params.len() {
            sum += params[j] * samples[i][j];
        }
        let get = sigmoid(sum);
        // println!(
        //     "calculate {:?} * {:?} = {}, expect {}, get {:.2}",
        //     samples[i], params, sum, expected[i], get
        // );
        r.push(get - expected[i]);
    }
    r
}

#[inline]
fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

fn vector_sum(left: Vector, right: Vector) -> Vector {
    assert_eq!(left.len(), right.len());
    let mut r = Vector::with_capacity(left.len());
    for i in 0..left.len() {
        r.push(left[i] + right[i]);
    }
    r
}
