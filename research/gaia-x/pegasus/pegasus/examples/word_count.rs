use std::collections::HashMap;
use std::io::BufRead;
use std::path::{Path, PathBuf};

use pegasus::api::{KeyBy, Map, ReduceByKey, Sink};
use pegasus::{Configuration, JobConf};
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(long = "data", parse(from_os_str))]
    path: PathBuf,

    #[structopt(short = "w", long = "workers", default_value = "2")]
    workers: u32,
}

fn main() {
    let config: Config = Config::from_args();
    let lines = open(&config.path).unwrap();

    // word count by iterator;
    let mut result = lines
        .into_iter()
        .flat_map(|line| line.into_words())
        .map(|word| (word, 1))
        .fold(HashMap::new(), |mut group, (word, cnt)| {
            let count = group.entry(word).or_insert(0);
            *count += cnt;
            group
        })
        .into_iter()
        .collect::<Vec<_>>();

    result.sort_by(|a, b| b.1.cmp(&a.1));

    for (word, count) in result {
        println!("word:{} count = {}", word, count);
    }

    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();

    let mut conf = JobConf::new("word count");
    conf.set_workers(config.workers);
    let mut result = pegasus::run(conf, || {
        let id = pegasus::get_current_worker().index;
        let path = config.path.clone();
        move |input, output| {
            let lines = if id == 0 { open(path).unwrap() } else { Box::new(std::iter::empty()) };
            input
                .input_from(lines)?
                .flat_map(|line| Ok(line.into_words()))?
                .key_by(|word| Ok((word, 1)))?
                .reduce_by_key(|| |a, b| Ok(a + b))?
                .sink_into(output)
        }
    })
    .expect("run job failure;");

    let result = result.next().unwrap().unwrap();
    for (word, count) in result {
        println!("word:{} count = {}", word, count);
    }

    pegasus::shutdown_all();
}

trait IntoWords {
    fn into_words(self) -> Box<dyn Iterator<Item = String> + Send>;
}

impl IntoWords for String {
    fn into_words(self) -> Box<dyn Iterator<Item = String> + Send> {
        todo!()
    }
}

fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Box<dyn Iterator<Item = String> + Send>> {
    let file = std::fs::File::open(path)?;
    let buf = std::io::BufReader::new(file);
    let iter = buf.lines().map(|line| line.unwrap());
    Ok(Box::new(iter))
}
