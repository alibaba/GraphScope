use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::time::Instant;

use bmcsr::graph_db::GraphDB;
use bmcsr::graph_modifier::{DeleteGenerator, GraphModifier};
use bmcsr::schema::InputSchema;
use graph_index::GraphIndex;
use pegasus::JobConf;
use rpc_server::queries::register::QueryRegister;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "g", long = "graph_data", default_value = "")]
    graph_data: PathBuf,
    #[structopt(short = "q", long = "queries_config", default_value = "")]
    queries_config: String,
    #[structopt(short = "i", long = "input", default_value = "")]
    input: PathBuf,
    #[structopt(short = "o", long = "output", default_value = "")]
    output: String,
    #[structopt(short = "r", long = "graph_raw_data", default_value = "")]
    graph_raw: PathBuf,
    #[structopt(short = "b", long = "batch_update_configs", default_value = "")]
    batch_update_configs: PathBuf,
    #[structopt(short = "w", long = "worker_num", default_value = "8")]
    worker_num: u32,
    #[structopt(short = "p", long = "parallel", default_value = "0")]
    parallel: u32,
}

fn parse_input(path: &PathBuf) -> Vec<(String, String)> {
    let mut ret = vec![];

    let file = File::open(&path).unwrap();
    let reader = BufReader::new(file);

    for result in reader.lines() {
        let record = result.unwrap();
        let parts: Vec<String> = record
            .split('|')
            .map(|s| s.to_string())
            .collect();
        assert!(parts.len() > 2);
        let query_name = parts[0].clone();
        let params = parts[1].clone();

        ret.push((query_name, params))
    }

    ret
}

fn parse_params(input: &String) -> HashMap<String, String> {
    let ret: HashMap<String, String> = serde_json::from_str(input.as_str()).unwrap();
    ret
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();

    let graph_data_str = config.graph_data.to_str().unwrap();
    let mut graph = GraphDB::<usize, usize>::deserialize(graph_data_str, 0, None).unwrap();
    let mut graph_index = GraphIndex::new(0);

    let mut query_register = QueryRegister::new();
    query_register.load(&PathBuf::from(&config.queries_config));

    let graph_raw = config.graph_raw;
    let batch_configs = config.batch_update_configs;

    let batch_id = "2012-11-29";

    let insert_file_name = format!("insert-{}.json", &batch_id);
    let insert_schema_file_path = batch_configs.join(insert_file_name);
    let delete_file_name = format!("delete-{}.json", &batch_id);
    let delete_schema_file_path = batch_configs.join(delete_file_name);

    let input_parameters = parse_input(&config.input);

    let update_ts = Instant::now();

    let mut graph_modifier = GraphModifier::new(&graph_raw);
    graph_modifier.skip_header();
    println!("parallel strategy: {}", config.parallel);
    graph_modifier.parallel(config.parallel);

    let insert_ts = Instant::now();
    let insert_schema = InputSchema::from_json_file(insert_schema_file_path, &graph.graph_schema).unwrap();
    graph_modifier
        .insert(&mut graph, &insert_schema)
        .unwrap();
    let insert_duration = insert_ts.elapsed().as_millis() as f64 / 1000.0;

    let delete_generate_ts = Instant::now();
    let mut delete_generator = DeleteGenerator::new(&graph_raw);
    delete_generator.skip_header();
    delete_generator.generate(&mut graph, &batch_id);
    let delete_generate_duration = delete_generate_ts.elapsed().as_millis() as f64 / 1000.0;

    let delete_ts = Instant::now();
    let delete_schema = InputSchema::from_json_file(delete_schema_file_path, &graph.graph_schema).unwrap();
    graph_modifier
        .delete(&mut graph, &delete_schema)
        .unwrap();
    let delete_duration = delete_ts.elapsed().as_millis() as f64 / 1000.0;

    println!(
        "Update: {} s ({} s, {} s, {} s)",
        update_ts.elapsed().as_millis() as f64 / 1000.0,
        insert_duration,
        delete_generate_duration,
        delete_duration
    );
    // return Ok(());

    let precompute_ts = Instant::now();
    if !config.queries_config.is_empty() {
        query_register.run_precomputes(&graph, &mut graph_index, config.worker_num);
    }
    println!("Precompute: {} s", precompute_ts.elapsed().as_millis() as f64 / 1000.0);

    let mut query_results = vec![];
    let mut metrics = HashMap::<String, (f64, usize)>::new();

    let query_ts = Instant::now();
    for (query_name, input) in input_parameters.iter() {
        let full_query_name = format!("bi{}", query_name);
        let query = query_register
            .get_read_query(&full_query_name)
            .expect("Could not find query");
        let mut conf = JobConf::new(full_query_name.clone());
        conf.set_workers(config.worker_num);
        conf.reset_servers(pegasus::ServerConf::Partial(vec![0]));
        let q_ts = Instant::now();
        let result = {
            pegasus::run(conf.clone(), || {
                query.Query(conf.clone(), &graph, &graph_index, parse_params(&input))
            })
            .expect("submit query failure")
        };
        let mut result_list = vec![];
        for x in result {
            let ret = x.expect("Fail to get result");
            let ret = String::from_utf8(ret).unwrap();
            let ret = format!("\"{}\"", ret);
            result_list.push(ret);
        }
        let q_ts = q_ts.elapsed().as_micros() as f64 / 1000000.0;
        if let Some((mut total, mut count)) = metrics.get_mut(&full_query_name) {
            total += q_ts;
            count += 1;
            metrics.insert(full_query_name.clone(), (total, count));
        } else {
            metrics.insert(full_query_name.clone(), (q_ts, 1));
        }
        let result_string = result_list.join(", ");
        query_results.push(result_string);
    }
    println!("Query: {} s", query_ts.elapsed().as_millis() as f64 / 1000.0);

    for (query_name, (total, count)) in metrics.iter() {
        println!("\t{}\t: avg = {:.4} s, count = {}", query_name, *total / (*count as f64), *count);
    }

    let mut output_file = File::create(&config.output).unwrap();
    let query_num = input_parameters.len();
    for i in 0..query_num {
        let line = format!("{}|{}|[{}]\n", input_parameters[i].0, input_parameters[i].1, query_results[i]);
        output_file.write_all(line.as_bytes()).unwrap();
    }
    output_file.flush().unwrap();

    Ok(())
}
