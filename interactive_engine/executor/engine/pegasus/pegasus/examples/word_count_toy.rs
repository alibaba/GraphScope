extern crate pegasus;

use std::path::PathBuf;

use pegasus::api::*;
use pegasus::{Configuration, JobConf};
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "s", long = "servers")]
    servers: Option<PathBuf>,
    #[structopt(short = "w", long = "workers", default_value = "1")]
    workers: u32,
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
    pegasus::startup(server_conf).ok();
    let mut conf = JobConf::new("word_count_toy");
    conf.set_workers(config.workers);
    let mut result = pegasus::run(conf, || {
        let id = pegasus::get_current_worker().index;
        move |input, output| {
            let lines = if id == 0 {
                vec!["This is a simple test".to_string(), "This is a wonderful world".to_string()]
                    .into_iter()
            } else {
                vec![].into_iter()
            };
            input
                .input_from(lines)?
                .flat_map(|line| {
                    let words = line
                        .split(' ')
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>();
                    Ok(words.into_iter())
                })?
                .key_by(|word| Ok((word, 1)))?
                .reduce_by_key(|| |a, b| Ok(a + b))?
                .unfold(|map| Ok(map.into_iter()))?
                .collect::<Vec<(String, u32)>>()?
                .sink_into(output)
        }
    })
    .expect("run job failure;");

    let mut result = result.next().unwrap().unwrap();
    result.sort_by(|a, b| b.1.cmp(&a.1));
    for (word, count) in result {
        println!("word: {}, count: {}", word, count);
    }
    pegasus::shutdown_all();
}
