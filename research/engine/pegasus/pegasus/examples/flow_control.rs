use std::time::Instant;

use pegasus::api::{Filter, Map, Sink};
use pegasus::{Configuration, JobConf};
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "n", long = "num", default_value = "1000")]
    number: usize,
    /// config size of each batch;
    #[structopt(short = "b", long = "batch", default_value = "1024")]
    batch_size: usize,
    #[structopt(short = "c", long = "cap", default_value = "64")]
    capacity: usize,
}

fn main() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let config: Config = Config::from_args();
    let mut conf = JobConf::new("flatmap");
    conf.plan_print = true;
    conf.batch_size = config.batch_size as u32;
    conf.batch_capacity = config.capacity as u32;
    let start = Instant::now();
    let num = config.number as u64;
    let mut result = pegasus::run(conf, move || {
        move |input, output| {
            input
                .input_from(1..num)?
                .flat_map(|i| Ok(0..i))?
                .filter(|_| Ok(false))?
                .sink_into(output)
        }
    })
    .expect("build job failure");

    while let Some(Ok(_)) = result.next() {
        //
    }

    println!("cost {:?}", start.elapsed());
    pegasus::shutdown_all();
}
