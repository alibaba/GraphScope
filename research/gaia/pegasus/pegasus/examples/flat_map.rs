use pegasus::preclude::{Filter, Map, Pipeline};
use pegasus::{Configuration, JobConf};
use std::time::Instant;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
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
    conf.set_workers(1);
    conf.plan_print = true;
    conf.batch_size = config.batch_size as u32;
    conf.output_capacity = config.capacity as u32;
    let start = Instant::now();
    let mut guard = pegasus::run(conf, |worker| {
        worker.dataflow(|builder| {
            let src = builder.input_from_iter(1..100_000u64)?;
            src.flat_map_with_fn(Pipeline, |i| Ok((0..i).into_iter().map(|i| Ok(i))))?
                .filter_with_fn(|_| Ok(false))?;
            Ok(())
        })
    })
    .expect("build job failure")
    .unwrap();

    guard.join().expect("run job failure");
    println!("cost {:?}", start.elapsed());
    pegasus::shutdown_all();
}
