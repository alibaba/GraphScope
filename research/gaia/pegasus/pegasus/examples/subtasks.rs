use pegasus::api::function::*;
use pegasus::api::{Exchange, Map, ResultSet, Sink, SubTask};
use pegasus::communication::Pipeline;
use pegasus::{route, Tag};
use pegasus::{Configuration, JobConf};
use std::time::Instant;
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "n", long = "num", default_value = "10")]
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
    let mut conf = JobConf::new("bench_subtask");
    conf.set_workers(2);
    conf.plan_print = true;
    conf.batch_size = config.batch_size as u32;
    conf.output_capacity = config.capacity as u32;
    let start = Instant::now();
    let num = config.number as u64;
    let (tx, rx) = crossbeam_channel::unbounded();
    let mut guard = pegasus::run(conf, move |worker| {
        let index = worker.id.index;
        let tx = tx.clone();
        worker.dataflow(move |builder| {
            let src = if index == 0 {
                builder.input_from_iter(0..num)?
            } else {
                builder.input_from_iter(num..2 * num)?
            };
            let sub = src.fork_subtask(|start| {
                start
                    .exchange(route!(|item: &u64| *item))?
                    .map_with_fn(Pipeline, |item| Ok(item + 1))
            })?;

            src.join_subtask(sub, |l, r| Some((*l, r)))?.sink_by(|_info| {
                move |_t: &Tag, result: ResultSet<(u64, u64)>| match result {
                    ResultSet::Data(vec) => {
                        for item in vec {
                            tx.send(item).ok();
                        }
                    }
                    ResultSet::End => {}
                }
            })?;
            Ok(())
        })
    })
    .expect("build job failure")
    .unwrap();
    let mut count = 0;
    while let Ok(d) = rx.recv() {
        if count < 10 {
            println!("{}: {}=>{}", count, d.0, d.1);
        }
        if count == 10 {
            println!("...");
        }
        assert_eq!(d.0 + 1, d.1);
        count += 1;
    }
    assert_eq!(count, num * 2);
    guard.join().expect("run job failure");
    println!("cost {:?}", start.elapsed());
    pegasus::shutdown_all();
}
