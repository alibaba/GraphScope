use pegasus::api::{Limit, Map, Sink};
use pegasus::{Configuration, JobConf};

fn main() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("bench_limit");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(1..1000u32)?
                .flat_map(|i| Ok(0..i))?
                .repartition(|x: &u32| Ok(*x as u64))
                .flat_map(|i| Ok(0..i))?
                .limit(10)?
                .sink_into(output)
        }
    })
    .expect("build job failure");

    let mut count = 0;
    while let Some(Ok(d)) = result.next() {
        assert!(d < 1000);
        count += 1;
    }

    assert_eq!(count, 10);
    pegasus::shutdown_all();
}
