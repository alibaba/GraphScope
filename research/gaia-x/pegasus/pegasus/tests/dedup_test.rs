use pegasus::JobConf;
use pegasus::api::{Sink, Map, Dedup, Count};

#[test]
fn map_dedup_flatmap_test() {
    let mut conf = JobConf::new("map_dedup_flatmap_test");
    conf.set_workers(2);
    let mut res = pegasus::run(conf, || {
        move |source, sink| {
            source.input_from(0..10_000u64)?
                .map(|x| Ok(x + 1))?
                .dedup()?
                .repartition(|x| Ok(*x))
                .flat_map(|x| Ok(std::iter::repeat(x).take(2)))?
                .sink_into(sink)
        }
    }).expect("submit job failure");

    let mut count = 0;
    while let Some(Ok(_)) = res.next() {
       count += 1;
    }
    assert_eq!(10_000 * 2, count);
}
