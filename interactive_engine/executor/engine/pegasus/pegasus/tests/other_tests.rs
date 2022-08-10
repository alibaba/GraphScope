use pegasus::api::{Map, Sink};
use pegasus::JobConf;

#[test]
fn flatmap_x_repartition_x_filtermap_x_broadcast_x_test() {
    let mut conf = JobConf::new("tests");
    conf.set_workers(3);

    let mut results = pegasus::run(conf, || {
        |input, output| {
            let worker_id = input.get_worker_index();
            let stream = input.input_from(vec![worker_id as u64])?;
            stream
                .flat_map(|id| Ok(Some(id).into_iter()))?
                .repartition(move |id| Ok(*id % 2))
                .filter_map(|source| Ok(Some(source)))?
                .broadcast()
                .sink_into(output)
        }
    })
    .expect("run job fail;");

    while let Some(next) = results.next() {
        let n = next.unwrap();
        println!("{}", n);
    }
}
