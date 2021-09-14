use pegasus::api::{CorrelatedSubTask, Count, Map, Sink};
use pegasus::JobConf;

#[test]
fn flat_map_test_1() {
    let conf = JobConf::new("flat_map_test_1");
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(1..1000u32)?
                .flat_map(|i| Ok(0..i))?
                .sink_into(output)
        }
    })
    .expect("build job failure");

    let mut count = 0;
    while let Some(Ok(d)) = result.next() {
        assert!(d < 1000);
        count += 1;
    }

    assert_eq!(count, 500 * (1000 - 1));
}

#[test]
fn flat_map_test_2() {
    let conf = JobConf::new("flat_map_test_2");
    let num = 1000u32;
    let mut result = pegasus::run(conf, move || {
        move |input, output| {
            input
                .input_from(1..num)?
                .repartition(|x| Ok(*x as u64))
                .flat_map(|i| Ok(0..i))?
                .count()?
                .sink_into(output)
        }
    })
    .expect("build job failure");
    let expected = (1..num).flat_map(|x| 0..x).count() as u64;
    assert_eq!(expected, result.next().unwrap().unwrap());
}

#[test]
fn flat_map_test_3() {
    let mut conf = JobConf::new("flat_map_test_3");
    conf.set_workers(2);
    let num = 100u32;
    let mut result = pegasus::run(conf, move || {
        let idx = pegasus::get_current_worker().index;
        let src = idx * num..(idx + 1) * num;
        move |input, output| {
            input
                .input_from(src)?
                .flat_map(|i| Ok(0..i + 1))?
                .repartition(|x| Ok(*x as u64))
                .flat_map(|i| Ok(0..i))?
                .count()?
                .sink_into(output)
        }
    })
    .expect("build job failure");

    let expected = (0..num * 2)
        .flat_map(|i| (0..i + 1))
        .flat_map(|i| (0..i))
        .count() as u64;
    assert_eq!(expected, result.next().unwrap().unwrap());
}

#[test]
fn flat_map_test_4() {
    let mut conf = JobConf::new("flat_map_test_4");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, move || {
        let src = if pegasus::get_current_worker().index == 0 { vec![100, 100] } else { vec![] };
        move |input, output| {
            input
                .input_from(src)?
                .apply(|sub| {
                    sub.flat_map(|i| Ok(0..i))?
                        .repartition(|x| Ok(*x as u64))
                        .flat_map(|i| Ok(0..i))?
                        .count()
                })?
                .sink_into(output)
        }
    })
    .expect("build job failure");

    let expected = (0..100u32).flat_map(|i| (0..i)).count() as u64;
    assert_eq!(expected, result.next().unwrap().unwrap().1);
    assert_eq!(expected, result.next().unwrap().unwrap().1);
}
