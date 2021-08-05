use pegasus::api::{Collect, CorrelatedSubTask, Iteration, Limit, Map, Sink};
use pegasus::JobConf;

// the most common case with early-stop
#[test]
fn limit_test_01() {
    let mut conf = JobConf::new("limit_test_01");
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
}

// early-stop with loop, triggered OUTSIDE loop
#[test]
fn limit_test_02() {
    let mut conf = JobConf::new("limit_test_02");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(1..1000u32)?
                .iterate(2, |start| {
                    start
                        .repartition(|x: &u32| Ok(*x as u64))
                        .flat_map(|i| Ok(0..i))
                })?
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
}

// early-stop with loop, triggered INSIDE loop
#[test]
fn limit_test_03() {
    let mut conf = JobConf::new("limit_test_03");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(1..1000u32)?
                .iterate(2, |start| {
                    start
                        .repartition(|x: &u32| Ok(*x as u64))
                        .flat_map(|i| Ok(0..i))?
                        .limit(10)
                })?
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
}

// early-stop with subtask, triggered OUTSIDE subtask
#[test]
fn limit_test_04() {
    let mut conf = JobConf::new("limit_test_04");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(1..1000u32)?
                .apply(|sub| {
                    sub.flat_map(|i| Ok(0..i))?
                        .repartition(|x: &u32| Ok(*x as u64))
                        .flat_map(|i| Ok(0..i))?
                        .limit(1)? // mock has_any operator
                        .collect::<Vec<_>>()
                })?
                .limit(10)?
                .sink_into(output)
        }
    })
    .expect("build job failure");

    let mut count = 0;
    while let Some(Ok(d)) = result.next() {
        assert!(d.0 < 1000);
        count += 1;
    }

    assert_eq!(count, 10);
}

// early-stop with subtask, triggered INSIDE subtask
#[test]
fn limit_test_05() {
    let mut conf = JobConf::new("limit_test_05");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(1..1000u32)?
                .apply(|sub| {
                    sub.flat_map(|i| Ok(0..i))?
                        .repartition(|x: &u32| Ok(*x as u64))
                        .flat_map(|i| Ok(0..i + 1))?
                        .limit(1)? // mock has_any operator
                        .collect::<Vec<_>>()
                })?
                .sink_into(output)
        }
    })
    .expect("build job failure");

    let mut count = 0;
    while let Some(Ok(d)) = result.next() {
        assert!(d.0 < 1000);
        count += 1;
    }

    assert_eq!(count, 1998);
}

// early-stop with subtask in loop, triggered INSIDE subtask
#[test]
fn limit_test_06() {
    let mut conf = JobConf::new("limit_test_06");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(1..1001u32)?
                .iterate(2, |start| {
                    start
                        .flat_map(|i| Ok(i..i + 2))?
                        .apply(|sub| {
                            sub.repartition(|x: &u32| Ok(*x as u64))
                                .flat_map(|i| Ok(0..i))?
                                .limit(1)? // mock has_any operator
                                .collect::<Vec<_>>()
                        })?
                        .map(|(i, _)| Ok(i))
                })?
                .sink_into(output)
        }
    })
    .expect("build job failure");

    let mut count = 0;
    while let Some(Ok(d)) = result.next() {
        assert!(d < 1003);
        count += 1;
    }

    assert_eq!(count, 8000);
}

// early-stop with subtask in loop, triggered between OUTSIDE subtask but INSIDE loop
#[test]
fn limit_test_07() {
    let mut conf = JobConf::new("limit_test_07");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(1..100u32)?
                .iterate(2, |start| {
                    start
                        .flat_map(|i| Ok(0..i))?
                        .apply(|sub| {
                            sub.repartition(|x: &u32| Ok(*x as u64))
                                .flat_map(|i| Ok(0..i))?
                                .limit(1)? // mock has_any operator
                                .collect::<Vec<_>>()
                        })?
                        .map(|(i, _)| Ok(i))?
                        .limit(10)
                })?
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
}

// early-stop with subtask in loop, triggered OUTSIDE loop
#[test]
fn limit_test_08() {
    let mut conf = JobConf::new("limit_test_08");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(1..100u32)?
                .iterate(2, |start| {
                    start
                        .flat_map(|i| Ok(0..i))?
                        .apply(|sub| {
                            sub.repartition(|x: &u32| Ok(*x as u64))
                                .flat_map(|i| Ok(0..i))?
                                .limit(1)? // mock has_any operator
                                .collect::<Vec<_>>()
                        })?
                        .map(|(i, _)| Ok(i))
                })?
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
}
