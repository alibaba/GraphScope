use pegasus::api::{Collect, CorrelatedSubTask, Iteration, Limit, Map, OrderLimit, OrderLimitBy, Sink};
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
                .input_from(1..500u32)?
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
        assert!(d.0 < 500);
        count += 1;
    }

    assert_eq!(count, 998);
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

#[test]
fn sort_limit_test() {
    let mut conf = JobConf::new("sort_limit_test");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            let src = if index == 0 { input.input_from(1..100u32) } else { input.input_from(vec![]) }?;
            src.repartition(|x: &u32| Ok(*x as u64))
                .sort_limit(10)?
                .sink_into(output)
        }
    })
    .expect("build job failure");

    let mut vec = vec![];
    while let Some(Ok(d)) = result.next() {
        assert!(d <= 10);
        vec.push(d);
    }

    let expected = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    assert_eq!(vec, expected);
}

#[test]
fn sort_limit_by_test() {
    let mut conf = JobConf::new("sort_limit_by_test");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            let src = if index == 0 { input.input_from(1..100u32) } else { input.input_from(vec![]) }?;
            src.repartition(|x: &u32| Ok(*x as u64))
                .sort_limit_by(10, |x, y| x.cmp(y).reverse())?
                .sink_into(output)
        }
    })
    .expect("build job failure");

    let mut vec = vec![];
    while let Some(Ok(d)) = result.next() {
        assert!(d >= 90);
        vec.push(d);
    }

    let expected = vec![99, 98, 97, 96, 95, 94, 93, 92, 91, 90];
    assert_eq!(vec, expected);
}
