use pegasus::api::{CorrelatedSubTask, HasAny, Iteration, Limit, Map, Merge, Sink};
use pegasus::JobConf;

// the most common case with early-stop
#[test]
fn flatmap_limit_test() {
    let mut conf = JobConf::new("flatmap_limit_test");
    conf.set_workers(2);
    conf.batch_capacity = 1;
    conf.batch_size = 64;
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(0..100_000_000u32)?
                .repartition(|x: &u32| Ok(*x as u64))
                .flat_map(|i| Ok(std::iter::repeat(i)))?
                .limit(10)?
                .sink_into(output)
        }
    })
    .expect("build job failure");

    let mut count = 0;
    while let Some(Ok(_)) = result.next() {
        count += 1;
    }

    assert_eq!(count, 10);
}

#[test]
fn flatmap_copied_limit_test() {
    let mut conf = JobConf::new("flatmap_copied_limit_test");
    conf.set_workers(2);
    conf.batch_capacity = 2;
    conf.batch_size = 64;
    let mut result = pegasus::run(conf, || {
        |input, output| {
            let (left, right) = input
                .input_from(0..100_0000_000u32)?
                .repartition(|x: &u32| Ok(*x as u64))
                .flat_map(|i| Ok(0..i + 1))?
                .copied()?;

            let left = left.limit(10)?;
            let right = right.limit(100)?;

            left.merge(right)?.sink_into(output)
        }
    })
    .expect("build job failure");

    let mut count = 0;
    while let Some(Ok(_)) = result.next() {
        count += 1;
    }

    assert_eq!(count, 110);
}

// iterate(...).limit(..)
#[test]
fn iterate_x_flatmap_x_limit_test() {
    let mut conf = JobConf::new("iterate_x_flatmap_x_limit_test");
    conf.set_workers(2);
    conf.batch_capacity = 2;
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from((1..1000).cycle())?
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

// iterate(..limit(x))
#[test]
fn iterate_x_flatmap_limit_x_test() {
    let mut conf = JobConf::new("iterate_x_flatmap_limit_x_test");
    conf.set_workers(2);
    conf.batch_capacity = 2;
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(0..10u32)?
                .iterate(2, |start| {
                    start
                        .repartition(|x: &u32| Ok(*x as u64))
                        .flat_map(|i| Ok(std::iter::repeat(i)))?
                        .limit(10)
                })?
                .sink_into(output)
        }
    })
    .expect("build job failure");

    let mut count = 0;
    while let Some(Ok(_)) = result.next() {
        count += 1;
    }

    assert_eq!(count, 10);
}

// early-stop with subtask, triggered OUTSIDE subtask
#[test]
fn apply_x_flatmap_any_x_limit_test() {
    let mut conf = JobConf::new("apply_x_flatmap_any_x_limit_test");
    conf.batch_capacity = 2;
    conf.plan_print = true;
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from((0..1000u32).cycle())?
                .apply_parallel(10, |sub| {
                    sub.repartition(|x: &u32| Ok(*x as u64))
                        .flat_map(|i| Ok(std::iter::repeat(i)))?
                        .any()
                })?
                .limit(10)?
                .sink_into(output)
        }
    })
    .expect("build job failure");

    let mut count = 0;
    while let Some(Ok(d)) = result.next() {
        assert!(d.0 < 1000);
        assert!(d.1);
        count += 1;
    }

    assert_eq!(count, 10);
}

// early-stop with subtask in loop, triggered INSIDE subtask
#[test]
fn iterate_x_apply_x_flatmap_any_x_flatmap_x_test() {
    let mut conf = JobConf::new("iterate_x_apply_x_flatmap_any_x_flatmap_x_test");
    conf.batch_capacity = 2;
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(0..10u32)?
                .iterate(2, |start| {
                    start
                        .apply(|sub| {
                            sub.repartition(|x: &u32| Ok(*x as u64))
                                .flat_map(|i| Ok(std::iter::repeat(i)))?
                                .any()
                        })?
                        .flat_map(|(i, _)| Ok(i..i + 2))
                })?
                .sink_into(output)
        }
    })
    .expect("build job failure");

    let mut count = 0;
    while let Some(Ok(_)) = result.next() {
        count += 1;
    }

    assert_eq!(count, 80);
}

// early-stop with subtask in loop, triggered between OUTSIDE subtask but INSIDE loop
#[test]
fn iterate_x_flatmap_apply_x_flatmap_any_x_limit_map_x_test() {
    let mut conf = JobConf::new("iterate_x_flatmap_apply_x_flatmap_any_x_limit_map_x_test");
    conf.batch_capacity = 2;
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(0..100u32)?
                .iterate(2, |start| {
                    start
                        .flat_map(|i| Ok(std::iter::repeat(i)))?
                        .apply_parallel(4, |sub| {
                            sub.repartition(|x: &u32| Ok(*x as u64))
                                .flat_map(|i| Ok(std::iter::repeat(i)))?
                                .any()
                        })?
                        .limit(10)?
                        .map(|(i, _)| Ok(i))
                })?
                .sink_into(output)
        }
    })
    .expect("build job failure");

    let mut count = 0;
    while let Some(Ok(_)) = result.next() {
        count += 1;
    }

    assert_eq!(count, 10);
}

// early-stop with subtask in loop, triggered OUTSIDE loop
#[test]
fn iterate_x_apply_x_flatmap_any_x_map_x_limit_test() {
    let mut conf = JobConf::new("iterate_x_apply_x_flatmap_any_x_map_x_limit_test");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from((0..100u32).cycle())?
                .iterate(2, |start| {
                    start
                        .apply(|sub| {
                            sub.repartition(|x: &u32| Ok(*x as u64))
                                .flat_map(|i| Ok(std::iter::repeat(i)))?
                                .any()
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
        assert!(d < 100);
        count += 1;
    }

    assert_eq!(count, 10);
}
