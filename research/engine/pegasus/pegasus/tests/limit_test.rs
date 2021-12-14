//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.
//

#[macro_use]
extern crate lazy_static;

use pegasus::api::{
    Collect, CorrelatedSubTask, HasAny, Iteration, Limit, Map, Merge, Sink, SortLimit, SortLimitBy,
};
use pegasus::JobConf;

lazy_static! {
    pub static ref MAP: std::collections::HashMap<u32, Vec<(u32, f32)>> = vec![
        (1, vec![(2, 0.5f32), (3, 0.4f32), (4, 1.0f32)]),
        (2, vec![]),
        (3, vec![]),
        (4, vec![(3, 0.4f32), (5, 1.0f32)]),
        (5, vec![]),
        (6, vec![(3, 0.2f32)])
    ]
    .into_iter()
    .collect();
}

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

#[test]
fn sort_limit_test() {
    let mut conf = JobConf::new("sort_limit_test");
    let num_workers = 2;
    conf.set_workers(num_workers);

    let result_stream = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            input
                .input_from((1_u32..2000).filter(move |x| *x % num_workers == index))?
                .sort_limit(3)?
                .sink_into(output)
        }
    })
    .expect("submit job failure");

    let results: Vec<u32> = result_stream.map(|x| x.unwrap()).collect();

    assert_eq!(results, vec![1_u32, 2, 3]);
}

#[test]
fn multi_scope_sort_limit_test() {
    let mut conf = JobConf::new("sort_limit_test");
    let num_workers = 2;
    conf.set_workers(num_workers);

    let result_stream = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            input
                .input_from((1_u32..5).filter(move |x| *x % num_workers == index))?
                .apply(|sub| {
                    sub.flat_map(|x| Ok(0..x))?
                        .sort_limit(3)?
                        .collect::<Vec<u32>>()
                })?
                .sort_limit_by(3, |x, y| y.0.cmp(&x.0))?
                .sink_into(output)
        }
    })
    .expect("submit job failure");

    let results: Vec<(u32, Vec<u32>)> = result_stream.map(|x| x.unwrap()).collect();

    assert_eq!(results, vec![(4, vec![0, 1, 2]), (3, vec![0, 1, 2]), (2, vec![0, 1])]);
}

#[test]
fn sort_limit_1_test() {
    let mut conf = JobConf::new("sort_limit_1_test");
    let num_workers = 2;
    conf.set_workers(num_workers);

    let result_stream = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            input
                .input_from(
                    (1_u32..2000)
                        .rev()
                        .filter(move |x| *x % num_workers == index),
                )?
                .sort_limit(1)?
                .sink_into(output)
        }
    })
    .expect("submit job failure");

    let results: Vec<u32> = result_stream.map(|x| x.unwrap()).collect();

    assert_eq!(results, vec![1_u32]);
}

#[test]
fn sort_limit_more_number_test() {
    let mut conf = JobConf::new("sort_limit_1_test");
    let num_workers = 2;
    conf.set_workers(num_workers);

    let result_stream = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            input
                .input_from(
                    (1_u32..5)
                        .rev()
                        .filter(move |x| *x % num_workers == index),
                )?
                .sort_limit(10)?
                .sink_into(output)
        }
    })
    .expect("submit job failure");

    let results: Vec<u32> = result_stream.map(|x| x.unwrap()).collect();

    assert_eq!(results, vec![1_u32, 2, 3, 4]);
}

#[test]
fn modern_graph_sort_limit_by_test() {
    let mut conf = JobConf::new("modern_graph_sort_limit_by_test");
    let num_workers = 2;
    conf.set_workers(num_workers);

    let result_stream = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            input
                .input_from((1..7).filter(move |x| *x % num_workers == index))?
                .flat_map(|v| Ok(MAP.get(&v).unwrap().iter().cloned()))?
                .sort_limit_by(3, |x, y| y.1.partial_cmp(&x.1).unwrap())?
                .map(|x| Ok(x.1))?
                .sink_into(output)
        }
    })
    .expect("submit job failure");

    let results: Vec<f32> = result_stream.map(|x| x.unwrap()).collect();

    assert_eq!(results, vec![1.0, 1.0, 0.5]);
}

#[test]
fn modern_graph_sort_limit_1_by_test() {
    let mut conf = JobConf::new("modern_graph_sort_limit_1_by_test");
    let num_workers = 2;
    conf.set_workers(num_workers);

    let result_stream = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            input
                .input_from((1..7).filter(move |x| *x % num_workers == index))?
                .flat_map(|v| Ok(MAP.get(&v).unwrap().iter().cloned()))?
                .sort_limit_by(1, |x, y| y.1.partial_cmp(&x.1).unwrap())?
                .map(|x| Ok(x.1))?
                .sink_into(output)
        }
    })
    .expect("submit job failure");

    let results: Vec<f32> = result_stream.map(|x| x.unwrap()).collect();

    assert_eq!(results, vec![1.0]);
}
