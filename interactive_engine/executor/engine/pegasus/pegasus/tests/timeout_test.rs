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

use std::time::Duration;

use pegasus::api::{Collect, HasAny, IterCondition, Iteration, Map, Sink};
use pegasus::JobConf;

static CORE_POOL_SIZE: &'static str = "PEGASUS_CORE_POOL_SIZE";

/// test binary merge pipeline;
#[test]
fn timeout_test_01() {
    let mut conf = JobConf::new("timeout_test_01");
    conf.time_limit = 5000;
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(vec![0u32])?
                .iterate_until(IterCondition::max_iters(20), |iter| {
                    iter.map(|input| {
                        std::thread::sleep(Duration::from_millis(1000));
                        Ok(input + 1)
                    })
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure;");

    let mut count = 0;
    while let Some(Ok(data)) = result.next() {
        count += data;
    }
    assert!(result.is_cancel());
    assert_eq!(0, count);
}

#[test]
fn timeout_test_02() {
    let mut conf = JobConf::new("timeout_test_2");
    conf.time_limit = 5000;
    conf.set_workers(2);
    let mut results = pegasus::run(conf, || {
        |input, output| {
            let worker_id = input.get_worker_index();
            input
                .input_from(vec![0u32])?
                .iterate_until(IterCondition::max_iters(20), move |iter| {
                    iter.map(move |input| {
                        if worker_id == 1 {
                            std::thread::sleep(Duration::from_millis(1000));
                        }
                        Ok(input + 1)
                    })
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure;");
    let mut count = 0;
    while let Some(result) = results.next() {
        if let Ok(data) = result {
            count += data;
        } else {
            let err = result.err().unwrap();
            assert_eq!(err.to_string(), "Job is canceled;".to_string());
            break;
        }
    }
    assert!(results.is_cancel());
}

#[test]
fn timeout_resubmit_test() {
    let mut conf = JobConf::new("timeout_test");
    conf.time_limit = 5000;
    conf.set_workers(2);
    let mut results = pegasus::run(conf, || {
        |input, output| {
            let worker_id = input.get_worker_index();
            input
                .input_from(vec![0u32])?
                .iterate_until(IterCondition::max_iters(20), move |iter| {
                    iter.map(move |input| {
                        if worker_id == 1 {
                            std::thread::sleep(Duration::from_millis(1000));
                        }
                        Ok(input + 1)
                    })
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure;");
    let mut count = 0;
    while let Some(result) = results.next() {
        if let Ok(data) = result {
            count += data;
        } else {
            let err = result.err().unwrap();
            assert_eq!(err.to_string(), "Job is canceled;".to_string());
            break;
        }
    }
    assert!(results.is_cancel());
    let mut conf = JobConf::new("resubmit_test");
    conf.time_limit = 5000;
    conf.set_workers(2);
    let mut results = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(vec![0u32])?
                .iterate_until(IterCondition::max_iters(20), move |iter| {
                    iter.map(move |input| Ok(input + 1))
                })?
                .collect::<Vec<u32>>()?
                .sink_into(output)
        }
    })
    .expect("submit job failure;");
    let mut result = results.next().unwrap().unwrap();
    result.sort();
    assert_eq!(result, [20, 20]);
}

#[test]
fn timeout_caused_by_large_job() {
    let core = ::std::env::var(CORE_POOL_SIZE)
        .map(|value| {
            value
                .parse::<usize>()
                .unwrap_or_else(|_| num_cpus::get())
        })
        .unwrap_or_else(|_| num_cpus::get());
    let mut conf_1 = JobConf::new("test");
    conf_1.time_limit = 5000;
    conf_1.set_workers(core as u32);
    let mut conf_2 = JobConf::new("block_job");
    conf_2.set_workers(core as u32);
    let mut results_1 = pegasus::run(conf_1, || {
        |input, output| {
            let worker_id = input.get_worker_index();
            input
                .input_from(vec![0u32])?
                .map(move |i| {
                    if worker_id != 0 {
                        std::thread::sleep(Duration::from_millis(2000));
                    }
                    Ok(i)
                })?
                .any()?
                .sink_into(output)
        }
    })
    .expect("submit job failure;");
    let _ = pegasus::run(conf_2, || {
        |input, output| {
            let worker_id = input.get_worker_index();
            input
                .input_from(vec![0u32])?
                .map(|i| {
                    std::thread::sleep(Duration::from_millis(4000));
                    Ok(i)
                })?
                .any()?
                .sink_into(output)
        }
    })
    .expect("submit job failure;");
    let mut count = 0;
    while let Some(result) = results_1.next() {
        if let Ok(data) = result {
            count += 1;
        } else {
            let err = result.err().unwrap();
            assert_eq!(err.to_string(), "Job is canceled;".to_string());
            break;
        }
    }
    assert!(!results_1.is_cancel());
}
