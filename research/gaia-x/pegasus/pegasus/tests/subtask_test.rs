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

use pegasus::api::{CorrelatedSubTask, Count, Map, Sink};
use pegasus::JobConf;

#[test]
fn subtask_test_1() {
    let mut conf = JobConf::new("subtask_test_1");
    conf.set_workers(2);
    let num = 1000u32;
    let mut result = pegasus::run(conf, move || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            let src = if index == 0 { input.input_from(0..num) } else { input.input_from(num..2 * num) }?;

            src.apply(|sub| {
                sub.map(|i| Ok(i + 1))?
                    .repartition(|x| Ok(*x as u64))
                    .flat_map(|i| Ok(0..i))?
                    .count()
            })?
            .sink_into(output)
        }
    })
    .expect("build job failure");

    let mut count = 0;
    while let Some(Ok(d)) = result.next() {
        if count < 10 {
            println!("{}: {}=>{}", count, d.0, d.1);
        }
        count += 1;
    }
    assert_eq!(count, num * 2);
}

fn subtask_test_2(workers: u32) {
    let name = format!("subtask_test_2_{}", workers);
    let mut conf = JobConf::new(name);
    conf.set_workers(workers);
    //conf.plan_print = true;
    let num = 100u32;
    let mut result = pegasus::run(conf, move || {
        let index = pegasus::get_current_worker().index;
        let source = (num * index)..(index + 1u32) * num;
        move |input, output| {
            let src = input.input_from(source)?;
            src.apply(|sub| {
                sub.flat_map(|i| Ok(0..i + 1))?
                    .repartition(|x| Ok(*x as u64))
                    .flat_map(|i| Ok(0..i))?
                    .count()
            })?
            .sink_into(output)
        }
    })
    .expect("build job failure");

    let mut count = 0;
    while let Some(Ok((p, cnt))) = result.next() {
        let expected = (0..p + 1).flat_map(|i| (0..i)).count() as u64;
        assert_eq!(expected, cnt, "{} expected cnt = {}", p, expected);
        count += 1;
    }
    assert_eq!(count, num * workers);
}

#[test]
fn subtask_test_2_2_workers() {
    subtask_test_2(2)
}

#[test]
fn subtask_test_2_3_workers() {
    subtask_test_2(3)
}

#[test]
fn subtask_test_2_4_workers() {
    subtask_test_2(4)
}

#[test]
fn subtask_test_2_5_workers() {
    subtask_test_2(5)
}

#[test]
fn subtask_test_2_6_workers() {
    subtask_test_2(6)
}

#[test]
fn subtask_test_2_7_workers() {
    subtask_test_2(7)
}

#[test]
fn subtask_test_2_8_workers() {
    subtask_test_2(8)
}

#[test]
fn subtask_test_3() {
    let mut conf = JobConf::new("subtask_test_3");
    conf.set_workers(2);
    conf.plan_print = true;

    let num = 100u32;
    let mut result = pegasus::run(conf, move || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            let src = if index == 0 { input.input_from(0..num) } else { input.input_from(num..2 * num) }?;
            src.apply(|sub| {
                sub.flat_map(|i| Ok(0..i + 1))?
                    .repartition(|x| Ok(*x as u64))
                    .flat_map(|i| Ok(0..i))?
                    .aggregate()
                    .map(|x| Ok(x + 1))?
                    .count()
            })?
            .sink_into(output)
        }
    })
    .expect("build job failure");

    let mut count = 0;
    while let Some(Ok((p, cnt))) = result.next() {
        let expected = (0..p + 1).flat_map(|i| (0..i)).count() as u64;
        assert_eq!(expected, cnt, "{} expected cnt = {}", p, expected);
        count += 1;
    }
    assert_eq!(count, num * 2);
}
