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

use pegasus::api::{CorrelatedSubTask, KeyBy, Map, Reduce, ReduceByKey, Sink};
use pegasus::JobConf;

#[test]
fn reduce_test() {
    let mut conf = JobConf::new("reduce_test");
    conf.set_workers(2);

    let mut results = pegasus::run(conf, || {
        move |input, output| {
            input
                .input_from(0..1000u32)?
                .repartition(|item| Ok(*item as u64))
                .map(|item| Ok(item + 1))?
                .reduce(|| |a, b| Ok(a + b))?
                .sink_into(output)
        }
    })
    .expect("submitted job failure");

    let sum = results.next().unwrap().unwrap();
    assert_eq!(
        sum,
        (0..1000u32)
            .map(|x| x + 1)
            .reduce(|a, b| a + b)
            .unwrap()
            * 2
    );
}

#[test]
fn reduce_in_apply_test() {
    let mut conf = JobConf::new("reduce_in_apply_test");
    conf.set_workers(2);

    let mut results = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        let src = 1000 * index..1000 * (index + 1);
        move |input, output| {
            input
                .input_from(src)?
                .repartition(|item| Ok(*item as u64))
                .apply(|sub| {
                    sub.map(|x| Ok(x + 1))?
                        .flat_map(|x| Ok(0..x))?
                        .reduce(|| |a, b| Ok(a + b))
                })?
                .sink_into(output)
        }
    })
    .expect("submitted job failure");
    let mut count = 0;
    while let Some(Ok((i, sum))) = results.next() {
        assert_eq!(sum, (0..i + 1).reduce(|a, b| a + b).unwrap());
        count += 1;
    }
    assert_eq!(count, 2000);
}

#[test]
fn reduce_by_key_test() {
    let mut conf = JobConf::new("reduce_by_key_test");
    conf.set_workers(2);

    let mut results = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(0..2000u32)?
                .map(|item| Ok(item + 1))?
                .key_by(|x| Ok((x % 4, 1u32)))?
                .reduce_by_key(|| |a, b| Ok(a + b))?
                .sink_into(output)
        }
    })
    .expect("");

    let groups = results.next().unwrap().unwrap();
    assert_eq!(groups.len(), 4);
    let cnt_0 = groups.get(&0).unwrap();
    assert_eq!(
        *cnt_0,
        (0..2000u32)
            .map(|x| x + 1)
            .filter(|x| x % 4 == 0)
            .count() as u32
            * 2
    );
    let cnt_1 = groups.get(&1).unwrap();
    assert_eq!(
        *cnt_1,
        (0..2000u32)
            .map(|x| x + 1)
            .filter(|x| x % 4 == 1)
            .count() as u32
            * 2
    );
    let cnt_2 = groups.get(&2).unwrap();
    assert_eq!(
        *cnt_2,
        (0..2000u32)
            .map(|x| x + 1)
            .filter(|x| x % 4 == 2)
            .count() as u32
            * 2
    );
    let cnt_3 = groups.get(&3).unwrap();
    assert_eq!(
        *cnt_3,
        (0..2000u32)
            .map(|x| x + 1)
            .filter(|x| x % 4 == 3)
            .count() as u32
            * 2
    );
}
