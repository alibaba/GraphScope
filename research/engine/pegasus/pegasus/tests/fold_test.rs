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

use pegasus::api::{Collect, CorrelatedSubTask, Count, Filter, FoldByKey, KeyBy, Map, Sink};
use pegasus::JobConf;

#[test]
fn count_test_01() {
    let mut conf = JobConf::new("count_test_01");
    conf.set_workers(3);
    let num = 4000;
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            if index == 0 { input.input_from(0..num) } else { input.input_from(0..0) }?
                .repartition(|item| Ok(*item as u64))
                .flat_map(|item| Ok(0..item))?
                .count()?
                .sink_into(output)
        }
    })
    .expect("submit job failure:");

    let mut sum = 0;
    let mut count = 0;
    while let Some(Ok(d)) = result.next() {
        sum += d;
        count += 1;
    }
    let expected = (0..num).flat_map(|x| 0..x).count() as u64;
    assert_eq!(expected, sum);
    assert_eq!(1, count);
}

#[test]
fn count_test_02() {
    let mut conf = JobConf::new("count_test_02");
    conf.set_workers(3);
    let num = 4000;
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            if index == 0 { input.input_from(0..num) } else { input.input_from(0..0) }?
                .repartition(|item| Ok(*item as u64))
                .flat_map(|item| Ok(0..item))?
                .filter(|_| Ok(false))?
                .count()?
                .sink_into(output)
        }
    })
    .expect("submit job failure:");

    let mut count = 0;
    let mut sum = 0;
    while let Some(Ok(d)) = result.next() {
        sum += d;
        count += 1;
    }

    assert_eq!(0, sum);
    assert_eq!(1, count);
}

#[test]
fn collect_test() {
    let mut conf = JobConf::new("collect_test");
    conf.set_workers(2);
    let num = 4000;
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        let src = index * num..(index + 1) * num;
        move |input, output| {
            input
                .input_from(src)?
                .repartition(|item| Ok(*item as u64))
                .collect::<Vec<_>>()?
                .sink_into(output)
        }
    })
    .expect("submit job failure:");
    let vec = result.next().unwrap().unwrap();
    assert_eq!(num * 2, vec.len() as u32);
}

#[test]
fn collect_in_apply_test() {
    let mut conf = JobConf::new("collect_in_apply");
    conf.set_workers(2);
    let num = 100u32;
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        let src = index * num..(index + 1) * num;
        move |input, output| {
            input
                .input_from(src)?
                .repartition(|x| Ok(*x as u64))
                .apply(|sub| {
                    sub.flat_map(|x| Ok(0..x))?
                        .aggregate()
                        .filter(|x| Ok(*x == 0))?
                        .collect::<Vec<_>>()
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure:");

    let mut count = 0;
    while let Some(Ok((p, cnt))) = result.next() {
        if p == 0 {
            assert_eq!(0, cnt.len(), "0 expected cnt = 0");
        } else {
            assert_eq!(1, cnt.len() as u64, "{} expected cnt = 1", p);
        }
        count += 1;
    }
    assert_eq!(count, num * 2);
}

#[test]
fn apply_x_flatmap_s_flatmap_a_filter_collect_x_test() {
    let mut conf = JobConf::new("apply_x_flatmap_s_flatmap_a_filter_collect_x_test");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        move |input, output| {
            input
                .input_from(1..100u32)?
                .apply(|sub| {
                    sub.flat_map(|i| Ok(0..i))?
                        .repartition(|x: &u32| Ok(*x as u64))
                        .flat_map(|i| Ok(0..i + 1))?
                        .aggregate()
                        .filter(|x| Ok(*x == 0))?
                        .collect::<Vec<_>>()
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure:");

    let mut count = 0;
    while let Some(Ok((p, cnt))) = result.next() {
        let expected = (0..p)
            .flat_map(|x| 0..x + 1)
            .filter(|x| *x == 0)
            .count() as u64;
        assert_eq!(expected, cnt.len() as u64, "{} expected cnt = {}", p, expected);
        count += 1;
    }
    assert_eq!(count, 198);
}

#[test]
fn fold_by_key_test() {
    let mut conf = JobConf::new("fold_by_key");
    conf.set_workers(2);
    let num = 1000u32;
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        let src = index * num..(index + 1) * num;
        move |input, output| {
            input
                .input_from(src)?
                .key_by(|x| Ok((x % 4, x)))?
                .fold_by_key(0u32, || |a, _| Ok(a + 1))?
                .sink_into(output)
        }
    })
    .expect("submit job failure:");
    let groups = result.next().unwrap().unwrap();
    println!("groups: {:?}", groups);

    assert_eq!(groups.len(), 4);
    let cnt_0 = groups.get(&0).unwrap();
    assert_eq!(*cnt_0, (0..num * 2).filter(|x| x % 4 == 0).count() as u32);
    let cnt_1 = groups.get(&1).unwrap();
    assert_eq!(*cnt_1, (0..num * 2).filter(|x| x % 4 == 1).count() as u32);
    let cnt_2 = groups.get(&2).unwrap();
    assert_eq!(*cnt_2, (0..num * 2).filter(|x| x % 4 == 2).count() as u32);
    let cnt_3 = groups.get(&3).unwrap();
    assert_eq!(*cnt_3, (0..num * 2).filter(|x| x % 4 == 3).count() as u32);
}
