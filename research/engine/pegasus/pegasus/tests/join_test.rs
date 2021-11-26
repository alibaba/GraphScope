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

use std::io;

use pegasus::api::{Collect, CorrelatedSubTask, Count, Join, KeyBy, Map, PartitionByKey, Sink, Source};
use pegasus::stream::Stream;
use pegasus::BuildJobError;
use pegasus::JobConf;
use pegasus_common::codec::{Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct I32 {
    item: i32,
}

impl Encode for I32 {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_i32(self.item)
    }
}

impl Decode for I32 {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let item = reader.read_i32()?;
        Ok(I32 { item })
    }
}

fn create_src(id: u32, source: &mut Source<i32>) -> Result<(Stream<i32>, Stream<i32>), BuildJobError> {
    let src1 = if id == 0 { source.input_from(1..5)? } else { source.input_from(8..10)? };
    let (src1, src2) = src1.copied()?;
    let src2 = src2.map(|x| Ok(x + 1))?;
    Ok((src1, src2))
}

#[test]
fn join_test_key_by() {
    let mut conf = JobConf::new("inner_join");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        let id = pegasus::get_current_worker().index;
        move |input, output| {
            let (src1, src2) = create_src(id, input)?;
            src1.key_by(|x| Ok((x, x)))?
                .partition_by_key()
                .inner_join(src2.key_by(|x| Ok((x, x)))?.partition_by_key())?
                .map(|(d1, d2)| Ok(((d1.key, d1.value), (d2.key, d2.value))))?
                .collect::<Vec<((i32, i32), (i32, i32))>>()?
                .sink_into(output)
        }
    })
    .expect("run job failure;");

    let mut result = result.next().unwrap().unwrap();
    result.sort_by_key(|x| x.0 .0);
    assert_eq!(result, [((2, 2), (2, 2)), ((3, 3), (3, 3)), ((4, 4), (4, 4)), ((9, 9), (9, 9)),]);
}

#[test]
fn join_test_keyed() {
    let mut conf = JobConf::new("inner_join");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        let id = pegasus::get_current_worker().index;
        move |input, output| {
            let (src1, src2) = create_src(id, input)?;
            let src1 = src1
                .map(|x| Ok(I32 { item: x }))?
                .partition_by_key();
            let src2 = src2
                .map(|x| Ok(I32 { item: x }))?
                .partition_by_key();
            src1.inner_join(src2)?
                .collect::<Vec<(I32, I32)>>()?
                .sink_into(output)
        }
    })
    .expect("run job failure;");

    let mut result = result.next().unwrap().unwrap();
    result.sort_by_key(|x| x.0.item);
    assert_eq!(
        result,
        [
            (I32 { item: 2 }, I32 { item: 2 }),
            (I32 { item: 3 }, I32 { item: 3 }),
            (I32 { item: 4 }, I32 { item: 4 }),
            (I32 { item: 9 }, I32 { item: 9 })
        ]
    );
}

/*
#[test]
fn join_test_key_by_with_keyed() {
    let mut conf = JobConf::new("inner_join");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        let id = pegasus::get_current_worker().index;
        move |input, output| {
            let (src1, src2) = create_src(id, input)?;
            let src1 = src1.key_by(|x| Ok((x, x)))?.partition_by_key();
            let new_src2 = src2
                .map(|x| Ok(I32 { item: x }))?
                .partition_by_key();

            src1.inner_join(new_src2)?
                .map(|(d1, d2)| Ok(((d1.key, d1.value), d2)))?
                .collect::<Vec<((i32, i32), I32)>>()?
                .sink_into(output)
        }
    })
    .expect("run job failure;");

    let mut result = result.next().unwrap().unwrap();
    result.sort_by_key(|x| x.0 .0);
    assert_eq!(
        result,
        [
            ((2, 2), I32 { item: 2 }),
            ((3, 3), I32 { item: 3 }),
            ((4, 4), I32 { item: 4 }),
            ((9, 9), I32 { item: 9 })
        ]
    );
}
 */

#[test]
fn join_test_empty_stream() {
    let mut conf = JobConf::new("inner_join");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        let id = pegasus::get_current_worker().index;
        move |input, output| {
            let (src1, src2) = create_src(id, input)?;
            let src2 = src2.filter_map(|_| Ok(None))?;
            src1.key_by(|x| Ok((x, x)))?
                .partition_by_key()
                .inner_join(src2.key_by(|x| Ok((x, x)))?.partition_by_key())?
                .map(|(d1, d2)| Ok(((d1.key, d1.value), (d2.key, d2.value))))?
                .collect::<Vec<((i32, i32), (i32, i32))>>()?
                .sink_into(output)
        }
    })
    .expect("run job failure;");

    let mut result = result.next().unwrap().unwrap();
    result.sort_by_key(|x| x.0 .0);
    assert_eq!(result, []);
}

#[test]
fn join_test_outer() {
    let mut conf = JobConf::new("inner_join");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        let id = pegasus::get_current_worker().index;
        move |input, output| {
            let (src1, src2) = create_src(id, input)?;
            src1.key_by(|x| Ok((x, x)))?
                .partition_by_key()
                .full_outer_join(src2.key_by(|x| Ok((x, x)))?.partition_by_key())?
                .map(|(d1, d2)| Ok((d1.map(|x| (x.key, x.value)), d2.map(|x| (x.key, x.value)))))?
                .collect::<Vec<(Option<(i32, i32)>, Option<(i32, i32)>)>>()?
                .sink_into(output)
        }
    })
    .expect("run job failure;");

    let mut result = result.next().unwrap().unwrap();
    result.sort();
    assert_eq!(
        result,
        [
            (None, Some((5, 5))),
            (None, Some((10, 10))),
            (Some((1, 1)), None),
            (Some((2, 2)), Some((2, 2))),
            (Some((3, 3)), Some((3, 3))),
            (Some((4, 4)), Some((4, 4))),
            (Some((8, 8)), None),
            (Some((9, 9)), Some((9, 9)))
        ]
    );
}

#[test]
fn join_test_semi() {
    let mut conf = JobConf::new("inner_join");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        let id = pegasus::get_current_worker().index;
        move |input, output| {
            let (src1, src2) = create_src(id, input)?;
            src1.key_by(|x| Ok((x, x)))?
                .partition_by_key()
                .semi_join(src2.key_by(|x| Ok((x, x)))?.partition_by_key())?
                .map(|d| Ok((d.key, d.value)))?
                .collect::<Vec<(i32, i32)>>()?
                .sink_into(output)
        }
    })
    .expect("run job failure;");

    let mut result = result.next().unwrap().unwrap();
    result.sort();
    assert_eq!(result, [(2, 2), (3, 3), (4, 4), (9, 9)]);
}

#[test]
fn join_test_anti() {
    let mut conf = JobConf::new("inner_join");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        let id = pegasus::get_current_worker().index;
        move |input, output| {
            let (src1, src2) = create_src(id, input)?;
            src1.key_by(|x| Ok((x, x)))?
                .partition_by_key()
                .anti_join(src2.key_by(|x| Ok((x, x)))?.partition_by_key())?
                .map(|d| Ok((d.key, d.value)))?
                .collect::<Vec<(i32, i32)>>()?
                .sink_into(output)
        }
    })
    .expect("run job failure;");

    let mut result = result.next().unwrap().unwrap();
    result.sort();
    assert_eq!(result, [(1, 1), (8, 8)]);
}

#[test]
fn join_test_different_tag_outer() {
    let mut conf = JobConf::new("inner_join");
    conf.set_workers(1);
    let mut result = pegasus::run(conf, || {
        move |input, output| {
            let src = input.input_from(1..5)?;
            src.apply(|src1| {
                let src1 = src1.flat_map(|x| Ok((x * 10)..(x * 11)))?;
                let (src1, src2) = src1.copied()?;
                let src2 = src2.map(|x| Ok(x + 1))?;
                let src1 = src1.key_by(|x| Ok((x, x)))?.partition_by_key();
                let src2 = src2.key_by(|x| Ok((x, x)))?.partition_by_key();
                src1.full_outer_join(src2)?.count()
            })?
            .collect::<Vec<(u64, u64)>>()?
            .sink_into(output)
        }
    })
    .expect("run job failure;");

    // println!("{:?}", result.next().unwrap());
    let mut result = result.next().unwrap().unwrap();
    result.sort();
    assert_eq!(result, [(1, 2), (2, 3), (3, 4), (4, 5)]);
}

#[test]
fn join_test_different_tag_semi() {
    let mut conf = JobConf::new("inner_join");
    conf.set_workers(1);
    let mut result = pegasus::run(conf, || {
        move |input, output| {
            let src = input.input_from(1..5)?;
            src.apply(|src1| {
                let src1 = src1.flat_map(|x| Ok((x * 10)..(x * 11)))?;
                let (src1, src2) = src1.copied()?;
                let src2 = src2.map(|x| Ok(x + 1))?;
                let src1 = src1.key_by(|x| Ok((x, x)))?.partition_by_key();
                let src2 = src2.key_by(|x| Ok((x, x)))?.partition_by_key();
                src1.semi_join(src2)?.count()
            })?
            .collect::<Vec<(u64, u64)>>()?
            .sink_into(output)
        }
    })
    .expect("run job failure;");

    // println!("{:?}", result.next().unwrap());
    let mut result = result.next().unwrap().unwrap();
    result.sort();
    assert_eq!(result, [(1, 0), (2, 1), (3, 2), (4, 3)]);
}
// #[test]
// fn join_test_wrong_type() {
//     let mut conf = JobConf::new("inner_join");
//     conf.set_workers(2);
//     let mut result = pegasus::run(conf, |worker| {
//         let id = worker.id.index;
//         worker.dataflow(move |source| {
//             let (src1, src2) = create_src(id, source)?;
//             src1.key_by(|x| Ok((x, x)))?
//                 .semi_join(src2.key_by(|x| Ok((x, x)))?, JoinType::LeftOuter)?
//                 .collect::<Vec<(i32, i32)>>()
//         })
//     })
//     .expect("run job failure;");
//
//     println!("{:?}", result.next().unwrap());
//     // let mut result = result.next().unwrap().unwrap();
//     // result.sort_unstable();
//     // println!("{:?}", result);
//     if let Err(_) = result.next().unwrap() {
//         assert!(true);
//     } else {
//         assert!(false)
//     }
// }
