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

use pegasus::api::function::*;
use pegasus::api::{Binary, Map, ResultSet, Sink};
use pegasus::box_route;
use pegasus::communication::Pipeline;
use pegasus::{Configuration, JobConf};

#[test]
fn binary_test_01() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let conf = JobConf::new("binary_test_01");
    let (tx, rx) = crossbeam_channel::unbounded();
    let _guard = pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|builder| {
            let source = builder.input_from_iter(vec![1u32; 1000].into_iter())?;
            let left = source.map_with_fn(Pipeline, |item| Ok(item + 1))?;
            let right = source.map_with_fn(Pipeline, |item| Ok(item + 2))?;
            left.binary("merge", &right, Pipeline, Pipeline, |_meta| {
                |input, output| {
                    input.left_for_each(|dataset| {
                        output.forward(dataset)?;
                        Ok(())
                    })?;
                    input.right_for_each(|dataset| {
                        output.forward(dataset)?;
                        Ok(())
                    })
                }
            })?
            .sink_by(|_meta| {
                move |_, result| match result {
                    ResultSet::Data(data) => tx.send(data).unwrap(),
                    _ => (),
                }
            })?;
            Ok(())
        })
    })
    .expect("submit job failure;");

    ::std::mem::drop(tx);
    let mut count = 0;
    while let Ok(data) = rx.recv() {
        data.iter().for_each(|item| {
            assert!(*item == 2 || *item == 3);
        });
        count += data.len();
    }

    assert_eq!(2000, count);
    pegasus::shutdown_all();
}

#[test]
fn binary_test_02() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let conf = JobConf::with_id(8, "binary_test_02", 2);

    let (tx, rx) = crossbeam_channel::unbounded();
    let _guard = pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|builder| {
            let source = builder.input_from_iter(0..1000)?;
            let right = source.map_with_fn(Pipeline, |item| Ok(item + 1))?;
            let shuffle = box_route!(|item: &u32| *item as u64);
            source
                .binary("merge", &right, Pipeline, shuffle, |_| {
                    |input, output| {
                        input.left_for_each(|dataset| {
                            output.forward(dataset)?;
                            Ok(())
                        })?;
                        input.right_for_each(|dataset| {
                            output.forward(dataset)?;
                            Ok(())
                        })
                    }
                })?
                .sink_by(|_| {
                    move |_, result| match result {
                        ResultSet::Data(data) => tx.send(data).unwrap(),
                        _ => (),
                    }
                })?;
            Ok(())
        })
    })
    .expect("submit job failure;");

    std::mem::drop(tx);
    let mut count = 0;
    while let Ok(data) = rx.recv() {
        data.iter().for_each(|item| {
            assert!(*item <= 1001);
        });
        count += data.len();
    }

    assert_eq!(4000, count);
    pegasus::shutdown_all();
}
//
// #[tests]
// fn binary_notify_test() {
//
//     #[derive(Default)]
//     struct BinaryNotifyImpl {
//         count: HashMap<Tag, usize>,
//         notifications : HashMap<Tag, usize>
//     }
//
//     impl BinaryNotify<u32, u32, u32> for BinaryNotifyImpl {
//         fn on_receive(&mut self, input: &mut BinaryInput<u32, u32>, output: &mut OutputSession<u32>) -> Result<(), JobError> {
//             input.left_for_each(|dataset| {
//                 let count = self.count.entry(dataset.tag()).or_insert(0);
//                 *count += dataset.len();
//                 output.forward(dataset)?;
//                 Ok(())
//             })?;
//             input.right_for_each(|dataset| {
//                 let count = self.count.entry(dataset.tag()).or_insert(0);
//                 *count += dataset.len();
//                 output.forward(dataset)?;
//                 Ok(())
//             })
//         }
//
//         fn on_notify(&mut self, n : BinaryNotification) -> Vec<u32> {
//             let n = n.end();
//             if {
//                 let g = self.notifications.entry(n.clone()).or_insert(0);
//                 *g += 1;
//                 *g == 2
//             } {
//                 if let Some(count) = self.count.remove(&n) {
//                     return vec![count as u32];
//                 }
//             }
//             vec![]
//         }
//     }
//
//     common::try_init_logger();
//
//     let (tx, rx) = crossbeam_channel::unbounded();
//     let (txx, rxx) = crossbeam_channel::unbounded();
//     let _guard = pegasus::run_example(2, |worker| {
//         let index = worker.id.index;
//         let rx = rx.clone();
//         let txx = txx.clone();
//         worker.dataflow("binary_notify", move |builder| {
//             let source = if index == 0 {
//                 rx.into_scope(common::Input::new(), builder)
//             } else {
//                 builder.empty_source::<u32>()
//             };
//             let right = source.exchange(|item: &u32| *item as u64);
//             let shuffle = Shuffle::new(|item: &u32| *item as u64);
//             // copied data in source;
//             source.binary_notify("binary_notify", &right, shuffle, Pipeline, |info| {
//                 info.set_pass();
//                 BinaryNotifyImpl::default()
//             }).sink(|_info| {
//                     move |t, result| {
//                         let t = t.current_uncheck();
//                         match result {
//                             ResultSet::Data(data) => txx.send((t, data)).unwrap(),
//                             _ => ()
//                         }
//                     }
//                 })
//         });
//     });
//
//     tx.send(vec![0;1000]).unwrap();
//     tx.send(vec![0;2000]).unwrap();
//     tx.send(vec![0;3000]).unwrap();
//
//     ::std::mem::drop(tx);
//     ::std::mem::drop(txx);
//
//     let mut count = vec![0u32, 0, 0];
//     while let Ok((id, data)) = rxx.recv() {
//         let index = id as usize;
//         data.iter().for_each(|item| count[index] += *item);
//     }
//
//     assert_eq!(count, vec![2000, 4000, 6000]);
//     pegasus_executor::reactor::try_termination();
// }
