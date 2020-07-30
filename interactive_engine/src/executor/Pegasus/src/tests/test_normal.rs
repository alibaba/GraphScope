//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::collections::HashMap;
use super::*;
use crate::operator::Unary;
use crate::operator::Binary;
use crate::operator::Branch;
use crate::channel::output::OutputHandle;

#[test]
fn test_unary() {
    try_init_logger().ok();

    run_local(1, 0,  |worker| {
        worker.dataflow("test_unary", |builder| {
            (0..2048).into_stream( builder)
                .unary("unary_op", Pipeline, |_info| {
                    |input, output| {
                        input.for_each_batch(|data| {
                            output.session(&data).give_batch(data.data())
                        }).unwrap();
                        Ok(())
                    }
                })
                .unary_state("count", Pipeline, |_info| {
                    (
                        |input, _output, states| {
                            input.for_each_batch(|data| {
                                let (t, d) = data.take();
                                let count = states.entry(t).or_insert(0);
                                *count += d.len();
                                Ok(true)
                            }).unwrap();
                            Ok(())
                        },
                        |output, states| {
                            for (t, count) in states {
                                output.session(&t).give(count)?;
                            }
                            Ok(())
                        }
                    )
                })
                .unary("print", Pipeline, |_info| {
                    |input, _output: &mut OutputHandle<()>| {
                        input.for_each_batch(|data| {
                            assert_eq!(data.len(), 1);
                            for datum in data.data() {
                                info!("### [Test Unary]: Get final count: {}", datum);
                                assert_eq!(datum, 2048);
                            }
                            Ok(true)
                        }).unwrap();
                        Ok(())
                    }
                });
            Ok(())
        }).unwrap();
    }).unwrap();
}

#[test]
fn test_binary_1() {
    super::try_init_logger().ok();
    run_local(1, 0, |worker| {
        worker.dataflow("binary_test", |builder| {
            let stream1 = (0..64).into_stream(builder);
            let stream2 = (0..2000).into_stream(builder);
            stream1.binary("binary_merge", &stream2, Pipeline, Pipeline, |_info| {
                |mut input, output| {
                    let size_1 = input.first_for_each(|data_set| {
                        let mut session = output.session(&data_set);
                        session.give_batch(data_set.data())
                    })?;

                    let size_2 = input.second_for_each(|data_set| {
                        let mut session = output.session(&data_set);
                        session.give_batch(data_set.data())
                    })?;

                    info!("merge {} records from left and {} records from right;", size_1, size_2);
                    Ok(())
                }
            })
                .unary_state("count", Pipeline, |_info| {
                    (
                        |input, _output: &mut OutputHandle<()>, states| {
                            input.for_each_batch(|dataset| {
                                let (t, d) = dataset.take();
                                let count = states.entry(t).or_insert(0);
                                *count += d.len();
                                Ok(true)
                            })?;
                            Ok(())
                        },
                        |_output: &mut OutputHandle<()>, states| {
                            for (_, count) in states {
                                info!("### [Test Binary]: Get final count : {}", count);
                                assert_eq!(count, 2064);
                            }
                            Ok(())
                        }
                    )
                });
            Ok(())
        }).unwrap();
    }).unwrap();
}

#[test]
fn test_binary_notify() {
    super::try_init_logger().ok();
    run_local(1, 0, |worker| {
        worker.dataflow("binary_test", |builder| {
            let stream1 = (0..64).into_stream(builder);
            let stream2 = (0..2000).into_stream(builder);
            stream1.binary_notify("binary_count", &stream2, Pipeline, Pipeline,
                                  |_info| {
                                      let mut counts = HashMap::new();
                                      move |mut input, output, notifies| {
                                          if let Some(ref mut inputs) = input {
                                              inputs.first_for_each(|dataset| {
                                                  let (t, d) = dataset.take();
                                                  let value = counts.entry(t).or_insert(0);
                                                  *value += d.len();
                                                  Ok(true)
                                              })?;

                                              inputs.second_for_each(|dataset| {
                                                  let (t, d) = dataset.take();
                                                  let value = counts.entry(t).or_insert(0);
                                                  *value += d.len();
                                                  Ok(true)
                                              })?;
                                          }

                                          if let Some(notifies) = notifies {
                                              for n in notifies {
                                                  if let Some(value) = counts.remove(n) {
                                                      let mut session = output.session(n);
                                                      session.give(value)?;
                                                  }
                                              }
                                          }
                                          Ok(())
                                      }
                                  })
                .unary("print", Pipeline, |_info| {
                    |input, _: &mut OutputHandle<()>| {
                        input.for_each_batch(|dataset| {
                            assert_eq!(1, dataset.len());
                            for datum in dataset.data() {
                                info!("### [Test Binary Notify]: Get final count: {}", datum);
                                assert_eq!(2064, datum);
                            }
                            Ok(false)
                        })?;
                        Ok(())
                    }
                });
            Ok(())
        }).unwrap();
    }).unwrap();
}

#[test]
fn test_binary_state() {
    super::try_init_logger().ok();
    run_local(1, 0, |worker| {
        worker.dataflow("binary_test", |builder| {
            let stream1 = (0..64).into_stream(builder);
            let stream2 = (0..2000).into_stream(builder);
            stream1.binary_state("binary_count", &stream2, Pipeline, Pipeline,
                                 |_info| {
                                     (
                                         |mut input, _, states| {
                                             input.first_for_each(|dataset| {
                                                 let (t, d) = dataset.take();
                                                 let count = states.entry(t).or_insert(0);
                                                 *count += d.len();
                                                 Ok(true)
                                             })?;
                                             input.second_for_each(|dataset| {
                                                 let (t, d) = dataset.take();
                                                 let count = states.entry(t).or_insert(0);
                                                 *count += d.len();
                                                 Ok(true)
                                             })?;
                                             Ok(())
                                         },
                                         |notifies, output, states| {
                                             for n in notifies {
                                                 if let Some(count) = states.remove(n) {
                                                     let mut session = output.session(n);
                                                     session.give(count)?;
                                                 }
                                             }
                                             Ok(())
                                         }
                                     )
                                 })
                .unary("print", Pipeline, |_info| {
                    |input, _: &mut OutputHandle<()>| {
                        input.for_each_batch(|dataset| {
                            assert_eq!(1, dataset.len());
                            for datum in dataset.data() {
                                info!("### [Test Binary State]: Get final count: {}", datum);
                                assert_eq!(2064, datum);
                            }
                            Ok(true)
                        })?;
                        Ok(())
                    }
                });
            Ok(())
        }).unwrap();
    }).unwrap();
}

#[test]
fn test_branch() {
    super::try_init_logger().ok();
    run_local(1, 0, |worker| {
        worker.dataflow("branch_test", |builder| {
            let condition = |r: &u32| *r % 3 == 0;
            let (s1, s2) = (0..100u32).into_stream(builder)
                .branch("branch", condition);
            s1.unary_state("count_1", Pipeline, |_| {
                (
                    |input, _: &mut OutputHandle<()>, states| {
                        input.for_each_batch(|dataset| {
                            let (t, d) = dataset.take();
                            let count = states.entry(t).or_insert(0usize);
                            *count += d.len();
                            Ok(true)
                        })?;
                        Ok(())
                    },
                    |_: &mut OutputHandle<()>, states| {
                        assert_eq!(1, states.len());
                        for (_, c) in states {
                            info!("### [Test Branch]: Left count {}", c);
                            assert_eq!(c, 34);
                        }
                        Ok(())
                    }
                )
            });

            s2.unary_state("count_2", Pipeline, |_| {
                (
                    |input, _: &mut OutputHandle<()>, states| {
                        input.for_each_batch(|dataset| {
                            let (t, d) = dataset.take();
                            let count = states.entry(t).or_insert(0usize);
                            *count += d.len();
                            Ok(true)
                        })?;
                        Ok(())
                    },
                    |_: &mut OutputHandle<()>, states| {
                        assert_eq!(1, states.len());
                        for (_, c) in states {
                            info!("### [Test Branch]: Right count {}", c);
                            assert_eq!(c, 66);
                        }
                        Ok(())
                    }
                )
            });
            Ok(())
        }).expect("build plan failure");
    }).unwrap();
}



