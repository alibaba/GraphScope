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

//#![feature(test)]
//extern crate test;
//extern crate tiny_dataflow;
//
//use std::rc::Rc;
//use std::cell::RefCell;
//
//use test::Bencher;
//use tiny_dataflow::channel;
//use tiny_dataflow::channel::*;
//
//#[bench]
//fn thread_push_pull_in_batch_1024(bench : &mut Bencher) {
//    let monitor = Rc::new(RefCell::new(Default::default()));
//    let(mut push, mut pull) = channel::new_thread_channel(1, monitor);
//    let total = 1024 as u64;
//    bench.iter(|| {
//        (0..total).for_each(|i| push.push(i));
//        push.flush();
//
//        loop {
//            if !pull.pull_batch().is_some() {
//                break
//            }
//        }
//    })
//}
//
//#[bench]
//fn thread_push_pull_in_batch_1(bench : &mut Bencher) {
//    let monitor = Rc::new(RefCell::new(Default::default()));
//    let(mut push, mut pull) = channel::new_thread_channel(1, monitor);
//    let total = 1024 as u64;
//    bench.iter(|| {
//        (0..total).for_each(|i| {
//            push.push(i);
//            push.flush();
//        });
//
//        loop {
//            if !pull.pull_batch().is_some() {
//                break
//            }
//        }
//    })
//}
