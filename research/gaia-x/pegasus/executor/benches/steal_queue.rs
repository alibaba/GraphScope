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

#![feature(test)]
extern crate test;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use pegasus_common::queue::*;
use test::Bencher;

#[allow(dead_code)]
struct TestEntry {
    id: usize,
    start: Instant,
}

impl TestEntry {
    pub fn new(id: usize) -> Self {
        TestEntry { id, start: Instant::now() }
    }
}

#[bench]
fn steal_queue_push(bench: &mut Bencher) {
    let mut factory = WorkStealFactory::new(1);
    let queue = factory.get_queue().unwrap();
    bench.iter(|| {
        queue.push(TestEntry::new(0));
    });
}

#[bench]
fn steal_queue_pop_empty(bench: &mut Bencher) {
    let mut factory = WorkStealFactory::new(1);
    let queue = factory.get_queue().unwrap();
    queue.push(TestEntry::new(0));
    queue.pop().unwrap();

    bench.iter(|| {
        queue.pop();
    })
}

#[bench]
fn steal_queue_pop_empty_concurrent(bench: &mut Bencher) {
    let signal = Arc::new(AtomicBool::new(true));
    let mut factory = WorkStealFactory::new(9);
    let queue = factory.get_queue().unwrap();
    queue.push(TestEntry::new(0));
    queue.pop().unwrap();
    let mut guards = Vec::with_capacity(8);
    for _i in 0..8 {
        let queue = factory.get_queue().unwrap();
        let signal = signal.clone();
        guards.push(::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                queue.pop();
            }
        }));
    }

    bench.iter(|| queue.pop());
    signal.store(false, Ordering::SeqCst);
}

#[bench]
fn steal_queue_push_pull(bench: &mut Bencher) {
    let signal = Arc::new(AtomicBool::new(true));
    let mut factory = WorkStealFactory::new(8);
    let queue = factory.get_queue().unwrap();
    for i in 0..100 {
        queue.push(TestEntry::new(i));
    }

    let mut guards = Vec::new();
    for _i in 0..7 {
        let queue = factory.get_queue().unwrap();
        let signal = signal.clone();
        guards.push(::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                if let Some(p) = queue.pop() {
                    queue.push(p);
                } else {
                    panic!("err");
                }
            }
        }));
    }

    bench.iter(|| {
        if let Some(p) = queue.pop() {
            queue.push(p);
        } else {
            panic!("err")
        }
    })
}
