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

use crossbeam_queue::SegQueue;
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
fn seg_queue_push(bench: &mut Bencher) {
    let queue = SegQueue::new();
    bench.iter(|| queue.push(TestEntry::new(0)));
}

#[bench]
fn seg_queue_push_2(bench: &mut Bencher) {
    let queue = Arc::new(SegQueue::new());
    let signal = Arc::new(AtomicBool::new(true));
    let _guard_0 = {
        let queue = queue.clone();
        let signal = signal.clone();
        ::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                queue.push(TestEntry::new(0));
            }
        })
    };
    bench.iter(|| queue.push(TestEntry::new(0)));
    signal.store(false, Ordering::SeqCst);
}

#[bench]
fn seg_queue_pop_empty(bench: &mut Bencher) {
    let queue = SegQueue::new();
    queue.push(TestEntry::new(0));
    queue.pop().unwrap();

    bench.iter(|| {
        queue.pop().ok();
    })
}

#[bench]
fn seg_queue_pop_empty_concurrent(bench: &mut Bencher) {
    let queue = Arc::new(SegQueue::new());
    let signal = Arc::new(AtomicBool::new(true));
    queue.push(TestEntry::new(0));
    queue.pop().unwrap();

    let mut guards = Vec::new();
    for _ in 0..8 {
        let queue = queue.clone();
        let signal = signal.clone();
        guards.push(::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                queue.pop().ok();
            }
        }));
    }

    bench.iter(|| queue.pop().ok());
    signal.store(false, Ordering::SeqCst);
}

#[bench]
fn seg_queue_1_push_1_pop(bench: &mut Bencher) {
    let signal = Arc::new(AtomicBool::new(true));
    let queue = Arc::new(SegQueue::new());
    let _guard_0 = {
        let queue = queue.clone();
        let signal = signal.clone();
        ::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                queue.push(TestEntry::new(0));
            }
        })
    };

    bench.iter(|| queue.pop().ok());

    signal.store(false, Ordering::SeqCst);
}

#[bench]
fn seg_queue_1_push_2_pop(bench: &mut Bencher) {
    let signal = Arc::new(AtomicBool::new(true));
    let queue = Arc::new(SegQueue::new());
    let _guard_0 = {
        let queue = queue.clone();
        let signal = signal.clone();
        ::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                queue.push(TestEntry::new(0));
            }
        })
    };

    let _guard_1 = {
        let queue = queue.clone();
        let signal = signal.clone();
        ::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                queue.pop().ok();
            }
        })
    };

    bench.iter(|| queue.pop().ok());

    signal.store(false, Ordering::SeqCst);
}

#[bench]
fn seg_queue_2_push_2_pop(bench: &mut Bencher) {
    let signal = Arc::new(AtomicBool::new(true));
    let queue = Arc::new(SegQueue::new());
    let _guard_0 = {
        let queue = queue.clone();
        let signal = signal.clone();
        ::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                queue.push(TestEntry::new(0));
            }
        })
    };

    let _guard_1 = {
        let queue = queue.clone();
        let signal = signal.clone();
        ::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                queue.push(TestEntry::new(0));
            }
        })
    };

    let _guard_2 = {
        let queue = queue.clone();
        let signal = signal.clone();
        ::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                queue.pop().ok();
            }
        })
    };

    bench.iter(|| queue.pop().ok());

    signal.store(false, Ordering::SeqCst);
}

#[bench]
fn seg_queue_1_push_4_pop(bench: &mut Bencher) {
    let signal = Arc::new(AtomicBool::new(true));
    let queue = Arc::new(SegQueue::new());
    let _guard_0 = {
        let queue = queue.clone();
        let signal = signal.clone();
        ::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                queue.push(TestEntry::new(0));
            }
        })
    };

    let mut pop_guards = Vec::with_capacity(3);
    for _ in 0..3 {
        let queue = queue.clone();
        let signal = signal.clone();
        pop_guards.push(::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                queue.pop().ok();
            }
        }));
    }

    bench.iter(|| queue.pop().ok());

    signal.store(false, Ordering::SeqCst);
}

#[bench]
fn seg_queue_push_pull(bench: &mut Bencher) {
    let signal = Arc::new(AtomicBool::new(true));
    let queue = Arc::new(SegQueue::new());
    for i in 0..100 {
        queue.push(TestEntry::new(i));
    }

    let mut guards = Vec::new();
    for _i in 0..7 {
        let queue = queue.clone();
        let signal = signal.clone();
        guards.push(::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                if let Some(p) = queue.pop().ok() {
                    queue.push(p);
                } else {
                    panic!("err");
                }
            }
        }));
    }

    bench.iter(|| {
        if let Some(p) = queue.pop().ok() {
            queue.push(p);
        } else {
            panic!("err")
        }
    })
}
