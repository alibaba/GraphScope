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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use test::Bencher;

fn fib(n: usize) -> usize {
    assert!(n <= 10);
    if n <= 1 {
        return 1;
    } else {
        fib(n - 1) + fib(n - 2)
    }
}

#[bench]
fn bench_fib(bench: &mut Bencher) {
    bench.iter(|| {
        fib(10);
    })
}

#[bench]
fn atomic_bool_read(bench: &mut Bencher) {
    let signal = Arc::new(AtomicBool::new(false));
    bench.iter(|| {
        assert!(!signal.load(Ordering::SeqCst));
    })
}

#[bench]
fn atomic_bool_concurrent_busy_read(bench: &mut Bencher) {
    let signal = Arc::new(AtomicBool::new(false));
    let signal_cp = signal.clone();
    let _g = ::std::thread::spawn(move || {
        while !signal_cp.load(Ordering::SeqCst) {
            //
        }
    });
    bench.iter(|| {
        assert!(!signal.load(Ordering::SeqCst));
    });
    signal.store(true, Ordering::SeqCst);
}

#[bench]
fn atomic_usize_concurrent_read_with_write(bench: &mut Bencher) {
    let signal = Arc::new(AtomicBool::new(false));
    let signal_usize = Arc::new(AtomicUsize::new(1));
    let mut guards = Vec::new();
    for i in 0..8 {
        let signal_cp = signal.clone();
        let signal_usize_cp = signal_usize.clone();
        guards.push(::std::thread::spawn(move || {
            while !signal_cp.load(Ordering::SeqCst) {
                signal_usize_cp.fetch_add(1, Ordering::SeqCst);
                fib(10);
            }
        }));
    }
    bench.iter(|| {
        assert!(signal_usize.load(Ordering::SeqCst) >= 1);
    });
    signal.store(true, Ordering::SeqCst);
}
