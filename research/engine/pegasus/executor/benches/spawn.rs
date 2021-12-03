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
use std::time::Duration;

use pegasus_executor::{Error, Executor, Task, TaskState};
use test::Bencher;

#[inline]
fn fib(n: usize) -> usize {
    assert!(n <= 10);
    if n <= 1 {
        return 1;
    } else {
        fib(n - 1) + fib(n - 2)
    }
}

pub struct TaskImpl;

impl Task for TaskImpl {
    fn execute(&mut self) -> Result<TaskState, Error> {
        unimplemented!()
    }

    fn check_ready(&mut self) -> Result<TaskState, Error> {
        unimplemented!()
    }
}

#[bench]
fn bench_spawn_fib(bench: &mut Bencher) {
    bench.iter(|| {
        fib(4);
    })
}

#[bench]
fn bench_spawn_1(bench: &mut Bencher) {
    let (_e, ep) = pegasus_executor::reactor::init_executor();
    let g = ep.spawn(TaskImpl).unwrap();
    ::std::mem::forget(g);
    bench.iter(|| {
        let g = ep.spawn(TaskImpl).unwrap();
        ::std::mem::forget(g);
    });
}

#[bench]
fn bench_spawn_2(bench: &mut Bencher) {
    let (_e, ep) = pegasus_executor::reactor::init_executor();
    let ep_x = ep.clone();
    let signal = Arc::new(AtomicBool::new(true));
    let signal_x = signal.clone();
    ::std::thread::spawn(move || {
        ::std::thread::sleep(Duration::from_millis(10));
        while signal.load(Ordering::SeqCst) {
            ::std::mem::forget(ep_x.spawn(TaskImpl).unwrap());
            fib(4);
        }
    });

    let g = ep.spawn(TaskImpl).unwrap();
    ::std::mem::forget(g);
    bench.iter(|| {
        let g = ep.spawn(TaskImpl).unwrap();
        ::std::mem::forget(g);
    });
    signal_x.store(false, Ordering::SeqCst);
}

#[bench]
fn bench_spawn_4(bench: &mut Bencher) {
    spawn_n(4, bench);
}

#[bench]
fn bench_spawn_8(bench: &mut Bencher) {
    spawn_n(8, bench);
}

#[inline]
fn spawn_n(n: usize, bench: &mut Bencher) {
    let (_e, ep) = pegasus_executor::reactor::init_executor();
    let signal = Arc::new(AtomicBool::new(true));
    let mut thread_guards = Vec::new();
    for _ in 0..n {
        let signal_x = signal.clone();
        let ep_x = ep.clone();
        thread_guards.push(::std::thread::spawn(move || {
            ::std::thread::sleep(Duration::from_millis(10));
            while signal_x.load(Ordering::SeqCst) {
                ::std::mem::forget(ep_x.spawn(TaskImpl).unwrap());
                fib(4);
            }
        }));
    }

    let g = ep.spawn(TaskImpl).unwrap();
    ::std::mem::forget(g);
    bench.iter(|| {
        let g = ep.spawn(TaskImpl).unwrap();
        ::std::mem::forget(g);
    });
    signal.store(false, Ordering::SeqCst);
    for g in thread_guards {
        g.join().unwrap();
    }
}
