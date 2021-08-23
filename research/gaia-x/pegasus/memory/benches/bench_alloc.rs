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

use test::Bencher;

pub const LEN: usize = 8096;

#[bench]
fn sequence_alloc(b: &mut Bencher) {
    pegasus_memory::alloc::enable_task_memory_trace();
    pegasus_memory::alloc::reset_current_task(0);
    b.iter(|| {
        Vec::<usize>::with_capacity(LEN);
    })
}

#[bench]
fn concurrent_alloc(b: &mut Bencher) {
    pegasus_memory::alloc::enable_task_memory_trace();
    pegasus_memory::alloc::reset_current_task(1);
    let signal = Arc::new(AtomicBool::new(false));
    let signal_cy = signal.clone();
    let _g = std::thread::spawn(move || {
        pegasus_memory::alloc::reset_current_task(1);
        while !signal_cy.load(Ordering::SeqCst) {
            Vec::<usize>::with_capacity(LEN);
        }
    });

    sequence_alloc(b);
    signal.store(true, Ordering::SeqCst);
}
