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

use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use std::thread;
use std::sync::atomic::{AtomicUsize, Ordering};

pub fn current_time_millis() -> u64 {
    let now = SystemTime::now();
    let d = now.duration_since(UNIX_EPOCH).unwrap();
    d.as_secs() * 1000 + (d.subsec_nanos() / 1000000) as u64
}

pub fn current_time_secs() -> u64 {
    let now = SystemTime::now();
    now.duration_since(UNIX_EPOCH).unwrap().as_secs()
}

pub fn duration_to_millis(d: &Duration) -> f64 {
    d.as_secs() as f64 * 1000.0 + (d.subsec_nanos() as f64 / 1000000.0)
}

pub fn duration_to_nanos(d: &Duration) -> u64 {
    d.as_secs()  * 1000_000_000 + d.subsec_nanos() as u64
}

pub fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}

pub fn sleep_until(flag: &AtomicUsize, target: usize) -> SleepGuard {
    while flag.load(Ordering::Relaxed) != target {
        sleep_ms(100);
    }
    SleepGuard::new(flag, target)
}

pub struct SleepGuard<'a> {
    flag: &'a AtomicUsize,
    target: usize,
}

impl<'a> SleepGuard<'a> {
    fn new(flag: &'a AtomicUsize, target: usize) -> Self {
        SleepGuard {
            flag,
            target,
        }
    }
}

impl<'a> Drop for SleepGuard<'a> {
    fn drop(&mut self) {
        self.flag.store(self.target + 1, Ordering::Relaxed);
    }
}

pub struct Timer {
    timer: Instant,
}

impl Timer {
    pub fn new() -> Self {
        Timer {
            timer: Instant::now(),
        }
    }

    pub fn elapsed_ms(&self) -> f64 {
        let t = self.timer.elapsed();
        t.as_secs() as f64 * 1000.0 + t.subsec_nanos() as f64 / 1000000.0
    }

    pub fn elasped_secs(&self) -> f64 {
        let t = self.timer.elapsed();
        t.as_secs() as f64 + t.subsec_nanos() as f64 / 1000000000.0
    }
}
