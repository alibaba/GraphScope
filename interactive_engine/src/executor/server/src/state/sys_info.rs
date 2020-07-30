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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

pub struct SysInfo {
    memory_usage: AtomicUsize,
    total_memory: u64,
}

impl SysInfo {
    pub fn new(total_memory: u64) -> Self {
        SysInfo {
            memory_usage: AtomicUsize::new(0),
            total_memory,
        }
    }

    pub(crate) fn set_memory_usage(&self, usage: u64) {
        self.memory_usage.store(usage as usize, Ordering::Release);
    }

    pub fn get_memory_usage(&self) -> u64 {
        self.memory_usage.load(Ordering::Acquire) as u64
    }

    pub fn get_memory_usage_percentage(&self) -> f64 {
        self.get_memory_usage() as f64 / self.total_memory as f64
    }
}
