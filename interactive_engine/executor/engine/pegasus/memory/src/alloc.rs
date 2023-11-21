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

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::sync::ShardedLock;

thread_local! {
    /// The id of task this thread is currently focus on;
    static TASK_ID: Cell<Option<usize>> = Cell::new(None);
    /// Enable tracing memory allocate, by default, it is disabled;
    static ENABLE_MEMORY_TRACE: Cell<bool> = Cell::new(false);
}

pub struct EnableMemTrace;

impl EnableMemTrace {
    fn new() -> Self {
        ENABLE_MEMORY_TRACE.with(|flag| flag.set(true));
        EnableMemTrace
    }
}

impl Drop for EnableMemTrace {
    fn drop(&mut self) {
        ENABLE_MEMORY_TRACE.with(|flag| flag.set(false))
    }
}

pub struct ShadeMemTrace(bool);

impl ShadeMemTrace {
    fn new() -> Self {
        let is_enable = ENABLE_MEMORY_TRACE.with(|flag| flag.replace(false));
        ShadeMemTrace(is_enable)
    }
}

impl Drop for ShadeMemTrace {
    fn drop(&mut self) {
        if self.0 {
            ENABLE_MEMORY_TRACE.with(|flag| flag.set(true))
        }
    }
}

#[inline]
pub fn trace_memory_alloc() -> EnableMemTrace {
    EnableMemTrace::new()
}

#[inline]
pub fn shade_memory_alloc_trace() -> ShadeMemTrace {
    ShadeMemTrace::new()
}

#[inline]
pub fn new_task(task_id: usize) {
    PER_TASK_MONITOR.trace_new_task(task_id);
}

#[inline]
pub fn remove_task(task_id: usize) {
    PER_TASK_MONITOR.remove_task(task_id);
}

#[inline]
pub fn reset_current_task(task_id: Option<usize>) {
    TASK_ID.with(|id| id.set(task_id));
}

#[inline]
pub fn check_task_memory(task_id: usize) -> Option<usize> {
    PER_TASK_MONITOR.get_task_memory(task_id)
}

#[inline]
pub fn check_current_task_memory() -> Option<usize> {
    TASK_ID
        .with(|id| id.get())
        .and_then(|id| check_task_memory(id))
}

#[inline]
pub fn get_current_task_and_memory() -> Option<(usize, usize)> {
    TASK_ID
        .with(|id| id.get())
        .and_then(|id| check_task_memory(id).map(|m| (id, m)))
}

pub struct TaskMemoryTrace {
    pub mask: usize,
    shards: Box<[ShardedLock<HashMap<usize, AtomicUsize>>]>,
}

impl TaskMemoryTrace {
    pub fn new(shard_size: usize) -> Self {
        let _s = ShadeMemTrace::new();
        let mask = shard_size - 1;
        assert_eq!(shard_size & mask, 0, "invalid shard size {};", shard_size);
        let mut shards = Vec::with_capacity(shard_size);
        for _ in 0..shard_size {
            shards.push(ShardedLock::new(HashMap::new()));
        }
        TaskMemoryTrace { mask, shards: shards.into_boxed_slice() }
    }

    pub fn trace_new_task(&self, task_id: usize) {
        let _s = ShadeMemTrace::new();
        let index = task_id & self.mask;
        let mut w = self.shards[index]
            .write()
            .expect("TaskMemoryTrace: write lock failure");
        w.insert(task_id, AtomicUsize::new(0));
    }

    pub fn remove_task(&self, task_id: usize) {
        let _s = ShadeMemTrace::new();
        let index = task_id & self.mask;
        let mut w = self.shards[index]
            .write()
            .expect("TaskMemoryTrace: write lock failure");
        w.remove(&task_id);
    }

    pub fn alloc(&self, task_id: usize, len: usize) {
        let _s = ShadeMemTrace::new();
        let index = task_id & self.mask;
        let r = self.shards[index]
            .read()
            .expect("TaskMemoryTrace: read lock failure");
        if let Some(size) = r.get(&task_id) {
            size.fetch_add(len, Ordering::SeqCst);
        }
    }

    pub fn dealloc(&self, task_id: usize, len: usize) {
        let _s = ShadeMemTrace::new();
        let index = task_id & self.mask;
        let r = self.shards[index]
            .read()
            .expect("TaskMemoryTrace: read lock failure");
        if let Some(size) = r.get(&task_id) {
            let mut x: usize = size.load(Ordering::Relaxed);
            loop {
                let y = if x > len { x - len } else { 0 };
                match size.compare_exchange_weak(x, y, Ordering::SeqCst, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(z) => x = z,
                }
            }
        }
    }

    pub fn get_task_memory(&self, id: usize) -> Option<usize> {
        let _s = ShadeMemTrace::new();
        ENABLE_MEMORY_TRACE.with(|flag| flag.set(false));
        let index = id & self.mask;
        let r = self.shards[index]
            .read()
            .expect("TaskMemoryMonitor#read lock failure");
        if let Some(size) = r.get(&id) {
            Some(size.load(Ordering::Relaxed))
        } else {
            None
        }
    }
}

lazy_static! {
    static ref PER_TASK_MONITOR: TaskMemoryTrace = TaskMemoryTrace::new(256);
}

pub struct MemoryStat;

unsafe impl GlobalAlloc for MemoryStat {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        if ENABLE_MEMORY_TRACE.with(|d| d.get()) && !ret.is_null() {
            let size = layout.size();
            if let Some(task_id) = TASK_ID.with(|id| id.get()) {
                PER_TASK_MONITOR.alloc(task_id, size);
            }
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        if ENABLE_MEMORY_TRACE.with(|d| d.get()) {
            let len = layout.size();
            if let Some(task_id) = TASK_ID.with(|id| id.get()) {
                PER_TASK_MONITOR.dealloc(task_id, len);
            }
        }
    }
}

#[cfg(feature = "mem")]
#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_alloc() {
        new_task(0);
        reset_current_task(0);
        let _g = trace_memory_alloc();
        let a = 0usize;
        let b = 0usize;
        let v = Vec::<usize>::with_capacity(1024);
        assert_eq!(v.capacity(), 1024);
        let m = check_current_task_memory().unwrap() + a + b;
        println!("task 0 used {} bytes;", m);
        assert!(m > 0);
        assert_eq!(m, std::mem::size_of::<usize>() * 1024);
    }
}
