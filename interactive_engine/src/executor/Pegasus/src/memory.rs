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

#![allow(unused)]
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering::SeqCst};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::alloc::{System, GlobalAlloc, Layout};
use std::fs::File;
use std::io::Write;
use std::time::Duration;

/// Memory usage in application view;
/// Be different from the `rss` in `/proc/$pid/statm`, this only measures valid memory usage without
/// which was already freed but not return to system(because of the allocator algorithms);
/// This value will be little then the `rss`;
static ALLOCATED_MEM: AtomicUsize = AtomicUsize::new(0);
pub static MB: usize = 1024 * 1024;

#[inline]
pub fn used_memory_in_bytes() -> usize {
    ALLOCATED_MEM.load(SeqCst)
}

pub struct MemoryStat;

unsafe impl GlobalAlloc for MemoryStat {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        if !ret.is_null() {
            ALLOCATED_MEM.fetch_add(layout.size(), SeqCst);
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        ALLOCATED_MEM.fetch_sub(layout.size(), SeqCst);
    }
}

pub struct MemMonitor {
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl MemMonitor {
    /// Spawn a monitor recording memory and latency.
    ///
    /// **Linux Only**:
    /// Memory of this process will be written to `output_file` every `interval_ms`.
    pub fn spawn(output_file: &str, interval_ms: u64) -> MemMonitor {
        #[cfg(target_os = "linux")]
            {
                let running = Arc::new(AtomicBool::new(true));
                let running2 = running.clone();
                let output_file = output_file.to_owned();
                let handle = thread::Builder::new().name("MemMonitor".to_owned())
                    .spawn(move || {
                        let mut max_res = 0;
                        let mut file = File::create(output_file).unwrap();
                        let pid = psutil::getpid();
                        'outer: while running2.load(SeqCst) {
                            let mem = psutil::process::Memory::new(pid).unwrap();

                            let res = mem.resident;
                            let valid = ALLOCATED_MEM.load(SeqCst);
                            file.write(format!("{}\t{}\n", res, valid).as_bytes()).unwrap();
                            max_res = max_res.max(res);
                            for i in 0..interval_ms {
                                thread::sleep(Duration::from_millis(1));
                                if !running2.load(SeqCst) {
                                    break 'outer;
                                }
                            }
                        }
                        info!("Max memory: {} MB", max_res as f64 / MB as f64);
                    })
                    .unwrap();
                MemMonitor { running, handle: Some(handle)}
            }
        #[cfg(not(target_os = "linux"))]
            {
                MemMonitor { running: Arc::new(AtomicBool::new(true)), handle: None}
            }
    }

    #[inline]
    pub fn print_memory_use(&self) {
        #[cfg(target_os = "linux")]
            {
                let pid = psutil::getpid();
                let mem = psutil::process::Memory::new(pid).unwrap();
                let valid = ALLOCATED_MEM.load(SeqCst) as f64;
                info!("current memory usage : {}({}) MB,", mem.resident as f64 / MB as f64, valid / MB as f64);
            }
    }

    pub fn shutdown(&mut self) {
        self.running.store(false, SeqCst);
        self.handle.take().map(|h| h.join().unwrap());
    }
}
