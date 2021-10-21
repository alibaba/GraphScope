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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::{Instant, Duration};
use std::fs::File;
use std::io::Write;

pub struct MemMonitor {
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    start: Instant,
}


impl MemMonitor {
    /// Spawn a monitor recording memory and latency.
    ///
    /// **Linux Only**:
    /// Memory of this process will be written to `output_file` every `interval_ms`.
    pub fn spawn(output_file: &str, interval_ms: u64) -> MemMonitor {
        let start = Instant::now();
        #[cfg(target_os = "linux")]
            {
                let running = Arc::new(AtomicBool::new(true));
                let running2 = running.clone();
                let output_file = output_file.to_owned();
                let handle = thread::spawn(move || {
                    let mut max_res = 0;
                    let mut file = File::create(output_file).unwrap();
                    let pid = psutil::getpid();
                    'outer: while running2.load(Ordering::Relaxed) {
                        let mem = psutil::process::Memory::new(pid).unwrap();
                        let res = mem.resident;
                        file.write(format!("{}\n", res).as_bytes()).unwrap();
                        max_res = max_res.max(res);
                        for i in 0..interval_ms {
                            thread::sleep(Duration::from_millis(1));
                            if !running2.load(Ordering::Relaxed) {
                                break 'outer;
                            }
                        }
                    }
                    info!("Max memory: {} MB", max_res as f64 / 1024.0 / 1024.0);
                });
                MemMonitor { running, handle: Some(handle), start }
            }
        #[cfg(not(target_os = "linux"))]
            {
                MemMonitor { running: Arc::new(AtomicBool::new(true)), handle: None, start }
            }
    }

    pub fn shutdown(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        self.handle.take().map(|h| h.join().unwrap());
        info!("MemMonitor shutdown, total runtime: {:?}", self.start.elapsed());
    }
}
