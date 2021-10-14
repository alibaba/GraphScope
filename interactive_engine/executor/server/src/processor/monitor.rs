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

use std::sync::Arc;
use crate::store::Store;
use std::thread;
use std::time::Duration;
use psutil;
use psutil::process::Memory;

/// Start a thread named "monitor", which is responsible for monitoring the system info, like memory
/// usage, cpu usage(not support yet) and so on. And it will update `SysInfo` in `Store` so some
/// other controller threads can know current sys info and decide how to work next. This thread
/// works for the system stability
///
/// # Arguments
/// * `store`: the global store object contains graph data and meta.
pub fn start(store: Arc<Store>) {
    thread::Builder::new().name("monitor".to_owned()).spawn(move || {
        let config = store.get_config();
        let sys_info = store.get_sys_info();
        let pid = psutil::getpid();
        info!("process id is: {}", pid);
        loop {
            if cfg!(target_os="linux") {
                if let Ok(m) = Memory::new(pid) {
                    sys_info.set_memory_usage(m.resident);
                } else {
                    error!("get memory stat fail");
                }
            } else {
                warn!("os is not linux, monitor will not work");
                break;
            }
            thread::sleep(Duration::from_millis(config.monitor_interval_ms));
        }
    }).unwrap();
}
