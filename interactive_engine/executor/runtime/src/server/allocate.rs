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
use std::sync::atomic::{AtomicBool, Ordering};
use std::net::TcpStream;
use std::net::TcpListener;
use maxgraph_store::config::StoreConfig;


/// Join handles for send and receive threads.
///
/// On drop, the guard joins with each of the threads to ensure that they complete
/// cleanly and send all necessary data.
pub struct CommsGuard {
    send_guards: Vec<::std::thread::JoinHandle<Result<(), String>>>,
    recv_guards: Vec<::std::thread::JoinHandle<Result<(), String>>>,
    server_switch: Arc<AtomicBool>,
}


impl Drop for CommsGuard {
    fn drop(&mut self) {
        for handle in self.send_guards.drain(..) {
            match handle.join() {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    self.server_switch.store(false, Ordering::Relaxed);
                    error!("Send thread break, caused by {:?}", e);
                }
                Err(e) => {
                    self.server_switch.store(false, Ordering::Relaxed);
                    error!("Send thread panic: {:?}", e);
                }
            }
        }
        // println!("SEND THREADS JOINED");
        for handle in self.recv_guards.drain(..) {
            match handle.join() {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    self.server_switch.store(false, Ordering::Relaxed);
                    error!("Recv thread break, caused by {:?}", e);
                }
                Err(e) => {
                    self.server_switch.store(false, Ordering::Relaxed);
                    error!("Recv thread panic: {:?}", e);
                }
            }
        }
    }
}

impl CommsGuard {
    pub fn new() -> CommsGuard {
        let server_switch = Arc::new(AtomicBool::new(true));
        CommsGuard {
            send_guards: Default::default(),
            recv_guards: Default::default(),
            server_switch,
        }
    }

    pub fn get_server_switch(&self) -> Arc<AtomicBool> {
        let server_switch_clone = self.server_switch.clone();
        server_switch_clone
    }

    pub fn join(&mut self) {
        for handle in self.send_guards.drain(..) {
            match handle.join() {
                Ok(_) => {}
                Err(e) => {
                    self.server_switch.store(false, Ordering::Relaxed);
                    info!("Send thread panic: {:?}", e);
                }
            }
        }
        // println!("SEND THREADS JOINED");
        for handle in self.recv_guards.drain(..) {
            match handle.join() {
                Ok(_) => {}
                Err(e) => {
                    self.server_switch.store(false, Ordering::Relaxed);
                    info!("Recv thread panic: {:?}", e);
                }
            }
        }
        // println!("RECV THREADS JOINED");
    }
}


/// Listen a port for rpc_timely server connection
pub fn register_tcp_listener() -> TcpListener {
    let default_addr = "0.0.0.0:0".to_string();
    let listener = TcpListener::bind(&default_addr).expect("bind address failed");
    listener
}

fn get_bound_file_dir(store_config: Arc<StoreConfig>) -> String {
    let paths: Vec<&str> = store_config.local_data_root.split(",").collect();
    let first_path = paths.get(0).unwrap();

    use std::path::PathBuf;
    use std::fs::create_dir_all;

    let mut path_buf = PathBuf::new();
    path_buf.push(*first_path);
    path_buf.push(store_config.graph_name.as_str());
    path_buf.push("../..");

    create_dir_all(path_buf.clone()).ok();

    path_buf.to_str().unwrap().to_owned()
}
