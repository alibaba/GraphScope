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

//! Persistent prepared dataflow.

use std::error::Error as StdError;

use zookeeper::ZooKeeper;
use zookeeper::Watcher;
use zookeeper::WatchedEvent;
use zookeeper::ZkError;
use std::io::Read;

#[derive(Copy, Clone, Debug)]
pub enum PreparedError {
    AlreadyPrepared,
    FailedToRemoveOld,
    Unexpected,
}

impl StdError for PreparedError {
    fn description(&self) -> &'static str {
        match self {
            PreparedError::AlreadyPrepared => "AlreadyPrepared",
            PreparedError::Unexpected => "UnexpectedError",
            PreparedError::FailedToRemoveOld => "FailedToRemoveOld",
        }
    }
}

impl ::std::fmt::Display for PreparedError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> Result<(), ::std::fmt::Error> {
        match self {
            PreparedError::AlreadyPrepared => write!(f, "AlreadyPrepared"),
            PreparedError::Unexpected => write!(f, "UnexpectedError"),
            PreparedError::FailedToRemoveOld => write!(f, "FailedToRemoveOld")
        }
    }
}

impl PreparedError {
    pub fn to_int(&self) -> i32 {
        match self {
            PreparedError::AlreadyPrepared => 1i32,
            PreparedError::FailedToRemoveOld => 2i32,
            PreparedError::Unexpected => 100i32
        }
    }
}

pub fn read_prepared_from_hdfs(_url: &str, _graph_name: &str) -> Vec<(String, Vec<u8>)> {
    warn!("unimplemented on non-linux system.");
    vec![]
}


struct LoggingWatcher;

impl Watcher for LoggingWatcher {
    fn handle(&self, e: WatchedEvent) {
        info!("Receive triggered event: {:?} from zk", e.keeper_state);
    }
}

pub fn read_prepared_from_zk(zk_url: &str, prepared_path: &str, timeout_ms: u64) -> Result<Vec<(String, Vec<u8>)>, String> {
    let zk_connect_result = ZooKeeper::connect(zk_url, ::std::time::Duration::from_millis(timeout_ms), LoggingWatcher);
    match zk_connect_result {
        Ok(zk_client) => {
            match zk_client.get_children(prepared_path, false) {
                Ok(prepare_child_paths) => {
                    let mut results: Vec<(String, Vec<u8>)> = vec![];
                    for prepare_child_path in prepare_child_paths {
                        let absolute_path = format!("{}/{}/prepare", prepared_path, prepare_child_path);
                        match zk_client.get_data(absolute_path.as_str(), false) {
                            Ok(binary_data) => {
                                results.push((prepare_child_path, binary_data.0));
                            },
                            Err(e) => {
                                warn!("Failed to get data from zk: {} / {}, caused: {:?}", zk_url, absolute_path, e);
                                return Err(format!("Failed to get data from zk: {} / {}", zk_url, absolute_path));
                            }
                        }
                    }
                    return Ok(results);
                },
                Err(ZkError::NoNode) => {
                    info!("There is no prepare query in path {}", prepared_path);
                    return Ok(vec![]);
                },
                Err(e) => {
                    warn!("Failed to get children from zk: {} / {}, caused: {:?}", zk_url, prepared_path, e);
                    return Err(format!("Failed to get children from zk: {} / {}", zk_url, prepared_path));
                }
            }
        },
        Err(e) => {
            warn!("Failed to connect to zk: {}, caused:  {:?}", zk_url, e);
            return Err(format!("Failed to connecto to zk {}", zk_url));
        }
    };
}
