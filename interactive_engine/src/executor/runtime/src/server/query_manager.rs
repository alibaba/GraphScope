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

use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Error;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;
use std::collections::HashMap;
use std::time::Instant;
use server::DataflowId;

#[derive(Debug, Clone)]
pub enum DataflowInfo {
    TimelyDataflowInfo(DataflowId),
    PegasusDataflowInfo(Arc<AtomicUsize>),
}


impl std::cmp::PartialEq for DataflowInfo {
    fn eq(&self, other: &Self) -> bool {
        match self {
            DataflowInfo::TimelyDataflowInfo(dataflow_id) => {
                match other {
                    DataflowInfo::TimelyDataflowInfo(dataflow_id_o) => {
                        dataflow_id == dataflow_id_o
                    },
                    _ => { false }
                }
            },
            DataflowInfo::PegasusDataflowInfo(timeout) => {
                match other {
                    DataflowInfo::PegasusDataflowInfo(timeout_o) => {
                        timeout.load(Ordering::Relaxed) == timeout_o.load(Ordering::Relaxed)
                    },
                    _ => { false },
                }
            }
        }
    }
}

impl Display for DataflowInfo {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match self {
            DataflowInfo::TimelyDataflowInfo(dataflow_id) => {
                write!(f, "{:?}", dataflow_id)
            },
            DataflowInfo::PegasusDataflowInfo(timeout) => {
                write!(f, "{}", timeout.load(Ordering::Relaxed))
            }
        }
    }
}

impl DataflowInfo {
    /// only for timely
    pub fn new_dataflow_id(dataflow_id: DataflowId) -> DataflowInfo {
        DataflowInfo::TimelyDataflowInfo(dataflow_id)
    }

    /// only for timely
    pub fn get_dataflow_id(&self) -> Option<DataflowId> {
        match self {
            DataflowInfo::TimelyDataflowInfo(dataflow_id) => {
                Some(*dataflow_id)
            },
            DataflowInfo::PegasusDataflowInfo(_) => {
                None
            }
        }
    }

    pub fn new_timeout(timeout_ms: Arc<AtomicUsize>) -> DataflowInfo {
        DataflowInfo::PegasusDataflowInfo(timeout_ms)
    }

    /// only for pegasus
    pub fn get_dataflow_timeout(&self) -> Option<Arc<AtomicUsize>> {
        match self {
            DataflowInfo::TimelyDataflowInfo(_) => {
                None
            },
            DataflowInfo::PegasusDataflowInfo(timeout) => {
                Some(timeout.clone())
            }
        }
    }
}

/// Manage running queries.
#[derive(Clone)]
pub struct QueryManager {
    /// query_id -> (script, start_time, dataflow_id, front_server_id)
    queries: Arc<RwLock<HashMap<String, (String, Instant, Option<DataflowInfo>, u32)>>>
}

pub struct QueryGuard {
    query_id: String,
    manager: Arc<QueryManager>,
}

impl Drop for QueryGuard {
    fn drop(&mut self) {
        let mut queries = self.manager.queries.write().unwrap();
        debug!("Query guard with query_id: {} is droped.", &self.query_id);
        queries.remove(&self.query_id);
    }
}

impl QueryGuard {
    /// Set dataflow id when query launched.
    pub fn set_dataflow_id(&self, query_id: &str, dataflow_info: DataflowInfo) {
        let mut queries = self.manager.queries.write().unwrap();
        queries.get_mut(query_id).map(|v| (*v).2 = Some(dataflow_info));
    }
}

impl QueryManager {
    pub fn new() -> QueryManager {
        QueryManager { queries: Arc::new(RwLock::new(HashMap::new())) }
    }

    /// Recording a query has started.
    pub fn new_query(&self, query_id: String, front_id: u32, script: String) -> QueryGuard {
        let mut queries = self.queries.write().unwrap();
        queries.insert(query_id.clone(), (script, Instant::now(), None, front_id));
        QueryGuard { query_id, manager: Arc::new(self.clone()) }
    }

    /// Get dataflow id of query if there is one.
    pub fn get_dataflow_info(&self, query_id: &str) -> Option<DataflowInfo> {
        let queries = self.queries.read().unwrap();
        match queries.get(query_id) {
            Some((_, _, Some(id), _)) => Some(id.clone()),
            _ => None
        }
    }

    /// Get dataflow of the specific front server..
    pub fn get_dataflow_of_front(&self, front_id: u32) -> Vec<(String, DataflowInfo)> {
        let queries = self.queries.read().unwrap();
        queries.iter()
            .filter(|(_, v)| v.3 == front_id && v.2.is_some())
            .map(|(k, v)| (k.clone(), v.2.as_ref().unwrap().clone()))
            .collect()
    }

    /// Get all running queries triples (query_id, script, elapsed_nano, dataflow_id, front_id)
    pub fn dump(&self) -> Vec<(String, String, u64, String, u32)> {
        let queries = self.queries.read().unwrap();
        queries.iter().map(|(k, v)| {
            let elapsed = v.1.elapsed();
            let elapsed_nano = elapsed.as_secs() * 1_000_000_000 + elapsed.subsec_nanos() as u64;
            let dataflow_id = match v.2.as_ref() {
                Some(dataflow_info) => {
                    match dataflow_info.get_dataflow_id() {
                        Some(dataflow_id) => format!("{}", dataflow_id),
                        None => "".to_owned(),
                    }
                },
                None => "".to_owned()
            };
            (k.clone(), v.0.clone(), elapsed_nano, dataflow_id, v.3)
        }).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_manager() {
        let m = QueryManager::new();
        let guard = m.new_query("a".to_owned(), 9, "g.V('a')".to_owned());
        let queries = m.dump();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].0, "a".to_owned());
        assert_eq!(queries[0].1, "g.V('a')".to_owned());
        assert!(queries[0].2 > 0);
        assert_eq!(queries[0].3, "".to_owned());
        assert_eq!(queries[0].4, 9);

        assert!(m.get_dataflow_info("a").is_none());
        assert_eq!(m.get_dataflow_of_front(9), vec![]);


        let dataflow_id = DataflowId { worker: 0, seq: 999 };
        guard.set_dataflow_id("a", DataflowInfo::TimelyDataflowInfo(dataflow_id));
        assert!(m.get_dataflow_info("a").is_some());

        let queries = m.dump();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].3, "0-999".to_owned());

        assert_eq!(m.get_dataflow_of_front(9), vec![("a".to_owned(), DataflowInfo::TimelyDataflowInfo(dataflow_id))]);
        assert_eq!(m.get_dataflow_of_front(10), vec![]);

        ::std::mem::drop(guard);
        assert!(m.dump().is_empty());
    }

    #[test]
    fn test_query_manager_multi_threads() {
        use std::thread;
        use std::time::Duration;

        let m = QueryManager::new();
        let m1 = m.clone();
        let m2 = m.clone();
        let t1 = thread::spawn(move || {
            let _guard = m1.new_query("a".to_owned(), 9, "g.V('a')".to_owned());
            thread::sleep(Duration::from_millis(100));
        });

        let t2 = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            let _guard = m2.new_query("b".to_owned(), 9, "g.V('b')".to_owned());
            thread::sleep(Duration::from_millis(100));
        });

        t1.join().unwrap();
        t2.join().unwrap();
        assert_eq!(m.dump().len(), 0);
    }
}
