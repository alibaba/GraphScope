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

use super::time;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Error;

#[macro_use]
#[cfg(debug_assertions)]
mod macros {
    #[macro_export]
    macro_rules! _trace {
        ($fmt:expr, $($arg:tt)*) => (trace!($fmt, $($arg)*));
    }

    #[macro_export]
    macro_rules! _debug {
        ($fmt:expr, $($arg:tt)*) => (debug!($fmt, $($arg)*));
    }

    #[macro_export]
    macro_rules! _info {
        ($fmt:expr, $($arg:tt)*) => (info!($fmt, $($arg)*));
    }

    #[macro_export]
    macro_rules! _warn {
        ($fmt:expr, $($arg:tt)*) => (warn!($fmt, $($arg)*));
    }

    #[macro_export]
    macro_rules! _error {
        ($fmt:expr, $($arg:tt)*) => (error!($fmt, $($arg)*));
    }
}

#[macro_use]
#[cfg(not(debug_assertions))]
mod macros {
    #[macro_export]
    macro_rules! _trace {
        ($fmt:expr, $($arg:tt)*) => ();
    }

    #[macro_export]
    macro_rules! _debug {
        ($fmt:expr, $($arg:tt)*) => ();
    }

    #[macro_export]
    macro_rules! _info {
        ($fmt:expr, $($arg:tt)*) => ();
    }

    #[macro_export]
    macro_rules! _warn {
        ($fmt:expr, $($arg:tt)*) => ();
    }

    #[macro_export]
    macro_rules! _error {
        ($fmt:expr, $($arg:tt)*) => ();
    }
}


/// write access log.
///
/// Format:
///     date_time|graph_name|rpc_method|latency_nano|dataflow_name|response_ok
/// Example:
///     2018-10-12 12:10:02.988587626|test_graph|query_p|item|true
#[macro_export]
macro_rules! access_log {
    ($graph:expr, $method:expr, $instant:expr, $slow_threshold:expr, $request:expr, $df_name:expr, $resp_ok:expr) => {
        let duration = $instant.elapsed();
        let nanos = duration.as_secs() * 1_000_000_000 + duration.subsec_nanos() as u64;
        info!(target: "maxgraph-access", "{}|{}|{}|{}|{}", $graph, $method, nanos, $df_name, $resp_ok);
        if nanos > $slow_threshold {
            info!(target: "maxgraph-access", "slow query, time(nano): {}, request: {:?}", nanos, $request);
        }
    }
}

/// Subset of QueryEvent defined in com.alibaba.maxgraph.logging.LogEvents.
pub enum QueryEvent {
    /// Executor receive a query.
    ExecutorReceived,

    /// Executor finish serving the query.
    ExecutorFinish { latency_nano: u64, result_num: u64, success: bool },
}

impl QueryEvent {
    fn name(&self) -> &'static str {
        match self {
            QueryEvent::ExecutorReceived => "EXECUTOR_RECEIVED",
            QueryEvent::ExecutorFinish { .. } => "EXECUTOR_FINISH",
        }
    }
}

/// Incoming query type.
pub enum QueryType {
    /// Execute directly by launching a dataflow for it.
    Execute,

    /// Prepare a dataflow for one kind of query.
    Prepare,

    /// Execute query on prepared dataflow.
    Query,
}

impl QueryType {
    fn name(&self) -> &'static str {
        match self {
            QueryType::Execute => "EXECUTE",
            QueryType::Prepare => "PREPARE",
            QueryType::Query => "QUERY",
        }
    }
}

/// Write alert log. Same format as java.
#[inline]
pub fn log_query(graph: &str, server_id: u32, query_id: &str, query_type: QueryType, query_event: QueryEvent) {
    let time = time::current_time_millis();
    match query_event {
        QueryEvent::ExecutorFinish { latency_nano, result_num, success } => {
            info!(target: "logging-query", "{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}",
                  time, graph, "EXECUTOR", server_id, query_id, query_type.name(), query_event.name(),
                  latency_nano, result_num, success);
        }
        _ => {
            info!(target: "logging-query", "{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}\u{1}\u{1}\u{1}",
                  time, graph, "EXECUTOR", server_id, query_id, query_type.name(), query_event.name());
        }
    }
}

/// Subset of StoreEvent defined in com.alibaba.maxgraph.logging.StoreEvent.
pub enum StoreEvent {
    /// Store online a snapshot.
    SnapshotStoreOnline,

    /// Store start serving a snapshot.
    SnapshotStoreServing,

    /// Store offline a snapshot.
    SnapshotStoreOffline,
}

impl StoreEvent {
    fn name(&self) -> &'static str {
        match self {
            StoreEvent::SnapshotStoreOnline => "SNAPSHOT_STORE_ONLINE",
            StoreEvent::SnapshotStoreServing => "SNAPSHOT_STORE_SERVING",
            StoreEvent::SnapshotStoreOffline => "SNAPSHOT_STORE_OFFLINE",
        }
    }

    fn get_type(&self) -> &'static str {
        match self {
            StoreEvent::SnapshotStoreOnline |
            StoreEvent::SnapshotStoreServing |
            StoreEvent::SnapshotStoreOffline => "SNAPSHOT"
        }
    }
}

/// Write store log. Same format as java.
#[inline]
pub fn log_store(graph: &str, server_id: u32, event: StoreEvent, key: &str, message: &str) {
    let time = time::current_time_millis();
    info!(target: "logging-store", "{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}",
          time, graph, "EXECUTOR", server_id, event.get_type(), event.name(), key, message);
}

/// Subset of RuntimeEvent defined in com.alibaba.maxgraph.logging.RuntimeEvent.
pub enum RuntimeEvent {
    /// Timely server enter STARTING state.
    ServerStarting,

    /// Timely server enter RUNNING state.
    ServerRunning,

    /// Timely server enter DOWN state.
    ServerDown,

    /// Timely server get a new version.
    VersionChange,
}


impl Display for RuntimeEvent {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match self {
            RuntimeEvent::ServerStarting => write!(f, "SERVER_STARTING"),
            RuntimeEvent::ServerRunning => write!(f, "SERVER_RUNNING"),
            RuntimeEvent::ServerDown => write!(f, "SERVER_DOWN"),
            RuntimeEvent::VersionChange => write!(f, "VERSION_CHANGE"),
        }
    }
}

/// Write runtime log. Same format as java.
#[inline]
pub fn log_runtime(graph: &str, server_id: u32, group_id: u32, worker_id: u32, event: RuntimeEvent, version: i64, message: &str) {
    let time = time::current_time_millis();
    info!(target: "logging-runtime", "{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}",
          time, graph, "EXECUTOR", server_id, event, group_id, worker_id, version, message);
}

/// Write alert log. Same format as java.
#[inline]
pub fn log_alert(graph: &str, server_id: u32, message: &str) {
    let time = time::current_time_millis();
    info!(target: "logging-alert", "{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}",
          time, graph, "EXECUTOR", server_id, message);
}


#[cfg(test)]
mod tests {
    use super::*;
    use util::log4rs::get_deserializer;
    use std::fs::File;
    use std::io::*;

    fn assert_log_content(log_file: &str, expected: Vec<Vec<&'static str>>) {
        let f = File::open(format!("/tmp/logs/{}", log_file)).unwrap();
        let file = BufReader::new(&f);
        for (num, line) in file.lines().enumerate() {
            let line = line.unwrap();
            let mut fields: Vec<_> = line.split("\u{1}").collect();
            fields.remove(0);
            assert_eq!(fields, expected[num]);
        }
    }

    #[test]
    fn test_logging() {
        let _result = ::std::fs::remove_dir_all("/tmp/logs").is_ok();

        use log4rs;
        log4rs::init_file("./log4rs_for_test.yml", get_deserializer()).unwrap();

        log_alert("testGraph", 99, "test alert");
        log_query("testGraph", 99, "abcde", QueryType::Execute, QueryEvent::ExecutorReceived);
        log_query("testGraph", 99, "abcde", QueryType::Execute,
                  QueryEvent::ExecutorFinish { latency_nano: 99999, result_num: 10, success: true });
        log_runtime("testGraph", 99, 0, 1,RuntimeEvent::ServerRunning, 9, "msg");
        log_store("testGraph", 99, StoreEvent::SnapshotStoreOnline, "key", "msg");

        assert_log_content("alert.log", vec![vec!["testGraph", "EXECUTOR", "99", "test alert"]]);
        assert_log_content("query.log", vec![
            vec!["testGraph", "EXECUTOR", "99", "abcde", "EXECUTE", "EXECUTOR_RECEIVED", "", "", "", ""],
            vec!["testGraph", "EXECUTOR", "99", "abcde", "EXECUTE", "EXECUTOR_FINISH", "99999", "10", "true", ""]]);

        assert_log_content("runtime.log", vec![vec!["testGraph", "EXECUTOR", "99", "SERVER_RUNNING", "0", "1", "9", "msg"]]);
        assert_log_content("store.log", vec![vec!["testGraph", "EXECUTOR", "99", "SNAPSHOT", "SNAPSHOT_STORE_ONLINE", "key", "msg"]])
    }
}
