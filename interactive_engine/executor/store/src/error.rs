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

#![allow(dead_code)]
use std::fmt::Debug;

pub type GraphTraceResult<T> = Result<T, GraphTraceError>;


pub struct GraphTraceError {
    backtrace: Vec<(String, String)>,
    msg: String,
    err_code: GraphErrorCode,
}

impl GraphTraceError {
    pub fn new(err_code: GraphErrorCode, msg: String) -> Self {
        GraphTraceError {
            err_code,
            msg,
            backtrace: Vec::new(),
        }
    }

    pub fn add_backtrace(&mut self, function: String, code_info: String) {
        self.backtrace.push((function, code_info));
    }
}

#[derive(Debug, Copy, Clone)]
pub enum GraphErrorCode {
    SystemError,
    DataError,
    InputDataError,
    InputArgumentsError,
    InvalidOperation,
    StorageError,
    IoError,
}

impl Debug for GraphTraceError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        writeln!(f, "")?;
        writeln!(f, "error code: {:?}, msg: {}", self.err_code, self.msg)?;
        for bt in self.backtrace.iter().rev() {
            writeln!(f, "\t{}", bt.0)?;
            writeln!(f, "\t\tat {}", bt.1)?;
        }
        write!(f, "")
    }
}

macro_rules! func_str {
    ($func:tt, $($x:tt),*) => {
        {
            let mut s = format!(concat!(stringify!($func), "(",concat!($(stringify!($x),"={:?},",)*)), $($x), *);
            s.truncate(s.len()-1);
            s.push_str(")");
            s
        }
    };
    ($func:tt) => {
        format!("{}()", stringify!($func))
    };
    () => {
        format!("...............................")
    };
}

macro_rules! code_pos {
    () => {
        format!("{}:{}", file!(), line!())
    };
}

#[macro_export]
macro_rules! try_unwrap {
    ($res:expr, $func:tt, $($x:tt),*) => {
        {
            $res.map_err(|mut e| {
                e.add_backtrace(func_str!($func, $($x),*), code_pos!());
                e
            })
        }

    };
    ($res:expr, $func:tt) => {
        {
            $res.map_err(|mut e| {
                e.add_backtrace(func_str!($func), code_pos!());
                e
            })
        }
    };
    ($res:expr) => {
        {
            $res.map_err(|mut e| {
                e.add_backtrace(func_str!(), code_pos!());
                e
            })
        }
    };
}

#[macro_export]
macro_rules! graph_err {
    ($err_code:expr, $msg:expr, $func:tt, $($x:tt), *) => {
        {
            let mut e = GraphTraceError::new($err_code, $msg);
            e.add_backtrace(func_str!($func, $($x),*), code_pos!());
            e
        }
    };

    ($err_code:expr, $msg:expr, $func:tt) => {
        {
            let mut e = GraphTraceError::new($err_code, $msg);
            e.add_backtrace(func_str!($func), code_pos!());
            e
        }
    };

    ($err_code:expr, $msg:expr) => {
        {
            let mut e = GraphTraceError::new($err_code, $msg);
            e.add_backtrace(func_str!(), code_pos!());
            e
        }
    };
}

#[macro_export]
macro_rules! try_lock_mutex {
    ($mutex:expr, $func:tt, $($x:tt), *) => {
        $mutex.lock().map_err(|e| {
            let msg = format!("lock error {:?}", e);
            graph_err!(GraphErrorCode::SystemError, msg, $func, $($x), *)
        })
    };
    ($mutex:expr, $func:tt) => {
        $mutex.lock().map_err(|e| {
            let msg = format!("lock error {:?}", e);
            graph_err!(GraphErrorCode::SystemError, msg, $func)
        })
    };
}
