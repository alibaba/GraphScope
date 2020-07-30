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

#[macro_export]
macro_rules! code_info {
    () => {
        format!("({}:{})", file!(), line!())
    };
}

#[macro_export]
macro_rules! err_msg {
    ($fmt:expr) => (format!(concat!($fmt, " {}"), code_info!()));
    ($fmt:expr, $($arg:tt)*) => (format!(concat!($fmt, " {}"), $($arg)*, code_info!()));
}


#[macro_export]
macro_rules! func_str {
    ($func:tt, $($x:tt),*) => {
        {
        let mut s = format!(concat!(stringify!($func), "(",concat!($(stringify!($x),"={:?},",)*)), $($x), *);
        s.truncate(s.len()-1);
        s.push_str(")");
        s
        }
    };
}

#[macro_export]
macro_rules! func_failed {
    ($msg:expr, $func:tt, $($x:tt), *) => {
        format!("{} failed, {}", func_str!($func, $($x), *), $msg)
    };
    ($func:tt, $($x:tt), *) => {
        format!("{} failed", func_str!($func, $($x), *))
    };
}
