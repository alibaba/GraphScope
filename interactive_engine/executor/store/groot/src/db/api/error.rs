use std::fmt::Debug;

use protobuf::ProtobufEnum;

pub use crate::db::proto::insight::Code as ErrorCode;

#[derive(Clone)]
pub struct GraphError {
    err_code: ErrorCode,
    ec: String,
    msg: String,
    backtrace: Vec<(String, String)>,
}

impl GraphError {
    pub fn new(err_code: ErrorCode, msg: String) -> Self {
        let ec = format!("06-{:04}", err_code.value());
        GraphError { err_code, ec, msg, backtrace: Vec::new() }
    }

    pub fn add_backtrace(&mut self, function: String, code_info: String) {
        self.backtrace.push((function, code_info));
    }
    pub fn get_error_code(&self) -> ErrorCode {
        self.err_code
    }
}

impl Debug for GraphError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "")?;
        writeln!(f, "error code: {:?}, ec: {}, msg: {}", self.err_code, self.ec, self.msg)?;
        for bt in self.backtrace.iter().rev() {
            writeln!(f, "\t{}", bt.0)?;
            writeln!(f, "\t\tat {}", bt.1)?;
        }
        write!(f, "")
    }
}

macro_rules! func_signature {
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
macro_rules! gen_graph_err {
    ($err_code:expr, $msg:expr, $func:tt, $($x:tt), *) => {
        {
            let mut e = GraphError::new($err_code, $msg);
            e.add_backtrace(func_signature!($func, $($x),*), code_pos!());
            e
        }
    };

    ($err_code:expr, $msg:expr, $func:tt) => {
        {
            let mut e = GraphError::new($err_code, $msg);
            e.add_backtrace(func_signature!($func), code_pos!());
            e
        }
    };

    ($err_code:expr, $msg:expr) => {
        {
            let mut e = GraphError::new($err_code, $msg);
            e.add_backtrace(func_signature!(), code_pos!());
            e
        }
    };
}

#[macro_export]
macro_rules! res_unwrap {
    ($res:expr, $func:tt, $($x:tt),*) => {
        {
            $res.map_err(|mut e| {
                e.add_backtrace(func_signature!($func, $($x),*), code_pos!());
                e
            })
        }

    };
    ($res:expr, $func:tt) => {
        {
            $res.map_err(|mut e| {
                e.add_backtrace(func_signature!($func), code_pos!());
                e
            })
        }
    };
    ($res:expr) => {
        {
            $res.map_err(|mut e| {
                e.add_backtrace(func_signature!(), code_pos!());
                e
            })
        }
    };
}
