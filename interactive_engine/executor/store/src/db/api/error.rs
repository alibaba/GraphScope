use std::fmt::Debug;

#[derive(Clone)]
pub struct ErrorWithBackTrace {
    err_code: GraphErrorCode,
    msg: String,
    backtrace: Vec<(String, String)>,
}

impl ErrorWithBackTrace {
    pub fn new(err_code: GraphErrorCode, msg: String) -> Self {
        ErrorWithBackTrace {
            err_code,
            msg,
            backtrace: Vec::new(),
        }
    }

    pub fn add_backtrace(&mut self, function: String, code_info: String) {
        self.backtrace.push((function, code_info));
    }

    pub fn get_error_code(&self) -> GraphErrorCode {
        self.err_code
    }
}

#[derive(Clone)]
pub enum GraphError {
    WithBackTrace(ErrorWithBackTrace),
    Internal(String),
    Rocksdb(String),
    InvalidArgument(String),
    TooManyVersions(usize),
}

impl GraphError {
    pub fn new(err_code: GraphErrorCode, msg: String) -> Self {
        let inner = ErrorWithBackTrace {
            err_code,
            msg,
            backtrace: Vec::new(),
        };
        GraphError::WithBackTrace(inner)
    }

    pub fn add_backtrace(&mut self, function: String, code_info: String) {
        match self {
            GraphError::WithBackTrace(inner) => {
                inner.add_backtrace(function, code_info);
            },
            _ => {
                unimplemented!()
            }
        }
    }

    pub fn get_error_code(&self) -> GraphErrorCode {
        match self {
            GraphError::WithBackTrace(inner) => {
                inner.get_error_code()
            },
            _ => {
                unimplemented!()
            }
        }
    }
}

impl Debug for GraphError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            GraphError::WithBackTrace(inner) => {
                writeln!(f, "")?;
                writeln!(f, "error code: {:?}, msg: {}", inner.err_code, inner.msg)?;
                for bt in inner.backtrace.iter().rev() {
                    writeln!(f, "\t{}", bt.0)?;
                    writeln!(f, "\t\tat {}", bt.1)?;
                }
                write!(f, "")
            }
            GraphError::Internal(s) | GraphError::Rocksdb(s) | GraphError::InvalidArgument(s) => {
                write!(f, "{}", s)
            }
            GraphError::TooManyVersions(limit) => write!(f, "version count exceed limit {}", limit)
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Copy, Clone)]
pub enum GraphErrorCode {
    // try to get a property in ValueType1, but the it's real type is ValueType2 and it's not ValueType1 and
    // compatible to ValueType1
    ValueTypeMismatch,
    // transform bytes to str failed because it's not in utf-8 format
    Utf8Error,
    // something error with binary data in storage
    InvalidData,
    // get lock failed
    LockFailed,
    // too many data of old versions in graph store, maybe something error with garbage collection
    TooManyVersions,
    // some fatal bug in graph store
    GraphStoreBug,
    // user's operation is invalid, like: create a type that already exists
    InvalidOperation,
    // when try to insert data, the type exists in storage but isn't visible at that moment
    DataNotExists,
    // vertex type or edge type is not found
    TypeNotFound,
    // vertex type or edge type already exists
    TypeAlreadyExists,
    // error in external storage like rocksdb
    ExternalStorageError,
    // decode property from bytes failed
    DecodeError,
    // vertex type not found, edge not found
    MetaNotFound,
    // operations or features is not supported
    NotSupported,
    // engine error
    EngineError,
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