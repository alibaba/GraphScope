//
//! Copyright 2024 Alibaba Group Holding Limited.
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

use std::collections::HashMap;

use pegasus::{
    errors::{ErrorKind, JobExecError},
    JobSubmitError,
};

use crate::insight_error::Code as ErrorCode;

#[derive(Clone)]
pub struct ServerError {
    err_code: ErrorCode,
    ec: String,
    msg: String,
    details: HashMap<String, String>,
}

impl ServerError {
    pub fn new(err_code: ErrorCode, msg: String) -> Self {
        let ec = format!("04-{:04}", err_code as i32);
        ServerError { err_code, ec, msg, details: HashMap::new() }
    }

    pub fn with_details(mut self, key: &str, value: String) -> Self {
        self.details.insert(key.to_string(), value);
        self
    }

    pub fn is_cancelled(&self) -> bool {
        self.err_code == ErrorCode::JobExecuteCancelled
    }
}

impl std::fmt::Debug for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "")?;
        writeln!(f, "ErrorCode: {:?}", self.err_code)?;
        writeln!(f, "EC: {}", self.ec)?;
        writeln!(f, "Message: \"{}\"", self.msg)?;
        for (k, v) in self.details.iter() {
            writeln!(f, "{}: {}", k, v)?;
        }
        write!(f, "")
    }
}

impl From<&JobExecError> for ServerError {
    fn from(err: &JobExecError) -> Self {
        match err.kind {
            ErrorKind::WouldBlock(_) => {
                let err_code = ErrorCode::JobExecuteWouldBlock;
                ServerError::new(err_code, format!("{}", err))
            }
            ErrorKind::Interrupted => {
                let err_code = ErrorCode::JobExecuteInterrupted;
                ServerError::new(err_code, format!("{}", err))
            }
            ErrorKind::IOError => {
                let err_code = ErrorCode::JobExecuteIoError;
                ServerError::new(err_code, format!("{}", err))
            }
            ErrorKind::IllegalScopeInput => {
                let err_code = ErrorCode::JobExecuteIlleagalScopeInput;
                ServerError::new(err_code, format!("{}", err))
            }
            ErrorKind::Canceled => {
                let err_code = ErrorCode::JobExecuteCancelled;
                ServerError::new(err_code, format!("{}", err))
            }
            ErrorKind::Others => {
                let err_code = ErrorCode::JobExecuteOthers;
                ServerError::new(err_code, format!("{}", err))
            }
        }
    }
}

impl From<&JobSubmitError> for ServerError {
    fn from(err: &JobSubmitError) -> Self {
        match err {
            JobSubmitError::Build(err) => match err {
                pegasus::BuildJobError::Unsupported(e) => {
                    let err_code = ErrorCode::JobSubmitBuildJobUnsupported;
                    ServerError::new(err_code, format!("{}", e))
                }
                pegasus::BuildJobError::InternalError(e) => {
                    let err_code = ErrorCode::JobSubmitBuildJobInternalError;
                    ServerError::new(err_code, format!("{}", e))
                }
                pegasus::BuildJobError::ServerError(e) => {
                    let err_code = ErrorCode::JobSubmitBuildJobServerError;
                    ServerError::new(err_code, format!("{}", e))
                }
                pegasus::BuildJobError::UserError(e) => {
                    let err_code = ErrorCode::JobSubmitBuildJobUserError;
                    ServerError::new(err_code, format!("{}", e))
                }
            },
            JobSubmitError::Spawn(e) => {
                let err_code = ErrorCode::JobSubmitSpawnJobError;
                ServerError::new(err_code, format!("{}", e))
            }
        }
    }
}
