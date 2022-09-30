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

use std::error::Error;
use std::fmt::{self, Debug, Display};
use std::io;

use pegasus_network::NetError;

mod io_error;
pub use io_error::IOError;
pub use io_error::IOErrorKind;

use crate::Tag;

pub type IOResult<D> = Result<D, IOError>;

#[derive(Clone, Eq, PartialEq)]
pub enum ErrorKind {
    WouldBlock(Option<Tag>),
    Interrupted,
    IOError,
    IllegalScopeInput,
    Others,
}

impl Debug for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ErrorKind::WouldBlock(_) => write!(f, "WouldBlock, retry later"),
            ErrorKind::Interrupted => write!(f, "Interrupted, retry later"),
            ErrorKind::IOError => write!(f, "IOError"),
            ErrorKind::IllegalScopeInput => write!(f, "IllegalScopeInput"),
            ErrorKind::Others => write!(f, "Unknown"),
        }
    }
}

pub struct JobExecError {
    pub kind: ErrorKind,
    pub is_system: bool,
    cause: Box<dyn Error + Send>,
}

impl JobExecError {
    pub fn new<E: Error + Send + 'static>(kind: ErrorKind, cause: E) -> Self {
        JobExecError { kind, is_system: false, cause: Box::new(cause) }
    }

    pub fn panic(msg: String) -> Self {
        let err: Box<dyn Error + Send + Sync> = msg.into();
        JobExecError { kind: ErrorKind::Others, is_system: false, cause: err as Box<dyn Error + Send> }
    }

    pub(crate) fn from_box(err: Box<dyn Error + Send>) -> Self {
        if let Some(e) = err.downcast_ref::<JobExecError>() {
            JobExecError { kind: e.kind.clone(), is_system: e.is_system, cause: err }
        } else if let Some(e) = err.downcast_ref::<IOError>() {
            if e.is_interrupted() {
                JobExecError { kind: ErrorKind::Interrupted, is_system: true, cause: err }
            } else if e.is_would_block() {
                JobExecError { kind: ErrorKind::WouldBlock(None), is_system: true, cause: err }
            } else {
                JobExecError { kind: ErrorKind::IOError, is_system: true, cause: err }
            }
        } else {
            JobExecError { kind: ErrorKind::Others, is_system: false, cause: err }
        }
    }

    pub fn set_system(&mut self) {
        self.is_system = true;
    }

    pub fn set_kind(&mut self, kind: ErrorKind) {
        self.kind = kind;
    }

    #[inline]
    pub fn can_be_retried(&self) -> bool {
        matches!(self.kind, ErrorKind::WouldBlock(_) | ErrorKind::Interrupted)
    }

    pub fn get_cause(&self) -> &Box<dyn Error + Send> {
        &self.cause
    }

    pub fn as_ref<E: Error + 'static>(&self) -> Option<&E> {
        self.cause.downcast_ref::<E>()
    }
}

impl Debug for JobExecError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.is_system {
            write!(f, "system error: ")?;
        } else {
            write!(f, "user error: ")?;
        }
        write!(f, "kind({:?}), caused by:{}", self.kind, self.cause)
    }
}

impl Display for JobExecError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Error for JobExecError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

impl From<IOError> for JobExecError {
    fn from(err: IOError) -> Self {
        if err.is_interrupted() {
            JobExecError { kind: ErrorKind::Interrupted, is_system: true, cause: Box::new(err) }
        } else if err.is_would_block() {
            JobExecError { kind: ErrorKind::WouldBlock(None), is_system: true, cause: Box::new(err) }
        } else {
            JobExecError { kind: ErrorKind::IOError, is_system: true, cause: Box::new(err) }
        }
    }
}

impl From<Box<dyn Error + Send>> for JobExecError {
    fn from(err: Box<dyn Error + Send>) -> Self {
        JobExecError::from_box(err)
    }
}

impl From<Box<dyn Error + Send + Sync>> for JobExecError {
    fn from(err: Box<dyn Error + Send + Sync>) -> Self {
        let err = err as Box<dyn Error + Send>;
        JobExecError::from_box(err)
    }
}

impl From<String> for JobExecError {
    fn from(msg: String) -> Self {
        let err: Box<dyn Error + Send + Sync> = msg.into();
        JobExecError::from(err)
    }
}

impl From<io::Error> for JobExecError {
    fn from(err: io::Error) -> Self {
        match err.kind() {
            io::ErrorKind::WouldBlock => {
                JobExecError { kind: ErrorKind::WouldBlock(None), is_system: true, cause: Box::new(err) }
            }
            io::ErrorKind::Interrupted => {
                JobExecError { kind: ErrorKind::Interrupted, is_system: true, cause: Box::new(err) }
            }
            _ => JobExecError { kind: ErrorKind::IOError, is_system: true, cause: Box::new(err) },
        }
    }
}

#[macro_export]
macro_rules! throw_user_error {
    ($kind: expr) => {{
        let pos = concat!(file!(), ':', line!());
        let str_err = format!("occurred at {}", pos);
        let mut err = JobExecError::from(str_err);
        err.set_kind($kind);
        return Err(err);
    }};
}

// TODO: Make build error enumerate.;
pub enum BuildJobError {
    Unsupported(String),
    ServerError(Box<dyn std::error::Error + Send>),
    UserError(Box<dyn std::error::Error + Send>),
}

impl Debug for BuildJobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuildJobError::Unsupported(msg) => write!(f, "unsupported combination: {}", msg),
            BuildJobError::UserError(e) => write!(f, "user defined error: {}", e),
            BuildJobError::ServerError(e) => write!(f, "server error: {}", e),
        }
    }
}

impl Display for BuildJobError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Error for BuildJobError {}

impl BuildJobError {
    pub(crate) fn unsupported<T, S: Into<String>>(msg: S) -> Result<T, Self> {
        Err(BuildJobError::Unsupported(msg.into()))
    }

    pub(crate) fn server_err<T, E: Into<Box<dyn std::error::Error + Send + Sync>>>(
        err: E,
    ) -> Result<T, Self> {
        Err(BuildJobError::ServerError(err.into()))
    }
}

impl From<NetError> for BuildJobError {
    fn from(e: NetError) -> Self {
        BuildJobError::ServerError(Box::new(e))
    }
}

impl From<String> for BuildJobError {
    fn from(msg: String) -> Self {
        let err: Box<dyn std::error::Error + Send + Sync> = msg.into();
        BuildJobError::UserError(err)
    }
}

impl From<&str> for BuildJobError {
    fn from(msg: &str) -> Self {
        BuildJobError::from(msg.to_owned())
    }
}

impl From<Box<dyn std::error::Error + Send>> for BuildJobError {
    fn from(e: Box<dyn Error + Send>) -> Self {
        BuildJobError::UserError(e)
    }
}

pub struct SpawnJobError(pub String);

impl Debug for SpawnJobError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Submit job to executor failure, caused by : {}", self.0)
    }
}

impl Display for SpawnJobError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Submit job to executor failure, caused by : {}", self.0)
    }
}

impl Error for SpawnJobError {}

impl From<&str> for SpawnJobError {
    fn from(msg: &str) -> Self {
        SpawnJobError(msg.to_owned())
    }
}

#[derive(Debug)]
pub enum JobSubmitError {
    Build(BuildJobError),
    Spawn(SpawnJobError),
}

impl Display for JobSubmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            JobSubmitError::Build(err) => write!(f, "Build job failure: {}", err),
            JobSubmitError::Spawn(err) => write!(f, "Spawn job failure: {}", err),
        }
    }
}

impl Error for JobSubmitError {}

impl From<BuildJobError> for JobSubmitError {
    fn from(err: BuildJobError) -> Self {
        JobSubmitError::Build(err)
    }
}

impl From<SpawnJobError> for JobSubmitError {
    fn from(err: SpawnJobError) -> Self {
        JobSubmitError::Spawn(err)
    }
}

#[derive(Debug)]
pub enum StartupError {
    ReadConfigError(std::io::Error),
    ParseConfigError(toml::de::Error),
    CannotFindServers,
    Network(NetError),
    AlreadyStarted(u64),
}

impl Display for StartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            StartupError::ReadConfigError(e) => {
                write!(f, "fail to read configuration, caused by {};", e)
            }
            StartupError::ParseConfigError(e) => write!(f, "parse configuration failure : {}", e),
            StartupError::CannotFindServers => write!(f, "can't detect other servers;"),
            StartupError::Network(e) => {
                write!(f, "startup failure, caused by network error: {:?}", e)
            }
            StartupError::AlreadyStarted(id) => write!(f, "server {} has already started;", id),
        }
    }
}

impl Error for StartupError {}

impl From<NetError> for StartupError {
    fn from(e: NetError) -> Self {
        StartupError::Network(e)
    }
}

impl From<toml::de::Error> for StartupError {
    fn from(e: toml::de::Error) -> Self {
        StartupError::ParseConfigError(e)
    }
}

impl From<std::io::Error> for StartupError {
    fn from(e: std::io::Error) -> Self {
        StartupError::ReadConfigError(e)
    }
}

#[macro_export]
macro_rules! throw_io_error {
    () => {{
        let mut err = IOError::default();
        let origin = concat!(file!(), ':', line!());
        err.set_origin(origin.to_string());
        err
    }};
    ($kind: expr) => {{
        let mut err = IOError::new($kind);
        let origin = concat!(file!(), ':', line!());
        err.set_origin(origin.to_string());
        err
    }};
    ($kind: expr, $id: expr) => {{
        let mut err = IOError::new($kind);
        let origin = concat!(file!(), ':', line!());
        err.set_origin(origin.to_string());
        err.set_ch_id($id);
        err
    }};
}

#[macro_export]
macro_rules! would_block {
    ($msg: expr) => {{
        let origin = concat!(file!(), ':', line!());
        let cause: Box<dyn std::error::Error + Send + Sync> = $msg.into();
        let mut err = IOError::would_block();
        err.set_origin(origin.to_string());
        err.set_cause(cause);
        Err(err)
    }};
}

#[macro_export]
macro_rules! interrupt {
    ($msg: expr) => {{
        let origin = concat!(file!(), ':', line!());
        let cause: Box<dyn std::error::Error + Send + Sync> = $msg.into();
        let mut err = IOError::interrupted();
        err.set_origin(origin.to_string());
        err.set_cause(cause);
        Err(err)
    }};
}
