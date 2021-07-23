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

use crate::channel_id::SubChannelId;
use std::error::Error;
use std::fmt::{Debug, Display};
use std::io;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Common(io::ErrorKind),
    SourceExhaust,
    Unknown,
}

impl From<io::ErrorKind> for ErrorKind {
    fn from(kind: io::ErrorKind) -> Self {
        ErrorKind::Common(kind)
    }
}

enum ErrorCause {
    Common(io::Error),
    Other(Box<dyn Error + Send>),
}

pub struct IOError {
    ch_id: Option<SubChannelId>,
    kind: ErrorKind,
    cause: Option<ErrorCause>,
    origin: Option<String>,
}

impl IOError {
    pub fn new<K: Into<ErrorKind>>(kind: K) -> Self {
        IOError { ch_id: None, kind: kind.into(), cause: None, origin: None }
    }

    pub fn source_exhaust() -> Self {
        IOError::new(ErrorKind::SourceExhaust)
    }

    pub fn set_ch_id(&mut self, ch_id: SubChannelId) {
        self.ch_id = Some(ch_id)
    }

    pub fn set_io_cause(&mut self, err: io::Error) {
        self.cause = Some(ErrorCause::Common(err))
    }

    pub fn set_cause(&mut self, err: Box<dyn Error + Send>) {
        self.cause = Some(ErrorCause::Other(err));
    }

    pub fn set_origin(&mut self, origin: String) {
        self.origin = Some(origin);
    }

    pub fn is_broken_pipe(&self) -> bool {
        match self.kind {
            ErrorKind::Common(io_kind) if io_kind == io::ErrorKind::BrokenPipe => true,
            _ => false,
        }
    }

    pub fn is_connection_aborted(&self) -> bool {
        match self.kind {
            ErrorKind::Common(io_kind) if io_kind == io::ErrorKind::ConnectionAborted => true,
            _ => false,
        }
    }

    pub fn is_interrupted(&self) -> bool {
        match self.kind {
            ErrorKind::Common(io_kind) if io_kind == io::ErrorKind::Interrupted => true,
            _ => false,
        }
    }

    pub fn is_would_block(&self) -> bool {
        match self.kind {
            ErrorKind::Common(io_kind) if io_kind == io::ErrorKind::WouldBlock => true,
            _ => false,
        }
    }

    pub fn is_source_exhaust(&self) -> bool {
        self.kind == ErrorKind::SourceExhaust
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl Default for IOError {
    fn default() -> Self {
        IOError::new(ErrorKind::Unknown)
    }
}

impl From<io::Error> for IOError {
    fn from(e: io::Error) -> Self {
        let mut error = IOError::new(ErrorKind::Common(e.kind()));
        error.set_io_cause(e);
        error
    }
}

impl Debug for IOError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "IOError(kind={:?}),", self.kind)?;
        if let Some(ref origin) = self.origin {
            write!(f, "\toccurred at: {},", origin)?;
        }

        if let Some(ref cause) = self.cause {
            match cause {
                ErrorCause::Common(e) => {
                    write!(f, "\tcaused by {}", e)?;
                }
                ErrorCause::Other(e) => {
                    write!(f, "\tcaused by {}", e)?;
                }
            }
        }
        write!(f, " ;")
    }
}

impl Display for IOError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Error for IOError {}
