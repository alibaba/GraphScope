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

use std::any::Any;
use std::error::Error;
use std::fmt::{Debug, Display};
use std::io;

use crate::channel_id::ChannelId;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ChannelErrorKind {
    // the consumer is dropped, while
    Disconnected,
    ConnectionAborted,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ChannelError {
    pub ch_id: ChannelId,
    pub kind: ChannelErrorKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IOErrorKind {
    SourceExhaust,
    // IO error from system's IO derive, like network(tcp..), files,
    System(io::ErrorKind),
    Channel(ChannelError),
    // block by flow control;
    WouldBlock,
    CannotBlock,
    Interrupt,
    Unknown,
}

impl From<io::ErrorKind> for IOErrorKind {
    fn from(kind: io::ErrorKind) -> Self {
        IOErrorKind::System(kind)
    }
}

enum ErrorCause {
    Common(io::Error),
    Other(Box<dyn Error + Send>),
}

pub struct IOError {
    ch_id: Option<ChannelId>,
    kind: IOErrorKind,
    cause: Option<ErrorCause>,
    origin: Option<String>,
    resource: Option<Box<dyn Any + Send>>,
}

impl IOError {
    pub fn new<K: Into<IOErrorKind>>(kind: K) -> Self {
        IOError { ch_id: None, kind: kind.into(), cause: None, origin: None, resource: None }
    }

    pub fn source_exhaust() -> Self {
        IOError::new(IOErrorKind::SourceExhaust)
    }

    pub fn would_block() -> Self {
        IOError::new(IOErrorKind::WouldBlock)
    }

    pub fn would_block_with<A: Send + 'static>(res: A) -> Self {
        let resource = Some(Box::new(res) as Box<dyn Any + Send>);
        IOError { ch_id: None, kind: IOErrorKind::WouldBlock, cause: None, origin: None, resource }
    }

    pub fn cannot_block() -> Self {
        IOError::new(IOErrorKind::CannotBlock)
    }

    pub fn interrupted() -> Self {
        IOError::new(IOErrorKind::Interrupt)
    }

    pub fn set_ch_id(&mut self, ch_id: ChannelId) {
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
            IOErrorKind::System(io_kind) if io_kind == io::ErrorKind::BrokenPipe => true,
            _ => false,
        }
    }

    pub fn is_connection_aborted(&self) -> bool {
        match self.kind {
            IOErrorKind::System(io_kind) if io_kind == io::ErrorKind::ConnectionAborted => true,
            _ => false,
        }
    }

    pub fn is_interrupted(&self) -> bool {
        match self.kind {
            IOErrorKind::System(io_kind) if io_kind == io::ErrorKind::Interrupted => true,
            IOErrorKind::Interrupt => true,
            _ => false,
        }
    }

    pub fn is_would_block(&self) -> bool {
        match self.kind {
            IOErrorKind::System(io_kind) if io_kind == io::ErrorKind::WouldBlock => true,
            IOErrorKind::WouldBlock => true,
            _ => false,
        }
    }

    pub fn is_source_exhaust(&self) -> bool {
        self.kind == IOErrorKind::SourceExhaust
    }

    pub fn kind(&self) -> &IOErrorKind {
        &self.kind
    }

    pub fn take_resource(&mut self) -> Option<Box<dyn Any + Send>> {
        self.resource.take()
    }
}

impl Default for IOError {
    fn default() -> Self {
        IOError::new(IOErrorKind::Unknown)
    }
}

impl From<io::Error> for IOError {
    fn from(e: io::Error) -> Self {
        let mut error = IOError::new(IOErrorKind::System(e.kind()));
        error.set_io_cause(e);
        error
    }
}

impl From<Box<dyn std::error::Error + Send>> for IOError {
    fn from(e: Box<dyn Error + Send>) -> Self {
        let mut err = IOError::new(IOErrorKind::Unknown);
        err.set_cause(e);
        err
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
