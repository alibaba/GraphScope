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
use std::fmt::{Display};
use std::net::{AddrParseError, SocketAddr};

#[derive(Debug)]
pub enum NetError {
    InvalidConfig(Option<String>),
    ReadConfigError(toml::de::Error),
    IllegalChannelId,
    NotConnected(u64),
    IOError(std::io::Error),
    ConflictConnect(u64),
    UnexpectedServer((u64, u64)),
    ServerNotFound(u64),
    ServerStarted(u64),
    AddrParseError(AddrParseError),
    HBAbnormal(SocketAddr),
    ChannelRxReset(u128),
}

impl Display for NetError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            NetError::InvalidConfig(msg) => {
                if let Some(msg) = msg {
                    write!(f, "invalid config: {}", msg)
                } else {
                    write!(f, "invalid config, unknown error;")
                }
            }
            NetError::ReadConfigError(e) => {
                write!(f, "parse configuration failure: {};", e)
            }
            NetError::IllegalChannelId => {
                write!(f, "channel id = 0 is retained by library;")
            }
            NetError::NotConnected(id) => {
                write!(f, "server with id = {} not connected;", id)
            }
            NetError::IOError(err) => {
                write!(f, "IOError: {};", err)
            }
            NetError::ConflictConnect(id) => {
                write!(f, "server {} is already connected and in use;", id)
            }
            NetError::UnexpectedServer((expect, actual)) => {
                write!(f, "unexpected server with id {}, expected {};", actual, expect)
            }
            NetError::ServerNotFound(id) => {
                write!(f, "server {} not found;", id)
            }
            NetError::ServerStarted(id) => {
                write!(f, "server {} has already started; ", id)
            }
            NetError::AddrParseError(err) => {
                write!(f, "invalid address: {};", err)
            }
            NetError::HBAbnormal(id) => {
                write!(f, "heartbeat from server {:?} lost;", id)
            }
            NetError::ChannelRxReset(id) => {
                write!(f, "channel {}'s receiver is already in use, multi-receivers is not allowed;", id)
            }
        }
    }
}

impl Error for NetError {}

impl From<std::io::Error> for NetError {
    fn from(err: std::io::Error) -> Self {
        NetError::IOError(err)
    }
}

impl From<AddrParseError> for NetError {
    fn from(err: AddrParseError) -> Self {
        NetError::AddrParseError(err)
    }
}

impl From<toml::de::Error> for NetError {
    fn from(err: toml::de::Error) -> Self {
        NetError::ReadConfigError(err)
    }
}
