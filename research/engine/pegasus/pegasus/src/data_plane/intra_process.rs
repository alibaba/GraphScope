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

use std::io;

use pegasus_common::channel::*;

use crate::channel_id::ChannelId;
use crate::data::Data;
use crate::data_plane::intra_thread::ThreadPull;
use crate::data_plane::{Pull, Push};
use crate::errors::{IOError, IOErrorKind};

pub struct IntraProcessPush<T: Send> {
    pub ch_id: ChannelId,
    sender: MessageSender<T>,
    last_failed: Option<T>,
}

impl<T: Send> IntraProcessPush<T> {
    pub fn new(ch_id: ChannelId, sender: MessageSender<T>) -> Self {
        IntraProcessPush { ch_id, sender, last_failed: None }
    }
}

impl<T: Data> Push<T> for IntraProcessPush<T> {
    fn push(&mut self, msg: T) -> Result<(), IOError> {
        self.sender.send(msg).map_err(|err| {
            error_worker!("IntraProcessPush#push: send data failure {:?}", err);
            self.last_failed.replace(err);
            throw_io_error!(io::ErrorKind::NotConnected, self.ch_id)
        })
    }

    #[inline]
    fn check_failed(&mut self) -> Option<T> {
        self.last_failed.take()
    }

    #[inline]
    fn close(&mut self) -> Result<(), IOError> {
        self.sender.close();
        Ok(())
    }
}

pub struct IntraProcessPull<T: Send> {
    pub ch_id: ChannelId,
    recv: (MessageReceiver<T>, bool),
    cached: Option<T>,
    local: (ThreadPull<T>, bool),
}

impl<T: Send> IntraProcessPull<T> {
    pub fn new(ch_id: ChannelId, local: ThreadPull<T>, recv: MessageReceiver<T>) -> Self {
        IntraProcessPull { ch_id, recv: (recv, false), cached: None, local: (local, false) }
    }
}

impl<T: Data> Pull<T> for IntraProcessPull<T> {
    fn next(&mut self) -> Result<Option<T>, IOError> {
        if let Some(data) = self.cached.take() {
            return Ok(Some(data));
        }

        if !self.local.1 {
            match self.local.0.next() {
                Ok(Some(data)) => return Ok(Some(data)),
                Err(err) => {
                    if err.is_source_exhaust() {
                        self.local.1 = true;
                    } else {
                        return Err(err);
                    }
                }
                _ => (),
            }
        }

        if !self.recv.1 {
            match self.recv.0.try_recv() {
                Ok(Some(data)) => return Ok(Some(data)),
                Err(e) => {
                    if e.kind() == io::ErrorKind::BrokenPipe {
                        self.recv.1 = true;
                    } else {
                        let mut err = throw_io_error!(e.kind(), self.ch_id);
                        err.set_io_cause(e);
                        return Err(err);
                    }
                }
                _ => (),
            }
        }

        if self.recv.1 && self.local.1 {
            Err(throw_io_error!(IOErrorKind::SourceExhaust, self.ch_id))
        } else {
            Ok(None)
        }
    }

    fn has_next(&mut self) -> Result<bool, IOError> {
        if self.cached.is_some() {
            Ok(true)
        } else if self.local.0.has_next()? {
            Ok(true)
        } else {
            if self.recv.1 {
                Ok(false)
            } else {
                match self.recv.0.try_recv() {
                    Ok(d) => self.cached = d,
                    Err(e) => {
                        if e.kind() == io::ErrorKind::BrokenPipe {
                            self.recv.1 = true;
                        } else {
                            let mut err = throw_io_error!(e.kind(), self.ch_id);
                            err.set_io_cause(e);
                            return Err(err);
                        }
                    }
                }
                Ok(self.cached.is_some())
            }
        }
    }
}
