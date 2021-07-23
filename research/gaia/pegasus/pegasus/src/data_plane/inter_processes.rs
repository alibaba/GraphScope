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
use crate::data_plane::intra_process::IntraProcessPull;
use crate::data_plane::{Pull, Push};
use crate::errors::IOError;
use crate::Data;
use pegasus_network::{IPCReceiver, IPCSender};

pub struct RemotePush<T: Data> {
    pub id: SubChannelId,
    push: IPCSender<T>,
}

impl<T: Data> Push<T> for RemotePush<T> {
    fn push(&mut self, msg: T) -> Result<(), IOError> {
        Ok(self.push.send(&msg)?)
    }

    fn check_failure(&mut self) -> Option<T> {
        None
    }

    fn flush(&mut self) -> Result<(), IOError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), IOError> {
        Ok(self.push.close()?)
    }
}

impl<T: Data> RemotePush<T> {
    pub fn new(id: SubChannelId, push: IPCSender<T>) -> Self {
        RemotePush { id, push }
    }
}

pub struct CombinationPull<T: Data> {
    pub id: SubChannelId,
    local: IntraProcessPull<T>,
    remote: IPCReceiver<T>,
    local_end: bool,
    remote_end: bool,
}

impl<T: Data> Pull<T> for CombinationPull<T> {
    fn pull(&mut self) -> Result<Option<T>, IOError> {
        if !self.local_end {
            match self.local.pull() {
                Ok(Some(data)) => return Ok(Some(data)),
                Err(err) => {
                    if err.is_source_exhaust() {
                        self.local_end = true;
                    } else {
                        return Err(err);
                    }
                }
                _ => (),
            }
        }

        if !self.remote_end {
            match self.remote.recv() {
                Ok(Some(data)) => return Ok(Some(data)),
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::BrokenPipe {
                        self.remote_end = true;
                    } else {
                        return Err(e)?;
                    }
                }
                _ => (),
            }
        }

        if self.local_end && self.remote_end {
            Err(IOError::source_exhaust())
        } else {
            Ok(None)
        }
    }
}

impl<T: Data> CombinationPull<T> {
    pub fn new(id: SubChannelId, local: IntraProcessPull<T>, remote: IPCReceiver<T>) -> Self {
        CombinationPull { id, local, remote, local_end: false, remote_end: false }
    }
}
