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
use std::cell::RefCell;
use std::collections::{HashMap, LinkedList};

use crate::data_plane::ChannelResource;
use crate::errors::{BuildJobError, IOError};
use crate::{Data, JobConf};

mod buffer;
pub(crate) mod cancel;
pub(crate) mod channel;
pub(crate) mod decorator;
pub(crate) mod input;
pub(crate) mod output;
pub use channel::Channel;

use crate::channel_id::ChannelId;

pub type IOResult<D> = Result<D, IOError>;
pub type Input<'a, D> = input::InputSession<'a, D>;
pub type Output<'a, D> = output::RefWrapOutput<D>;

thread_local! {
    static CHANNEL_RESOURCES : RefCell<HashMap<ChannelId, LinkedList<Box<dyn Any>>>> = RefCell::new(Default::default());
}

#[derive(Copy, Clone)]
enum Magic {
    Modulo(u64),
    And(u64),
}

impl Magic {
    pub fn new(len: usize) -> Self {
        if len & (len - 1) == 0 {
            Magic::And(len as u64 - 1)
        } else {
            Magic::Modulo(len as u64)
        }
    }

    #[inline(always)]
    pub fn exec(&self, id: u64) -> u64 {
        if id == 0 {
            return 0;
        }
        match self {
            Magic::Modulo(x) => id % *x,
            Magic::And(x) => id & *x,
        }
    }
}

pub(crate) fn build_channel<T: Data>(
    ch_id: ChannelId, conf: &JobConf,
) -> Result<ChannelResource<T>, BuildJobError> {
    let worker_id = crate::worker_id::get_current_worker();
    let ch = CHANNEL_RESOURCES.with(|res| {
        let mut map = res.borrow_mut();
        map.get_mut(&ch_id)
            .and_then(|ch| ch.pop_front())
    });

    if let Some(ch) = ch {
        let ch = ch
            .downcast::<ChannelResource<T>>()
            .map_err(|_| {
                BuildJobError::Unsupported(format!(
                    "type {} is unsupported in channel {}",
                    std::any::type_name::<T>(),
                    ch_id.index
                ))
            })?;
        Ok(*ch)
    } else {
        let local_workers = worker_id.local_peers;
        let server_index = worker_id.server_index;
        let mut resources =
            crate::data_plane::build_channels::<T>(ch_id, local_workers, server_index, conf.servers())?;
        if let Some(ch) = resources.pop_front() {
            if !resources.is_empty() {
                let mut upcast = LinkedList::new();
                for item in resources {
                    upcast.push_back(Box::new(item) as Box<dyn Any>);
                }
                CHANNEL_RESOURCES.with(|res| {
                    let mut map = res.borrow_mut();
                    map.insert(ch_id, upcast);
                })
            }
            Ok(ch)
        } else {
            BuildJobError::server_err(format!("channel {} resources is empty;", ch_id.index))
        }
    }
}
