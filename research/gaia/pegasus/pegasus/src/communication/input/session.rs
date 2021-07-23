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

use crate::api::notify::NotifySubscriber;
use crate::communication::input::InboundChannel;
use crate::data::DataSet;
use crate::errors::JobExecError;
use crate::{Data, Tag};
use std::cell::RefMut;

pub struct InputSession<'a, D: Data> {
    pub tag: Tag,
    pub(super) input: RefMut<'a, InboundChannel<D>>,
    has_outstanding: bool,
    subscriber: Option<NotifySubscriber<'a>>,
}

impl<'a, D: Data> InputSession<'a, D> {
    pub fn new(tag: Tag, input: RefMut<'a, InboundChannel<D>>) -> Self {
        let has_outstanding = input.pin(&tag);
        InputSession { tag, input, has_outstanding, subscriber: None }
    }

    pub fn set_notify_sub(&mut self, subscriber: NotifySubscriber<'a>) {
        self.subscriber = Some(subscriber);
    }

    #[inline]
    pub fn has_outstanding(&self) -> bool {
        self.has_outstanding
    }

    pub fn for_each_batch<F>(&mut self, mut func: F) -> Result<(), JobExecError>
    where
        F: FnMut(&mut DataSet<D>) -> Result<(), JobExecError>,
    {
        if self.input.is_exhaust() {
            Ok(())
        } else {
            while let Some((mut data, has_more)) = self.input.pull_scope(&self.tag)? {
                if let Err(err) = func(&mut data) {
                    return if err.can_be_retried() { Ok(()) } else { Err(err) };
                }
                if !has_more {
                    break;
                }
            }
            Ok(())
        }
    }

    pub fn cancel_scope(&mut self) {
        let tag = &self.tag;
        self.input.cancel(tag)
    }

    pub fn subscribe_notify(&mut self) {
        let tag = &self.tag;
        if let Some(ref mut n) = self.subscriber {
            n.subscribe(tag);
        }
    }
}
