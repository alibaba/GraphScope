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

use super::state::StateMap;
use crate::Tag;

/// Represents a notification which always indicates that the data stream of scope with the tag
/// has exhaust on the operator's input port;
#[derive(Clone, Debug)]
pub struct Notification {
    /// The port of operator's input this notification belongs to;
    pub port: usize,
    /// The tag of the scope this notification belongs to;
    pub tag: Tag,
}

impl Notification {
    pub fn new(port: usize, tag: Tag) -> Self {
        Notification { port, tag }
    }

    #[inline]
    pub fn is_belong_to(&self, scope_depth: usize) -> bool {
        self.tag.len() == scope_depth
    }

    pub fn take(self) -> (usize, Tag) {
        (self.port, self.tag)
    }
}

impl AsRef<Tag> for Notification {
    fn as_ref(&self) -> &Tag {
        &self.tag
    }
}

pub struct NotifySubscriber<'a> {
    state: &'a mut StateMap<()>,
}

impl<'a> NotifySubscriber<'a> {
    pub fn new(state: &'a mut StateMap<()>) -> Self {
        NotifySubscriber { state }
    }

    #[inline]
    pub fn subscribe<T: AsRef<Tag>>(&mut self, key: T) {
        if !self.state.contains(key.as_ref()) {
            self.state.insert(key.as_ref().clone(), ());
        }
    }
}
