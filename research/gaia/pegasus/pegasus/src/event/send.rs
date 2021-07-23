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

use super::utils::CountDownLatchTree;
use crate::graph::{Edge, Port};
use crate::Tag;
use std::cell::RefMut;

pub struct ChannelTxState {
    pub rx_peers: usize,
    pub scope_depth: usize,
    pub port: Port,
    scope_skip: CountDownLatchTree,
}

impl ChannelTxState {
    pub fn new(edge: Edge) -> Self {
        ChannelTxState {
            rx_peers: edge.dst_peers,
            scope_depth: edge.scope_depth,
            port: edge.source,
            scope_skip: CountDownLatchTree::new(edge.dst_peers),
        }
    }

    #[inline]
    pub fn skip_scope(&self, tag: Tag, source: u32) -> RefMut<Vec<Tag>> {
        self.scope_skip.count_down(tag, source)
    }
}
