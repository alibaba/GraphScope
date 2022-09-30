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

use super::{Edge, Vertex};
use crate::GraphResult;

pub trait ElemFilter {
    fn filter_vertex<V: Vertex>(&self, vertex: &V) -> GraphResult<bool>;
    fn filter_edge<E: Edge>(&self, edge: &E) -> GraphResult<bool>;
}
