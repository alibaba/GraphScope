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

use super::{VertexId, LabelId, EdgeId};
use super::property::Property;
use crate::schema::prelude::*;

pub trait Vertex: Send + Sync {
    type PI: Iterator<Item=(PropId, Property)>;
    fn get_id(&self) -> VertexId;
    fn get_label_id(&self) -> LabelId;
    fn get_property(&self, prop_id: PropId) -> Option<Property>;
    fn get_properties(&self) -> Self::PI;
}

pub trait Edge: Send + Sync {
    type PI: Iterator<Item=(PropId, Property)>;
    fn get_label_id(&self) -> LabelId;
    fn get_src_label_id(&self) -> LabelId;
    fn get_dst_label_id(&self) -> LabelId;
    fn get_src_id(&self) -> VertexId;
    fn get_dst_id(&self) -> VertexId;
    fn get_edge_id(&self) -> EdgeId;
    fn get_property(&self, prop_id: PropId) -> Option<Property>;
    fn get_properties(&self) -> Self::PI;
}
