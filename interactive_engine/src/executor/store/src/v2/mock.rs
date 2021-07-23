//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//!     http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use crate::v2::api::*;
use crate::v2::Result;

pub struct PropertyImpl {
    value : PropertyValue
}

impl PropertyImpl {
    fn new() -> PropertyImpl {
        PropertyImpl {
            value : PropertyValue::Null
        }
    }
}

impl Property for PropertyImpl {
    fn get_property_id(&self) -> PropertyId {
        println!("[Rust Internal] <PropertyImpl::GetPropertyId>");
        3333
    }

    fn get_property_value(&self) -> &PropertyValue {
        println!("[Rust Internal] <PropertyImpl::GetPropertyValue>");
        &self.value
    }
}

pub struct PropertyIteratorImpl {
    num : i32
}

impl PropertyIteratorImpl {
    fn new() -> PropertyIteratorImpl {
        PropertyIteratorImpl {
            num : 4
        }
    }
}

impl Iterator for PropertyIteratorImpl {
    type Item = Result<PropertyImpl>;

    fn next(&mut self) -> Option<Self::Item> {
        println!("[Rust Internal] <PropertyIteratorImpl::Next>");
        if self.num == 0 {
            return None;
        }
        self.num -= 1;
        Some(Ok(PropertyImpl::new()))
    }
}

pub struct VertexImpl {
    id : VertexId,
    label : LabelId
}

impl VertexImpl {
    fn new() -> VertexImpl {
        VertexImpl {
            id : 1234,
            label : 1111
        }
    }
}

impl PropertyReader for VertexImpl {
    type P = PropertyImpl;
    type PropertyIterator = PropertyIteratorImpl;

    fn get_property(&self, property_id: u32) -> Option<Self::P> {
        println!("[Rust Internal] <VertexImpl::GetPropertyByPropId> Input PropertyId = {}", property_id);
        Some(PropertyImpl::new())
    }

    fn get_property_iterator(&self) -> Self::PropertyIterator {
        println!("[Rust Internal] <VertexImpl::GetPropertyIterator>");
        PropertyIteratorImpl::new()
    }
}

impl Vertex for VertexImpl {
    fn get_vertex_id(&self) -> u64 {
        println!("[Rust Internal] <VertexImpl::GetVertexId>");
        self.id
    }

    fn get_label_id(&self) -> u32 {
        println!("[Rust Internal] <VertexImpl::GetLabelId>");
        self.label
    }
}

pub struct VertexIteratorImpl {
    num : i32
}

impl VertexIteratorImpl {
    fn new() -> VertexIteratorImpl {
        VertexIteratorImpl {
            num : 4
        }
    }
}

unsafe impl Send for VertexIteratorImpl {}

impl Iterator for VertexIteratorImpl {
    type Item = Result<VertexImpl>;

    fn next(&mut self) -> Option<Self::Item> {
        println!("[Rust Internal] <VertexIteratorImpl::Next>");
        if self.num == 0 {
            return None;
        }
        self.num -= 1;
        Some(Ok(VertexImpl::new()))
    }
}

pub struct EdgeImpl {
    edge_id : EdgeId,
    edge_relation : EdgeRelation
}

impl EdgeImpl {
    fn new() -> EdgeImpl {
        EdgeImpl {
            edge_id : EdgeId::new(1234, 1234, 1234),
            edge_relation : EdgeRelation::new(2222, 1111, 1111)
        }
    }
}

impl PropertyReader for EdgeImpl {
    type P = PropertyImpl;
    type PropertyIterator = PropertyIteratorImpl;

    fn get_property(&self, property_id: u32) -> Option<Self::P> {
        println!("[Rust Internal] <EdgeImpl::GetPropertyByPropId> Input PropertyId = {}", property_id);
        Some(PropertyImpl::new())
    }

    fn get_property_iterator(&self) -> Self::PropertyIterator {
        println!("[Rust Internal] <EdgeImpl::GetPropertyIterator>");
        PropertyIteratorImpl::new()
    }
}

impl Edge for EdgeImpl {
    fn get_edge_id(&self) -> EdgeId {
        println!("[Rust Internal] <EdgeImpl::GetEdgeId>");
        EdgeId::new(self.edge_id.get_edge_inner_id(),
            self.edge_id.get_src_vertex_id(),
            self.edge_id.get_dst_vertex_id())
    }

    fn get_edge_relation(&self) -> EdgeRelation {
        println!("[Rust Internal] <EdgeImpl::GetEdgeRelation>");
        EdgeRelation::new(self.edge_relation.get_edge_label_id(),
            self.edge_relation.get_src_vertex_label_id(),
            self.edge_relation.get_dst_vertex_label_id())
    }
}

pub struct EdgeIteratorImpl {
    num : i32
}

impl EdgeIteratorImpl {
    fn new() -> EdgeIteratorImpl {
        EdgeIteratorImpl {
            num : 5
        }
    }
}

unsafe impl Send for EdgeIteratorImpl {}

impl Iterator for EdgeIteratorImpl {
    type Item = Result<EdgeImpl>;

    fn next(&mut self) -> Option<Self::Item> {
        println!("[Rust Internal] <EdgeIteratorImpl::Next>");
        if self.num == 0 {
            return None;
        }
        self.num -= 1;
        Some(Ok(EdgeImpl::new()))
    }
}

pub struct PartitionSnapshotImpl {}

impl PartitionSnapshotImpl {
    pub(crate) fn new() -> PartitionSnapshotImpl {
        PartitionSnapshotImpl {}
    }
}

unsafe impl Send for PartitionSnapshotImpl {}

unsafe impl Sync for PartitionSnapshotImpl {}

impl PartitionSnapshot for PartitionSnapshotImpl {
    type V = VertexImpl;
    type E = EdgeImpl;

    fn get_vertex(&self, vertex_id: VertexId, label_id: Option<LabelId>, property_ids: Option<&Vec<PropertyId>>) -> Result<Option<Self::V>> {
        let label_str = match label_id {
            Some(l) => l.to_string(),
            None => "None".to_string()
        };
        let selector = match property_ids {
            Some(_) => "Something".to_string(),
            None => "None".to_string()
        };
        println!("[Rust Internal] <Get Vertex> (vid: {}, label_id: {}, prop_selector: {})", vertex_id, label_str, selector);
        Ok(Some(VertexImpl::new()))
    }

    fn get_edge(&self, edge_id: EdgeId, edge_relation: Option<&EdgeRelation>, property_ids: Option<&Vec<PropertyId>>) -> Result<Option<Self::E>> {
        let edge_id_str = format!("({}, {}, {})", edge_id.get_edge_inner_id(), edge_id.get_src_vertex_id(), edge_id.get_dst_vertex_id());
        let edge_relation_str = match edge_relation {
            Some(er) => {
                format!("({}, {}, {})", er.get_edge_label_id(), er.get_src_vertex_label_id(), er.get_dst_vertex_label_id())
            }
            None => "None".to_string()
        };
        let selector = match property_ids {
            Some(_) => "Something".to_string(),
            None => "None".to_string()
        };
        println!("[Rust Internal] <Get Edge> (eid: {}, e_relation: {}, prop_selector: {})", edge_id_str, edge_relation_str, selector);
        Ok(Some(EdgeImpl::new()))
    }

    fn scan_vertex(&self, label_id: Option<LabelId>, condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>) -> Result<Records<Self::V>> {
        let label_str = match label_id {
            Some(l) => l.to_string(),
            None => "None".to_string()
        };
        let condition_str = match condition {
            Some(_) => "Something".to_string(),
            None => "None".to_string()
        };
        let selector = match property_ids {
            Some(_) => "Something".to_string(),
            None => "None".to_string()
        };
        println!("[Rust Internal] <Scan Vertex> (label: {}, condition: {}, prop_selector: {})", label_str, condition_str, selector);
        Ok(Box::new(VertexIteratorImpl::new()))
    }

    fn scan_edge(&self, edge_relation: Option<&EdgeRelation>, condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>) -> Result<Records<Self::E>> {
        let edge_relation_str = match edge_relation {
            Some(er) => {
                format!("({}, {}, {})", er.get_edge_label_id(), er.get_src_vertex_label_id(), er.get_dst_vertex_label_id())
            }
            None => "None".to_string()
        };
        let condition_str = match condition {
            Some(_) => "Something".to_string(),
            None => "None".to_string()
        };
        let selector = match property_ids {
            Some(_) => "Something".to_string(),
            None => "None".to_string()
        };
        println!("[Rust Internal] <Scan Edge> (e_relation: {}, condition: {}, prop_selector: {})", edge_relation_str, condition_str, selector);
        Ok(Box::new(EdgeIteratorImpl::new()))
    }

    fn get_out_edges(&self, vertex_id: VertexId, label_id: Option<LabelId>, condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>) -> Result<Records<Self::E>> {
        let label_str = match label_id {
            Some(l) => l.to_string(),
            None => "None".to_string()
        };
        let condition_str = match condition {
            Some(_) => "Something".to_string(),
            None => "None".to_string()
        };
        let selector = match property_ids {
            Some(_) => "Something".to_string(),
            None => "None".to_string()
        };
        println!("[Rust Internal] <Get Out Edges> (vid: {}, label: {}, condition: {}, prop_selector: {})", vertex_id, label_str, condition_str, selector);
        Ok(Box::new(EdgeIteratorImpl::new()))
    }

    fn get_in_edges(&self, vertex_id: VertexId, label_id: Option<LabelId>, condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>) -> Result<Records<Self::E>> {
        let label_str = match label_id {
            Some(l) => l.to_string(),
            None => "None".to_string()
        };
        let condition_str = match condition {
            Some(_) => "Something".to_string(),
            None => "None".to_string()
        };
        let selector = match property_ids {
            Some(_) => "Something".to_string(),
            None => "None".to_string()
        };
        println!("[Rust Internal] <Get In Edges> (vid: {}, label: {}, condition: {}, prop_selector: {})", vertex_id, label_str, condition_str, selector);
        Ok(Box::new(EdgeIteratorImpl::new()))
    }

    fn get_out_degree(&self, vertex_id: VertexId, edge_relation: &EdgeRelation) -> Result<usize> {
        let edge_relation_str = format!("({}, {}, {})", edge_relation.get_edge_label_id(), edge_relation.get_src_vertex_label_id(), edge_relation.get_dst_vertex_label_id());
        println!("[Rust Internal] <Get Out Degree> (vid: {}, e_relation: {})", vertex_id, edge_relation_str);
        Ok(10)
    }

    fn get_in_degree(&self, vertex_id: VertexId, edge_relation: &EdgeRelation) -> Result<usize> {
        let edge_relation_str = format!("({}, {}, {})", edge_relation.get_edge_label_id(), edge_relation.get_src_vertex_label_id(), edge_relation.get_dst_vertex_label_id());
        println!("[Rust Internal] <Get In Degree> (vid: {}, e_relation: {})", vertex_id, edge_relation_str);
        Ok(5)
    }

    fn get_kth_out_edge(&self, vertex_id: VertexId, edge_relation: &EdgeRelation, k: SerialId, property_ids: Option<&Vec<PropertyId>>) -> Result<Option<Self::E>> {
        let edge_relation_str = format!("({}, {}, {})", edge_relation.get_edge_label_id(), edge_relation.get_src_vertex_label_id(), edge_relation.get_dst_vertex_label_id());
        let selector = match property_ids {
            Some(_) => "Something".to_string(),
            None => "None".to_string()
        };
        println!("[Rust Internal] <Get Kth Out Edge> (vid: {}, e_relation: {}, k: {}, prop_selector: {})", vertex_id, edge_relation_str, k, selector);
        Ok(Some(EdgeImpl::new()))
    }

    fn get_kth_in_edge(&self, vertex_id: VertexId, edge_relation: &EdgeRelation, k: SerialId, property_ids: Option<&Vec<PropertyId>>) -> Result<Option<Self::E>> {
        let edge_relation_str = format!("({}, {}, {})", edge_relation.get_edge_label_id(), edge_relation.get_src_vertex_label_id(), edge_relation.get_dst_vertex_label_id());
        let selector = match property_ids {
            Some(_) => "Something".to_string(),
            None => "None".to_string()
        };
        println!("[Rust Internal] <Get Kth In Edge> (vid: {}, e_relation: {}, k: {}, prop_selector: {})", vertex_id, edge_relation_str, k, selector);
        Ok(Some(EdgeImpl::new()))
    }

    fn get_snapshot_id(&self) -> SnapshotId {
        println!("[Rust Internal] <Get SnapshotId Degree>");
        20210830
    }
}
