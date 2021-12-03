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

use super::LabelId;
use maxgraph_common::proto::schema::*;

#[derive(Debug, Eq, PartialEq, Hash, Default, Clone)]
pub struct Relation {
    pub label: LabelId,
    pub src_label: LabelId,
    pub dst_label: LabelId,
}

impl Relation {
    pub fn new(label: LabelId, src_label: LabelId, dst_label: LabelId) -> Self {
        Relation {
            label,
            src_label,
            dst_label,
        }
    }

    pub fn as_ptr(&self) -> *const Self {
        self as *const Relation
    }

    pub fn to_proto(&self) -> RelationShipProto {
        let mut proto = RelationShipProto::new();
        let mut id = TypeIdProto::new();
        id.set_id(self.label);
        proto.set_edgeId(id);
        let mut id = TypeIdProto::new();
        id.set_id(self.src_label);
        proto.set_srcId(id);
        let mut id = TypeIdProto::new();
        id.set_id(self.dst_label);
        proto.set_dstId(id);
        proto
    }
}

impl<'a> From<&'a RelationShipProto> for Relation {
    fn from(proto: &'a RelationShipProto) -> Self {
        let label = proto.get_edgeId().id;
        let src_label = proto.get_srcId().id;
        let dst_label = proto.get_dstId().id;
        Relation::new(label, src_label, dst_label)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relation_from_proto() {
        let mut proto = RelationShipProto::new();
        let mut edge_id_proto = TypeIdProto::new();
        edge_id_proto.set_id(1);
        let mut src_id_proto = TypeIdProto::new();
        src_id_proto.set_id(2);
        let mut dst_id_proto = TypeIdProto::new();
        dst_id_proto.set_id(3);
        proto.set_edgeId(edge_id_proto);
        proto.set_srcId(src_id_proto);
        proto.set_dstId(dst_id_proto);
        let relation = Relation::from(&proto);
        assert_eq!(relation, Relation::new(1, 2, 3));
    }

    #[test]
    fn test_relation_to_proto() {
        let relation = Relation::new(1, 2, 3);
        let proto = relation.to_proto();
        let relation1 = Relation::from(&proto);
        assert_eq!(relation, relation1);
    }
}
