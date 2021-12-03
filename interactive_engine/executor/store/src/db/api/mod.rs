#![allow(dead_code)]

use crate::db::proto::common::DataLoadTargetPb;
use crate::db::proto::model::{EdgeIdPb, EdgeKindPb};

pub use self::config::*;
pub use self::error::*;
pub use self::property::*;
pub use self::schema::*;

#[macro_use]
pub mod error;
pub mod property;
mod schema;
mod config;
pub mod multi_version_graph;
pub mod partition_snapshot;
pub mod types;
pub mod condition;
pub mod partition_graph;

pub type SnapshotId = i64;
pub type VertexId = i64;
pub type LabelId = i32;
pub type PropertyId = i32;
pub type BackupId = i32;
pub type EdgeInnerId = i64;
pub type SerialId = u32;
pub type GraphResult<T> = Result<T, GraphError>;
pub type Records<T> = Box<dyn Iterator<Item=GraphResult<T>> + Send>;

pub const MAX_SI: SnapshotId = SnapshotId::max_value() - 1;
pub const INFINITE_SI: SnapshotId = SnapshotId::max_value();

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EdgeId {
    pub src_id: VertexId,
    pub dst_id: VertexId,
    pub inner_id: EdgeInnerId,
}

impl EdgeId {
    pub fn new(src_id: VertexId, dst_id: VertexId, inner_id: EdgeInnerId) -> Self {
        EdgeId {
            src_id,
            dst_id,
            inner_id,
        }
    }

    pub fn from_proto(proto: &EdgeIdPb) -> Self {
        Self::new(proto.get_srcId().get_id(), proto.get_dstId().get_id(), proto.get_id())
    }

    pub fn get_src_vertex_id(&self) -> VertexId {
        self.src_id
    }

    pub fn get_dst_vertex_id(&self) -> VertexId {
        self.dst_id
    }

    pub fn get_edge_inner_id(&self) -> EdgeInnerId {
        self.inner_id
    }
}

#[repr(C)]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct EdgeKind {
    pub edge_label_id: LabelId,
    pub src_vertex_label_id: LabelId,
    pub dst_vertex_label_id: LabelId,
}

impl EdgeKind {
    pub fn new(edge_label_id: LabelId, src_vertex_label_id: LabelId, dst_vertex_label_id: LabelId) -> Self {
        EdgeKind {
            edge_label_id,
            src_vertex_label_id,
            dst_vertex_label_id,
        }
    }

    pub fn get_edge_label_id(&self) -> LabelId {
        self.edge_label_id
    }
    pub fn get_src_vertex_label_id(&self) -> LabelId {
        self.src_vertex_label_id
    }
    pub fn get_dst_vertex_label_id(&self) -> LabelId {
        self.dst_vertex_label_id
    }

    pub fn from_proto(proto: &EdgeKindPb) -> Self {
        Self::new(proto.get_edgeLabelId().get_id(), proto.get_srcVertexLabelId().get_id(), proto.get_dstVertexLabelId().get_id())
    }

    pub fn to_proto(&self) -> EdgeKindPb {
        let mut pb = EdgeKindPb::new();
        pb.mut_edgeLabelId().set_id(self.edge_label_id);
        pb.mut_srcVertexLabelId().set_id(self.src_vertex_label_id);
        pb.mut_dstVertexLabelId().set_id(self.dst_vertex_label_id);
        pb
    }
}

impl From<&EdgeKind> for EdgeKind {
    fn from(relation: &EdgeKind) -> Self {
        EdgeKind::new(relation.get_edge_label_id() as LabelId, relation.get_src_vertex_label_id() as LabelId, relation.get_dst_vertex_label_id() as LabelId)
    }
}

impl std::fmt::Debug for EdgeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "[{}-{}->{}]", self.src_vertex_label_id, self.edge_label_id, self.dst_vertex_label_id)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum EdgeDirection {
    In,
    Out,
    Both,
}

#[derive(Clone, Default, Debug, PartialEq)]
pub struct DataLoadTarget {
    pub label_id: i32,
    pub src_label_id: i32,
    pub dst_label_id: i32,
}

impl DataLoadTarget {
    pub fn new(label_id: i32, src_label_id: i32, dst_label_id: i32) -> Self {
        DataLoadTarget {
            label_id,
            src_label_id,
            dst_label_id,
        }
    }

    pub fn from_proto(proto: &DataLoadTargetPb) -> Self {
        Self::new(proto.get_labelId(),
                  proto.get_srcLabelId(),
                  proto.get_dstLabelId(), )
    }

    pub fn to_proto(&self) -> DataLoadTargetPb {
        let mut pb = DataLoadTargetPb::new();
        pb.set_labelId(self.label_id);
        pb.set_srcLabelId(self.src_label_id);
        pb.set_dstLabelId(self.dst_label_id);
        pb
    }
}
