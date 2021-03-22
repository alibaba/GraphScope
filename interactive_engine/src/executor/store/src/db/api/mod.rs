#![allow(dead_code)]
#[macro_use]
pub mod error;
pub mod property;
mod entity;
mod graph;
mod condition;
mod schema;
mod config;

pub use self::error::*;
pub use self::entity::*;
pub use self::condition::*;
pub use self::config::*;
pub use self::graph::*;
pub use self::property::*;
pub use self::schema::*;
use crate::db::proto::common::{EdgeKindPb, EdgeIdPb};

pub type SnapshotId = i64;
pub type VertexId = i64;
pub type LabelId = i32;
pub type PropId = i32;
pub type GraphResult<T> = Result<T, GraphError>;

pub const MAX_SI: SnapshotId = SnapshotId::max_value() - 1;
pub const INFINITE_SI: SnapshotId = SnapshotId::max_value();

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EdgeId {
    pub src_id: VertexId,
    pub dst_id: VertexId,
    pub inner_id: i64,
}

impl EdgeId {
    pub fn new(src_id: VertexId, dst_id: VertexId, inner_id: i64) -> Self {
        EdgeId {
            src_id,
            dst_id,
            inner_id,
        }
    }

    pub fn from_proto(proto: &EdgeIdPb) -> Self {
        Self::new(proto.get_srcId().get_id(), proto.get_dstId().get_id(), proto.get_id())
    }
}

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