use std::sync::Arc;
use std::marker::PhantomData;
use super::schema::*;
use super::{VertexId, SnapshotId, LabelId, GraphResult, EdgeId,
            Vertex, Edge, EdgeKind, PropId, PropertyMap, Condition,
            PropertiesRef, ValueRef};
use crate::db::api::DataLoadTarget;

pub trait GraphStorage {
    type V: Vertex;
    type E: Edge;

    /// Return the vertex with the `id` at `si`. This interface is thread safe.
    ///
    /// if `label` is None, all vertex types will be searched and the first match vertex will be returned.
    ///
    /// If vertex type of `label` is not found, something error when query storage, something error
    /// in meta or other errors, `GraphError` will be returned
    fn get_vertex(&self, si: SnapshotId, id: VertexId, label: Option<LabelId>) -> GraphResult<Option<VertexWrapper<Self::V>>>;

    /// Return the edge with the `id` at `si`. This interface is thread safe.
    ///
    /// If `kind` is None, all edge kinds will be searched and the first match edge will be returned
    ///
    /// If edge kind of `kind` is not found, something error when query storage, something error in
    /// meta or other errors, `GraphError` will be returned
    fn get_edge(&self, si: SnapshotId, id: EdgeId, kind: Option<&EdgeKind>) -> GraphResult<Option<EdgeWrapper<Self::E>>>;

    /// Return all vertices of `label` matching `condition` at `si`. This interface is thread safe.
    ///
    /// If `condition` is None, all vertices of `label` will be scanned without filter.
    /// If `label` is None, all vertex types will be scanned.
    ///
    /// If vertex type of `label` is not found, something error when query storage, error in meta,
    /// decoding properties error or other errors, `GraphError` will be returned.
    fn query_vertices<'a>(&'a self, si: SnapshotId, label: Option<LabelId>, condition: Option<Arc<Condition>>) -> GraphResult<Box<dyn VertexResultIter<V=Self::V> + 'a>>;

    /// Return all out edges of vertex with id=`src_id` matching `condition` at `si`. This interface is thread safe.
    ///
    /// if `label` is None, all out edges will be returned, otherwise only edges with the `label` will be returned.
    /// if `condition` is None, all edges will be scanned without filter.
    ///
    /// If edge type of `label` is not found, something error when query storage, error in meta,
    /// decoding properties error or other errors, `GraphError` will be returned.
    fn get_out_edges<'a>(&'a self, si: SnapshotId, src_id: VertexId, label: Option<LabelId>, condition: Option<Arc<Condition>>) -> GraphResult<Box<dyn EdgeResultIter<E=Self::E> + 'a>>;

    /// Return all in edges of vertex with id=`dst_id` matching `condition` at `si`. This interface is thread safe.
    ///
    /// if `label` is None, all out edges will be returned, otherwise only edges with the `label` will be returned.
    /// if `condition` is None, all edges will be scanned without filter.
    ///
    /// If edge type of `label` is not found, something error when query storage, error in meta,
    /// decoding properties error or other errors, `GraphError` will be returned.
    fn get_in_edges<'a>(&'a self, si: SnapshotId, dst_id: VertexId, label: Option<LabelId>, condition: Option<Arc<Condition>>) -> GraphResult<Box<dyn EdgeResultIter<E=Self::E> + 'a>>;

    /// Return all edges of `label` matching `condition` at `si`. This interface is thread safe.
    ///
    /// If `condition` is None, all edges of `label` will be scanned without filter.
    /// If `label` is None, all edge types will be scanned.
    ///
    /// If edge type of `label` is not found, something error when query storage, error in meta,
    /// decoding properties error or other errors, `GraphError` will be returned.
    fn query_edges<'a>(&'a self, si: SnapshotId, label: Option<LabelId>, condition: Option<Arc<Condition>>) -> GraphResult<Box<dyn EdgeResultIter<E=Self::E> + 'a>>;

    /// Create a new vertex type with `label` and `type_def` at `si` and `schema_version`. This interface is thread safe.
    ///
    /// If vertex type already exists, `si` is smaller than last operation, get lock error, storage error
    /// or other errors, `GraphError` will be returned.
    /// Returns true if schema_version changed, false otherwise.
    fn create_vertex_type(&self, si: SnapshotId, schema_version: i64, label: LabelId, type_def: &TypeDef, table_id: i64) -> GraphResult<bool>;

    /// Create a new edge type with `label` and `type_def` at `si` and `schema_version`. This interface is thread safe.
    ///
    /// If edge type already exists, `si` is smaller than last operation, get lock error, storage error
    /// or other errors, `GraphError` will be returned.
    /// Returns true if schema_version changed, false otherwise.
    fn create_edge_type(&self, si: SnapshotId, schema_version: i64, label: LabelId, type_def: &TypeDef) -> GraphResult<bool>;

    /// Add a new edge kind of `kind` to edge type with `kind.label` at `si` and `schema_version`. This interface is thread safe.
    ///
    /// If edge type with `kind.label` not exists, edge kind `kind` already exists, `si` is smaller
    /// than last operation, get lock error, storage error or other errors, `GraphError` will be returned.
    /// Returns true if schema_version changed, false otherwise.
    fn add_edge_kind(&self, si: SnapshotId, schema_version: i64, kind: &EdgeKind, table_id: i64) -> GraphResult<bool>;

    /// Drop a vertex type of `label` at `si` and `schema_version`.  This interface is thread safe.
    ///
    /// If storage error, `si` is smaller than last operation, get lock error or other errors,
    /// `GraphError` will be returned.
    ///
    /// Returns true if schema_version changed, false otherwise.
    fn drop_vertex_type(&self, si: SnapshotId, schema_version: i64, label_id: LabelId) -> GraphResult<bool>;

    /// Drop an edge type of `label` as well as all edge kinds of `label` at `si` and `schema_version`.  This interface is thread safe.
    ///
    /// If storage error, `si` is smaller than last operation, get lock error or other errors,
    /// `GraphError` will be returned.
    ///
    /// Returns true if schema_version changed, false otherwise.
    fn drop_edge_type(&self, si: SnapshotId, schema_version: i64, label_id: LabelId) -> GraphResult<bool>;

    /// Remove an edge type of `edge_kind` at `si` and `schema_version`. This interface is thread safe.
    ///
    /// If storage error, `si` is smaller than last operation, get lock error or other errors,
    /// `GraphError` will be returned.
    ///
    /// Returns true if schema_version changed, false otherwise.
    fn remove_edge_kind(&self, si: SnapshotId, schema_version: i64, edge_kind: &EdgeKind) -> GraphResult<bool>;

    /// realtime write interfaces
    /// These realtime write interfaces should be thread safe and user should ensure all data are in
    /// ascending order by si, or error will be return. The distribute protocol ensure si of all data
    /// sending to store will be monotonically increasing, so it's easy to meet the constrain and if not,
    /// there must be some problems in protocol or local realtime write logic.

    /// Insert a vertex with `id`, `label` and `properties` at `si`. It'll overwrite the vertex with
    /// `id` no matter whether it exits. This interface is thread safe.
    ///
    /// If vertex type of `label` not found, storage error, encoding error, meta error or other errors,
    /// `GraphError` will be returned.
    fn insert_overwrite_vertex(&self, si: SnapshotId, id: VertexId, label: LabelId, properties: &dyn PropertyMap) -> GraphResult<()>;

    /// Insert or update a vertex with `id` and `label` at `si`. If the vertex already exists, merge its
    /// properties with the provided `properties` and create a new version of this vertex. Otherwise
    /// insert a new vertex with `id`, `label` and `properties`. This interface is thread safe.
    ///
    /// If vertex type of `label` not found, storage error, encoding error, meta error or other errors,
    /// `GraphError` will be returned.
    fn insert_update_vertex(&self, si: SnapshotId, id: VertexId, label: LabelId, properties: &dyn PropertyMap) -> GraphResult<()>;

    /// Delete a vertex with `id` and `label` at `si`. The existence will not be checked. This interface is thread safe.
    ///
    /// If vertex type of `label` not found, storage error or other errors, `GraphError` will be returned.
    fn delete_vertex(&self, si: SnapshotId, id: VertexId, label: LabelId) -> GraphResult<()>;

    /// Insert an edge with `id`, `edge_kind`, `forward` and `properties` at `si`. It'll overwrite the edge with
    /// `id` no matter whether it exits. This interface is thread safe.
    ///
    /// If edge kind of `edge_kind` not found, storage error, encoding error, meta error or other errors,
    /// `GraphError` will be returned.
    fn insert_overwrite_edge(&self, si: SnapshotId, id: EdgeId, edge_kind: &EdgeKind, forward: bool, properties: &dyn PropertyMap) -> GraphResult<()>;

    /// Insert or update an edge with `id` and `edge_kind` at `si`. If the edge already exists, merge its
    /// properties with the provided `properties` and create a new version of this edge. Otherwise
    /// insert a new edge with `id`, `edge_kind` and `properties`. This interface is thread safe.
    ///
    /// If edge kind of `edge_kind` not found, storage error, encoding error, meta error or other errors,
    /// `GraphError` will be returned.
    fn insert_update_edge(&self, si: SnapshotId, id: EdgeId, edge_kind: &EdgeKind, forward: bool, properties: &dyn PropertyMap) -> GraphResult<()>;

    /// Delete an edge with `id` and `edge_kind` at `si`. The existence will not be checked. This interface is thread safe.
    ///
    /// If edge kind of `edge_kind` not found, storage error or other errors, `GraphError` will be returned.
    fn delete_edge(&self, si: SnapshotId, id: EdgeId, edge_kind: &EdgeKind, forward: bool) -> GraphResult<()>;

    /// garbage collection at `si`
    fn gc(&self, si: SnapshotId);

    /// Returns current GraphDefPb bytes
    fn get_graph_def_blob(&self) -> GraphResult<Vec<u8>>;

    /// prepare data load
    fn prepare_data_load(&self, si: SnapshotId, schema_version: i64, target: &DataLoadTarget, table_id: i64) -> GraphResult<bool>;

    fn commit_data_load(&self, si: SnapshotId, schema_version: i64, target: &DataLoadTarget, table_id: i64) -> GraphResult<bool>;
}

pub trait VertexResultIter {
    type V: Vertex;
    fn next(&mut self) -> Option<VertexWrapper<Self::V>>;
    fn ok(&self) -> GraphResult<()>;
}

pub trait EdgeResultIter {
    type E: Edge;
    fn next(&mut self) -> Option<EdgeWrapper<Self::E>>;
    fn ok(&self) -> GraphResult<()>;
}

pub struct VertexWrapper<'a, V> {
    v: V,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, V> VertexWrapper<'a, V> {
    pub fn new(v: V) -> Self {
        VertexWrapper {
            v,
            _phantom: Default::default(),
        }
    }
}

impl<'a, V: Vertex> Vertex for VertexWrapper<'a, V> {
    type PI = V::PI;

    fn get_id(&self) -> VertexId {
        self.v.get_id()
    }

    fn get_label(&self) -> LabelId {
        self.v.get_label()
    }

    fn get_property(&self, prop_id: PropId) -> Option<ValueRef> {
        self.v.get_property(prop_id)
    }

    fn get_properties_iter(&self) -> PropertiesRef<Self::PI> {
        self.v.get_properties_iter()
    }
}

impl<'a, V: Vertex> std::fmt::Debug for VertexWrapper<'a, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.v)
    }
}

pub struct EdgeWrapper<'a, E> {
    e: E,
    _phantom: PhantomData<&'a ()>,
}

impl<'a, E> EdgeWrapper<'a, E> {
    pub fn new(e: E) -> Self {
        EdgeWrapper {
            e,
            _phantom: Default::default(),
        }
    }
}

impl<'a, E: Edge> Edge for EdgeWrapper<'a, E> {
    type PI = E::PI;

    fn get_id(&self) -> &EdgeId {
        self.e.get_id()
    }

    fn get_src_id(&self) -> i64 {
        self.e.get_src_id()
    }

    fn get_dst_id(&self) -> i64 {
        self.e.get_dst_id()
    }

    fn get_kind(&self) -> &EdgeKind {
        self.e.get_kind()
    }

    fn get_property(&self, prop_id: i32) -> Option<ValueRef> {
        self.e.get_property(prop_id)
    }

    fn get_properties_iter(&self) -> PropertiesRef<Self::PI> {
        self.e.get_properties_iter()
    }
}

impl<'a, E: Edge> std::fmt::Debug for EdgeWrapper<'a, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.e)
    }
}
