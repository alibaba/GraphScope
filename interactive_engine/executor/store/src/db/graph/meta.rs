use std::sync::{Arc};

use crate::db::api::*;
use crate::db::storage::ExternalStorage;
use crate::db::common::bytes::transform;
use crate::db::common::str::parse_str;

use super::table_manager::*;
use super::types::*;
use protobuf::Message;
use crate::db::api::GraphErrorCode::InvalidData;
use crate::db::util::lock::GraphMutexLock;
use crate::db::common::bytes::util::parse_pb;
use crate::db::proto::model::EdgeKindPb;
use std::collections::{HashMap, HashSet};
use crate::db::proto::common::DataLoadTargetPb;

const META_TABLE_ID: TableId = i64::min_value();

pub struct Meta {
    store: Arc<dyn ExternalStorage>,
    graph_def_lock: GraphMutexLock<GraphDef>,
}

impl Meta {
    pub fn new(store: Arc<dyn ExternalStorage>) -> Self {
        Meta {
            store,
            graph_def_lock: GraphMutexLock::new(GraphDef::default()),
        }
    }

    pub fn get_graph_def(&self) -> &GraphMutexLock<GraphDef> {
        &self.graph_def_lock
    }

    pub fn recover(&self) -> GraphResult<(VertexTypeManager, EdgeTypeManager)> {
        {
            let mut graph_def_val = self.graph_def_lock.lock()?;
            *graph_def_val = GraphDef::new(
                0,
                HashMap::new(),
                HashSet::new(),
                HashMap::new(),
                0,
                0,
                HashMap::new(),
                HashMap::new(),
                i64::min_value() / 2 + 1,
            );
        }
        let mut all: Vec<MetaItem> = Vec::new();
        let store_ref = self.store.as_ref();
        let create_vertex_items = res_unwrap!(get_items::<CreateVertexTypeItem>(store_ref), recover)?;
        all.extend(create_vertex_items.into_iter().map(|i| MetaItem::CreateVertexType(i)));
        let create_edge_items = res_unwrap!(get_items::<CreateEdgeTypeItem>(store_ref), recover)?;
        all.extend(create_edge_items.into_iter().map(|i| MetaItem::CreateEdgeType(i)));
        let add_edge_kind_items = res_unwrap!(get_items::<AddEdgeKindItem>(store_ref), recover)?;
        all.extend(add_edge_kind_items.into_iter().map(|i| MetaItem::AddEdgeKind(i)));
        let drop_vertex_items = res_unwrap!(get_items::<DropVertexTypeItem>(store_ref), recover)?;
        all.extend(drop_vertex_items.into_iter().map(|i| MetaItem::DropVertexType(i)));
        let drop_edge_items = res_unwrap!(get_items::<DropEdgeTypeItem>(store_ref), recover)?;
        all.extend(drop_edge_items.into_iter().map(|i| MetaItem::DropEdgeType(i)));
        let remove_edge_kind_items = res_unwrap!(get_items::<RemoveEdgeKindItem>(store_ref), recover)?;
        all.extend(remove_edge_kind_items.into_iter().map(|i| MetaItem::RemoveEdgeKind(i)));
        let prepare_data_load_items = res_unwrap!(get_items::<PrepareDataLoadItem>(store_ref), recover)?;
        all.extend(prepare_data_load_items.into_iter().map(|i| MetaItem::PrepareDataLoad(i)));
        let commit_data_load_items = res_unwrap!(get_items::<CommitDataLoadItem>(store_ref), recover)?;
        all.extend(commit_data_load_items.into_iter().map(|i| MetaItem::CommitDataLoad(i)));
        all.sort_by(|a, b| {
            let s1 = a.get_schema_version();
            let s2 = b.get_schema_version();
            return s1.cmp(&s2);
        });

        let mut vertex_manager_builder = VertexTypeManagerBuilder::new();
        let mut edge_manager_builder = EdgeManagerBuilder::new();
        for item in all {
            match item {
                MetaItem::CreateVertexType(x) => {
                    let label_id = x.type_def.get_label_id();
                    let mut graph_def = self.graph_def_lock.lock()?;
                    let current_label_idx = graph_def.get_label_idx();
                    if current_label_idx >= label_id {
                        let msg = format!("current label idx {}, create label id {}", current_label_idx, label_id);
                        return Err(GraphError::new(GraphErrorCode::InvalidOperation, msg));
                    }
                    graph_def.add_type(label_id, x.type_def.clone())?;
                    graph_def.put_vertex_table_id(label_id, x.table_id);
                    graph_def.set_label_idx(label_id);
                    graph_def.set_table_idx(x.table_id);
                    graph_def.increase_version();
                    vertex_manager_builder.create(x.si, x.label_id, &x.type_def)?;
                    vertex_manager_builder.get_info(x.si, x.label_id).and_then(|info| {
                        info.online_table(Table::new(x.si, x.table_id))
                    })?;
                }
                MetaItem::DropVertexType(x) => {
                    vertex_manager_builder.drop(x.si, x.label_id)?;
                    let mut graph_def = self.graph_def_lock.lock()?;
                    graph_def.remove_type(&x.label_id);
                    graph_def.increase_version();
                }
                MetaItem::CreateEdgeType(x) => {
                    let label_id = x.type_def.get_label_id();
                    let mut graph_def = self.graph_def_lock.lock()?;
                    let current_label_idx = graph_def.get_label_idx();
                    if current_label_idx >= label_id {
                        let msg = format!("current label idx {}, create label id {}", current_label_idx, label_id);
                        return Err(GraphError::new(GraphErrorCode::InvalidOperation, msg));
                    }
                    graph_def.add_type(label_id, x.type_def.clone())?;
                    graph_def.set_label_idx(label_id);
                    graph_def.increase_version();
                    edge_manager_builder.create_edge_type(x.si, x.label_id, &x.type_def)?;
                }
                MetaItem::AddEdgeKind(x) => {
                    edge_manager_builder.add_edge_kind(x.si, &x.edge_kind)?;
                    edge_manager_builder.add_edge_table(x.si, &x.edge_kind, Table::new(x.si, x.table_id))?;
                    let mut graph_def = self.graph_def_lock.lock()?;
                    graph_def.add_edge_kind(x.edge_kind.clone());
                    graph_def.put_edge_table_id(x.edge_kind.clone(), x.table_id);
                    graph_def.set_table_idx(x.table_id);
                    graph_def.increase_version();
                }
                MetaItem::DropEdgeType(x) => {
                    edge_manager_builder.drop_edge_type(x.si, x.label_id)?;
                    let mut graph_def = self.graph_def_lock.lock()?;
                    graph_def.remove_type(&x.label_id);
                    graph_def.increase_version();
                }
                MetaItem::RemoveEdgeKind(x) => {
                    edge_manager_builder.remove_edge_kind(x.si, &x.edge_kind)?;
                    let mut graph_def = self.graph_def_lock.lock()?;
                    graph_def.remove_edge_kind(&x.edge_kind);
                    graph_def.increase_version();
                }
                MetaItem::PrepareDataLoad(x) => {
                    let mut graph_def = self.graph_def_lock.lock()?;
                    if x.target.src_label_id > 0 {
                        let edge_kind = EdgeKind::new(x.target.label_id, x.target.src_label_id, x.target.dst_label_id);
                        graph_def.put_edge_table_id(edge_kind, x.table_id);
                    } else {
                        graph_def.put_vertex_table_id(x.target.label_id, x.table_id);
                    }
                    graph_def.set_table_idx(x.table_id);
                    graph_def.increase_version();
                }
                MetaItem::CommitDataLoad(x) => {
                    let mut graph_def = self.graph_def_lock.lock()?;
                    graph_def.increase_version();
                    if x.target.src_label_id > 0 {
                        let edge_kind = EdgeKind::new(x.target.label_id, x.target.src_label_id, x.target.dst_label_id);
                        edge_manager_builder.add_edge_table(x.si, &edge_kind, Table::new(x.si, x.table_id))?;
                    } else {
                        vertex_manager_builder.get_info(x.si, x.target.label_id).and_then(|info| {
                            info.online_table(Table::new(x.si, x.table_id))
                        })?;
                    }
                }
            }
        }
        Ok((vertex_manager_builder.build(), edge_manager_builder.build()))
    }

    pub fn check_version(&self, schema_version: i64) -> GraphResult<()> {
        let graph_def = self.graph_def_lock.lock()?;
        let current_version = graph_def.get_version();
        if current_version >= schema_version {
            let msg = format!("current schema version {} >= {}", current_version, schema_version);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, create_vertex_type);
            return Err(err);
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn gc(&self, _si: SnapshotId) -> GraphResult<()> {
        unimplemented!()
    }

    pub fn prepare_data_load(&self, si: SnapshotId, schema_version: i64, target: &DataLoadTarget,
                             table_id: i64) -> GraphResult<()> {
        self.check_version(schema_version)?;
        let item = PrepareDataLoadItem::new(si, schema_version, target.clone(), table_id);
        self.write_item(item)?;
        {
            let mut graph_def = self.graph_def_lock.lock()?;
            if target.src_label_id > 0 {
                // EdgeKind
                let edge_kind = EdgeKind::new(target.label_id, target.src_label_id, target.dst_label_id);
                graph_def.put_edge_table_id(edge_kind, table_id);
            } else {
                // Vertex
                graph_def.put_vertex_table_id(target.label_id, table_id);
            }
            graph_def.set_table_idx(table_id);
            graph_def.increase_version();
        }
        Ok(())
    }

    pub fn commit_data_load(&self, si: SnapshotId, schema_version: i64, target: &DataLoadTarget,
                            table_id: i64) -> GraphResult<()> {
        self.check_version(schema_version)?;
        let item = CommitDataLoadItem::new(si, schema_version, target.clone(), table_id);
        self.write_item(item)?;
        {
            let mut graph_def = self.graph_def_lock.lock()?;
            graph_def.increase_version();
        }
        Ok(())
    }

    pub fn create_vertex_type(&self, si: SnapshotId, schema_version: i64, label_id: LabelId, type_def: &TypeDef, table_id: i64) -> GraphResult<Table> {
        self.check_version(schema_version)?;
        let item = CreateVertexTypeItem::new(si, schema_version, label_id, table_id, type_def.clone());
        self.write_item(item)?;
        {
            let mut graph_def = self.graph_def_lock.lock()?;
            let current_label_idx = graph_def.get_label_idx();
            if current_label_idx >= label_id {
                let msg = format!("current label idx {}, create label id {}", current_label_idx, label_id);
                return Err(GraphError::new(GraphErrorCode::InvalidOperation, msg));
            }
            graph_def.add_type(label_id, type_def.clone())?;
            graph_def.put_vertex_table_id(label_id, table_id);
            graph_def.set_label_idx(label_id);
            graph_def.set_table_idx(table_id);
            graph_def.increase_version();
        }
        Ok(Table::new(si, table_id))
    }

    pub fn drop_vertex_type(&self, si: SnapshotId, schema_version: i64, label_id: LabelId) -> GraphResult<()> {
        self.check_version(schema_version)?;
        let item = DropVertexTypeItem::new(si, schema_version, label_id);
        self.write_item(item)?;
        {
            let mut graph_def = self.graph_def_lock.lock()?;
            graph_def.remove_type(&label_id);
            graph_def.increase_version();
        }
        Ok(())
    }

    pub fn create_edge_type(&self, si: SnapshotId, schema_version: i64, label_id: LabelId, type_def: &TypeDef) -> GraphResult<()> {
        self.check_version(schema_version)?;
        let item = CreateEdgeTypeItem::new(si, schema_version, label_id, type_def.clone());
        self.write_item(item)?;
        {
            let mut graph_def = self.graph_def_lock.lock()?;
            let current_label_idx = graph_def.get_label_idx();
            if current_label_idx >= label_id {
                let msg = format!("current label idx {}, create label id {}", current_label_idx, label_id);
                return Err(GraphError::new(GraphErrorCode::InvalidOperation, msg));
            }
            graph_def.add_type(type_def.get_label_id(), type_def.clone())?;
            graph_def.set_label_idx(label_id);
            graph_def.increase_version();
        }
        Ok(())
    }

    pub fn add_edge_kind(&self, si: SnapshotId, schema_version: i64, edge_kind: &EdgeKind, table_id: i64) -> GraphResult<Table> {
        self.check_version(schema_version)?;
        let item = AddEdgeKindItem::new(si, schema_version, table_id, edge_kind.clone());
        self.write_item(item)?;
        {
            let mut graph_def = self.graph_def_lock.lock()?;
            graph_def.add_edge_kind(edge_kind.clone());
            graph_def.put_edge_table_id(edge_kind.clone(), table_id);
            graph_def.set_table_idx(table_id);
            graph_def.increase_version();
        }
        Ok(Table::new(si, table_id))
    }

    pub fn drop_edge_type(&self, si: SnapshotId, schema_version: i64, label_id: LabelId) -> GraphResult<()> {
        self.check_version(schema_version)?;
        let item = DropEdgeTypeItem::new(si, schema_version, label_id);
        self.write_item(item)?;
        {
            let mut graph_def = self.graph_def_lock.lock()?;
            graph_def.remove_type(&label_id);
            graph_def.increase_version();
        }
        Ok(())
    }

    pub fn remove_edge_kind(&self, si: SnapshotId, schema_version: i64, edge_kind: &EdgeKind) -> GraphResult<()> {
        self.check_version(schema_version)?;
        let item = RemoveEdgeKindItem::new(si, schema_version, edge_kind.clone());
        self.write_item(item)?;
        {
            let mut graph_def = self.graph_def_lock.lock()?;
            graph_def.remove_edge_kind(edge_kind);
            graph_def.increase_version();
        }
        Ok(())
    }

    pub fn _gen_next_table_id(&self) -> GraphResult<TableId> {
        let key = _gen_key("NextTableId");
        let table_id = match res_unwrap!(self.store.get(&key), get_next_table_id)? {
            Some(v) => {
                let res = transform::bytes_to_i64(v.as_bytes()).and_then(|id| {
                    let table_id = id.to_be() + 1;
                    if table_id >= 0 {
                        let msg = format!("cannot assign any more table id");
                        let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, get_next_table_id);
                        return Err(err);
                    }
                    Ok(table_id)
                });
                res_unwrap!(res, get_next_table_id)?
            }
            None => {
                // initial table id. We using incremental table id. As vertex and edge tables's prefix
                // is calculated by table id: 2 * X and 2 * X + 1, we use i64::min_value() + 1 to make
                // sure all the prefixes will be in i64's range
                i64::min_value() / 2 + 1
            }
        };
        let v = transform::i64_to_vec(table_id.to_be());
        res_unwrap!(self.store.put(&key, &v), get_next_table_id)?;
        Ok(table_id)
    }

    fn write_item<I: ItemCommon>(&self, item: I) -> GraphResult<()> {
        let (k, v) = item.to_kv()?;
        res_unwrap!(self.store.put(&k, &v), write_item)
    }
}

fn _gen_key(key: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend(transform::i64_to_vec(META_TABLE_ID.to_be()));
    buf.extend(key.as_bytes());
    buf
}

trait ItemCommon: Sized {
    fn from_kv(k: &[u8], v: &[u8]) -> GraphResult<Self>;
    fn prefix() -> &'static str;
    fn to_kv(&self) -> GraphResult<(Vec<u8>, Vec<u8>)>;
}


#[derive(PartialEq, Debug)]
enum MetaItem {
    CreateVertexType(CreateVertexTypeItem),
    CreateEdgeType(CreateEdgeTypeItem),
    AddEdgeKind(AddEdgeKindItem),
    DropVertexType(DropVertexTypeItem),
    DropEdgeType(DropEdgeTypeItem),
    RemoveEdgeKind(RemoveEdgeKindItem),
    PrepareDataLoad(PrepareDataLoadItem),
    CommitDataLoad(CommitDataLoadItem),
}

impl MetaItem {
    fn get_schema_version(&self) -> i64 {
        match *self {
            MetaItem::CreateVertexType(ref item) => item.schema_version,
            MetaItem::CreateEdgeType(ref item) => item.schema_version,
            MetaItem::AddEdgeKind(ref item) => item.schema_version,
            MetaItem::DropVertexType(ref item) => item.schema_version,
            MetaItem::DropEdgeType(ref item) => item.schema_version,
            MetaItem::RemoveEdgeKind(ref item) => item.schema_version,
            MetaItem::PrepareDataLoad(ref item) => item.schema_version,
            MetaItem::CommitDataLoad(ref item) => item.schema_version,
        }
    }
}


fn common_parse_key<'a>(k: &'a [u8], prefix: &str, size: usize) -> GraphResult<Vec<&'a str>> {
    if transform::bytes_to_i64(&k[0..8])?.to_be() != META_TABLE_ID {
        let msg = format!("invalid meta key");
        let err = gen_graph_err!(GraphErrorCode::InvalidData, msg, common_parse_key, k, prefix, size);
        return Err(err);
    }
    let key = res_unwrap!(transform::bytes_to_str(&k[8..]), common_parse_key, k, prefix, size)?;
    let items: Vec<&str> = key.split("#").collect();
    if key.starts_with(prefix) && items.len() == size {
        return Ok(items);
    }
    let msg = format!("invalid key {}", key);
    let err = gen_graph_err!(GraphErrorCode::InvalidData, msg, common_parse_key, k, prefix, size);
    Err(err)
}

fn meta_key(key: &str) -> Vec<u8> {
    let bytes = key.as_bytes();
    let mut ret = Vec::with_capacity(8 + key.len());
    let prefix = transform::i64_to_arr(META_TABLE_ID.to_be());
    ret.extend_from_slice(&prefix);
    ret.extend_from_slice(bytes);
    ret
}

#[derive(Debug, Clone, PartialEq)]
struct PrepareDataLoadItem {
    si: SnapshotId,
    schema_version: i64,
    target: DataLoadTarget,
    table_id: TableId,
}

impl PrepareDataLoadItem {
    fn new(si: SnapshotId, schema_version: i64, target: DataLoadTarget, table_id: TableId) -> Self {
        PrepareDataLoadItem {
            si,
            schema_version,
            target,
            table_id,
        }
    }
}

impl ItemCommon for PrepareDataLoadItem {
    fn from_kv(k: &[u8], v: &[u8]) -> GraphResult<Self> {
        let items = res_unwrap!(common_parse_key(k, Self::prefix(), 4), from_kv)?;
        let si = res_unwrap!(parse_str(items[1]), from_kv)?;
        let schema_version = res_unwrap!(parse_str(items[2]), from_kv)?;
        let table_id = res_unwrap!(parse_str(items[3]), from_kv)?;

        let pb = parse_pb::<DataLoadTargetPb>(v)?;
        let target = DataLoadTarget::from_proto(&pb);
        let ret = Self::new(si, schema_version, target, table_id);
        Ok(ret)
    }

    fn prefix() -> &'static str {
        "PrepareDataLoad"
    }

    fn to_kv(&self) -> GraphResult<(Vec<u8>, Vec<u8>)> {
        let key = format!("{}#{}#{}#{}", Self::prefix(), self.si, self.schema_version, self.table_id);
        let target_pb = self.target.to_proto();
        let bytes = target_pb.write_to_bytes()
            .map_err(|e| GraphError::new(InvalidData, format!("{:?}", e)))?;
        Ok((meta_key(&key), bytes))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct CommitDataLoadItem {
    si: SnapshotId,
    schema_version: i64,
    target: DataLoadTarget,
    table_id: TableId,
}

impl CommitDataLoadItem {
    fn new(si: SnapshotId, schema_version: i64, target: DataLoadTarget, table_id: TableId) -> Self {
        CommitDataLoadItem {
            si,
            schema_version,
            target,
            table_id,
        }
    }
}

impl ItemCommon for CommitDataLoadItem {
    fn from_kv(k: &[u8], v: &[u8]) -> GraphResult<Self> {
        let items = res_unwrap!(common_parse_key(k, Self::prefix(), 4), from_kv)?;
        let si = res_unwrap!(parse_str(items[1]), from_kv)?;
        let schema_version = res_unwrap!(parse_str(items[2]), from_kv)?;
        let table_id = res_unwrap!(parse_str(items[3]), from_kv)?;

        let pb = parse_pb::<DataLoadTargetPb>(v)?;
        let target = DataLoadTarget::from_proto(&pb);
        let ret = Self::new(si, schema_version, target, table_id);
        Ok(ret)
    }

    fn prefix() -> &'static str {
        "CommitDataLoad"
    }

    fn to_kv(&self) -> GraphResult<(Vec<u8>, Vec<u8>)> {
        let key = format!("{}#{}#{}#{}", Self::prefix(), self.si, self.schema_version, self.table_id);
        let target_pb = self.target.to_proto();
        let bytes = target_pb.write_to_bytes()
            .map_err(|e| GraphError::new(InvalidData, format!("{:?}", e)))?;
        Ok((meta_key(&key), bytes))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct CreateVertexTypeItem {
    si: SnapshotId,
    schema_version: i64,
    label_id: LabelId,
    table_id: TableId,
    type_def: TypeDef,
}

impl CreateVertexTypeItem {
    fn new(si: SnapshotId, schema_version: i64, label_id: LabelId, table_id: TableId, type_def: TypeDef) -> Self {
        CreateVertexTypeItem {
            si,
            schema_version,
            label_id,
            table_id,
            type_def,
        }
    }
}

impl ItemCommon for CreateVertexTypeItem {
    fn from_kv(k: &[u8], v: &[u8]) -> GraphResult<Self> {
        let items = res_unwrap!(common_parse_key(k, Self::prefix(), 5), from_kv)?;
        let label_id = res_unwrap!(parse_str(items[1]), from_kv)?;
        let si = res_unwrap!(parse_str(items[2]), from_kv)?;
        let schema_version = res_unwrap!(parse_str(items[3]), from_kv)?;
        let table_id = res_unwrap!(parse_str(items[4]), from_kv)?;
        let type_def = res_unwrap!(TypeDef::from_bytes(v), from_kv)?;
        let ret = Self::new(si, schema_version, label_id, table_id, type_def);
        Ok(ret)
    }

    fn prefix() -> &'static str {
        "CreateVertexType"
    }

    fn to_kv(&self) -> GraphResult<(Vec<u8>, Vec<u8>)> {
        let key = format!("{}#{}#{}#{}#{}", Self::prefix(), self.label_id, self.si, self.schema_version, self.table_id);
        let typedef_pb = self.type_def.to_proto()?;
        let bytes = typedef_pb.write_to_bytes()
            .map_err(|e| GraphError::new(InvalidData, format!("{:?}", e)))?;
        Ok((meta_key(&key), bytes))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct DropVertexTypeItem {
    si: SnapshotId,
    schema_version: i64,
    label_id: LabelId,
}

impl DropVertexTypeItem {
    fn new(si: SnapshotId, schema_version: i64, label_id: LabelId) -> Self {
        DropVertexTypeItem {
            si,
            schema_version,
            label_id,
        }
    }
}

impl ItemCommon for DropVertexTypeItem {
    fn from_kv(k: &[u8], v: &[u8]) -> GraphResult<Self> {
        let items = res_unwrap!(common_parse_key(k, Self::prefix(), 3), from_kv)?;
        let label_id = res_unwrap!(parse_str(items[1]), from_kv)?;
        let schema_version = res_unwrap!(parse_str(items[2]), from_kv)?;
        let si = res_unwrap!(transform::bytes_to_i64(v), from_kv)?.to_be();
        let ret = Self::new(si, schema_version, label_id);
        Ok(ret)
    }

    fn prefix() -> &'static str {
        "DropVertexType"
    }

    fn to_kv(&self) -> GraphResult<(Vec<u8>, Vec<u8>)> {
        let key = format!("{}#{}#{}", Self::prefix(), self.label_id, self.schema_version);
        Ok((meta_key(&key), transform::i64_to_vec(self.si.to_be())))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct CreateEdgeTypeItem {
    si: SnapshotId,
    schema_version: i64,
    label_id: LabelId,
    type_def: TypeDef,
}

impl CreateEdgeTypeItem {
    fn new(si: SnapshotId, schema_version: i64, label_id: LabelId, type_def: TypeDef) -> Self {
        CreateEdgeTypeItem {
            si,
            schema_version,
            label_id,
            type_def,
        }
    }
}

impl ItemCommon for CreateEdgeTypeItem {
    fn from_kv(k: &[u8], v: &[u8]) -> GraphResult<Self> {
        let items = res_unwrap!(common_parse_key(k, Self::prefix(), 4), from_kv)?;
        let label_id = res_unwrap!(parse_str(items[1]), from_kv)?;
        let si = res_unwrap!(parse_str(items[2]), from_kv)?;
        let schema_version = res_unwrap!(parse_str(items[3]), from_kv)?;
        let type_def = TypeDef::from_bytes(v)?;
        Ok(Self::new(si, schema_version, label_id, type_def))
    }

    fn prefix() -> &'static str {
        "CreateEdgeType"
    }

    fn to_kv(&self) -> GraphResult<(Vec<u8>, Vec<u8>)> {
        let key = format!("{}#{}#{}#{}", Self::prefix(), self.label_id, self.si, self.schema_version);
        Ok((meta_key(&key), self.type_def.to_bytes()?))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct AddEdgeKindItem {
    si: SnapshotId,
    schema_version: i64,
    table_id: TableId,
    edge_kind: EdgeKind,
}

impl AddEdgeKindItem {
    fn new(si: SnapshotId, schema_version: i64, table_id: TableId, edge_kind: EdgeKind) -> Self {
        AddEdgeKindItem {
            si,
            schema_version,
            table_id,
            edge_kind,
        }
    }
}

impl ItemCommon for AddEdgeKindItem {
    fn from_kv(k: &[u8], v: &[u8]) -> GraphResult<Self> {
        let items = res_unwrap!(common_parse_key(k, Self::prefix(), 4), from_kv)?;
        let si = res_unwrap!(parse_str(items[1]), from_kv)?;
        let schema_version = res_unwrap!(parse_str(items[2]), from_kv)?;
        let table_id = res_unwrap!(parse_str(items[3]), from_kv)?;
        let pb = parse_pb::<EdgeKindPb>(v)?;
        let edge_kind = EdgeKind::from_proto(&pb);
        let ret = Self::new(si, schema_version, table_id, edge_kind);
        Ok(ret)
    }

    fn prefix() -> &'static str {
        "AddEdgeKind"
    }

    fn to_kv(&self) -> GraphResult<(Vec<u8>, Vec<u8>)> {
        let key = format!("{}#{}#{}#{}", Self::prefix(), self.si, self.schema_version, self.table_id);
        let bytes = self.edge_kind.to_proto().write_to_bytes()
            .map_err(|e| GraphError::new(InvalidData, format!("{:?}", e)))?;
        Ok((meta_key(&key), bytes))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct DropEdgeTypeItem {
    si: SnapshotId,
    schema_version: i64,
    label_id: LabelId,
}

impl DropEdgeTypeItem {
    fn new(si: SnapshotId, schema_version: i64, label_id: LabelId) -> Self {
        DropEdgeTypeItem {
            si,
            schema_version,
            label_id,
        }
    }
}

impl ItemCommon for DropEdgeTypeItem {
    fn from_kv(k: &[u8], v: &[u8]) -> GraphResult<Self> {
        let items = res_unwrap!(common_parse_key(k, Self::prefix(), 3), from_kv)?;
        let label_id = res_unwrap!(parse_str(items[1]), from_kv)?;
        let schema_version = res_unwrap!(parse_str(items[2]), from_kv)?;
        let si = res_unwrap!(transform::bytes_to_i64(v), from_kv)?.to_be();
        let ret = Self::new(si, schema_version, label_id);
        Ok(ret)
    }

    fn prefix() -> &'static str {
        "DropEdgeType"
    }

    fn to_kv(&self) -> GraphResult<(Vec<u8>, Vec<u8>)> {
        let key = format!("{}#{}#{}", Self::prefix(), self.label_id, self.schema_version);
        Ok((meta_key(&key), transform::i64_to_vec(self.si.to_be())))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct RemoveEdgeKindItem {
    si: SnapshotId,
    schema_version: i64,
    edge_kind: EdgeKind,
}

impl RemoveEdgeKindItem {
    fn new(si: SnapshotId, schema_version: i64, edge_kind: EdgeKind) -> Self {
        RemoveEdgeKindItem {
            si,
            schema_version,
            edge_kind,
        }
    }
}

impl ItemCommon for RemoveEdgeKindItem {
    fn from_kv(k: &[u8], v: &[u8]) -> GraphResult<Self> {
        let items = res_unwrap!(common_parse_key(k, Self::prefix(), 3), from_kv)?;
        let si = res_unwrap!(parse_str(items[1]), from_kv)?;
        let schema_version = res_unwrap!(parse_str(items[2]), from_kv)?;
        let pb = parse_pb::<EdgeKindPb>(v)?;
        let edge_kind = EdgeKind::from_proto(&pb);
        let ret = Self::new(si, schema_version, edge_kind);
        Ok(ret)
    }

    fn prefix() -> &'static str {
        "RemoveEdgeKind"
    }

    fn to_kv(&self) -> GraphResult<(Vec<u8>, Vec<u8>)> {
        let key = format!("{}#{}#{}", Self::prefix(), self.si, self.schema_version);
        let bytes = self.edge_kind.to_proto().write_to_bytes()
            .map_err(|e| GraphError::new(InvalidData, format!("{:?}", e)))?;
        Ok((meta_key(&key), bytes))
    }
}

fn get_items<I: ItemCommon>(store: &dyn ExternalStorage) -> GraphResult<Vec<I>> {
    let mut ret = Vec::new();
    let mut prefix = Vec::new();
    let table_prefix = transform::i64_to_arr(META_TABLE_ID.to_be());
    prefix.extend_from_slice(&table_prefix);
    prefix.extend_from_slice(I::prefix().as_bytes());
    let mut iter = res_unwrap!(store.scan_prefix(&prefix), get_items)?;
    while let Some((k, v)) = iter.next() {
        let item = res_unwrap!(I::from_kv(k, v), get_items)?;
        ret.push(item);
    }
    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashSet, HashMap};
    use crate::db::storage::rocksdb::RocksDB;
    use crate::db::graph::tests::types;
    use crate::db::graph::tests::types::TableInfoTest;
    use crate::db::util::fs;

    #[test]
    fn test_meta_item() {
        let type_def = TypeDef::new_test();
        let item = CreateVertexTypeItem::new(1, 2, 1, 1, type_def.clone());
        let (k, v) = item.to_kv().unwrap();
        let item2 = CreateVertexTypeItem::from_kv(&k, &v).unwrap();
        assert_eq!(item, item2);

        let item = CreateEdgeTypeItem::new(1, 2, 1, type_def.clone());
        let (k, v) = item.to_kv().unwrap();
        let item2 = CreateEdgeTypeItem::from_kv(&k, &v).unwrap();
        assert_eq!(item, item2);

        let item = AddEdgeKindItem::new(1, 1, 1, EdgeKind::new(1, 2, 3));
        let (k, v) = item.to_kv().unwrap();
        let item2 = AddEdgeKindItem::from_kv(&k, &v).unwrap();
        assert_eq!(item, item2);

        let item = DropVertexTypeItem::new(2, 2, 1);
        let (k, v) = item.to_kv().unwrap();
        let item2 = DropVertexTypeItem::from_kv(&k, &v).unwrap();
        assert_eq!(item, item2);

        let item = DropEdgeTypeItem::new(123, 44, 3);
        let (k, v) = item.to_kv().unwrap();
        let item2 = DropEdgeTypeItem::from_kv(&k, &v).unwrap();
        assert_eq!(item, item2);

        let item = RemoveEdgeKindItem::new(1, 1, EdgeKind::new(51231, 51215, 666));
        let (k, v) = item.to_kv().unwrap();
        let item2 = RemoveEdgeKindItem::from_kv(&k, &v).unwrap();
        assert_eq!(item, item2);
    }

    #[test]
    fn test_meta_normal() {
        let path = "test_meta_normal";
        fs::rmr(path).unwrap();
        {
            let db = RocksDB::open(&HashMap::new(), path).unwrap();
            let store = Arc::new(db);
            let meta = Meta::new(store.clone());
            let mut schema_version = 1;
            let mut label_to_vertex_table = HashMap::new();
            for label in 1..=10 {
                let type_def = types::create_test_type_def(label);
                let table = meta.create_vertex_type(10, schema_version, label, &type_def, schema_version).unwrap();
                label_to_vertex_table.insert(label, table);
                schema_version += 1;
            }

            let mut label_to_edge_table = HashMap::new();
            for label in 11..=20 {
                let type_def = types::create_test_type_def(label);
                meta.create_edge_type(10, schema_version, label, &type_def).unwrap();
                schema_version += 1;
                for edge_kind in gen_edge_kinds(label) {
                    let table = meta.add_edge_kind(10, schema_version, &edge_kind, schema_version).unwrap();
                    label_to_edge_table.insert(edge_kind, table);
                    schema_version += 1;
                }
            }
            let (vertex_manager, edge_manager) = meta.recover().unwrap();
            check_vertex_manager(&vertex_manager, &label_to_vertex_table);
            check_edge_manager(&edge_manager, &label_to_edge_table);
        }
        fs::rmr(path).unwrap();
    }

    fn gen_edge_kinds(label: LabelId) -> HashSet<EdgeKind> {
        let mut ret = HashSet::new();
        for si in 10..=20 {
            let edge_kind = types::create_edge_kind(si, label);
            ret.insert(edge_kind);
        }
        ret
    }

    fn check_vertex_manager(manager: &VertexTypeManager, label_to_table: &HashMap<LabelId, Table>) {
        for si in 1..=20 {
            for label in 1..=20 {
                if si < 10 || label > 10 {
                    assert!(manager.get_type(si, label).is_err());
                } else {
                    let info = manager.get_type(si, label).unwrap();
                    // let tables = gen_vertex_tables(label);
                    let mut tables = Vec::new();
                    tables.push(label_to_table.get(&label).unwrap().clone());
                    types::VertexInfoTest::new(info).test(tables);
                }
            }
        }
    }

    fn check_edge_manager(manager: &EdgeTypeManager, label_to_table: &HashMap<EdgeKind, Table>) {
        for si in 1..=20 {
            for label in 1..=20 {
                if si < 10 || label <= 10 {
                    assert!(manager.get_edge(si, label).is_err());
                } else {
                    let info = manager.get_edge(si, label).unwrap();
                    assert_eq!(info.get_label(), label);
                    check_edge_info(info, label_to_table);
                }
            }
        }
    }

    fn check_edge_info(info: EdgeInfoRef, label_to_table: &HashMap<EdgeKind, Table>) {
        let mut set = gen_edge_kinds(info.get_label());
        let mut iter = info.into_iter();
        while let Some(ei) = iter.next() {
            assert!(set.remove(ei.get_type()));
            let mut tables = Vec::new();
            tables.push(label_to_table.get(ei.get_type()).unwrap().clone());
            types::EdgeTypeInfoTest::new(ei).test(tables);
        }
        assert!(set.is_empty());
    }
}
