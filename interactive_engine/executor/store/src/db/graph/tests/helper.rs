#![allow(dead_code)]
use crate::db::api::*;
use std::collections::{HashMap, HashSet};
use super::data;
use crate::db::api::multi_version_graph::MultiVersionGraph;
use crate::db::api::types::{RocksVertex, Property, PropertyValue, RocksEdge};

pub struct GraphTestHelper<'a, G: MultiVersionGraph> {
    graph: &'a G,
    cur_si: SnapshotId,
    vertex_data: VertexDataManager,
    edge_data: EdgeDataManager,
    vertex_type_manager: VertexTypeManager,
    edge_type_manager: EdgeTypeManager,
}

impl<'a, G: MultiVersionGraph> GraphTestHelper<'a, G> {
    pub fn new(graph: &'a G) -> Self {
        GraphTestHelper {
            graph,
            cur_si: 0,
            vertex_data: VertexDataManager::new(),
            edge_data: EdgeDataManager::new(),
            vertex_type_manager: VertexTypeManager::new(),
            edge_type_manager: EdgeTypeManager::new(),
        }
    }

    pub fn insert_vertex(&mut self, si: SnapshotId, label: LabelId, list: Vec<VertexId>) -> GraphResult<()> {
        self.check_and_update_si(si)?;
        let type_def = self.vertex_type_manager.get_type_def(si, label).unwrap();
        for id in list {
            let properties = data::gen_vertex_properties(si, label, id, type_def);
            self.graph.insert_overwrite_vertex(si, id, label, &properties)?;
            self.vertex_data.insert(si, id, label, properties);
        }
        Ok(())
    }

    pub fn update_vertex(&mut self, si: SnapshotId, label: LabelId, list: Vec<VertexId>) -> GraphResult<()> {
        self.check_and_update_si(si)?;
        let type_def = self.vertex_type_manager.get_type_def(si, label).unwrap();
        for id in list {
            let properties = data::gen_vertex_update_properties(si, label, id, type_def);
            self.graph.insert_update_vertex(si, id, label, &properties)?;
            self.vertex_data.update(si, id, label, properties);
        }
        Ok(())
    }

    pub fn insert_edge<I: Iterator<Item=EdgeId>>(&mut self, si: SnapshotId, edge_kind: &EdgeKind, list: I) -> GraphResult<()> {
        self.check_and_update_si(si)?;
        assert!(self.edge_type_manager.edge_type_alive_at(si, edge_kind));
        let type_def = self.edge_type_manager.get_type_def(si, edge_kind.edge_label_id).unwrap();
        for id in list {
            let properties = data::gen_edge_properties(si, edge_kind, &id, type_def);
            self.graph.insert_overwrite_edge(si, id, edge_kind, true, &properties)?;
            self.graph.insert_overwrite_edge(si, id, edge_kind, false, &properties)?;
            self.edge_data.insert(si, id, edge_kind, properties);
        }
        Ok(())
    }

    pub fn update_edge<I: Iterator<Item=EdgeId>>(&mut self, si: SnapshotId, edge_kind: &EdgeKind, list: I) -> GraphResult<()> {
        self.check_and_update_si(si)?;
        assert!(self.edge_type_manager.edge_type_alive_at(si, edge_kind));
        let type_def = self.edge_type_manager.get_type_def(si, edge_kind.edge_label_id).unwrap();
        for id in list {
            let properties = data::gen_edge_update_properties(si, edge_kind, &id, &type_def);
            self.graph.insert_update_edge(si, id, edge_kind,  true, &properties)?;
            self.graph.insert_update_edge(si, id, edge_kind,  false, &properties)?;
            self.edge_data.update(si, id, edge_kind, properties);
        }
        Ok(())
    }

    pub fn delete_vertex(&mut self, si: SnapshotId, label: LabelId, list: Vec<VertexId>) -> GraphResult<()> {
        self.check_and_update_si(si)?;
        assert!(self.vertex_type_manager.get_type_def(si, label).is_some());
        for id in list {
            self.vertex_data.delete(si, id, label);
            self.graph.delete_vertex(si, id, label)?;
        }
        Ok(())
    }

    pub fn delete_edge(&mut self, si: SnapshotId, edge_kind: &EdgeKind, list: Vec<EdgeId>) -> GraphResult<()> {
        self.check_and_update_si(si)?;
        assert!(self.edge_type_manager.edge_type_alive_at(si, edge_kind));
        for id in list {
            self.edge_data.delete(si, id, edge_kind);
            self.graph.delete_edge(si, id, edge_kind, true)?;
            self.graph.delete_edge(si, id, edge_kind, false)?;
        }
        Ok(())
    }

    pub fn create_vertex_type(&mut self, si: SnapshotId, schema_version: i64, label: LabelId, type_def: TypeDef) -> GraphResult<()> {
        self.check_and_update_si(si)?;
        self.graph.create_vertex_type(si, schema_version, label, &type_def, schema_version)?;
        self.vertex_type_manager.create(si, label, type_def.clone());
        Ok(())
    }

    pub fn drop_vertex_type(&mut self, si: SnapshotId, schema_version: i64, label: LabelId) -> GraphResult<()> {
        self.check_and_update_si(si)?;
        self.vertex_data.drop(si, label);
        self.graph.drop_vertex_type(si, schema_version, label)?;
        self.vertex_type_manager.drop(si, label);
        Ok(())
    }

    pub fn create_edge_type(&mut self, si: SnapshotId, schema_version: i64, label: LabelId, type_def: TypeDef) -> GraphResult<()> {
        self.check_and_update_si(si)?;
        self.graph.create_edge_type(si, schema_version, label, &type_def)?;
        self.edge_type_manager.create_edge(si, label, type_def);
        Ok(())
    }

    pub fn drop_edge_type(&mut self, si: SnapshotId, schema_version: i64, label: LabelId) -> GraphResult<()> {
        self.check_and_update_si(si)?;
        self.graph.drop_edge_type(si, schema_version, label)?;
        self.edge_type_manager.drop_edge(si, label);
        self.edge_data.drop_edge(si, label);
        Ok(())
    }

    pub fn add_edge_kind(&mut self, si: SnapshotId, schema_version: i64, edge_kind: &EdgeKind) -> GraphResult<()> {
        self.check_and_update_si(si)?;
        self.graph.add_edge_kind(si, schema_version, edge_kind, schema_version)?;
        self.edge_type_manager.add_edge_kind(si, edge_kind);
        Ok(())
    }

    pub fn remove_edge_kind(&mut self, si: SnapshotId, schema_version: i64, edge_kind: &EdgeKind) -> GraphResult<()> {
        self.check_and_update_si(si)?;
        self.graph.remove_edge_kind(si, schema_version, edge_kind)?;
        self.edge_type_manager.remove_edge_kind(si, edge_kind);
        self.edge_data.remove_edge_kind(si, edge_kind);
        Ok(())
    }

    pub fn check_get_vertex(&self, si: SnapshotId, label: LabelId, ids: &Vec<VertexId>) {
        for id in ids {
            let ans = self.vertex_data.get(si, *id, label).unwrap();
            let v = self.graph.get_vertex(si, *id, Some(label), None).unwrap().unwrap();
            check_vertex(&v, &ans);
            let v = self.graph.get_vertex(si, *id, None, None).unwrap().unwrap();
            check_vertex(&v, &ans);
        }
    }

    pub fn check_get_vertex_none(&self, si: SnapshotId, label: LabelId, ids: &Vec<VertexId>) {
        for id in ids {
            assert!(self.vertex_data.get(si, *id, label).is_none());
            assert!(self.graph.get_vertex(si, *id, Some(label), None).unwrap().is_none());
            assert!(self.graph.get_vertex(si, *id, None, None).unwrap().is_none());
        }
    }

    pub fn check_get_vertex_err(&self, si: SnapshotId, label: LabelId, ids: &Vec<VertexId>) {
        for id in ids {
            assert!(self.graph.get_vertex(si, *id, Some(label), None).is_err());
            assert!(self.graph.get_vertex(si, *id, None, None).unwrap().is_none());
        }
    }

    pub fn check_query_vertices(&self, si: SnapshotId, label: Option<LabelId>, mut ids: HashSet<VertexId>) {
        let mut ans = self.vertex_data.scan(si, label);
        let mut iter = self.graph.scan_vertex(si, label, None, None).unwrap();
        while let Some(v) = iter.next() {
            let v = v.unwrap();
            assert!(ids.remove(&v.get_vertex_id()));
            let ans_v = ans.remove(&v.get_vertex_id()).unwrap();
            check_vertex(&v, &ans_v);
        }
        assert!(ids.is_empty());
        assert!(ans.is_empty(), "some id in helper is not found in data");
    }

    pub fn check_query_vertices_empty(&self, si: SnapshotId, label: LabelId) {
        assert!(self.graph.scan_vertex(si, Some(label), None, None).unwrap().next().is_none());
    }

    pub fn check_get_edge<'b, I: Iterator<Item=&'b EdgeId>>(&self, si: SnapshotId, edge_kind: &EdgeKind, ids: I) {
        for id in ids {
            let ans = self.edge_data.get(si, *id, edge_kind).expect(format!("{:?} not found in helper", id).as_str());
            let e = self.graph.get_edge(si, *id, Some(edge_kind), None).unwrap().unwrap();
            check_edge(&e, &ans);
            let e = self.graph.get_edge(si, *id, None, None).unwrap().unwrap();
            check_edge(&e, &ans);
        }
    }

    pub fn check_get_edge_none<'b, I: Iterator<Item=&'b EdgeId>>(&self, si: SnapshotId, edge_kind: &EdgeKind, ids: I) {
        for id in ids {
            assert!(self.edge_data.get(si, *id, edge_kind).is_none());
            assert!(self.graph.get_edge(si, *id, Some(edge_kind), None).unwrap().is_none());
            assert!(self.graph.get_edge(si, *id, None, None).unwrap().is_none());
        }
    }

    pub fn check_get_edge_err<'b, I: Iterator<Item=&'b EdgeId>>(&self, si: SnapshotId, edge_kind: &EdgeKind, ids: I) {
        for id in ids {
            assert!(self.graph.get_edge(si, *id, Some(edge_kind), None).is_err());
            assert!(self.graph.get_edge(si, *id, None, None).unwrap().is_none());
        }
    }

    /// `ids`: user's answer
    pub fn check_query_edges(&self, si: SnapshotId, label: Option<LabelId>, ids: HashSet<EdgeId>) {
        let ans = self.edge_data.scan(si, label);
        let iter = self.graph.scan_edge(si, label, None, None).unwrap();
        check_edge_iter(iter, ans, ids);
    }

    pub fn check_query_edges_empty(&self, si: SnapshotId, label: Option<LabelId>) {
        assert!(self.graph.scan_edge(si, label, None, None).unwrap().next().is_none());
    }

    pub fn check_get_out_edges(&self, si: SnapshotId, src_id: VertexId, label: Option<LabelId>, ids: HashSet<EdgeId>) {
        let ans = self.edge_data.get_out_edges(si, src_id, label);
        let iter = self.graph.get_out_edges(si, src_id, label, None, None).unwrap();
        check_edge_iter(iter, ans, ids);
    }

    pub fn check_get_out_edges_empty(&self, si: SnapshotId, src_id: VertexId, label: Option<LabelId>) {
        assert!(self.graph.get_out_edges(si, src_id, label, None, None).unwrap().next().is_none());
    }

    pub fn check_get_in_edges(&self, si: SnapshotId, dst_id: VertexId, label: Option<LabelId>, ids: HashSet<EdgeId>) {
        let ans = self.edge_data.get_in_edges(si, dst_id, label);
        let iter = self.graph.get_in_edges(si, dst_id, label, None, None).unwrap();
        check_edge_iter(iter, ans, ids);
    }

    pub fn check_get_in_edges_empty(&self, si: SnapshotId, dst_id: VertexId, label: Option<LabelId>) {
        assert!(self.graph.get_in_edges(si, dst_id, label, None, None).unwrap().next().is_none());
    }

    fn check_and_update_si(&mut self, si: SnapshotId) -> GraphResult<()> {
        if si < self.cur_si {
            let msg = format!("si#{} is less than cur_si#{}", si, self.cur_si);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, check_and_update_si, si);
            return Err(err);
        }
        self.cur_si = si;
        Ok(())
    }
}

struct VertexTypeManager {
    map: HashMap<LabelId, TypeInfoList>,
}

impl VertexTypeManager {
    fn new() -> Self {
        VertexTypeManager {
            map: HashMap::new(),
        }
    }

    fn create(&mut self, si: SnapshotId, label: LabelId, type_def: TypeDef) {
        self.map.entry(label).or_insert_with(|| TypeInfoList::new()).add(si, type_def);
    }

    fn drop(&mut self, si: SnapshotId, label: LabelId) {
        self.map.entry(label).or_insert_with(|| TypeInfoList::new()).drop(si);
    }

    fn get_type_def(&self, si: SnapshotId, label: LabelId) -> Option<&TypeDef> {
        self.map.get(&label)?.get(si)
    }
}

struct EdgeTypeManager {
    map: HashMap<LabelId, TypeInfoList>,
    type_map: HashMap<LabelId, EdgeTypeMap>,
}

impl EdgeTypeManager {
    fn new() -> Self {
        EdgeTypeManager {
            map: HashMap::new(),
            type_map: HashMap::new(),
        }
    }

    fn get_type_def(&self, si: SnapshotId, label: LabelId) -> Option<&TypeDef> {
        self.map.get(&label)?.get(si)
    }

    fn edge_alive_at(&self, si: SnapshotId, label: LabelId) -> bool {
        self.get_type_def(si, label).is_some()
    }

    fn edge_type_alive_at(&self, si: SnapshotId, edge_kind: &EdgeKind) -> bool {
        if !self.edge_alive_at(si, edge_kind.edge_label_id) {
            return false;
        }
        self.type_map.get(&edge_kind.edge_label_id).unwrap().is_alive_at(si, edge_kind)
    }

    fn create_edge(&mut self, si: SnapshotId, label: LabelId, type_def: TypeDef) {
        self.map.entry(label).or_insert_with(|| TypeInfoList::new()).add(si, type_def);
        self.type_map.insert(label, EdgeTypeMap::new());
    }

    fn add_edge_kind(&mut self, si: SnapshotId, edge_kind: &EdgeKind) {
        assert!(self.edge_alive_at(si, edge_kind.edge_label_id));
        self.type_map.get_mut(&edge_kind.edge_label_id).unwrap().add(si, edge_kind);
    }

    fn drop_edge(&mut self, si: SnapshotId, label: LabelId) {
        self.map.entry(label).or_insert_with(|| TypeInfoList::new()).drop(si);
    }

    fn remove_edge_kind(&mut self, si: SnapshotId, edge_kind: &EdgeKind) {
        self.type_map.entry(edge_kind.edge_label_id).or_insert_with(|| EdgeTypeMap::new()).remove(si, edge_kind);
    }
}

struct TypeInfoList {
    list: Vec<TypeInfo>,
}

impl TypeInfoList {
    fn new() -> Self {
        TypeInfoList {
            list: Vec::new(),
        }
    }

    fn add(&mut self, si: SnapshotId, type_def: TypeDef) {
        if self.list.len() == 0 || si > self.list.last().unwrap().get_si() {
            self.list.push(TypeInfo::Define((si, type_def)));
            return;
        }
        panic!("invalid si");
    }

    fn get(&self, si: SnapshotId) -> Option<&TypeDef> {
        for info in self.list.iter().rev() {
            if si >= info.get_si() {
                return match *info {
                    TypeInfo::Define((_, ref type_def)) => Some(type_def),
                    TypeInfo::Tombstone(_) => None,
                };
            }
        }
        None
    }

    fn drop(&mut self, si: SnapshotId) {
        if self.list.len() == 0 || si > self.list.last().unwrap().get_si() {
            self.list.push(TypeInfo::Tombstone(si));
            return;
        }
        panic!("invalid si");
    }
}

enum TypeInfo {
    Tombstone(SnapshotId),
    Define((SnapshotId, TypeDef)),
}

impl TypeInfo {
    fn get_si(&self) -> SnapshotId {
        match *self {
            TypeInfo::Tombstone(si) => si,
            TypeInfo::Define((si, _)) => si,
        }
    }
}

struct EdgeTypeMap {
    map: HashMap<EdgeKind, EdgeTypeInfo>,
}

impl EdgeTypeMap {
    fn new() -> Self {
        EdgeTypeMap {
            map: HashMap::new(),
        }
    }

    fn add(&mut self, si: SnapshotId, edge_kind: &EdgeKind) {
        self.map.entry(edge_kind.clone()).or_insert_with(|| EdgeTypeInfo::new(edge_kind.clone())).add(si);
    }

    fn remove(&mut self, si: SnapshotId, edge_kind: &EdgeKind) {
        self.map.entry(edge_kind.clone()).or_insert_with(|| EdgeTypeInfo::new(edge_kind.clone())).remove(si);
    }

    fn is_alive_at(&self, si: SnapshotId, edge_kind: &EdgeKind) -> bool {
        self.map.get(edge_kind).unwrap().is_alive_at(si)
    }
}

struct EdgeTypeInfo {
    edge_kind: EdgeKind,
    versions: Vec<EdgeTypeVersionInfo>,
}

impl EdgeTypeInfo {
    fn new(edge_kind: EdgeKind) -> Self {
        EdgeTypeInfo {
            edge_kind,
            versions: Vec::new(),
        }
    }

    fn add(&mut self, si: SnapshotId) {
        if self.versions.len() == 0 {
            self.versions.push(EdgeTypeVersionInfo::Add(si));
        } else {
            let last = self.versions.last().unwrap();
            if si > last.get_si() && last.is_tombstone() {
                self.versions.push(EdgeTypeVersionInfo::Add(si));
            } else {
                panic!("error");
            }
        }
    }

    fn remove(&mut self, si: SnapshotId) {
        if self.versions.len() == 0 || si > self.versions.last().unwrap().get_si() {
            self.versions.push(EdgeTypeVersionInfo::Tombstone(si));
            return;
        }
        panic!("invalid si");
    }


    fn is_alive_at(&self, si: SnapshotId) -> bool {
        for v in self.versions.iter().rev() {
            if si >= v.get_si() {
                return !v.is_tombstone();
            }
        }
        false
    }
}

enum EdgeTypeVersionInfo {
    Tombstone(SnapshotId),
    Add(SnapshotId),
}

impl EdgeTypeVersionInfo {
    fn get_si(&self) -> SnapshotId {
        match *self {
            EdgeTypeVersionInfo::Tombstone(si) => si,
            EdgeTypeVersionInfo::Add(si) => si,
        }
    }

    fn is_tombstone(&self) -> bool {
        match *self {
            EdgeTypeVersionInfo::Tombstone(_) => true,
            _ => false,
        }
    }
}

struct VertexDataManager {
    map: HashMap<LabelId, VertexDataMap>,
}

impl VertexDataManager {
    fn new() -> Self {
        VertexDataManager {
            map: HashMap::new(),
        }
    }

    fn insert(&mut self, si: SnapshotId, id: VertexId, label: LabelId, properties: HashMap<PropertyId, Value>) {
        let data = self.map.entry(label).or_insert_with(|| VertexDataMap::new(label));
        data.insert(si, id, properties);
    }

    fn update(&mut self, si: SnapshotId, id: VertexId, label: LabelId, properties: HashMap<PropertyId, Value>) {
        let data = self.map.entry(label).or_insert_with(|| VertexDataMap::new(label));
        data.update(si, id, properties);
    }

    fn delete(&mut self, si: SnapshotId, id: VertexId, label: LabelId) {
        let data = self.map.entry(label).or_insert_with(|| VertexDataMap::new(label));
        data.delete(si, id);
    }

    fn get(&self, si: SnapshotId, id: VertexId, label: LabelId) -> Option<VertexDataRef> {
        self.map.get(&label)?.get(si, id)
    }

    fn scan(&self, si: SnapshotId, label: Option<LabelId>) -> HashMap<VertexId, VertexDataRef> {
        self.map.iter()
            .filter(|(l, _)| label.is_none() || **l == label.unwrap())
            .flat_map(|(_, data)| data.scan(si))
            .collect()
    }

    fn drop(&mut self, si: SnapshotId, label: LabelId) {
        self.map.get_mut(&label).unwrap().drop_at = si;
    }
}

type Props = HashMap<PropertyId, Value>;

struct VertexDataMap {
    label: LabelId,
    map: HashMap<VertexId, DataList>,
    drop_at: SnapshotId,
}

impl VertexDataMap {
    fn new(label: LabelId) -> Self {
        VertexDataMap {
            label,
            map: HashMap::new(),
            drop_at: INFINITE_SI,
        }
    }

    fn insert(&mut self, si: SnapshotId, id: VertexId, properties: Props) {
        let list = self.map.entry(id).or_insert_with(|| DataList::new());
        list.insert(si, properties);
    }

    fn delete(&mut self, si: SnapshotId, id: VertexId) {
        let list = self.map.entry(id).or_insert_with(|| DataList::new());
        list.delete(si);
    }

    fn update(&mut self, si: SnapshotId, id: VertexId, properties: Props) {
        let list = self.map.entry(id).or_insert_with(|| DataList::new());
        list.update(si, properties);
    }

    fn get(&self, si: SnapshotId, id: VertexId) -> Option<VertexDataRef> {
        let list = self.map.get(&id)?;
        list.get(si).map(|data| VertexDataRef::new(self.label, data))
    }

    fn scan<'a>(&'a self, si: SnapshotId) -> Box<dyn Iterator<Item=(VertexId, VertexDataRef)> + 'a> {
        if si >= self.drop_at {
            return Box::new(vec![].into_iter());
        }
        let ret = self.map.iter()
            .map(move |(id, data)| (*id, data.get(si)))
            .filter(|(_id, data)| data.is_some())
            .map(move |(id, data)| (id, VertexDataRef::new(self.label, data.unwrap())));
        Box::new(ret)
    }
}

struct VertexDataRef<'a> {
    label: LabelId,
    properties: &'a Props,
}

impl<'a> VertexDataRef<'a> {
    fn new(label: LabelId, properties: &'a Props) -> Self {
        VertexDataRef {
            label,
            properties,
        }
    }
}

fn check_vertex<V: RocksVertex>(v: &V, ans: &VertexDataRef) {
    assert_eq!(v.get_label_id(), ans.label);
    for (prop_id, ans_val) in ans.properties {
        let val = v.get_property(*prop_id).unwrap();
        assert_eq!(*val.get_property_value(), PropertyValue::from(ans_val.as_ref()));
    }
    let mut set = HashSet::new();
    let mut iter = v.get_property_iterator();
    while let Some(property) = iter.next() {
        let p = property.unwrap();
        let prop_id = p.get_property_id();
        let val = p.get_property_value();
        let ans_val = ans.properties.get(&prop_id).unwrap();
        assert_eq!(PropertyValue::from(ans_val.as_ref()), *val);
        set.insert(prop_id);
    }
    assert_eq!(set.len(), ans.properties.len());
}

struct EdgeDataManager {
    map: HashMap<EdgeKind, EdgeDataMap>,
}

impl EdgeDataManager {
    fn new() -> Self {
        EdgeDataManager {
            map: HashMap::new(),
        }
    }

    fn insert(&mut self, si: SnapshotId, id: EdgeId, edge_kind: &EdgeKind, properties: Props) {
        self.map.entry(edge_kind.clone())
            .or_insert_with(|| EdgeDataMap::new(edge_kind.clone()))
            .insert(si, id, properties);
    }

    fn update(&mut self, si: SnapshotId, id: EdgeId, edge_kind: &EdgeKind, properties: Props) {
        self.map.entry(edge_kind.clone())
            .or_insert_with(|| EdgeDataMap::new(edge_kind.clone()))
            .update(si, id, properties);
    }

    fn delete(&mut self, si: SnapshotId, id: EdgeId, edge_kind: &EdgeKind) {
        self.map.entry(edge_kind.clone())
            .or_insert_with(|| EdgeDataMap::new(edge_kind.clone()))
            .delete(si, id);
    }

    fn get(&self, si: SnapshotId, id: EdgeId, edge_kind: &EdgeKind) -> Option<EdgeDataRef> {
        self.map.get(edge_kind)?.get(si, id)
    }

    fn scan(&self, si: SnapshotId, label: Option<LabelId>) -> HashMap<EdgeId, EdgeDataRef> {
        self.map.iter()
            .filter(|(edge_kind, _)| label.is_none() || edge_kind.edge_label_id == label.unwrap())
            .flat_map(|(_, datas)| datas.scan(si))
            .collect()
    }

    fn get_out_edges(&self, si: SnapshotId, src_id: VertexId, label: Option<LabelId>) -> HashMap<EdgeId, EdgeDataRef> {
        self.map.iter()
            .filter(|(edge_kind, _)| label.is_none() || edge_kind.edge_label_id == label.unwrap())
            .flat_map(|(_, data)| data.get_out_edges(si, src_id))
            .collect()
    }

    fn get_in_edges(&self, si: SnapshotId, dst_id: VertexId, label: Option<LabelId>) -> HashMap<EdgeId, EdgeDataRef> {
        self.map.iter()
            .filter(|(edge_kind, _)| label.is_none() || edge_kind.edge_label_id == label.unwrap())
            .flat_map(|(_, data)| data.get_in_edges(si, dst_id))
            .collect()
    }

    fn drop_edge(&mut self, si: SnapshotId, label: LabelId) {
        self.map.iter_mut()
            .filter(|(t, _)| t.edge_label_id == label)
            .for_each(|(_, map)| map.drop_at = si);
    }

    fn remove_edge_kind(&mut self, si: SnapshotId, edge_kind: &EdgeKind) {
        self.map.get_mut(edge_kind).unwrap().drop_at = si;
    }
}

struct EdgeDataMap {
    edge_kind: EdgeKind,
    map: HashMap<EdgeId, DataList>,
    drop_at: SnapshotId,
}

impl EdgeDataMap {
    fn new(edge_kind: EdgeKind) -> Self {
        EdgeDataMap {
            edge_kind,
            map: HashMap::new(),
            drop_at: INFINITE_SI,
        }
    }

    fn insert(&mut self, si: SnapshotId, id: EdgeId, properties: Props) {
        self.map.entry(id).or_insert_with(|| DataList::new()).insert(si, properties);
    }

    fn update(&mut self, si: SnapshotId, id: EdgeId, properties: Props) {
        self.map.entry(id).or_insert_with(|| DataList::new()).update(si, properties);
    }

    fn delete(&mut self, si: SnapshotId, id: EdgeId) {
        self.map.entry(id).or_insert_with(|| DataList::new()).delete(si);
    }

    fn get(&self, si: SnapshotId, id: EdgeId) -> Option<EdgeDataRef> {
        if si >= self.drop_at {
            return None;
        }
        self.map.get(&id)?.get(si).map(|properties| EdgeDataRef::new(&self.edge_kind, properties))
    }

    fn scan<'a>(&'a self, si: SnapshotId) -> Box<dyn Iterator<Item=(EdgeId, EdgeDataRef)> + 'a> {
        if si >= self.drop_at {
            return Box::new(vec![].into_iter());
        }
        let ret = self.map.iter()
            .map(move |(id, data)| (*id, data.get(si)))
            .filter(|(_, data)| data.is_some())
            .map(move |(id, data)| (id, EdgeDataRef::new(&self.edge_kind, data.unwrap())));
        Box::new(ret)
    }

    fn get_out_edges<'a>(&'a self, si: SnapshotId, src_id: VertexId) -> Box<dyn Iterator<Item=(EdgeId, EdgeDataRef)> + 'a> {
        if si >= self.drop_at {
            return Box::new(vec![].into_iter());
        }
        let ret = self.map.iter()
            .filter(move |(id, _)| id.src_id == src_id)
            .map(move |(id, data)| (*id, data.get(si)))
            .filter(|(_, data)| data.is_some())
            .map(move |(id, data)| (id, EdgeDataRef::new(&self.edge_kind, data.unwrap())));
        Box::new(ret)
    }

    fn get_in_edges<'a>(&'a self, si: SnapshotId, dst_id: VertexId) -> Box<dyn Iterator<Item=(EdgeId, EdgeDataRef)> + 'a> {
        if si >= self.drop_at {
            return Box::new(vec![].into_iter());
        }
        let ret = self.map.iter()
            .filter(move |(id, _)| id.dst_id == dst_id)
            .map(move |(id, data)| (*id, data.get(si)))
            .filter(|(_, data)| data.is_some())
            .map(move |(id, data)| (id, EdgeDataRef::new(&self.edge_kind, data.unwrap())));
        Box::new(ret)
    }
}

struct EdgeDataRef<'a> {
    edge_kind: &'a EdgeKind,
    properties: &'a Props,
}

impl<'a> EdgeDataRef<'a> {
    fn new(edge_kind: &'a EdgeKind, properties: &'a Props) -> Self {
        EdgeDataRef {
            edge_kind,
            properties,
        }
    }
}

fn check_edge<E: RocksEdge>(e: &E, ans: &EdgeDataRef) {
    assert_eq!(e.get_edge_relation(), ans.edge_kind);
    for (prop_id, ans_val) in ans.properties {
        let val = e.get_property(*prop_id).unwrap();
        assert_eq!(*val.get_property_value(), PropertyValue::from(ans_val.as_ref()));
    }
    let mut set = HashSet::new();
    let mut iter = e.get_property_iterator();
    while let Some(p) = iter.next() {
        let p = p.unwrap();
        let prop_id = p.get_property_id();
        let val = p.get_property_value();
        let ans_val = ans.properties.get(&prop_id).unwrap();
        assert_eq!(*val, PropertyValue::from(ans_val.as_ref()));
        set.insert(prop_id);
    }
    assert_eq!(set.len(), ans.properties.len());
}

fn check_edge_iter<E: RocksEdge>(mut iter: Records<E>, mut ans: HashMap<EdgeId, EdgeDataRef>, mut ids: HashSet<EdgeId>) {
    while let Some(edge) = iter.next() {
        let e = edge.unwrap();
        assert!(ids.remove(e.get_edge_id()), "find an edge not in user's answer, this edge is {:?}", e);
        let ans_e = ans.remove(e.get_edge_id()).unwrap();
        check_edge(&e, &ans_e);
    }
    assert!(ids.is_empty(), "some edge in user's answer is not found in data");
    assert!(ans.is_empty(), "some edge in helper is not found in data");
}

struct DataList {
    list: Vec<Data>,
}

impl DataList {
    fn new() -> Self {
        DataList {
            list: Vec::new(),
        }
    }

    fn insert(&mut self, si: SnapshotId, properties: Props) {
        if self.list.len() == 0 || self.list.last().unwrap().get_si() <= si {
            self.list.push(Data::Content((si, properties)));
            return;
        }
        panic!("invalid si");
    }

    fn delete(&mut self, si: SnapshotId) {
        if self.list.len() == 0 {
            return;
        }
        if si >= self.list.last().unwrap().get_si() {
            self.list.push(Data::Tombstone(si));
            return;
        }
        panic!("invalid si");
    }

    fn update(&mut self, si: SnapshotId, properties: Props) {
        if self.list.len() > 0 && si < self.list.last().unwrap().get_si() {
            panic!("invalid si");
        }
        if self.list.len() == 0 {
            return self.insert(si, properties);
        }
        match self.list.last().unwrap() {
            &Data::Tombstone(_) => self.insert(si, properties),
            &Data::Content((_, ref props)) => {
                let mut map = props.clone();
                for (prop_id, v) in properties {
                    map.insert(prop_id, v);
                }
                self.insert(si, map);
            }
        }
    }

    fn get(&self, si: SnapshotId) -> Option<&Props> {
        for i in (0..self.list.len()).rev() {
            if si >= self.list[i].get_si() {
                return match self.list[i] {
                    Data::Tombstone(_) => None,
                    Data::Content((_, ref properties)) => Some(properties),
                };
            }
        }
        None
    }

}

enum Data {
    Tombstone(SnapshotId),
    Content((SnapshotId, Props)),
}

impl Data {
    fn is_tombstone(&self) -> bool {
        match *self {
            Data::Tombstone(_) => true,
            _ => false,
        }
    }

    fn get_si(&self) -> SnapshotId {
        match *self {
            Data::Tombstone(si) => si,
            Data::Content((si, _)) => si,
        }
    }
}
