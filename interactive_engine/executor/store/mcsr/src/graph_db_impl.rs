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

use std::collections::{HashMap, HashSet};
use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use crate::col_table::ColTable;
use crate::edge_trim::EdgeTrimJson;
use crate::error::GDBResult;
use crate::graph::{Direction, IndexType};
use crate::graph_db::{CsrTrait, GlobalCsrTrait, LocalEdge, LocalVertex, NbrIter, NbrOffsetIter};
use crate::ldbc_parser::LDBCVertexParser;
use crate::mcsr::MutableCsr;
use crate::schema::{CsrGraphSchema, Schema};
use crate::scsr::SingleCsr;
use crate::types::*;
use crate::utils::{Iter, LabeledIterator, LabeledRangeIterator, Range};
use crate::vertex_map::VertexMap;

pub trait BasicSubgraphOps<'a>: Send + Sync {
    type GIDType: Send + Sync + IndexType;
    type IIDType: Send + Sync + IndexType;

    fn get_src_label(&self) -> LabelId;

    fn get_dst_label(&self) -> LabelId;

    fn get_edge_label(&self) -> LabelId;

    fn get_vertex_num(&self) -> Self::IIDType;

    fn get_edge_num(&self) -> usize;

    fn degree(&self, src_id: Self::IIDType) -> i64;

    fn get_adj_list(&self, src_id: Self::IIDType) -> Option<NbrIter<Self::IIDType>>;

    fn get_adj_list_with_offset(&self, src_id: Self::IIDType) -> Option<NbrOffsetIter<Self::IIDType>>;

    fn get_internal_id(&self, id: Self::GIDType) -> Option<(LabelId, Self::IIDType)>;

    fn get_src_global_id(&self, id: Self::IIDType) -> Option<Self::GIDType>;

    fn get_dst_global_id(&self, id: Self::IIDType) -> Option<Self::GIDType>;

    fn get_vertex(&self, id: Self::GIDType) -> Self::IIDType;

    fn get_properties(&self) -> Option<&'a ColTable>;
}

#[derive(Copy, Clone)]
pub struct SubGraph<'a, G: Send + Sync + IndexType = DefaultId, I: Send + Sync + IndexType = InternalId> {
    pub csr: &'a MutableCsr<I>,
    pub vm: &'a VertexMap<G, I>,
    pub src_label: LabelId,
    pub dst_label: LabelId,
    pub e_label: LabelId,

    pub vertex_data: &'a ColTable,
    pub edge_data: Option<&'a ColTable>,
}

impl<'a, G, I> SubGraph<'a, G, I>
where
    G: Send + Sync + IndexType,
    I: Send + Sync + IndexType,
{
    pub fn new(
        csr: &'a MutableCsr<I>, vm: &'a VertexMap<G, I>, src_label: LabelId, dst_label: LabelId,
        e_label: LabelId, vertex_data: &'a ColTable, edge_data: Option<&'a ColTable>,
    ) -> Self {
        Self { csr, vm, src_label, dst_label, e_label, vertex_data, edge_data }
    }
}

impl<'a, G, I> BasicSubgraphOps<'a> for SubGraph<'a, G, I>
where
    G: Send + Sync + IndexType,
    I: Send + Sync + IndexType,
{
    type GIDType = G;
    type IIDType = I;

    fn get_src_label(&self) -> LabelId {
        self.src_label
    }

    fn get_dst_label(&self) -> LabelId {
        self.dst_label
    }

    fn get_edge_label(&self) -> LabelId {
        self.e_label
    }

    fn get_vertex_num(&self) -> Self::IIDType {
        self.csr.vertex_num()
    }

    fn get_edge_num(&self) -> usize {
        self.csr.edge_num()
    }

    fn degree(&self, src_id: Self::IIDType) -> i64 {
        self.csr.degree(src_id)
    }

    fn get_adj_list(&self, src_id: Self::IIDType) -> Option<NbrIter<Self::IIDType>> {
        self.csr.get_edges(src_id)
    }

    fn get_adj_list_with_offset(&self, src_id: I) -> Option<NbrOffsetIter<Self::IIDType>> {
        self.csr.get_edges_with_offset(src_id)
    }

    fn get_internal_id(&self, id: Self::GIDType) -> Option<(LabelId, Self::IIDType)> {
        self.vm.get_internal_id(id)
    }

    fn get_src_global_id(&self, id: Self::IIDType) -> Option<Self::GIDType> {
        self.vm.get_global_id(self.src_label, id)
    }

    fn get_dst_global_id(&self, id: Self::IIDType) -> Option<Self::GIDType> {
        self.vm.get_global_id(self.dst_label, id)
    }

    fn get_vertex(&self, id: Self::GIDType) -> Self::IIDType {
        self.vm.get_internal_id(id).unwrap().1
    }

    fn get_properties(&self) -> Option<&'a ColTable> {
        self.edge_data
    }
}

#[derive(Copy, Clone)]
pub struct SingleSubGraph<
    'a,
    G: Send + Sync + IndexType = DefaultId,
    I: Send + Sync + IndexType = InternalId,
> {
    pub csr: &'a SingleCsr<I>,
    pub vm: &'a VertexMap<G, I>,
    pub src_label: LabelId,
    pub dst_label: LabelId,
    pub e_label: LabelId,

    pub vertex_data: &'a ColTable,
    pub edge_data: Option<&'a ColTable>,
}

impl<'a, G, I> SingleSubGraph<'a, G, I>
where
    G: Send + Sync + IndexType,
    I: Send + Sync + IndexType,
{
    pub fn new(
        csr: &'a SingleCsr<I>, vm: &'a VertexMap<G, I>, src_label: LabelId, dst_label: LabelId,
        e_label: LabelId, vertex_data: &'a ColTable, edge_data: Option<&'a ColTable>,
    ) -> Self {
        Self { csr, vm, src_label, dst_label, e_label, vertex_data, edge_data }
    }

    pub fn desc(&self) {
        self.csr.desc();
    }
}

impl<'a, G, I> BasicSubgraphOps<'a> for SingleSubGraph<'a, G, I>
where
    G: Send + Sync + IndexType,
    I: Send + Sync + IndexType,
{
    type GIDType = G;
    type IIDType = I;

    fn get_src_label(&self) -> LabelId {
        self.src_label
    }

    fn get_dst_label(&self) -> LabelId {
        self.dst_label
    }

    fn get_edge_label(&self) -> LabelId {
        self.e_label
    }

    fn get_vertex_num(&self) -> Self::IIDType {
        self.csr.vertex_num()
    }

    fn get_edge_num(&self) -> usize {
        self.csr.edge_num()
    }

    fn degree(&self, src_id: Self::IIDType) -> i64 {
        self.csr.degree(src_id)
    }

    fn get_adj_list(&self, src_id: Self::IIDType) -> Option<NbrIter<Self::IIDType>> {
        self.csr.get_edges(src_id)
    }

    fn get_adj_list_with_offset(&self, src_id: Self::IIDType) -> Option<NbrOffsetIter<Self::IIDType>> {
        self.csr.get_edges_with_offset(src_id)
    }

    fn get_internal_id(&self, id: Self::GIDType) -> Option<(LabelId, Self::IIDType)> {
        self.vm.get_internal_id(id)
    }

    fn get_src_global_id(&self, id: Self::IIDType) -> Option<Self::GIDType> {
        self.vm.get_global_id(self.src_label, id)
    }

    fn get_dst_global_id(&self, id: Self::IIDType) -> Option<Self::GIDType> {
        self.vm.get_global_id(self.dst_label, id)
    }

    fn get_vertex(&self, id: Self::GIDType) -> Self::IIDType {
        self.vm.get_internal_id(id).unwrap().1
    }

    fn get_properties(&self) -> Option<&'a ColTable> {
        self.edge_data
    }
}

pub struct CsrDB<G: Send + Sync + IndexType = DefaultId, I: Send + Sync + IndexType = InternalId> {
    pub partition: usize,
    pub ie: Vec<Box<dyn CsrTrait<I>>>,
    pub oe: Vec<Box<dyn CsrTrait<I>>>,

    pub graph_schema: Arc<CsrGraphSchema>,
    pub vertex_prop_table: Vec<ColTable>,
    pub vertex_map: VertexMap<G, I>,

    pub edge_prop_table: HashMap<usize, ColTable>,

    pub vertex_label_num: usize,
    pub edge_label_num: usize,
}

pub trait BasicOps<G: IndexType + Sync + Send, I: IndexType + Sync + Send> {
    fn edge_label_to_index(
        &self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId, dir: Direction,
    ) -> usize;
    fn get_adj_list(
        &self, src_index: I, src_label: LabelId, dst_label: LabelId, edge_label: LabelId, dir: Direction,
    ) -> Option<NbrIter<I>>;

    fn index_to_local_vertex(&self, label: LabelId, index: I, with_property: bool) -> LocalVertex<G, I>;
    fn edge_ref_to_local_edge(
        &self, src_label: LabelId, src_lid: I, dst_label: LabelId, dst_lid: I, label: LabelId,
        dir: Direction, offset: usize,
    ) -> LocalEdge<G, I>;
}

impl<G, I> BasicOps<G, I> for CsrDB<G, I>
where
    G: Eq + IndexType + Send + Sync,
    I: IndexType + Send + Sync,
{
    fn edge_label_to_index(
        &self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId, dir: Direction,
    ) -> usize {
        match dir {
            Direction::Incoming => {
                dst_label as usize * self.vertex_label_num * self.edge_label_num
                    + src_label as usize * self.edge_label_num
                    + edge_label as usize
            }
            Direction::Outgoing => {
                src_label as usize * self.vertex_label_num * self.edge_label_num
                    + dst_label as usize * self.edge_label_num
                    + edge_label as usize
            }
        }
    }

    fn get_adj_list(
        &self, src_index: I, src_label: LabelId, dst_label: LabelId, edge_label: LabelId, dir: Direction,
    ) -> Option<NbrOffsetIter<I>> {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label, dir);
        match dir {
            Direction::Incoming => self.ie[index].get_edges_with_offset(src_index),
            Direction::Outgoing => self.oe[index].get_edges_with_offset(src_index),
        }
    }

    fn index_to_local_vertex(&self, label_id: LabelId, index: I, with_property: bool) -> LocalVertex<G, I> {
        if with_property {
            LocalVertex::with_property(
                index,
                label_id,
                &self.vertex_map.index_to_global_id[label_id as usize],
                &self.vertex_map.index_to_corner_global_id[label_id as usize],
                Some(&self.vertex_prop_table[label_id as usize]),
            )
        } else {
            LocalVertex::new(
                index,
                label_id,
                &self.vertex_map.index_to_global_id[label_id as usize],
                &self.vertex_map.index_to_corner_global_id[label_id as usize],
            )
        }
    }

    fn edge_ref_to_local_edge(
        &self, src_label: LabelId, src_lid: I, dst_label: LabelId, dst_lid: I, label: LabelId,
        dir: Direction, offset: usize,
    ) -> LocalEdge<G, I> {
        let index = self.edge_label_to_index(src_label, dst_label, label, dir);
        let properties = self.edge_prop_table.get(&index);
        match dir {
            Direction::Incoming => LocalEdge::new(
                dst_lid,
                src_lid,
                label,
                dst_label,
                src_label,
                &self.vertex_map,
                offset,
                properties,
            ),
            Direction::Outgoing => LocalEdge::new(
                src_lid,
                dst_lid,
                label,
                src_label,
                dst_label,
                &self.vertex_map,
                offset,
                properties,
            ),
        }
    }
}

impl<G, I> CsrDB<G, I>
where
    G: Eq + IndexType + Send + Sync,
    I: IndexType + Send + Sync,
{
    fn _get_adj_lists_vertices(
        &self, src_id: G, edge_labels: Option<&Vec<LabelId>>, dir: Direction,
    ) -> (Vec<LabelId>, Vec<NbrOffsetIter<I>>) {
        let mut iters = vec![];
        let mut labels = vec![];
        if let Some(index) = self.vertex_map.get_internal_id(src_id) {
            if edge_labels.is_none() {
                for dst_label in 0..self.vertex_label_num {
                    for edge_label in 0..self.edge_label_num {
                        let iter = self.get_adj_list(
                            index.1,
                            index.0,
                            dst_label as LabelId,
                            edge_label as LabelId,
                            dir,
                        );
                        if iter.is_some() {
                            iters.push(iter.unwrap());
                            labels.push(dst_label as LabelId);
                        }
                    }
                }
            } else {
                for dst_label in 0..self.vertex_label_num {
                    for edge_label in edge_labels.unwrap() {
                        let iter =
                            self.get_adj_list(index.1, index.0, dst_label as LabelId, *edge_label, dir);
                        if iter.is_some() {
                            iters.push(iter.unwrap());
                            labels.push(dst_label as LabelId);
                        }
                    }
                }
            }
        }
        (labels, iters)
    }

    fn _get_adj_lists_edges(
        &self, src_label: LabelId, src_lid: I, edge_labels: Option<&Vec<LabelId>>, dir: Direction,
    ) -> (Vec<(LabelId, LabelId)>, Vec<NbrOffsetIter<I>>) {
        let mut iters = vec![];
        let mut labels = vec![];
        if edge_labels.is_none() {
            for dst_label in 0..self.vertex_label_num {
                for edge_label in 0..self.edge_label_num {
                    let iter = self.get_adj_list(
                        src_lid,
                        src_label,
                        dst_label as LabelId,
                        edge_label as LabelId,
                        dir,
                    );
                    if iter.is_some() {
                        iters.push(iter.unwrap());
                        labels.push((dst_label as LabelId, edge_label as LabelId));
                    }
                }
            }
        } else {
            for dst_label in 0..self.vertex_label_num {
                for edge_label in edge_labels.unwrap() {
                    let iter =
                        self.get_adj_list(src_lid, src_label, dst_label as LabelId, *edge_label, dir);
                    if iter.is_some() {
                        iters.push(iter.unwrap());
                        labels.push((dst_label as LabelId, *edge_label));
                    }
                }
            }
        }
        (labels, iters)
    }

    pub fn serialize(&self, path: &str) -> GDBResult<()> {
        let root_dir = PathBuf::from_str(path).unwrap();
        let partition_dir = root_dir
            .join(DIR_BINARY_DATA)
            .join(format!("partition_{}", self.partition));
        create_dir_all(&partition_dir)?;

        for e_label_i in 0..self.edge_label_num {
            let edge_label_name = self.graph_schema.edge_label_names()[e_label_i].clone();
            for src_label_i in 0..self.vertex_label_num {
                let src_label_name = self.graph_schema.vertex_label_names()[src_label_i].clone();
                for dst_label_i in 0..self.vertex_label_num {
                    let dst_label_name = self.graph_schema.vertex_label_names()[dst_label_i].clone();
                    let index: usize = src_label_i * self.vertex_label_num * self.edge_label_num
                        + dst_label_i * self.edge_label_num
                        + e_label_i;
                    let ie_path = &partition_dir
                        .join(format!("ie_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    let ie_path_str = ie_path.to_str().unwrap().to_string();
                    let ie_csr = &self.ie[index];
                    if ie_csr.edge_num() != 0 {
                        ie_csr.serialize(&ie_path_str);
                    }
                    let oe_path = &partition_dir
                        .join(format!("oe_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    let oe_path_str = oe_path.to_str().unwrap().to_string();
                    let oe_csr = &self.oe[index];
                    if oe_csr.edge_num() != 0 {
                        oe_csr.serialize(&oe_path_str);
                    }
                }
            }
        }
        for (i, table) in self.vertex_prop_table.iter().enumerate() {
            let v_label_name = self.graph_schema.vertex_label_names()[i].clone();
            let table_path = &partition_dir.join(format!("vp_{}", v_label_name));
            let table_path_str = table_path.to_str().unwrap().to_string();
            table.serialize_table(&table_path_str);
        }

        let vm_path = &partition_dir.join("vm");
        let vm_path_str = vm_path.to_str().unwrap().to_string();
        self.vertex_map.serialize(&vm_path_str);

        Ok(())
    }

    pub fn deserialize(dir: &str, partition: usize, trim_json_path: Option<String>) -> GDBResult<Self> {
        let root_dir = PathBuf::from_str(dir).unwrap();
        let schema_path = root_dir
            .join(DIR_GRAPH_SCHEMA)
            .join(FILE_SCHEMA);
        let graph_schema = CsrGraphSchema::from_json_file(schema_path)?;
        let partition_dir = root_dir
            .join(DIR_BINARY_DATA)
            .join(format!("partition_{}", partition));

        let (ie_enable, oe_enable) = if let Some(trim_json_path) = &trim_json_path {
            let edge_trim_path = PathBuf::from_str(trim_json_path).unwrap();
            let file = File::open(edge_trim_path)?;
            let trim_json =
                serde_json::from_reader::<File, EdgeTrimJson>(file).map_err(std::io::Error::from)?;
            trim_json.get_enable_indexs(&graph_schema)
        } else {
            (HashSet::<usize>::new(), HashSet::<usize>::new())
        };

        let vertex_label_num = graph_schema.vertex_type_to_id.len();
        let edge_label_num = graph_schema.edge_type_to_id.len();

        let csr_num = vertex_label_num * vertex_label_num * edge_label_num;
        let mut ie: Vec<Box<dyn CsrTrait<I>>> = vec![];
        let mut oe: Vec<Box<dyn CsrTrait<I>>> = vec![];
        for _ in 0..csr_num {
            ie.push(Box::new(SingleCsr::<I>::new()));
            oe.push(Box::new(SingleCsr::<I>::new()));
        }

        for e_label_i in 0..edge_label_num {
            let edge_label_name = graph_schema.edge_label_names()[e_label_i].clone();
            for src_label_i in 0..vertex_label_num {
                let src_label_name = graph_schema.vertex_label_names()[src_label_i].clone();
                for dst_label_i in 0..vertex_label_num {
                    let dst_label_name = graph_schema.vertex_label_names()[dst_label_i].clone();
                    let index: usize = src_label_i * vertex_label_num * edge_label_num
                        + dst_label_i * edge_label_num
                        + e_label_i;

                    let ie_path = &partition_dir
                        .join(format!("ie_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    if Path::exists(ie_path) && (trim_json_path.is_none() || ie_enable.contains(&index)) {
                        info!("importing {}", ie_path.as_os_str().to_str().unwrap());
                        let path_str = ie_path.to_str().unwrap().to_string();
                        if graph_schema.is_single_ie(
                            src_label_i as LabelId,
                            e_label_i as LabelId,
                            dst_label_i as LabelId,
                        ) {
                            let mut ie_csr = SingleCsr::<I>::new();
                            ie_csr.deserialize(&path_str);
                            ie[index] = Box::new(ie_csr);
                        } else {
                            let mut ie_csr = MutableCsr::<I>::new();
                            ie_csr.deserialize(&path_str);
                            ie[index] = Box::new(ie_csr);
                        }
                    }

                    let oe_path = &partition_dir
                        .join(format!("oe_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    if Path::exists(oe_path) && (trim_json_path.is_none() || oe_enable.contains(&index)){
                        info!("importing {}", oe_path.as_os_str().to_str().unwrap());
                        let path_str = oe_path.to_str().unwrap().to_string();
                        if graph_schema.is_single_oe(
                            src_label_i as LabelId,
                            e_label_i as LabelId,
                            dst_label_i as LabelId,
                        ) {
                            let mut oe_csr = SingleCsr::<I>::new();
                            oe_csr.deserialize(&path_str);
                            oe[index] = Box::new(oe_csr);
                        } else {
                            let mut oe_csr = MutableCsr::<I>::new();
                            oe_csr.deserialize(&path_str);
                            oe[index] = Box::new(oe_csr);
                        }
                    }
                }
            }
        }
        info!("finished import csrs");
        info!("start import vertex properties");
        let mut vertex_prop_table = vec![];
        for i in 0..vertex_label_num {
            let v_label_name = graph_schema.vertex_label_names()[i].clone();
            let mut table = ColTable::new(vec![]);
            let table_path = &partition_dir.join(format!("vp_{}", v_label_name));
            let table_path_str = table_path.to_str().unwrap().to_string();
            info!("importing vertex property: {}, {}", v_label_name, table_path_str);
            table.deserialize_table(&table_path_str);
            vertex_prop_table.push(table);
        }
        info!("finished import vertex properties");

        info!("start import edge properties");
        let mut edge_prop_table = HashMap::new();
        for e_label_i in 0..edge_label_num {
            for src_label_i in 0..vertex_label_num {
                for dst_label_i in 0..vertex_label_num {
                    let edge_index = src_label_i * vertex_label_num * edge_label_num
                        + dst_label_i * edge_label_num
                        + e_label_i;
                    let src_label_name = graph_schema.vertex_label_names()[src_label_i].clone();
                    let dst_label_name = graph_schema.vertex_label_names()[dst_label_i].clone();
                    let edge_label_name = graph_schema.edge_label_names()[e_label_i].clone();
                    let edge_property_path = &partition_dir
                        .join(format!("ep_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    let edge_property_path_str = edge_property_path.to_str().unwrap().to_string();

                    if Path::new(&edge_property_path_str).exists() {
                        let mut table = ColTable::new(vec![]);
                        info!(
                            "importing edge property: {}_{}_{}, {}",
                            src_label_name, edge_label_name, dst_label_name, edge_property_path_str
                        );
                        table.deserialize_table(&edge_property_path_str);
                        edge_prop_table.insert(edge_index, table);
                    }
                }
            }
        }
        info!("finished import edge properties");

        let mut vertex_map = VertexMap::new(vertex_label_num);
        info!("start import native vertex map");
        let vm_path = &partition_dir.join("vm");
        let vm_path_str = vm_path.to_str().unwrap().to_string();
        vertex_map.deserialize(&vm_path_str);
        info!("finish import corner vertex map");

        Ok(Self {
            partition,
            ie,
            oe,
            graph_schema: Arc::new(graph_schema),
            vertex_prop_table,
            vertex_map,
            edge_prop_table,
            vertex_label_num,
            edge_label_num,
        })
    }

    pub fn get_sub_graph(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, dir: Direction,
    ) -> SubGraph<'_, G, I> {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label, dir);
        match dir {
            Direction::Outgoing => SubGraph::new(
                &self.oe[index]
                    .as_any()
                    .downcast_ref::<MutableCsr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.edge_prop_table.get(&index),
            ),
            Direction::Incoming => SubGraph::new(
                &self.ie[index]
                    .as_any()
                    .downcast_ref::<MutableCsr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.edge_prop_table.get(&index),
            ),
        }
    }

    pub fn get_single_sub_graph(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, dir: Direction,
    ) -> SingleSubGraph<'_, G, I> {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label, dir);
        match dir {
            Direction::Outgoing => SingleSubGraph::new(
                &self.oe[index]
                    .as_any()
                    .downcast_ref::<SingleCsr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.edge_prop_table.get(&index),
            ),
            Direction::Incoming => SingleSubGraph::new(
                &self.ie[index]
                    .as_any()
                    .downcast_ref::<SingleCsr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.edge_prop_table.get(&index),
            ),
        }
    }

    pub fn get_vertices_num(&self, label: LabelId) -> usize {
        self.vertex_map.vertex_num(label)
    }

    pub fn get_global_id(&self, id: I, label: LabelId) -> Option<G> {
        self.vertex_map.get_global_id(label, id)
    }

    pub fn get_internal_id(&self, id: G) -> I {
        self.vertex_map.get_internal_id(id).unwrap().1
    }

    pub fn get_partitioned_vertices(
        &self, labels: Option<&Vec<LabelId>>, worker_id: u32, worker_num: u32,
    ) -> Iter<LocalVertex<G, I>> {
        let local_id = worker_id % worker_num;
        if labels.is_none() {
            let mut iters = vec![];
            let mut got_labels = vec![];
            for v in 0..self.vertex_label_num {
                let vertex_count = self.vertex_map.vertex_num(v as LabelId);
                let partial_count = (vertex_count + worker_num as usize - 1) / worker_num as usize;
                let start_index = if local_id as usize * partial_count > vertex_count {
                    vertex_count
                } else {
                    local_id as usize * partial_count
                };
                let end_index = if (local_id + 1) as usize * partial_count > vertex_count {
                    vertex_count
                } else {
                    (local_id + 1) as usize * partial_count
                };
                iters.push(Range::new(I::new(start_index), I::new(end_index)).into_iter());
                got_labels.push(v as LabelId);
            }
            Iter::from_iter(
                LabeledIterator::new(got_labels, iters)
                    .map(move |(label, v)| self.index_to_local_vertex(label, v, true)),
            )
        } else if labels.unwrap().len() == 1 {
            let label = labels.unwrap()[0];
            let vertex_count = self.vertex_map.vertex_num(label);
            let partial_count = (vertex_count + worker_num as usize - 1) / worker_num as usize;
            let start_index = if local_id as usize * partial_count > vertex_count {
                vertex_count
            } else {
                local_id as usize * partial_count
            };
            let end_index = if (local_id + 1) as usize * partial_count > vertex_count {
                vertex_count
            } else {
                (local_id + 1) as usize * partial_count
            };
            let range = Range::<I>::new(I::new(start_index), I::new(end_index));
            Iter::from_iter(
                range
                    .into_iter()
                    .map(move |v| self.index_to_local_vertex(label, v, true)),
            )
        } else {
            let mut iters = vec![];
            let mut got_labels = vec![];
            for v in labels.unwrap() {
                let vertex_count = self.vertex_map.vertex_num(*v);
                let partial_count = (vertex_count + worker_num as usize - 1) / worker_num as usize;
                let start_index = if local_id as usize * partial_count > vertex_count {
                    vertex_count
                } else {
                    local_id as usize * partial_count
                };
                let end_index = if (local_id + 1) as usize * partial_count > vertex_count {
                    vertex_count
                } else {
                    (local_id + 1) as usize * partial_count
                };
                iters.push(Range::new(I::new(start_index), I::new(end_index)).into_iter());
                got_labels.push(*v);
            }
            Iter::from_iter(
                LabeledRangeIterator::new(got_labels, iters)
                    .map(move |(label, v)| self.index_to_local_vertex(label, v, true)),
            )
        }
    }

    pub fn get_partitioned_edges(
        &self, labels: Option<&Vec<LabelId>>, worker_id: u32, worker_num: u32,
    ) -> Iter<LocalEdge<G, I>> {
        let local_id = (worker_id % worker_num) as usize;
        let mut iters = vec![];
        let mut got_labels = vec![];
        if labels.is_none() {
            for src_label in 0..self.vertex_label_num {
                for dst_label in 0..self.vertex_label_num {
                    for edge_label in 0..self.edge_label_num {
                        let index = self.edge_label_to_index(
                            src_label as LabelId,
                            dst_label as LabelId,
                            edge_label as LabelId,
                            Direction::Outgoing,
                        );

                        if self.oe[index].edge_num() != 0 {
                            let edge_count = self.oe[index].edge_num();
                            let partial_count =
                                (edge_count + worker_num as usize - 1) / worker_num as usize;
                            iters.push(
                                Iter::from_iter_box(self.oe[index].get_all_edges())
                                    .skip(local_id * partial_count)
                                    .take(partial_count),
                            );
                            got_labels.push((
                                src_label as LabelId,
                                dst_label as LabelId,
                                edge_label as LabelId,
                            ));
                        }
                    }
                }
            }
        } else {
            for src_label in 0..self.vertex_label_num {
                for dst_label in 0..self.vertex_label_num {
                    for edge_label in labels.unwrap() {
                        let index = self.edge_label_to_index(
                            src_label as LabelId,
                            dst_label as LabelId,
                            *edge_label,
                            Direction::Outgoing,
                        );

                        if self.oe[index].edge_num() != 0 {
                            let edge_count = self.oe[index].edge_num();
                            let partial_count =
                                (edge_count + worker_num as usize - 1) / worker_num as usize;
                            iters.push(
                                Iter::from_iter_box(self.oe[index].get_all_edges())
                                    .skip(local_id * partial_count)
                                    .take(partial_count),
                            );
                            got_labels.push((src_label as LabelId, dst_label as LabelId, *edge_label));
                        }
                    }
                }
            }
        }
        Iter::from_iter(LabeledIterator::new(got_labels, iters).map(
            move |((src_label, dst_label, edge_label), (src_lid, e))| {
                let offset = if let Some(offset) = e.1 { *offset } else { 0 };
                self.edge_ref_to_local_edge(
                    src_label,
                    src_lid,
                    dst_label,
                    e.0.neighbor,
                    edge_label,
                    Direction::Outgoing,
                    offset,
                )
            },
        ))
    }

    pub fn is_single_oe_csr(&self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId) -> bool {
        self.graph_schema
            .is_single_oe(src_label, edge_label, dst_label)
    }

    pub fn is_single_ie_csr(&self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId) -> bool {
        self.graph_schema
            .is_single_ie(src_label, edge_label, dst_label)
    }
}

impl<G, I> GlobalCsrTrait<G, I> for CsrDB<G, I>
where
    G: Eq + IndexType + Send + Sync,
    I: IndexType + Send + Sync,
{
    fn get_adj_vertices(
        &self, src_id: G, edge_labels: Option<&Vec<LabelId>>, dir: Direction,
    ) -> Iter<LocalVertex<G, I>> {
        let (labels, iters) = self._get_adj_lists_vertices(src_id, edge_labels, dir);
        if labels.is_empty() {
            Iter::from_iter(vec![].into_iter())
        } else {
            Iter::from_iter(
                LabeledIterator::new(labels, iters)
                    .map(move |(label, e)| self.index_to_local_vertex(label, e.0.neighbor, false)),
            )
        }
    }

    fn get_adj_edges(
        &self, src_id: G, edge_labels: Option<&Vec<LabelId>>, dir: Direction,
    ) -> Iter<LocalEdge<G, I>> {
        if let Some(src_lid) = self.vertex_map.get_internal_id(src_id) {
            let (labels, iters) = self._get_adj_lists_edges(src_lid.0, src_lid.1, edge_labels, dir);
            Iter::from_iter(LabeledIterator::new(labels, iters).map(move |((dst_label, edge_label), e)| {
                let offset = if let Some(offset) = e.1 { *offset } else { 0 };
                self.edge_ref_to_local_edge(
                    src_lid.0,
                    src_lid.1,
                    dst_label,
                    e.0.neighbor,
                    edge_label,
                    dir,
                    offset,
                )
            }))
        } else {
            Iter::from_iter(vec![].into_iter())
        }
    }

    fn get_vertex(&self, id: G) -> Option<LocalVertex<G, I>> {
        if let Some(index) = self.vertex_map.get_internal_id(id) {
            Some(self.index_to_local_vertex(index.0, index.1, true))
        } else {
            None
        }
    }

    fn get_all_vertices(&self, labels: Option<&Vec<LabelId>>) -> Iter<LocalVertex<G, I>> {
        if labels.is_none() {
            let mut iters = vec![];
            let mut got_labels = vec![];
            for v in 0..self.vertex_label_num {
                iters.push(
                    Range::new(I::new(0), I::new(self.vertex_map.vertex_num(v as LabelId))).into_iter(),
                );
                got_labels.push(v as LabelId);
            }
            Iter::from_iter(
                LabeledIterator::new(got_labels, iters)
                    .map(move |(label, v)| self.index_to_local_vertex(label, v, true)),
            )
        } else if labels.unwrap().len() == 1 {
            let label = labels.unwrap()[0];
            let range = Range::<I>::new(I::new(0), I::new(self.vertex_map.vertex_num(label)));
            Iter::from_iter(
                range
                    .into_iter()
                    .map(move |v| self.index_to_local_vertex(label, v, true)),
            )
        } else {
            let mut iters = vec![];
            let mut got_labels = vec![];
            for v in labels.unwrap() {
                iters.push(Range::new(I::new(0), I::new(self.vertex_map.vertex_num(*v))).into_iter());
                got_labels.push(*v);
            }
            Iter::from_iter(
                LabeledRangeIterator::new(got_labels, iters)
                    .map(move |(label, v)| self.index_to_local_vertex(label, v, true)),
            )
        }
    }

    fn get_all_edges(&self, labels: Option<&Vec<LabelId>>) -> Iter<LocalEdge<G, I>> {
        let mut iters = vec![];
        let mut got_labels = vec![];
        if labels.is_none() {
            for src_label in 0..self.vertex_label_num {
                for dst_label in 0..self.vertex_label_num {
                    for edge_label in 0..self.edge_label_num {
                        let index = self.edge_label_to_index(
                            src_label as LabelId,
                            dst_label as LabelId,
                            edge_label as LabelId,
                            Direction::Outgoing,
                        );

                        if self.oe[index].edge_num() != 0 {
                            iters.push(Iter::from_iter_box(self.oe[index].get_all_edges()));
                            got_labels.push((
                                src_label as LabelId,
                                dst_label as LabelId,
                                edge_label as LabelId,
                            ));
                        }
                    }
                }
            }
        } else {
            for src_label in 0..self.vertex_label_num {
                for dst_label in 0..self.vertex_label_num {
                    for edge_label in labels.unwrap() {
                        let index = self.edge_label_to_index(
                            src_label as LabelId,
                            dst_label as LabelId,
                            *edge_label,
                            Direction::Outgoing,
                        );

                        if self.oe[index].edge_num() != 0 {
                            iters.push(Iter::from_iter_box(self.oe[index].get_all_edges()));
                            got_labels.push((src_label as LabelId, dst_label as LabelId, *edge_label));
                        }
                    }
                }
            }
        }
        Iter::from_iter(LabeledIterator::new(got_labels, iters).map(
            move |((src_label, dst_label, edge_label), (src_lid, e))| {
                let offset = if let Some(offset) = e.1 { *offset } else { 0 };
                self.edge_ref_to_local_edge(
                    src_label,
                    src_lid,
                    dst_label,
                    e.0.neighbor,
                    edge_label,
                    Direction::Outgoing,
                    offset,
                )
            },
        ))
    }

    fn count_all_vertices(&self, labels: Option<&Vec<LabelId>>) -> usize {
        if labels.is_none() {
            self.vertex_map.total_vertex_num()
        } else {
            let mut ret = 0_usize;
            for label in labels.unwrap() {
                ret += self.vertex_map.vertex_num(*label)
            }
            ret
        }
    }

    fn count_all_edges(&self, labels: Option<&Vec<LabelId>>) -> usize {
        let mut ret = 0_usize;
        if labels.is_none() {
            for csr in self.oe.iter() {
                ret += csr.edge_num();
            }
        } else {
            for label in labels.unwrap() {
                ret += self.oe[*label as usize].edge_num();
            }
        }
        ret
    }

    fn get_schema(&self) -> Arc<dyn Schema> {
        self.graph_schema.clone()
    }

    fn get_current_partition(&self) -> usize {
        self.partition
    }
}
