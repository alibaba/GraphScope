use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use rayon::iter::IntoParallelIterator;
use rayon::prelude::*;

use crate::bmcsr::BatchMutableCsr;
use crate::bmscsr::BatchMutableSingleCsr;
use crate::col_table::ColTable;
use crate::columns::{Item, RefItem};
use crate::csr::CsrTrait;
use crate::edge_trim::EdgeTrimJson;
use crate::error::GDBResult;
use crate::graph::{Direction, IndexType};
use crate::schema::CsrGraphSchema;
use crate::sub_graph::{SingleSubGraph, SubGraph};
use crate::types::*;
use crate::utils::{Iter, LabeledIterator, Range};
use crate::vertex_map::VertexMap;

/// A data structure to maintain a local view of the vertex.
#[derive(Debug, Clone)]
pub struct LocalVertex<'a, G: IndexType + Sync + Send, I: IndexType + Sync + Send> {
    /// The vertex's global id
    index: I,
    /// The vertex's label
    label: LabelId,
    /// A property reference maintains a `Row` view of the properties, which is either
    /// a reference or an owned structure, depending on the form of storage.
    ///
    table: Option<&'a ColTable>,
    id_list: &'a Vec<G>,
    corner_id_list: &'a Vec<G>,
}

impl<'a, G: IndexType + Sync + Send, I: IndexType + Sync + Send> LocalVertex<'a, G, I> {
    pub fn new(index: I, label: LabelId, id_list: &'a Vec<G>, corner_id_list: &'a Vec<G>) -> Self {
        LocalVertex { index, label, id_list, table: None, corner_id_list }
    }

    pub fn with_property(
        index: I, label: LabelId, id_list: &'a Vec<G>, corner_id_list: &'a Vec<G>,
        table: Option<&'a ColTable>,
    ) -> Self {
        LocalVertex { index, label, id_list, table, corner_id_list }
    }

    pub fn is_valid(&self) -> bool {
        self.get_id() != <G as IndexType>::max()
    }

    pub fn get_id(&self) -> G {
        let index = self.index.index();
        if index < self.id_list.len() {
            self.id_list[index]
        } else {
            self.corner_id_list[<I as IndexType>::max().index() - index - 1]
        }
    }

    pub fn get_label(&self) -> LabelId {
        self.label
    }

    pub fn get_property(&self, key: &str) -> Option<RefItem> {
        if let Some(prop) = self.table {
            prop.get_item(key, self.index.index())
        } else {
            None
        }
    }

    pub fn get_all_properties(&self) -> Option<HashMap<String, RefItem>> {
        if let Some(prop) = self.table {
            let mut property_table = HashMap::new();
            for head in prop.header.keys() {
                property_table.insert(head.clone(), prop.get_item(head, self.index.index()).unwrap());
            }
            Some(property_table)
        } else {
            None
        }
    }
}

/// A data structure to maintain a local view of the edge.
#[derive(Clone)]
pub struct LocalEdge<'a, G: IndexType + Sync + Send, I: IndexType + Sync + Send> {
    /// The start vertex's global id
    start: I,
    /// The end vertex's global id
    end: I,
    /// The edge label id
    label: LabelId,
    src_label: LabelId,
    dst_label: LabelId,

    offset: usize,
    /// A property reference maintains a `Row` view of the properties, which is either
    /// a reference or an owned structure, depending on the form of storage.
    table: Option<&'a ColTable>,

    vertex_map: &'a VertexMap<G, I>,
}

impl<'a, G: IndexType + Sync + Send, I: IndexType + Sync + Send> LocalEdge<'a, G, I> {
    pub fn new(
        start: I, end: I, label: LabelId, src_label: LabelId, dst_label: LabelId,
        vertex_map: &'a VertexMap<G, I>, offset: usize, properties: Option<&'a ColTable>,
    ) -> Self {
        LocalEdge { start, end, label, src_label, dst_label, offset, table: properties, vertex_map }
    }

    pub fn get_src_id(&self) -> G {
        self.vertex_map
            .get_global_id(self.src_label, self.start)
            .unwrap()
    }

    pub fn get_dst_id(&self) -> G {
        self.vertex_map
            .get_global_id(self.dst_label, self.end)
            .unwrap()
    }

    pub fn get_src_label(&self) -> LabelId {
        self.src_label
    }

    pub fn get_offset(&self) -> usize {
        self.offset
    }

    pub fn get_dst_label(&self) -> LabelId {
        self.dst_label
    }

    pub fn get_label(&self) -> LabelId {
        self.label
    }

    pub fn get_src_lid(&self) -> I {
        self.start
    }

    pub fn get_dst_lid(&self) -> I {
        self.end
    }

    pub fn get_property(&self, key: &str) -> Option<RefItem> {
        if let Some(prop) = self.table {
            prop.get_item(key, self.offset)
        } else {
            None
        }
    }

    pub fn get_all_properties(&self) -> Option<HashMap<String, RefItem>> {
        if let Some(prop) = self.table {
            let mut property_table = HashMap::new();
            for head in prop.header.keys() {
                property_table.insert(head.clone(), prop.get_item(head, self.offset).unwrap());
            }
            Some(property_table)
        } else {
            None
        }
    }
}

pub struct GraphDB<G: Send + Sync + IndexType = DefaultId, I: Send + Sync + IndexType = InternalId> {
    pub partition: usize,
    pub ie: Vec<Box<dyn CsrTrait<I>>>,
    pub oe: Vec<Box<dyn CsrTrait<I>>>,

    pub graph_schema: Arc<CsrGraphSchema>,

    pub vertex_map: VertexMap<G, I>,

    pub vertex_prop_table: Vec<ColTable>,
    pub ie_edge_prop_table: HashMap<usize, ColTable>,
    pub oe_edge_prop_table: HashMap<usize, ColTable>,

    pub vertex_label_num: usize,
    pub edge_label_num: usize,
}

impl<G, I> GraphDB<G, I>
where
    G: Eq + IndexType + Send + Sync,
    I: IndexType + Send + Sync,
{
    pub fn edge_label_to_index(
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

    pub fn get_vertices_num(&self, label: LabelId) -> usize {
        self.vertex_map.vertex_num(label)
    }

    pub fn get_edges_num(&self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId) -> usize {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label, Direction::Outgoing);
        self.oe[index].edge_num()
    }

    pub fn get_max_edge_offset(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, dir: Direction,
    ) -> usize {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label, Direction::Outgoing);
        match dir {
            Direction::Incoming => self.ie[index].max_edge_offset(),
            Direction::Outgoing => self.oe[index].max_edge_offset(),
        }
    }

    pub fn get_global_id(&self, id: I, label: LabelId) -> Option<G> {
        self.vertex_map.get_global_id(label, id)
    }

    pub fn get_internal_id(&self, id: G) -> I {
        self.vertex_map.get_internal_id(id).unwrap().1
    }

    pub fn get_internal_id_beta(&self, id: G) -> Option<I> {
        if let Some((_, id)) = self.vertex_map.get_internal_id(id) {
            Some(id)
        } else {
            None
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

    pub fn get_all_vertices(&self, labels: Option<&Vec<LabelId>>) -> Iter<LocalVertex<G, I>> {
        if labels.is_none() {
            let mut iters = vec![];
            let mut got_labels = vec![];
            for v in 0..self.vertex_label_num {
                iters.push(Range::new(I::new(0), I::new(self.get_vertices_num(v as LabelId))).into_iter());
                got_labels.push(v as LabelId)
            }
            Iter::from_iter(
                LabeledIterator::new(got_labels, iters)
                    .map(move |(label, index)| self.index_to_local_vertex(label, index, true)),
            )
        } else if labels.unwrap().len() == 1 {
            let label = labels.unwrap()[0];
            let range = Range::new(I::new(0), I::new(self.get_vertices_num(label)));
            Iter::from_iter(
                range
                    .into_iter()
                    .map(move |index| self.index_to_local_vertex(label, index, true)),
            )
        } else {
            let mut iters = vec![];
            let mut got_labels = vec![];
            for v in labels.unwrap() {
                iters.push(Range::new(I::new(0), I::new(self.get_vertices_num(*v))).into_iter());
                got_labels.push(*v)
            }
            Iter::from_iter(
                LabeledIterator::new(got_labels, iters)
                    .map(move |(label, index)| self.index_to_local_vertex(label, index, true)),
            )
        }
    }

    pub fn deserialize(dir: &str, partition: usize, trim_json_path: Option<String>) -> GDBResult<Self> {
        let root_dir = PathBuf::from_str(dir).unwrap();
        let schema_path = root_dir
            .join(DIR_GRAPH_SCHEMA)
            .join(FILE_SCHEMA);
        let graph_schema = CsrGraphSchema::from_json_file(schema_path)?;
        // graph_schema.desc();
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
            ie.push(Box::new(BatchMutableSingleCsr::<I>::new()));
            oe.push(Box::new(BatchMutableSingleCsr::<I>::new()));
        }

        let mut csr_tasks = vec![];
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
                        csr_tasks.push((src_label_i, e_label_i, dst_label_i, Direction::Incoming));
                    }
                    let oe_path = &partition_dir
                        .join(format!("oe_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    if Path::exists(oe_path) && (trim_json_path.is_none() || oe_enable.contains(&index)) {
                        csr_tasks.push((src_label_i, e_label_i, dst_label_i, Direction::Outgoing));
                    }
                }
            }
        }

        let csr_return: Vec<Box<dyn CsrTrait<I>>> = csr_tasks
            .par_iter()
            .map(|(src_label, edge_label, dst_label, dir)| {
                let src_label_name = graph_schema.vertex_label_names()[*src_label].clone();
                let dst_label_name = graph_schema.vertex_label_names()[*dst_label].clone();
                let edge_label_name = graph_schema.edge_label_names()[*edge_label].clone();
                let index: usize =
                    src_label * vertex_label_num * edge_label_num + dst_label * edge_label_num + edge_label;
                if *dir == Direction::Outgoing {
                    let oe_path = &partition_dir
                        .join(format!("oe_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    if Path::exists(oe_path) && (trim_json_path.is_none() || oe_enable.contains(&index)) {
                        info!("importing {}", oe_path.as_os_str().to_str().unwrap());
                        let path_str = oe_path.to_str().unwrap().to_string();
                        let mut oe_csr: Box<dyn CsrTrait<I>> = if graph_schema.is_single_oe(
                            *src_label as LabelId,
                            *edge_label as LabelId,
                            *dst_label as LabelId,
                        ) {
                            Box::new(BatchMutableSingleCsr::<I>::new())
                        } else {
                            Box::new(BatchMutableCsr::<I>::new())
                        };
                        oe_csr.deserialize(&path_str);
                        return oe_csr;
                    }
                } else {
                    let ie_path = &partition_dir
                        .join(format!("ie_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    if Path::exists(ie_path) && (trim_json_path.is_none() || ie_enable.contains(&index)) {
                        info!("importing {}", ie_path.as_os_str().to_str().unwrap());
                        let path_str = ie_path.to_str().unwrap().to_string();
                        let mut ie_csr: Box<dyn CsrTrait<I>> = if graph_schema.is_single_ie(
                            *src_label as LabelId,
                            *edge_label as LabelId,
                            *dst_label as LabelId,
                        ) {
                            Box::new(BatchMutableSingleCsr::<I>::new())
                        } else {
                            Box::new(BatchMutableCsr::<I>::new())
                        };
                        ie_csr.deserialize(&path_str);
                        return ie_csr;
                    }
                }
                Box::new(BatchMutableSingleCsr::<I>::new())
            })
            .collect();

        for ((src_label_i, e_label_i, dst_label_i, dir), csr) in csr_tasks
            .into_iter()
            .zip(csr_return.into_iter())
        {
            let index: usize =
                src_label_i * vertex_label_num * edge_label_num + dst_label_i * edge_label_num + e_label_i;
            if dir == Direction::Outgoing {
                oe[index] = csr;
            } else {
                ie[index] = csr;
            }
        }

        let vertex_prop_table: Vec<ColTable> = (0..vertex_label_num)
            .into_par_iter()
            .map(|v_label| {
                let v_label_name = graph_schema.vertex_label_names()[v_label].clone();
                let mut table = ColTable::new(vec![]);
                let table_path = &partition_dir.join(format!("vp_{}", v_label_name));
                let table_path_str = table_path.to_str().unwrap().to_string();
                table.deserialize_table(&table_path_str);
                table
            })
            .collect();

        let mut oe_edge_prop_table = HashMap::new();
        let mut ie_edge_prop_table = HashMap::new();
        for e_label_i in 0..edge_label_num {
            for src_label_i in 0..vertex_label_num {
                for dst_label_i in 0..vertex_label_num {
                    let edge_index = src_label_i * vertex_label_num * edge_label_num
                        + dst_label_i * edge_label_num
                        + e_label_i;
                    let src_label_name = graph_schema.vertex_label_names()[src_label_i].clone();
                    let dst_label_name = graph_schema.vertex_label_names()[dst_label_i].clone();
                    let edge_label_name = graph_schema.edge_label_names()[e_label_i].clone();

                    let oe_edge_property_path = &partition_dir
                        .join(format!("oep_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    let oe_edge_property_path_str = oe_edge_property_path
                        .to_str()
                        .unwrap()
                        .to_string();
                    if Path::new(&oe_edge_property_path_str).exists() {
                        let mut table = ColTable::new(vec![]);
                        info!(
                            "importing oe edge property: {}_{}_{}, {}",
                            src_label_name, edge_label_name, dst_label_name, oe_edge_property_path_str
                        );
                        table.deserialize_table(&oe_edge_property_path_str);
                        oe_edge_prop_table.insert(edge_index, table);
                    }

                    let ie_edge_property_path = &partition_dir
                        .join(format!("iep_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    let ie_edge_property_path_str = ie_edge_property_path
                        .to_str()
                        .unwrap()
                        .to_string();
                    if Path::new(&ie_edge_property_path_str).exists() {
                        let mut table = ColTable::new(vec![]);
                        info!(
                            "importing ie edge property: {}_{}_{}, {}",
                            src_label_name, edge_label_name, dst_label_name, oe_edge_property_path_str
                        );
                        table.deserialize_table(&ie_edge_property_path_str);
                        ie_edge_prop_table.insert(edge_index, table);
                    }
                }
            }
        }

        let mut vertex_map = VertexMap::new(vertex_label_num);
        let vm_path = &partition_dir.join("vm");
        let vm_path_str = vm_path.to_str().unwrap().to_string();
        vertex_map.deserialize(&vm_path_str);

        Ok(Self {
            partition,
            ie,
            oe,
            graph_schema: Arc::new(graph_schema),
            vertex_prop_table,
            vertex_map,
            ie_edge_prop_table,
            oe_edge_prop_table,
            vertex_label_num,
            edge_label_num,
        })
    }

    pub fn get_sub_graph(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, dir: Direction,
    ) -> SubGraph<'_, G, I> {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label, dir);
        info!(
            "get_sub_graph: {} - {} - {}, {:?}",
            self.graph_schema.vertex_label_names()[src_label as usize],
            self.graph_schema.edge_label_names()[edge_label as usize],
            self.graph_schema.vertex_label_names()[dst_label as usize],
            dir
        );
        match dir {
            Direction::Outgoing => SubGraph::new(
                &self.oe[index]
                    .as_any()
                    .downcast_ref::<BatchMutableCsr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.oe_edge_prop_table.get(&index),
            ),
            Direction::Incoming => SubGraph::new(
                &self.ie[index]
                    .as_any()
                    .downcast_ref::<BatchMutableCsr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.ie_edge_prop_table.get(&index),
            ),
        }
    }

    pub fn get_single_sub_graph(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, dir: Direction,
    ) -> SingleSubGraph<'_, G, I> {
        info!(
            "get_single_sub_graph: {} - {} - {}, {:?}",
            self.graph_schema.vertex_label_names()[src_label as usize],
            self.graph_schema.edge_label_names()[edge_label as usize],
            self.graph_schema.vertex_label_names()[dst_label as usize],
            dir
        );
        let index = self.edge_label_to_index(src_label, dst_label, edge_label, dir);
        match dir {
            Direction::Outgoing => SingleSubGraph::new(
                &self.oe[index]
                    .as_any()
                    .downcast_ref::<BatchMutableSingleCsr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.oe_edge_prop_table.get(&index),
            ),
            Direction::Incoming => SingleSubGraph::new(
                &self.ie[index]
                    .as_any()
                    .downcast_ref::<BatchMutableSingleCsr<I>>()
                    .unwrap(),
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.ie_edge_prop_table.get(&index),
            ),
        }
    }

    pub fn insert_vertex(&mut self, label: LabelId, id: G, properties: Option<Vec<Item>>) {
        let lid = self.vertex_map.add_vertex(id, label);
        if let Some(properties) = properties {
            self.vertex_prop_table[label as usize].insert(lid.index(), &properties);
        }
    }
}
