use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use crate::bmcsr::{BatchMutableCsr, BatchMutableCsrBuilder};
use crate::bmscsr::BatchMutableSingleCsr;
use crate::col_table::ColTable;
use crate::columns::{Item, RefItem};
use crate::csr::CsrTrait;
use crate::edge_trim::EdgeTrimJson;
use crate::error::GDBResult;
use crate::graph::{Direction, IndexType};
use crate::schema::{CsrGraphSchema, Schema};
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

pub struct GraphDBModification<
    G: Send + Sync + IndexType = DefaultId,
    I: Send + Sync + IndexType = InternalId,
> {
    pub delete_edges: Vec<Vec<(G, G)>>,
    pub delete_outgoing_edges: Vec<HashSet<(I, I)>>,
    pub delete_incoming_edges: Vec<HashSet<(I, I)>>,

    pub delete_vertices: Vec<Vec<G>>,

    pub add_edges: Vec<Vec<(G, G)>>,
    pub add_edge_prop_tables: HashMap<usize, ColTable>,

    pub vertex_label_num: usize,
    pub edge_label_num: usize,
}

impl<G, I> GraphDBModification<G, I>
where
    G: Eq + IndexType + Send + Sync,
    I: Send + Sync + IndexType,
{
    pub fn new() -> Self {
        Self {
            delete_edges: vec![],
            delete_outgoing_edges: vec![],
            delete_incoming_edges: vec![],
            delete_vertices: vec![],

            add_edges: vec![],
            add_edge_prop_tables: HashMap::new(),

            vertex_label_num: 0,
            edge_label_num: 0,
        }
    }

    pub fn edge_label_to_index(
        &self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId,
    ) -> usize {
        src_label as usize * self.vertex_label_num * self.edge_label_num
            + dst_label as usize * self.edge_label_num
            + edge_label as usize
    }

    pub fn edge_label_tuple_num(&self) -> usize {
        self.vertex_label_num * self.vertex_label_num * self.edge_label_num
    }

    pub fn init(&mut self, graph_schema: &CsrGraphSchema) {
        self.vertex_label_num = graph_schema.vertex_label_names().len();
        self.edge_label_num = graph_schema.edge_label_names().len();

        self.delete_edges
            .resize(self.edge_label_tuple_num(), vec![]);
        self.delete_outgoing_edges
            .resize(self.edge_label_tuple_num(), HashSet::new());
        self.delete_incoming_edges
            .resize(self.edge_label_tuple_num(), HashSet::new());
        self.delete_vertices
            .resize(self.vertex_label_num, vec![]);
        self.add_edges
            .resize(self.edge_label_tuple_num(), vec![]);

        for e_label_i in 0..self.edge_label_num {
            for src_label_i in 0..self.vertex_label_num {
                for dst_label_i in 0..self.vertex_label_num {
                    let mut header = vec![];
                    if let Some(headers) = graph_schema.get_edge_header(
                        src_label_i as LabelId,
                        e_label_i as LabelId,
                        dst_label_i as LabelId,
                    ) {
                        for pair in headers {
                            header.push((pair.1.clone(), pair.0.clone()));
                        }
                    }
                    if !header.is_empty() {
                        let index = self.edge_label_to_index(
                            src_label_i as LabelId,
                            dst_label_i as LabelId,
                            e_label_i as LabelId,
                        );
                        self.add_edge_prop_tables
                            .insert(index, ColTable::new(header));
                    }
                }
            }
        }
    }

    pub fn delete_vertex(&mut self, label: LabelId, id: G) {
        let index = label as usize;
        self.delete_vertices[index].push(id);
    }

    pub fn insert_edge(
        &mut self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId, src: G, dst: G,
        properties: Option<Vec<Item>>,
    ) {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label);
        self.insert_edge_opt(index, src, dst, properties);
    }

    pub fn insert_edge_opt(&mut self, index: usize, src: G, dst: G, properties: Option<Vec<Item>>) {
        self.add_edges[index].push((src, dst));
        if let Some(properties) = properties {
            self.add_edge_prop_tables
                .get_mut(&index)
                .unwrap()
                .push(&properties);
        }
    }

    pub fn delete_edge(
        &mut self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId, src: G, dst: G,
    ) {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label);
        self.delete_edge_opt(index, src, dst);
    }

    pub fn delete_edge_opt(&mut self, index: usize, src: G, dst: G) {
        self.delete_edges[index].push((src, dst));
    }

    pub fn delete_outgoing_edge_opt(&mut self, index: usize, src: I, dst: I) {
        self.delete_outgoing_edges[index].insert((src, dst));
    }

    pub fn delete_incoming_edge_opt(&mut self, index: usize, src: I, dst: I) {
        self.delete_incoming_edges[index].insert((src, dst));
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

    pub modification: GraphDBModification<G, I>,
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

    pub fn get_global_id(&self, id: I, label: LabelId) -> Option<G> {
        self.vertex_map.get_global_id(label, id)
    }

    pub fn get_internal_id(&self, id: G) -> I {
        self.vertex_map.get_internal_id(id).unwrap().1
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
                            let mut ie_csr = BatchMutableSingleCsr::<I>::new();
                            ie_csr.deserialize(&path_str);
                            ie[index] = Box::new(ie_csr);
                        } else {
                            let mut ie_csr = BatchMutableCsr::<I>::new();
                            ie_csr.deserialize(&path_str);
                            ie[index] = Box::new(ie_csr);
                        }
                    }

                    let oe_path = &partition_dir
                        .join(format!("oe_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    if Path::exists(oe_path) && (trim_json_path.is_none() || oe_enable.contains(&index)) {
                        info!("importing {}", oe_path.as_os_str().to_str().unwrap());
                        let path_str = oe_path.to_str().unwrap().to_string();
                        if graph_schema.is_single_oe(
                            src_label_i as LabelId,
                            e_label_i as LabelId,
                            dst_label_i as LabelId,
                        ) {
                            let mut oe_csr = BatchMutableSingleCsr::<I>::new();
                            oe_csr.deserialize(&path_str);
                            oe[index] = Box::new(oe_csr);
                        } else {
                            let mut oe_csr = BatchMutableCsr::<I>::new();
                            oe_csr.deserialize(&path_str);
                            oe[index] = Box::new(oe_csr);
                        }
                    }
                }
            }
        }
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

        let mut modification = GraphDBModification::new();
        modification.init(&graph_schema);

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
            modification,
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

    pub fn insert_edges_to_oe_csr(
        &mut self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId, edges: &Vec<(I, I)>,
        table: &Option<ColTable>,
    ) {
        let csr_index = self.edge_label_to_index(src_label, dst_label, edge_label, Direction::Outgoing);
        let mut builder = BatchMutableCsrBuilder::new();
        let new_vertex_num = self.vertex_map.vertex_num(src_label);
        let mut new_degree = vec![0 as i64; new_vertex_num];
        for (src, _) in edges {
            new_degree[src.index()] += 1;
        }
        {
            let oe = self.oe[csr_index]
                .as_any()
                .downcast_ref::<BatchMutableCsr<I>>()
                .unwrap();
            let old_vertex_num = oe.vertex_num();
            for v in 0..old_vertex_num.index() {
                new_degree[v] += oe.degree(I::new(v)) as i64;
            }
        }
        builder.init(&new_degree, 1.0);
        if let Some(input_table) = table {
            let mut new_table = ColTable::new({
                let cols = self
                    .graph_schema
                    .get_edge_header(src_label, edge_label, dst_label)
                    .unwrap();
                let mut header = vec![];
                for pair in cols {
                    header.push((pair.1.clone(), pair.0.clone()));
                }
                header
            });

            {
                let oe = self.oe[csr_index]
                    .as_any()
                    .downcast_ref::<BatchMutableCsr<I>>()
                    .unwrap();
                let old_table = self.oe_edge_prop_table.get(&csr_index).unwrap();
                let old_vertex_num = oe.vertex_num().index();
                for v in 0..old_vertex_num {
                    for (nbr, offset) in oe.get_edges_with_offset(I::new(v)).unwrap() {
                        let new_offset = builder.put_edge(I::new(v), nbr).unwrap();
                        let row = old_table.get_row(offset).unwrap();
                        new_table.insert(new_offset, &row);
                    }
                }
                for (index, (src, dst)) in edges.iter().enumerate() {
                    let new_offset = builder.put_edge(*src, *dst).unwrap();
                    let row = input_table.get_row(index).unwrap();
                    new_table.insert(new_offset, &row);
                }
            }

            self.oe_edge_prop_table
                .insert(csr_index, new_table);
        } else {
            {
                let oe = self.oe[csr_index]
                    .as_any()
                    .downcast_ref::<BatchMutableCsr<I>>()
                    .unwrap();
                let old_vertex_num = oe.vertex_num().index();
                for v in 0..old_vertex_num {
                    for nbr in oe.get_edges(I::new(v)).unwrap() {
                        builder.put_edge(I::new(v), *nbr).unwrap();
                    }
                }
                for (src, dst) in edges {
                    builder.put_edge(*src, *dst).unwrap();
                }
            }
        }
        self.oe[csr_index] = Box::new(builder.finish().unwrap());
    }

    pub fn insert_edges_to_ie_csr(
        &mut self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId, edges: &Vec<(I, I)>,
        table: &Option<ColTable>,
    ) {
        let csr_index = self.edge_label_to_index(src_label, dst_label, edge_label, Direction::Outgoing);
        let mut builder = BatchMutableCsrBuilder::new();
        let new_vertex_num = self.vertex_map.vertex_num(dst_label);
        let mut new_degree = vec![0 as i64; new_vertex_num];
        for (_, dst) in edges {
            new_degree[dst.index()] += 1;
        }
        {
            let ie = self.ie[csr_index]
                .as_any()
                .downcast_ref::<BatchMutableCsr<I>>()
                .unwrap();
            let old_vertex_num = ie.vertex_num();
            for v in 0..old_vertex_num.index() {
                new_degree[v] += ie.degree(I::new(v)) as i64;
            }
        }
        builder.init(&new_degree, 1.0);
        if let Some(input_table) = table {
            let mut new_table = ColTable::new({
                let cols = self
                    .graph_schema
                    .get_edge_header(src_label, edge_label, dst_label)
                    .unwrap();
                let mut header = vec![];
                for pair in cols {
                    header.push((pair.1.clone(), pair.0.clone()));
                }
                header
            });

            {
                let ie = self.ie[csr_index]
                    .as_any()
                    .downcast_ref::<BatchMutableCsr<I>>()
                    .unwrap();
                let old_table = self.ie_edge_prop_table.get(&csr_index).unwrap();
                let old_vertex_num = ie.vertex_num().index();
                for v in 0..old_vertex_num {
                    for (nbr, offset) in ie.get_edges_with_offset(I::new(v)).unwrap() {
                        let new_offset = builder.put_edge(I::new(v), nbr).unwrap();
                        let row = old_table.get_row(offset).unwrap();
                        new_table.insert(new_offset, &row);
                    }
                }
                for (index, (src, dst)) in edges.iter().enumerate() {
                    let new_offset = builder.put_edge(*dst, *src).unwrap();
                    let row = input_table.get_row(index).unwrap();
                    new_table.insert(new_offset, &row);
                }
            }

            self.ie_edge_prop_table
                .insert(csr_index, new_table);
        } else {
            {
                let ie = self.ie[csr_index]
                    .as_any()
                    .downcast_ref::<BatchMutableCsr<I>>()
                    .unwrap();
                let old_vertex_num = ie.vertex_num().index();
                for v in 0..old_vertex_num {
                    for nbr in ie.get_edges(I::new(v)).unwrap() {
                        builder.put_edge(I::new(v), *nbr).unwrap();
                    }
                }
                for (src, dst) in edges {
                    builder.put_edge(*dst, *src).unwrap();
                }
            }
        }
        self.ie[csr_index] = Box::new(builder.finish().unwrap());
    }

    pub fn insert_edges_to_oe_single_csr(
        &mut self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId, edges: &Vec<(I, I)>,
        table: &Option<ColTable>,
    ) {
        let csr_index = self.edge_label_to_index(src_label, dst_label, edge_label, Direction::Outgoing);
        let oe = self.oe[csr_index]
            .as_mut_any()
            .downcast_mut::<BatchMutableSingleCsr<I>>()
            .unwrap();
        oe.resize_vertex(self.vertex_map.vertex_num(src_label));
        if let Some(new_table) = table {
            let old_table = self
                .oe_edge_prop_table
                .get_mut(&csr_index)
                .unwrap();
            for (index, (src, dst)) in edges.iter().enumerate() {
                oe.insert_edge(*src, *dst);
                let row = new_table.get_row(index).unwrap();
                old_table.insert(src.index(), &row);
            }
        } else {
            for (src, dst) in edges {
                oe.insert_edge(*src, *dst);
            }
        }
    }

    pub fn insert_edges_to_ie_single_csr(
        &mut self, src_label: LabelId, dst_label: LabelId, edge_label: LabelId, edges: &Vec<(I, I)>,
        table: &Option<ColTable>,
    ) {
        let csr_index = self.edge_label_to_index(src_label, dst_label, edge_label, Direction::Outgoing);
        let ie = self.ie[csr_index]
            .as_mut_any()
            .downcast_mut::<BatchMutableSingleCsr<I>>()
            .unwrap();
        ie.resize_vertex(self.vertex_map.vertex_num(dst_label));
        if let Some(new_table) = table {
            let old_table = self
                .ie_edge_prop_table
                .get_mut(&csr_index)
                .unwrap();
            for (index, (src, dst)) in edges.iter().enumerate() {
                ie.insert_edge(*dst, *src);
                let row = new_table.get_row(index).unwrap();
                old_table.insert(dst.index(), &row);
            }
        } else {
            for (src, dst) in edges {
                ie.insert_edge(*dst, *src);
            }
        }
    }

    pub fn insert_edges(
        &mut self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, edges: Vec<(G, G)>,
        table: Option<ColTable>,
    ) {
        let mut parsed_edges = vec![];
        for (src, dst) in edges {
            let (got_src_label, src_lid) = self.vertex_map.get_internal_id(src).unwrap();
            let (got_dst_label, dst_lid) = self.vertex_map.get_internal_id(dst).unwrap();
            if got_src_label != src_label || got_dst_label != dst_label {
                warn!("insert edges with wrong label");
                parsed_edges.push((<I as IndexType>::max(), <I as IndexType>::max()));
                continue;
            }
            parsed_edges.push((src_lid, dst_lid));
        }
        let oe_single = self
            .graph_schema
            .is_single_oe(src_label, edge_label, dst_label);
        let ie_single = self
            .graph_schema
            .is_single_ie(src_label, edge_label, dst_label);

        if oe_single {
            self.insert_edges_to_oe_single_csr(src_label, dst_label, edge_label, &parsed_edges, &table);
        } else {
            self.insert_edges_to_oe_csr(src_label, dst_label, edge_label, &parsed_edges, &table);
        }

        if ie_single {
            self.insert_edges_to_ie_single_csr(src_label, dst_label, edge_label, &parsed_edges, &table);
        } else {
            self.insert_edges_to_ie_csr(src_label, dst_label, edge_label, &parsed_edges, &table);
        }
    }

    pub fn insert_vertex(&mut self, label: LabelId, id: G, properties: Option<Vec<Item>>) {
        let lid = self.vertex_map.add_vertex(id, label);
        if let Some(properties) = properties {
            self.vertex_prop_table[label as usize].insert(lid.index(), &properties);
        }
    }

    pub fn delete_edges(
        &mut self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
        src_delete_set: &HashSet<I>, dst_delete_set: &HashSet<I>, edges: &HashSet<(I, I)>,
    ) {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label, Direction::Outgoing);
        let mut oe_to_delete = HashSet::new();
        let mut ie_to_delete = HashSet::new();
        {
            let oe_csr = &self.oe[index];
            for v in src_delete_set.iter() {
                if let Some(oe_list) = oe_csr.get_edges(*v) {
                    for e in oe_list {
                        if !dst_delete_set.contains(e) {
                            oe_to_delete.insert((*v, *e));
                        }
                    }
                }
            }
            let ie_csr = &self.ie[index];
            for v in dst_delete_set.iter() {
                if let Some(ie_list) = ie_csr.get_edges(*v) {
                    for e in ie_list {
                        if !src_delete_set.contains(e) {
                            ie_to_delete.insert((*e, *v));
                        }
                    }
                }
            }
        }
        self.oe[index].delete_vertices(src_delete_set);
        self.oe[index].delete_edges(&edges, false);
        self.oe[index].delete_edges(&ie_to_delete, false);
        self.ie[index].delete_vertices(dst_delete_set);
        self.ie[index].delete_edges(&edges, true);
        self.ie[index].delete_edges(&oe_to_delete, true);
    }
}
