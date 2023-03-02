use std::collections::{HashMap, HashSet};
use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use crate::col_table::ColTable;
use crate::edge_trim::EdgeTrimJson;
use crate::error::GDBResult;
use crate::graph::{Direction, IndexType};
use crate::graph_db::{GlobalCsrTrait, LocalEdge, LocalVertex};
use crate::io::{export, import};
use crate::mcsr::{MutableCsr, Nbr, NbrIter};
use crate::schema::{LDBCGraphSchema, Schema};
use crate::scsr::SingleCsr;
use crate::types::*;
use crate::utils::{Iter, LabeledIterator, LabeledRangeIterator, Range};
use crate::vertex_map::VertexMap;

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

    pub fn get_src_label(&self) -> LabelId {
        self.src_label
    }

    pub fn get_dst_label(&self) -> LabelId {
        self.dst_label
    }

    pub fn get_edge_label(&self) -> LabelId {
        self.e_label
    }

    pub fn get_vertex_num(&self) -> I {
        self.csr.vertex_num()
    }

    pub fn get_edge_num(&self) -> usize {
        self.csr.edge_num()
    }

    pub fn degree(&self, src_id: I) -> i64 {
        self.csr.degree(src_id)
    }

    pub fn get_adj_list(&self, src_id: I) -> Option<NbrIter<I>> {
        self.csr.get_edges(src_id)
    }

    pub fn get_internal_id(&self, id: G) -> Option<(LabelId, I)> {
        self.vm.get_internal_id(id)
    }

    pub fn get_src_global_id(&self, id: I) -> Option<G> {
        self.vm.get_global_id(self.src_label, id)
    }

    pub fn get_dst_global_id(&self, id: I) -> Option<G> {
        self.vm.get_global_id(self.dst_label, id)
    }

    pub fn get_vertex(&self, id: G) -> I {
        self.vm.get_internal_id(id).unwrap().1
    }

    pub fn get_properties(&self) -> Option<&ColTable> {
        self.edge_data
    }
}

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

    pub fn get_src_label(&self) -> LabelId {
        self.src_label
    }

    pub fn get_dst_label(&self) -> LabelId {
        self.dst_label
    }

    pub fn get_edge_label(&self) -> LabelId {
        self.e_label
    }

    pub fn get_vertex_num(&self) -> I {
        self.csr.vertex_num()
    }

    pub fn get_edge_num(&self) -> usize {
        self.csr.edge_num()
    }

    pub fn get_adj_list(&self, src_id: I) -> Option<NbrIter<I>> {
        self.csr.get_edges(src_id)
    }

    pub fn get_edge(&self, src_id: I) -> Option<Nbr<I>> {
        self.csr.get_edge(src_id)
    }

    pub fn get_internal_id(&self, id: G) -> Option<(LabelId, I)> {
        self.vm.get_internal_id(id)
    }

    pub fn get_src_global_id(&self, id: I) -> Option<G> {
        self.vm.get_global_id(self.src_label, id)
    }

    pub fn get_dst_global_id(&self, id: I) -> Option<G> {
        self.vm.get_global_id(self.dst_label, id)
    }

    pub fn get_vertex(&self, id: G) -> I {
        self.vm.get_internal_id(id).unwrap().1
    }

    pub fn desc(&self) {
        self.csr.desc();
    }

    pub fn get_properties(&self) -> Option<&ColTable> {
        self.edge_data
    }
}

pub struct CsrDB<G: Send + Sync + IndexType = DefaultId, I: Send + Sync + IndexType = InternalId> {
    pub partition: usize,
    pub ie: Vec<MutableCsr<I>>,
    pub oe: Vec<MutableCsr<I>>,

    pub single_ie: Vec<SingleCsr<I>>,
    pub single_oe: Vec<SingleCsr<I>>,

    pub graph_schema: Arc<LDBCGraphSchema>,
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

pub fn is_single_oe_csr(src_label: LabelId, dst_label: LabelId, edge_label: LabelId) -> bool {
    if src_label == 2 && dst_label == 1 && edge_label == 0 {
        true
    } else if src_label == 3 && dst_label == 1 && edge_label == 0 {
        true
    } else if src_label == 2 && dst_label == 2 && edge_label == 5 {
        true
    } else if src_label == 2 && dst_label == 3 && edge_label == 5 {
        true
    } else if src_label == 2 && dst_label == 0 && edge_label == 2 {
        true
    } else if src_label == 3 && dst_label == 0 && edge_label == 2 {
        true
    } else if src_label == 1 && dst_label == 0 && edge_label == 2 {
        true
    } else if src_label == 4 && dst_label == 1 && edge_label == 6 {
        true
    } else {
        false
    }
}

pub fn is_single_ie_csr(src_label: LabelId, dst_label: LabelId, edge_label: LabelId) -> bool {
    if src_label == 4 && dst_label == 3 && edge_label == 10 {
        true
    } else {
        false
    }
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
    ) -> Option<NbrIter<I>> {
        let index = self.edge_label_to_index(src_label, dst_label, edge_label, dir);
        match dir {
            Direction::Incoming => {
                if is_single_ie_csr(src_label, dst_label, edge_label) {
                    self.single_ie[index].get_edges(src_index)
                } else {
                    self.ie[index].get_edges(src_index)
                }
            }
            Direction::Outgoing => {
                if is_single_oe_csr(src_label, dst_label, edge_label) {
                    self.single_oe[index].get_edges(src_index)
                } else {
                    self.oe[index].get_edges(src_index)
                }
            }
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
    ) -> (Vec<LabelId>, Vec<NbrIter<I>>) {
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
    ) -> (Vec<(LabelId, LabelId)>, Vec<NbrIter<I>>) {
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

    pub fn export(&self, path: &str) -> GDBResult<()> {
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
                    if is_single_ie_csr(
                        src_label_i as LabelId,
                        dst_label_i as LabelId,
                        e_label_i as LabelId,
                    ) {
                        let ie_csr = &self.single_ie[index];
                        if ie_csr.edge_num() != 0 {
                            export(
                                ie_csr,
                                &partition_dir.join(format!(
                                    "ie_{}_{}_{}",
                                    src_label_name, edge_label_name, dst_label_name
                                )),
                            )?;
                        }
                    } else {
                        let ie_csr = &self.ie[index];
                        if ie_csr.edge_num() != 0 {
                            export(
                                ie_csr,
                                &partition_dir.join(format!(
                                    "ie_{}_{}_{}",
                                    src_label_name, edge_label_name, dst_label_name
                                )),
                            )?;
                        }
                    }
                    if is_single_oe_csr(
                        src_label_i as LabelId,
                        dst_label_i as LabelId,
                        e_label_i as LabelId,
                    ) {
                        let oe_csr = &self.single_oe[index];
                        if oe_csr.edge_num() != 0 {
                            export(
                                oe_csr,
                                &partition_dir.join(format!(
                                    "oe_{}_{}_{}",
                                    src_label_name, edge_label_name, dst_label_name
                                )),
                            )?;
                        }
                    } else {
                        let oe_csr = &self.oe[index];
                        if oe_csr.edge_num() != 0 {
                            export(
                                oe_csr,
                                &partition_dir.join(format!(
                                    "oe_{}_{}_{}",
                                    src_label_name, edge_label_name, dst_label_name
                                )),
                            )?;
                        }
                    }
                }
            }
        }
        for (i, table) in self.vertex_prop_table.iter().enumerate() {
            let v_label_name = self.graph_schema.vertex_label_names()[i].clone();
            table.export(&partition_dir.join(format!("vp_{}", v_label_name)))?;
        }

        self.vertex_map
            .export_native(&partition_dir.join("vm.native"))?;
        self.vertex_map
            .export_corner(&partition_dir.join("vm.corner"))?;

        Ok(())
    }

    pub fn import(dir: &str, partition: usize) -> GDBResult<Self> {
        let root_dir = PathBuf::from_str(dir).unwrap();
        let schema_path = root_dir
            .join(DIR_GRAPH_SCHEMA)
            .join(FILE_SCHEMA);
        let graph_schema = LDBCGraphSchema::from_json_file(schema_path)?;
        let partition_dir = root_dir
            .join(DIR_BINARY_DATA)
            .join(format!("partition_{}", partition));

        let vertex_label_num = graph_schema.vertex_type_to_id.len();
        let edge_label_num = graph_schema.edge_type_to_id.len();

        let csr_num = vertex_label_num * vertex_label_num * edge_label_num;
        let mut ie = vec![];
        let mut oe = vec![];
        let mut single_ie = vec![];
        let mut single_oe = vec![];
        for _ in 0..csr_num {
            ie.push(MutableCsr::<I>::new());
            oe.push(MutableCsr::<I>::new());
            single_ie.push(SingleCsr::<I>::new());
            single_oe.push(SingleCsr::<I>::new());
        }
        info!("start import csrs");
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
                    if Path::exists(ie_path) {
                        info!("importing {}", ie_path.as_os_str().to_str().unwrap());
                        if is_single_ie_csr(
                            src_label_i as LabelId,
                            dst_label_i as LabelId,
                            e_label_i as LabelId,
                        ) {
                            single_ie[index] = import::<SingleCsr<I>, _>(ie_path).unwrap();
                        } else {
                            println!("ie path is {:?}", ie_path);
                            ie[index] = import::<MutableCsr<I>, _>(ie_path).unwrap();
                        }
                    }

                    let oe_path = &partition_dir
                        .join(format!("oe_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    if Path::exists(oe_path) {
                        info!("importing {}", oe_path.as_os_str().to_str().unwrap());
                        if is_single_oe_csr(
                            src_label_i as LabelId,
                            dst_label_i as LabelId,
                            e_label_i as LabelId,
                        ) {
                            single_oe[index] = import::<SingleCsr<I>, _>(oe_path).unwrap();
                        } else {
                            oe[index] = import::<MutableCsr<I>, _>(oe_path).unwrap();
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
            info!("importing vertex property: {}", v_label_name);
            vertex_prop_table
                .push(ColTable::import(&partition_dir.join(format!("vp_{}", v_label_name))).unwrap());
        }
        info!("finished import vertex properties");

        let mut vertex_map = VertexMap::new(vertex_label_num);
        info!("start import native vertex map");
        vertex_map.import_native(&partition_dir.join("vm.native"))?;
        info!("start import corner vertex map");
        vertex_map.import_corner(&partition_dir.join("vm.corner"))?;
        vertex_map.desc();
        info!("finish import corner vertex map");

        info!("start import edge properties");
        let mut edge_prop_table = HashMap::new();
        for e_label_i in 0..edge_label_num {
            for src_label_i in 0..vertex_label_num {
                for dst_label_i in 0..vertex_label_num {
                    let edge_index = src_label_i * vertex_label_num * edge_label_num
                        + dst_label_i as usize * edge_label_num
                        + e_label_i as usize;
                    let src_label_name = graph_schema.vertex_label_names()[src_label_i].clone();
                    let dst_label_name = graph_schema.vertex_label_names()[dst_label_i].clone();
                    let edge_label_name = graph_schema.edge_label_names()[e_label_i].clone();
                    let edge_property_path = &partition_dir
                        .join(format!("ep_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    let edge_property_path_str = edge_property_path.to_str().unwrap().to_string();

                    if Path::new(&edge_property_path_str).exists() {
                        edge_prop_table
                            .insert(edge_index, ColTable::import(&edge_property_path_str).unwrap());
                    }
                }
            }
        }
        info!("finished import edge properties");

        Ok(Self {
            partition,
            ie,
            oe,
            single_ie,
            single_oe,
            graph_schema: Arc::new(graph_schema),
            vertex_prop_table,
            vertex_map,
            edge_prop_table,
            vertex_label_num,
            edge_label_num,
        })
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
                    if is_single_ie_csr(
                        src_label_i as LabelId,
                        dst_label_i as LabelId,
                        e_label_i as LabelId,
                    ) {
                        let ie_csr = &self.single_ie[index];

                        if ie_csr.edge_num() != 0 {
                            ie_csr.serialize(&ie_path_str);
                        }
                    } else {
                        let ie_csr = &self.ie[index];
                        if ie_csr.edge_num() != 0 {
                            ie_csr.serialize(&ie_path_str);
                        }
                    }
                    let oe_path = &partition_dir
                        .join(format!("oe_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    let oe_path_str = oe_path.to_str().unwrap().to_string();
                    if is_single_oe_csr(
                        src_label_i as LabelId,
                        dst_label_i as LabelId,
                        e_label_i as LabelId,
                    ) {
                        let oe_csr = &self.single_oe[index];
                        if oe_csr.edge_num() != 0 {
                            oe_csr.serialize(&oe_path_str);
                        }
                    } else {
                        let oe_csr = &self.oe[index];
                        if oe_csr.edge_num() != 0 {
                            oe_csr.serialize(&oe_path_str);
                        }
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

    pub fn deserialize(dir: &str, edge_trim_file: &str, partition: usize) -> GDBResult<Self> {
        let root_dir = PathBuf::from_str(dir).unwrap();
        let schema_path = root_dir
            .join(DIR_GRAPH_SCHEMA)
            .join(FILE_SCHEMA);
        let graph_schema = LDBCGraphSchema::from_json_file(schema_path)?;
        let partition_dir = root_dir
            .join(DIR_BINARY_DATA)
            .join(format!("partition_{}", partition));

        let (ie_enable, oe_enable) = if !edge_trim_file.is_empty() {
            let edge_trim_path = PathBuf::from_str(edge_trim_file).unwrap();
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
        let mut ie = vec![];
        let mut oe = vec![];
        let mut single_ie = vec![];
        let mut single_oe = vec![];
        for _ in 0..csr_num {
            ie.push(MutableCsr::<I>::new());
            oe.push(MutableCsr::<I>::new());
            single_ie.push(SingleCsr::<I>::new());
            single_oe.push(SingleCsr::<I>::new());
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
                    if Path::exists(ie_path) && (edge_trim_file.is_empty() || ie_enable.contains(&index)) {
                        info!("importing {}", ie_path.as_os_str().to_str().unwrap());
                        let path_str = ie_path.to_str().unwrap().to_string();
                        if is_single_ie_csr(
                            src_label_i as LabelId,
                            dst_label_i as LabelId,
                            e_label_i as LabelId,
                        ) {
                            single_ie[index].deserialize(&path_str);
                        } else {
                            ie[index].deserialize(&path_str);
                        }
                    }

                    let oe_path = &partition_dir
                        .join(format!("oe_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                    if Path::exists(oe_path) && (edge_trim_file.is_empty() || oe_enable.contains(&index)) {
                        info!("importing {}", oe_path.as_os_str().to_str().unwrap());
                        let path_str = oe_path.to_str().unwrap().to_string();
                        if is_single_oe_csr(
                            src_label_i as LabelId,
                            dst_label_i as LabelId,
                            e_label_i as LabelId,
                        ) {
                            single_oe[index].deserialize(&path_str);
                        } else {
                            oe[index].deserialize(&path_str);
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
            single_ie,
            single_oe,
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
                &self.oe[index],
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.edge_prop_table.get(&index),
            ),
            Direction::Incoming => SubGraph::new(
                &self.ie[index],
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
                &self.single_oe[index],
                &self.vertex_map,
                src_label,
                dst_label,
                edge_label,
                &self.vertex_prop_table[src_label as usize],
                self.edge_prop_table.get(&index),
            ),
            Direction::Incoming => SingleSubGraph::new(
                &self.single_ie[index],
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
        if labels.is_none() {
            let mut iters = vec![];
            let mut got_labels = vec![];
            for v in 0..self.vertex_label_num {
                let vertex_count = self.vertex_map.vertex_num(v as LabelId);
                let partial_count = (vertex_count + worker_num as usize - 1) / worker_num as usize;
                let start_index = if worker_id as usize * partial_count > vertex_count {
                    vertex_count
                } else {
                    worker_id as usize * partial_count
                };
                let end_index = if (worker_id + 1) as usize * partial_count > vertex_count {
                    vertex_count
                } else {
                    (worker_id + 1) as usize * partial_count
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
            let start_index = if worker_id as usize * partial_count > vertex_count {
                vertex_count
            } else {
                worker_id as usize * partial_count
            };
            let end_index = if (worker_id + 1) as usize * partial_count > vertex_count {
                vertex_count
            } else {
                (worker_id + 1) as usize * partial_count
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
                let start_index = if worker_id as usize * partial_count > vertex_count {
                    vertex_count
                } else {
                    worker_id as usize * partial_count
                };
                let end_index = if (worker_id + 1) as usize * partial_count > vertex_count {
                    vertex_count
                } else {
                    (worker_id + 1) as usize * partial_count
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
                    .map(move |(label, e)| self.index_to_local_vertex(label, e.neighbor, false)),
            )
        }
    }

    fn get_adj_edges(
        &self, src_id: G, edge_labels: Option<&Vec<LabelId>>, dir: Direction,
    ) -> Iter<LocalEdge<G, I>> {
        if let Some(src_lid) = self.vertex_map.get_internal_id(src_id) {
            let (labels, iters) = self._get_adj_lists_edges(src_lid.0, src_lid.1, edge_labels, dir);
            Iter::from_iter(LabeledIterator::new(labels, iters).map(move |((dst_label, edge_label), e)| {
                self.edge_ref_to_local_edge(
                    src_lid.0, src_lid.1, dst_label, e.neighbor, edge_label, dir, e.offset,
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
                        if is_single_oe_csr(
                            src_label as LabelId,
                            dst_label as LabelId,
                            edge_label as LabelId,
                        ) {
                            if self.single_oe[index].edge_num() != 0 {
                                iters.push(Iter::from_iter(self.single_oe[index].get_all_edges()));
                                got_labels.push((
                                    src_label as LabelId,
                                    dst_label as LabelId,
                                    edge_label as LabelId,
                                ));
                            }
                        } else {
                            if self.oe[index].edge_num() != 0 {
                                iters.push(Iter::from_iter(self.oe[index].get_all_edges()));
                                got_labels.push((
                                    src_label as LabelId,
                                    dst_label as LabelId,
                                    edge_label as LabelId,
                                ));
                            }
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
                        if is_single_oe_csr(src_label as LabelId, dst_label as LabelId, *edge_label) {
                            if self.single_oe[index].edge_num() != 0 {
                                iters.push(Iter::from_iter(self.single_oe[index].get_all_edges()));
                                got_labels.push((src_label as LabelId, dst_label as LabelId, *edge_label));
                            }
                        } else {
                            if self.oe[index].edge_num() != 0 {
                                iters.push(Iter::from_iter(self.oe[index].get_all_edges()));
                                got_labels.push((src_label as LabelId, dst_label as LabelId, *edge_label));
                            }
                        }
                    }
                }
            }
        }
        Iter::from_iter(LabeledIterator::new(got_labels, iters).map(
            move |((src_label, dst_label, edge_label), (src_lid, e))| {
                self.edge_ref_to_local_edge(
                    src_label,
                    src_lid,
                    dst_label,
                    e.neighbor,
                    edge_label,
                    Direction::Outgoing,
                    e.offset,
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
