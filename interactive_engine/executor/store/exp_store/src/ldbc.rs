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

// Contains some dummy codes for LDBC dataset

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
use std::fs::read_dir;
use std::fs::File;
use std::io::{BufReader, Read};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use csv::{Reader, ReaderBuilder};
use petgraph::graph::IndexType;

use super::graph_db::*;
use crate::common::{DefaultId, InternalId, LabelId, INVALID_LABEL_ID};
use crate::config::{GraphDBConfig, JsonConf};
use crate::error::{GDBError, GDBResult};
use crate::graph_db::graph_db_impl::{LargeGraphDB, MutableGraphDB};
use crate::parser::{parse_properties, ColumnMeta, DataType, EdgeMeta, ParserTrait, VertexMeta};
use crate::schema::{LDBCGraphSchema, END_ID_FIELD, ID_FIELD, LABEL_FIELD, START_ID_FIELD};
use crate::table::Row;

/// A ldbc raw file ends with "_0_0.csv"
pub static LDBC_SUFFIX: &'static str = "_0_0.csv";

/// A ldbc raw file uses | to split data fields
pub static SPLITTER: &'static str = "|";
/// A hdfs partitioned data starts wtih "part-"
pub static PARTITION_PREFIX: &'static str = "part-";

/// Given a worker of ID `worker`, identify the files it will processed
pub fn get_partition_names(prefix: &str, worker: usize, peers: usize, num_parts: usize) -> Vec<String> {
    let shares = (num_parts as f32 / peers as f32).ceil() as usize;
    let mut parts = Vec::with_capacity(shares);
    for i in 0..shares {
        let which_part = worker + i * peers;
        let name = format!("{}{number:>0width$}", prefix, number = which_part, width = 5);
        parts.push(name);
    }

    parts
}

/// Verify if a given file is a hidden file in Unix system.
pub fn is_hidden_file(fname: &str) -> bool {
    fname.starts_with('.')
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct Columns {
    /// To record each column' name and data type sequentially in a row
    columns: Vec<ColumnMeta>,
    /// To record mapping from columns' name to its position in a row
    column_indices: BTreeMap<String, usize>,
}

impl Columns {
    fn get_columns(&self) -> &[ColumnMeta] {
        self.columns.as_slice()
    }

    fn get_column_index(&self, col_name: &str) -> Option<usize> {
        self.column_indices.get(col_name).cloned()
    }
}

#[derive(Clone, Default)]
pub struct LDBCMetaData {
    /// To map from a vertex type to the vertex metadata
    vertex_map: BTreeMap<String, Arc<Columns>>,
    /// To map from an edge type to the edge metadata,
    edge_map: BTreeMap<String, Arc<Columns>>,
}

impl LDBCMetaData {
    pub fn get_vertex_header(&self, vertex_type: &str) -> Option<&[ColumnMeta]> {
        self.vertex_map
            .get(vertex_type)
            .map(|meta| meta.get_columns())
    }

    pub fn get_edge_header(&self, edge_type: &str) -> Option<&[ColumnMeta]> {
        self.edge_map
            .get(edge_type)
            .map(|meta| meta.get_columns())
    }
}

/// An edge's type is a 3-tuple: (edge_type, src_vertex_type, dst_vertex_type).
/// For example, a 'KNOWS' edge connects two 'PERSON' vertices, then its type is
/// '(KNOWS, PERSON, PERSON)'
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EdgeTypeTuple {
    etype: String,
    src_vertex_type: String,
    dst_vertex_type: String,
}

impl EdgeTypeTuple {
    pub fn new(etype: String, src_vertex_type: String, dst_vertex_type: String) -> Self {
        Self { etype, src_vertex_type, dst_vertex_type }
    }

    /// Get edge's type
    pub fn get(&self) -> &str {
        &self.etype
    }

    /// Get source vertex's type
    pub fn get_src(&self) -> &str {
        &self.src_vertex_type
    }

    /// Get destination vertex's type
    pub fn get_dst(&self) -> &str {
        &self.dst_vertex_type
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LDBCParserJsonSer {
    vertex_table_to_type: BTreeMap<String, String>,
    edge_table_to_type: BTreeMap<String, EdgeTypeTuple>,
    vertex_type_to_id: BTreeMap<String, LabelId>,
    edge_type_to_id: BTreeMap<String, LabelId>,
    vertex_type_hierarchy: BTreeMap<String, Vec<String>>,
    column_name_to_id: BTreeMap<String, LabelId>,
    vertex_columns: BTreeMap<String, Vec<(String, DataType, bool)>>,
    edge_columns: BTreeMap<String, Vec<(String, DataType, bool)>>,
}

impl JsonConf for LDBCParserJsonSer {}

impl From<LDBCParserJsonSer> for LDBCParser {
    fn from(json: LDBCParserJsonSer) -> Self {
        let mut meta_data = LDBCMetaData::default();
        for (ty, columns) in &json.vertex_columns {
            let mut vertex_meta = Columns::default();
            for (index, col_tuple) in columns.iter().enumerate() {
                vertex_meta
                    .columns
                    .push(col_tuple.clone().into());
                vertex_meta
                    .column_indices
                    .insert(col_tuple.0.clone(), index);
            }
            meta_data
                .vertex_map
                .insert(ty.clone(), Arc::new(vertex_meta));
        }
        for (ty, columns) in &json.edge_columns {
            let mut edge_meta = Columns::default();
            for (index, col_tuple) in columns.iter().enumerate() {
                edge_meta.columns.push(col_tuple.clone().into());
                edge_meta
                    .column_indices
                    .insert(col_tuple.0.clone(), index);
            }
            meta_data
                .edge_map
                .insert(ty.clone(), Arc::new(edge_meta));
        }
        LDBCParser {
            vertex_table_to_type: json.vertex_table_to_type.into_iter().collect(),
            edge_table_to_type: json.edge_table_to_type.into_iter().collect(),
            vertex_type_to_id: Arc::new(json.vertex_type_to_id.into_iter().collect()),
            edge_type_to_id: json.edge_type_to_id.into_iter().collect(),
            vertex_type_hierarchy: json.vertex_type_hierarchy.into_iter().collect(),
            meta_data,
        }
    }
}

/// `LDBCParser` defines parsing from the original LDBC-generated raw files.
#[derive(Clone)]
pub struct LDBCParser {
    vertex_table_to_type: HashMap<String, String>,
    edge_table_to_type: HashMap<String, EdgeTypeTuple>,
    vertex_type_to_id: Arc<HashMap<String, LabelId>>,
    edge_type_to_id: HashMap<String, LabelId>,
    /// Some vertex type many contain sub-types, such as:
    /// PLACE: COUNTRY, CITY, CONTINENT
    vertex_type_hierarchy: HashMap<String, Vec<String>>,
    /// The LDBC metadata
    meta_data: LDBCMetaData,
}

impl LDBCParser {
    pub fn get_vertex_type(&self, vertex_table: &str) -> Option<&String> {
        self.vertex_table_to_type.get(vertex_table)
    }

    pub fn get_edge_type(&self, edge_table: &str) -> Option<&EdgeTypeTuple> {
        self.edge_table_to_type.get(edge_table)
    }

    /// Get the parser for the specific vertex file
    pub fn get_vertex_parser<G>(&self, vertex_type: &str) -> GDBResult<LDBCVertexParser<G>> {
        let vertex_meta = self
            .meta_data
            .vertex_map
            .get(vertex_type)
            .ok_or(GDBError::InvalidTypeError(vertex_type.to_string()))?;
        let id_index = vertex_meta
            .get_column_index(ID_FIELD)
            .ok_or(GDBError::FieldNotExistError)?;
        let label_index = vertex_meta.get_column_index(LABEL_FIELD);
        let vertex_type_id = *self
            .vertex_type_to_id
            .get(vertex_type)
            .ok_or(GDBError::InvalidTypeError(vertex_type.to_string()))?;

        Ok(LDBCVertexParser {
            vertex_type: vertex_type_id,
            id_index,
            label_index,
            vertex_type_to_id: self.vertex_type_to_id.clone(),
            columns: vertex_meta.clone(),
            ph: PhantomData,
        })
    }

    pub fn get_edge_parser<G>(&self, edge_type: &EdgeTypeTuple) -> GDBResult<LDBCEdgeParser<G>> {
        let edge_meta = self
            .meta_data
            .edge_map
            .get(edge_type.get())
            .ok_or(GDBError::InvalidTypeError(edge_type.get().to_string()))?;
        let src_id_index = edge_meta
            .get_column_index(START_ID_FIELD)
            .ok_or(GDBError::ParseError)?;
        let dst_id_index = edge_meta
            .get_column_index(END_ID_FIELD)
            .ok_or(GDBError::ParseError)?;
        let edge_type_id = *self
            .edge_type_to_id
            .get(edge_type.get())
            .ok_or(GDBError::InvalidTypeError(edge_type.get().to_string()))?;
        let src_vertex_type_id = *self
            .vertex_type_to_id
            .get(edge_type.get_src())
            .ok_or(GDBError::InvalidTypeError(edge_type.get_src().to_string()))?;
        let dst_vertex_type_id = *self
            .vertex_type_to_id
            .get(edge_type.get_dst())
            .ok_or(GDBError::InvalidTypeError(edge_type.get_dst().to_string()))?;

        Ok(LDBCEdgeParser {
            src_id_index,
            dst_id_index,
            src_vertex_type: src_vertex_type_id,
            dst_vertex_type: dst_vertex_type_id,
            edge_type: edge_type_id,
            columns: edge_meta.clone(),
            ph: PhantomData,
        })
    }

    pub fn get_graph_schema(&self) -> LDBCGraphSchema {
        let mut vertex_prop_meta: HashMap<_, HashMap<String, (DataType, usize)>> = HashMap::new();
        let mut vertex_prop_vec: HashMap<_, Vec<(String, DataType)>> = HashMap::new();
        let mut edge_prop_meta: HashMap<_, HashMap<String, (DataType, usize)>> = HashMap::new();
        let mut edge_prop_vec: HashMap<_, Vec<(String, DataType)>> = HashMap::new();
        for (ty, meta) in &self.meta_data.vertex_map {
            if let Some(&v_type_id) = self.vertex_type_to_id.get(ty) {
                let vertex_map = vertex_prop_meta.entry(v_type_id).or_default();
                let vertex_vec = vertex_prop_vec.entry(v_type_id).or_default();
                let mut index: usize = 0;
                for column_meta in meta.get_columns() {
                    let col_name = column_meta.name.as_str();
                    let dt = column_meta.data_type;
                    if col_name != LABEL_FIELD {
                        vertex_map.insert(col_name.to_string(), (dt, index));
                        vertex_vec.push((col_name.to_string(), dt));
                        index += 1;
                    }
                }

                // There is extra type for this vertex
                if let Some(subtypes) = self.vertex_type_hierarchy.get(ty) {
                    let clone_vertex_map = vertex_map.clone();
                    let clone_vertex_vec = vertex_vec.clone();
                    for subtype in subtypes {
                        if let Some(&v_type_id) = self.vertex_type_to_id.get(subtype) {
                            vertex_prop_meta.insert(v_type_id, clone_vertex_map.clone());
                            vertex_prop_vec.insert(v_type_id, clone_vertex_vec.clone());
                        }
                    }
                }
            }
        }
        for (ty, meta) in &self.meta_data.edge_map {
            if let Some(&e_type_id) = self.edge_type_to_id.get(ty) {
                let edge_map = edge_prop_meta.entry(e_type_id).or_default();
                let edge_vec = edge_prop_vec.entry(e_type_id).or_default();
                let mut index: usize = 0;
                for column_meta in meta.get_columns() {
                    let col_name = column_meta.name.as_str();
                    let dt = column_meta.data_type;
                    if col_name != START_ID_FIELD && col_name != END_ID_FIELD {
                        edge_map.insert(col_name.to_string(), (dt, index));
                        edge_vec.push((col_name.to_string(), dt));
                        index += 1;
                    }
                }
            }
        }

        LDBCGraphSchema::new(
            self.vertex_type_to_id.as_ref().clone(),
            self.edge_type_to_id.clone(),
            vertex_prop_meta,
            vertex_prop_vec,
            edge_prop_meta,
            edge_prop_vec,
        )
    }
}

/// Define parsing a LDBC vertex
#[derive(Clone)]
pub struct LDBCVertexParser<G = DefaultId> {
    vertex_type: LabelId,
    id_index: usize,
    label_index: Option<usize>,
    vertex_type_to_id: Arc<HashMap<String, LabelId>>,
    columns: Arc<Columns>,
    ph: PhantomData<G>,
}

/// In `LDBCVertexParser`, a vertex's global id is identified by:
///     `label_id << LABEL_SHIFT_BITS | ldbc_id`,
/// where label_id and ldbc_id are in the vertex's ldbc raw data
pub const LABEL_SHIFT_BITS: usize = 8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

impl<G: IndexType> LDBCVertexParser<G> {
    pub fn to_global_id(ldbc_id: usize, label_id: LabelId) -> G {
        let global_id: usize = ((label_id as usize) << LABEL_SHIFT_BITS) | ldbc_id;
        G::new(global_id)
    }
}

impl<G: FromStr + PartialEq + Default + IndexType> ParserTrait<G> for LDBCVertexParser<G> {
    fn parse_vertex_meta<'a, Iter: Iterator<Item = &'a str>>(
        &self, record_iter: Iter,
    ) -> GDBResult<VertexMeta<G>> {
        let mut id = 0_usize;
        let mut extra_label_id = INVALID_LABEL_ID;
        for (index, record) in record_iter.enumerate() {
            if let Some(label_index) = self.label_index {
                if index == self.id_index {
                    id = record.parse::<usize>()?;
                } else if index == label_index {
                    extra_label_id = *self
                        .vertex_type_to_id
                        .get(&record.to_uppercase())
                        .ok_or(GDBError::FieldNotExistError)?;
                    // can break here because id always presents before label
                    break;
                }
            } else {
                if index == self.id_index {
                    id = record.parse::<usize>()?;
                    break;
                }
            }
        }

        let global_id = Self::to_global_id(id, self.vertex_type);
        let label = [self.vertex_type, extra_label_id];

        let vertex_meta = VertexMeta { global_id, label };
        debug!("Parse vertex_meta successfully: {:?}", vertex_meta);

        Ok(vertex_meta)
    }

    fn parse_edge_meta<'a, Iter: Iterator<Item = &'a str>>(
        &self, mut _record_iter: Iter,
    ) -> GDBResult<EdgeMeta<G>> {
        // return error as a VertexParser should not parse an edge
        Err(GDBError::InvalidFunctionCallError)
    }

    fn parse_properties<'a, Iter: Iterator<Item = &'a str>>(&self, record_iter: Iter) -> GDBResult<Row> {
        parse_properties(record_iter, Some(self.columns.get_columns()))
    }
}

/// Define parsing a LDBC edge
pub struct LDBCEdgeParser<G = DefaultId> {
    src_id_index: usize,
    dst_id_index: usize,
    src_vertex_type: LabelId,
    dst_vertex_type: LabelId,
    edge_type: LabelId,
    columns: Arc<Columns>,
    ph: PhantomData<G>,
}

impl<G: FromStr + PartialEq + Default + IndexType> ParserTrait<G> for LDBCEdgeParser<G> {
    fn parse_vertex_meta<'a, Iter: Iterator<Item = &'a str>>(
        &self, mut _record_iter: Iter,
    ) -> GDBResult<VertexMeta<G>> {
        // return error as an EdgeParser should not parse a vertex
        Err(GDBError::InvalidFunctionCallError)
    }

    fn parse_edge_meta<'a, Iter: Iterator<Item = &'a str>>(
        &self, record_iter: Iter,
    ) -> GDBResult<EdgeMeta<G>> {
        let mut src_ldbc_id: i64 = -1;
        let mut dst_ldbc_id: i64 = -1;

        for (index, record) in record_iter.enumerate() {
            if index == self.src_id_index {
                src_ldbc_id = record.parse::<i64>()?;
            } else if index == self.dst_id_index {
                dst_ldbc_id = record.parse::<i64>()?;
            }
            if src_ldbc_id != -1 && dst_ldbc_id != -1 {
                break;
            }
        }

        let src_global_id = LDBCVertexParser::to_global_id(src_ldbc_id as usize, self.src_vertex_type);
        let dst_global_id = LDBCVertexParser::to_global_id(dst_ldbc_id as usize, self.dst_vertex_type);

        let edge_meta = EdgeMeta {
            src_global_id,
            src_label_id: self.src_vertex_type,
            dst_global_id,
            dst_label_id: self.dst_vertex_type,
            label_id: self.edge_type,
        };

        debug!("Parse edge_meta successfully: {:?}", edge_meta);

        Ok(edge_meta)
    }

    fn parse_properties<'a, Iter: Iterator<Item = &'a str>>(&self, record_iter: Iter) -> GDBResult<Row> {
        parse_properties(record_iter, Some(self.columns.get_columns()))
    }
}

/// Load the Graph's raw data into `LargeGraphDB`
pub struct GraphLoader<
    G: FromStr + Send + Sync + IndexType = DefaultId,
    I: Send + Sync + IndexType = InternalId,
> {
    /// Directory to the raw data
    raw_data_dir: PathBuf,
    /// The graph loading toolkits
    graph_builder: MutableGraphDB<G, I>,
    /// The schema for loading graph data
    ldbc_parser: LDBCParser,
    /// A delimiter for splitting the fields in the raw data
    delim: u8,
    /// A timer to measure the time of loading
    timer: Instant,
    /// This processor's id
    work_id: usize,
    /// How many processors all together
    peers: usize,
    /// Detailed performance metrics
    perf_metrics: PerfMetrices,
    /// Phantomize the generic types
    ph1: PhantomData<G>,
    ph2: PhantomData<I>,
}

fn keep_vertex<G: IndexType>(vid: G, peers: usize, work_id: usize) -> bool {
    vid.index() % peers == work_id
}

impl<G: IndexType + Eq + FromStr + Send + Sync, I: IndexType + Send + Sync> GraphLoader<G, I> {
    /// Load vertices recorded in the file of `vertex_type` into the database.
    /// Return the number of vertices that are successfully loaded.
    fn load_vertices_to_db<R: Read>(&mut self, vertex_table: &str, mut rdr: Reader<R>) -> usize {
        let mut num_vertices = 0_usize;
        let graph_db = &mut self.graph_builder;
        let vertex_type = self
            .ldbc_parser
            .get_vertex_type(vertex_table)
            .expect("the vertex file is not recorded");
        let parser = self
            .ldbc_parser
            .get_vertex_parser(vertex_type)
            .expect("Get vertex parser error!");
        let timer = Instant::now();
        let mut start;
        let mut end;
        for result in rdr.records() {
            if let Ok(record) = result {
                start = timer.elapsed().as_secs_f64();
                let record_iter = record.iter();
                let record_iter_cloned = record_iter.clone();
                let mut parse_error = true;
                if let Ok(vertex_meta) = parser.parse_vertex_meta(record_iter) {
                    if keep_vertex(vertex_meta.global_id, self.peers, self.work_id) {
                        if let Ok(properties) = parser.parse_properties(record_iter_cloned) {
                            end = timer.elapsed().as_secs_f64();
                            self.perf_metrics.vertex_parse_time_s += end - start;
                            start = end;
                            if !properties.len() > 0 {
                                let add_vertex_rst = graph_db.add_vertex_with_properties(
                                    vertex_meta.global_id,
                                    vertex_meta.label,
                                    properties,
                                );
                                if add_vertex_rst.is_ok() {
                                    num_vertices += 1;
                                } else {
                                    error!("Error while adding the vertex {:?}", vertex_meta);
                                }
                            } else {
                                if graph_db.add_vertex(vertex_meta.global_id, vertex_meta.label) {
                                    num_vertices += 1;
                                }
                            }
                            end = timer.elapsed().as_secs_f64();
                            self.perf_metrics.vertex_to_db_time_s += end - start;
                            parse_error = false;
                        }
                    } else {
                        parse_error = false;
                    }
                }
                if parse_error {
                    debug!("Error while parsing the vertex {:?}", record);
                }
            }

            if num_vertices != 0 && num_vertices % 50000 == 0 {
                info!(
                    "In the {}th batch, all together {:?} vertices loaded",
                    (num_vertices - 1) / 50000,
                    num_vertices
                );
            }
        }
        info!(
            "In the {}th batch, all together {:?} vertices loaded",
            if num_vertices == 0 { 0 } else { (num_vertices - 1) / 50000 },
            num_vertices
        );

        num_vertices
    }

    /// Load edges recorded in the file of `edge_type` into the database.
    /// Return the number of edges that are successfully loaded.
    fn load_edges_to_db<R: Read>(&mut self, edge_table: &str, mut rdr: Reader<R>) -> usize {
        let mut num_edges = 0_usize;
        let graph_db = &mut self.graph_builder;
        let edge_type = self
            .ldbc_parser
            .get_edge_type(edge_table)
            .expect("the give edge file is not recorded");
        let parser = self
            .ldbc_parser
            .get_edge_parser(edge_type)
            .expect("Get edge parser error!");
        let timer = Instant::now();
        let mut start;
        let mut end;
        for result in rdr.records() {
            if let Ok(record) = result {
                start = timer.elapsed().as_secs_f64();
                let mut parse_error = true;
                let record_iter = record.iter();
                let record_iter_cloned = record_iter.clone();

                if let Ok(edge_meta) = parser.parse_edge_meta(record_iter) {
                    if let Ok(properties) = parser.parse_properties(record_iter_cloned) {
                        end = timer.elapsed().as_secs_f64();
                        self.perf_metrics.edge_parse_time_s += end - start;
                        start = end;
                        // add edge
                        //TODO: in this part, we read all edges and add corner if not in current work_id
                        if keep_vertex(edge_meta.src_global_id, self.peers, self.work_id)
                            || keep_vertex(edge_meta.dst_global_id, self.peers, self.work_id)
                        {
                            if !graph_db.is_vertex_local(edge_meta.src_global_id) {
                                graph_db.add_corner_vertex(edge_meta.src_global_id, edge_meta.src_label_id);
                            }
                            if !graph_db.is_vertex_local(edge_meta.dst_global_id) {
                                graph_db.add_corner_vertex(edge_meta.dst_global_id, edge_meta.dst_label_id);
                            }
                            if properties.len() > 0 {
                                if graph_db
                                    .add_edge_with_properties(
                                        edge_meta.src_global_id,
                                        edge_meta.dst_global_id,
                                        edge_meta.label_id,
                                        properties,
                                    )
                                    .is_ok()
                                {
                                    num_edges += 1;
                                }
                            } else {
                                if graph_db.add_edge(
                                    edge_meta.src_global_id,
                                    edge_meta.dst_global_id,
                                    edge_meta.label_id,
                                ) {
                                    num_edges += 1;
                                }
                            }
                            end = timer.elapsed().as_secs_f64();
                            self.perf_metrics.edge_to_db_time_s += end - start;
                            parse_error = false;
                        }
                    } else {
                        parse_error = false;
                    }
                }
                if parse_error {
                    debug!("Error while parsing the edge {:?}", record);
                }
            }

            if num_edges != 0 && num_edges % 50000 == 0 {
                info!("In the {}th batch, {:?} edges loaded", (num_edges - 1) / 50000, num_edges);
            }
        }
        info!(
            "In the {}th batch, {:?} edges loaded",
            if num_edges == 0 { 0 } else { (num_edges - 1) / 50000 },
            num_edges
        );

        num_edges
    }

    /// Load from raw data to a graph database.
    pub fn load(&mut self) -> GDBResult<()> {
        let files = get_graph_files(self.raw_data_dir.clone(), self.work_id, self.peers)?;

        for (filename, path) in files.iter() {
            if self
                .ldbc_parser
                .get_vertex_type(filename)
                .is_some()
            {
                let rdr = ReaderBuilder::new()
                    .delimiter(self.delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(false)
                    .from_reader(BufReader::new(File::open(path).unwrap()));

                self.load_vertices_to_db(filename, rdr);
            }
        }

        for (filename, path) in files.iter() {
            if self
                .ldbc_parser
                .get_edge_type(filename)
                .is_some()
            {
                let rdr = ReaderBuilder::new()
                    .delimiter(self.delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(false)
                    .from_reader(BufReader::new(File::open(path).unwrap()));

                self.load_edges_to_db(filename, rdr);
            }
        }

        info!("Total time: {:?}", self.timer.elapsed().as_secs_f64());
        info!("Time in details: {:?}", self.perf_metrics);

        Ok(())
    }

    pub fn get_ldbc_parser(&self) -> &LDBCParser {
        &self.ldbc_parser
    }
}

impl<G: FromStr + Send + Sync + IndexType, I: Send + Sync + IndexType> GraphLoader<G, I> {
    pub fn new<D: AsRef<Path>>(
        raw_data_dir: D, graph_data_dir: D, ldbc_meta_file: D, work_id: usize, peers: usize,
    ) -> GraphLoader<G, I> {
        let config = GraphDBConfig::default()
            .root_dir(graph_data_dir)
            .partition(work_id);

        let parser_json =
            LDBCParserJsonSer::from_json_file(ldbc_meta_file).expect("Read ldbc metadata error!");
        let ldbc_parser = parser_json.into();

        Self {
            raw_data_dir: raw_data_dir.as_ref().to_path_buf(),
            graph_builder: config.new(),
            ldbc_parser,
            delim: b'|',
            timer: Instant::now(),
            work_id,
            peers,
            perf_metrics: PerfMetrices::default(),
            ph1: PhantomData,
            ph2: PhantomData,
        }
    }

    /// For specifying a different delimiter
    pub fn with_delimiter(mut self, delim: u8) -> Self {
        self.delim = delim;
        self
    }

    pub fn into_mutable_graph(self) -> MutableGraphDB<G, I> {
        self.graph_builder
    }

    pub fn into_graph(self) -> LargeGraphDB<G, I> {
        let schema = self.ldbc_parser.get_graph_schema();
        self.graph_builder.into_graph(schema)
    }
}

fn get_fname_from_path(path: &PathBuf) -> GDBResult<&str> {
    let fname = path
        .file_name()
        .ok_or(GDBError::UnknownError)?
        .to_str()
        .ok_or(GDBError::UnknownError)?;

    Ok(fname)
}

/// Recursively visit the directory in order to locate the input raw files.
fn visit_dirs(
    files: &mut BTreeMap<String, PathBuf>, raw_data_dir: PathBuf, work_id: usize, peers: usize,
) -> GDBResult<()> {
    if raw_data_dir.is_dir() {
        for _entry in read_dir(&raw_data_dir)? {
            let entry = _entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(files, path, work_id, peers)?
            } else {
                let fname = get_fname_from_path(&path)?;
                if is_hidden_file(fname) {
                    continue;
                }
                files.insert(fname.to_string(), path);
            }
        }
    }
    Ok(())
}

/// To recursively visit the directory from `raw_data_dir` to obtain all files
/// that record vertex/edge data.
fn get_graph_files(
    raw_data_dir: PathBuf, work_id: usize, peers: usize,
) -> GDBResult<BTreeMap<String, PathBuf>> {
    let mut files = BTreeMap::new();
    visit_dirs(&mut files, raw_data_dir, work_id, peers)?;

    Ok(files)
}

/// Record the performance metrics
#[derive(Copy, Clone, Default)]
struct PerfMetrices {
    vertex_parse_time_s: f64,
    edge_parse_time_s: f64,
    vertex_to_db_time_s: f64,
    edge_to_db_time_s: f64,
}

impl Debug for PerfMetrices {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("PerfMetrics")
            .field("vertex_parsing_time(s): ", &self.vertex_parse_time_s)
            .field("vertex_loading_time(s): ", &self.vertex_to_db_time_s)
            .field("edge_parsing_time(s): ", &self.edge_parse_time_s)
            .field("edge_loading_time(s): ", &self.edge_to_db_time_s)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use itertools::Itertools;

    use super::*;
    use crate::common::Label;
    use crate::config::JsonConf;
    use crate::parser::DataType;
    use crate::table::{ItemType, Row};

    static CHINA_ID: DefaultId = 1;
    static BEIJING_ID: DefaultId = 446;
    static SHANGHAI_ID: DefaultId = 459;
    static PDD_ID: DefaultId = 5 << LABEL_SHIFT_BITS | 6000;
    static TSINGHUA_ID: DefaultId = 5 << LABEL_SHIFT_BITS | 6455;

    fn check_properties<G: IndexType + Send + Sync, I: IndexType + Send + Sync>(
        graph: &LargeGraphDB<G, I>, vertex: &LocalVertex<G>, record: &str,
    ) {
        let expected_results: Vec<&str> = record.split('|').collect();

        let mut index = 0;
        for (name, dt) in graph
            .get_schema()
            .get_vertex_header(vertex.get_label()[0])
            .unwrap()
        {
            // does not store LABEL as properties
            match dt {
                DataType::String => {
                    assert_eq!(
                        vertex
                            .get_property(name)
                            .unwrap()
                            .as_str()
                            .unwrap(),
                        expected_results[index]
                    )
                }
                _ => assert_eq!(
                    vertex
                        .get_property(name)
                        .unwrap()
                        .as_u64()
                        .unwrap(),
                    expected_results[index].parse::<u64>().unwrap()
                ),
            }
            index += 1;
        }
    }

    #[test]
    fn test_ldbc_parse() {
        let org_id: LabelId = 5;
        let company_id: LabelId = 11;
        let place_id: LabelId = 0;
        let country_id: LabelId = 8;
        let org_in_place_id: LabelId = 11;

        let ldbc_parser: LDBCParser = LDBCParserJsonSer::from_json_file("data/ldbc_metadata.json")
            .unwrap()
            .into();

        // Test parsing an organisation record from LDBC
        let parser = ldbc_parser
            .get_vertex_parser("ORGANISATION")
            .expect("Get vertex parser error");
        let record = "0|Company|Kam_Air|http://dbpedia.org/resource/Kam_Air";
        let mut record_iter = record.split('|');
        let vertex_meta = parser.parse_vertex_meta(&mut record_iter);
        assert!(vertex_meta.is_ok());
        assert_eq!(
            vertex_meta.unwrap(),
            VertexMeta {
                global_id: (org_id as usize) << LABEL_SHIFT_BITS | 0,
                label: [org_id, company_id],
            }
        );

        let record_iter = record.split('|');
        let properties = parse_properties(
            record_iter,
            ldbc_parser
                .meta_data
                .get_vertex_header("ORGANISATION"),
        );
        assert!(properties.is_ok());
        assert_eq!(
            properties.unwrap(),
            Row::from(vec![object!(0), object!("Kam_Air"), object!("http://dbpedia.org/resource/Kam_Air")])
        );

        // Test parsing a place record from LDBC
        let parser = ldbc_parser
            .get_vertex_parser("PLACE")
            .expect("Get vertex parser error");

        let record = "0|India|http://dbpedia.org/resource/India|Country";
        let record_iter = record.split('|');
        let vertex_meta = parser.parse_vertex_meta(record_iter);
        assert!(vertex_meta.is_ok());
        assert_eq!(
            vertex_meta.unwrap(),
            VertexMeta {
                global_id: (place_id as usize) << LABEL_SHIFT_BITS | 0,
                label: [place_id, country_id],
            }
        );

        let record_iter = record.split('|');
        let properties = parse_properties(record_iter, ldbc_parser.meta_data.get_vertex_header("PLACE"));
        assert!(properties.is_ok());
        // Not label field will be skipped in the properties
        assert_eq!(
            properties.unwrap(),
            Row::from(vec![object!(0), object!("India"), object!("http://dbpedia.org/resource/India"),])
        );

        // Test edge parser
        let parser = ldbc_parser
            .get_edge_parser(&EdgeTypeTuple {
                etype: "ISLOCATEDIN".to_string(),
                src_vertex_type: "ORGANISATION".to_string(),
                dst_vertex_type: "PLACE".to_string(),
            })
            .expect("Get edge parser error");

        let record = "0|59";
        let record_iter = record.split('|');
        let edge_meta = parser.parse_edge_meta(record_iter);
        assert!(edge_meta.is_ok());
        assert_eq!(
            edge_meta.unwrap(),
            EdgeMeta {
                src_global_id: (org_id as usize) << LABEL_SHIFT_BITS | 0,
                src_label_id: org_id,
                dst_global_id: (place_id as usize) << LABEL_SHIFT_BITS | 59,
                dst_label_id: place_id,
                label_id: org_in_place_id,
            }
        );

        let record_iter = record.split('|');
        let properties = parse_properties(
            record_iter,
            ldbc_parser
                .meta_data
                .get_edge_header("ISLOCATEDIN"),
        );

        assert!(properties.is_ok());
        assert!(properties.unwrap().is_empty());
    }

    #[test]
    fn test_load_graph() {
        // with hierarchical vertex labels
        let data_dir = "data/small_data";
        let root_dir = "data/small_data";
        let schema_file = "data/ldbc_metadata.json";
        let mut loader = GraphLoader::<DefaultId, InternalId>::new(data_dir, root_dir, schema_file, 0, 1);
        // load whole graph
        loader.load().expect("Load ldbc data error!");
        let graphdb = loader.into_graph();
        graphdb.print_statistics();

        // Check the loaded properties
        let records: Vec<&str> = vec![
            "1|China|http://dbpedia.org/resource/China",
            "446|Beijing|http://dbpedia.org/resource/Beijing",
            "459|Shanghai|http://dbpedia.org/resource/Shanghai",
            "6000|PDD|http://dbpedia.org/resource/PDD_Company",
            "6455|Tsing_Hua_University|http://dbpedia.org/resource/Tsing_Hua_University",
        ];

        let vertices: Vec<LocalVertex<DefaultId>> = vec![
            graphdb.get_vertex(CHINA_ID).unwrap(),
            graphdb.get_vertex(BEIJING_ID).unwrap(),
            graphdb.get_vertex(SHANGHAI_ID).unwrap(),
            graphdb.get_vertex(PDD_ID).unwrap(),
            graphdb.get_vertex(TSINGHUA_ID).unwrap(),
        ];

        for index in 0..5 {
            check_properties(&graphdb, &vertices[index], records[index]);
        }

        let all_properties = graphdb
            .get_vertex(CHINA_ID)
            .unwrap()
            .clone_all_properties();

        let expected_all_properties: HashMap<String, ItemType> = vec![
            ("id".to_string(), object!(1)),
            ("name".to_string(), object!("China")),
            ("url".to_string(), object!("http://dbpedia.org/resource/China")),
        ]
        .into_iter()
        .collect();

        assert_eq!(all_properties.unwrap(), expected_all_properties);

        // test get_in_vertices..
        let in_vertices: Vec<(DefaultId, Label)> = graphdb
            .get_adj_vertices(CHINA_ID, None, Direction::Incoming)
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();

        assert_eq!(
            in_vertices,
            vec![(BEIJING_ID, [0, 9]), (SHANGHAI_ID, [0, 9]), (PDD_ID, [5, 11]), (TSINGHUA_ID, [5, 12])],
        );

        let in_vertices: Vec<(DefaultId, Label)> = graphdb
            .get_adj_vertices(CHINA_ID, Some(&vec![17]), Direction::Incoming)
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();
        assert_eq!(in_vertices, vec![(BEIJING_ID, [0, 9]), (SHANGHAI_ID, [0, 9]),],);

        let in_vertices: Vec<(DefaultId, Label)> = graphdb
            .get_adj_vertices(CHINA_ID, Some(&vec![11]), Direction::Incoming)
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();
        assert_eq!(in_vertices, vec![(PDD_ID, [5, 11]), (TSINGHUA_ID, [5, 12])],);

        let in_edges: Vec<DefaultId> = graphdb
            .get_adj_edges(CHINA_ID, Some(&vec![11]), Direction::Incoming)
            .map(|item| item.get_src_id())
            .sorted()
            .collect();
        assert_eq!(in_edges, vec![PDD_ID, TSINGHUA_ID],);

        let out_vertices: Vec<(DefaultId, Label)> = graphdb
            .get_out_vertices(PDD_ID, None)
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();

        assert_eq!(out_vertices, vec![(CHINA_ID, [0, 8]), (SHANGHAI_ID, [0, 9]),],);

        let out_vertices: Vec<(DefaultId, Label)> = graphdb
            .get_out_vertices(PDD_ID, Some(&vec![11]))
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();

        assert_eq!(out_vertices, vec![(CHINA_ID, [0, 8]), (SHANGHAI_ID, [0, 9]),],);

        let out_vertices: Vec<(DefaultId, Label)> = graphdb
            .get_out_vertices(PDD_ID, Some(&vec![17]))
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();

        assert_eq!(out_vertices, vec![],);

        let out_edges: Vec<DefaultId> = graphdb
            .get_adj_edges(PDD_ID, None, Direction::Outgoing)
            .map(|item| item.get_dst_id())
            .sorted()
            .collect();

        assert_eq!(out_edges, vec![CHINA_ID, SHANGHAI_ID],);

        // Get vertices
        // with hierarchical vertex labels
        let vertices: Vec<(DefaultId, Label)> = graphdb
            .get_all_vertices(None)
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();

        assert_eq!(
            vertices,
            vec![
                (CHINA_ID, [0, 8]),
                (BEIJING_ID, [0, 9]),
                (SHANGHAI_ID, [0, 9]),
                (PDD_ID, [5, 11]),
                (TSINGHUA_ID, [5, 12])
            ],
        );

        let vertices_place: Vec<(DefaultId, Label)> = graphdb
            .get_all_vertices(Some(&vec![0]))
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();

        assert_eq!(vertices_place, vec![(CHINA_ID, [0, 8]), (BEIJING_ID, [0, 9]), (SHANGHAI_ID, [0, 9]),],);

        let vertices_city: Vec<(DefaultId, Label)> = graphdb
            .get_all_vertices(Some(&vec![9]))
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();

        assert_eq!(vertices_city, vec![(BEIJING_ID, [0, 9]), (SHANGHAI_ID, [0, 9]),],);

        let vertices_country: Vec<(DefaultId, Label)> = graphdb
            .get_all_vertices(Some(&vec![8]))
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();

        assert_eq!(vertices_country, vec![(CHINA_ID, [0, 8]),],);

        let vertices_org: Vec<(DefaultId, Label)> = graphdb
            .get_all_vertices(Some(&vec![5]))
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();

        assert_eq!(vertices_org, vec![(PDD_ID, [5, 11]), (TSINGHUA_ID, [5, 12])],);

        let vertices_company: Vec<(DefaultId, Label)> = graphdb
            .get_all_vertices(Some(&vec![11]))
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();

        assert_eq!(vertices_company, vec![(PDD_ID, [5, 11]),],);

        let vertices_university: Vec<(DefaultId, Label)> = graphdb
            .get_all_vertices(Some(&vec![12]))
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();

        assert_eq!(vertices_university, vec![(TSINGHUA_ID, [5, 12])],);

        let vertices_company_city: Vec<(DefaultId, Label)> = graphdb
            .get_all_vertices(Some(&vec![11, 9]))
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();

        assert_eq!(
            vertices_company_city,
            vec![(BEIJING_ID, [0, 9]), (SHANGHAI_ID, [0, 9]), (PDD_ID, [5, 11]),],
        );

        let vertices_place_city: Vec<(DefaultId, Label)> = graphdb
            .get_all_vertices(Some(&vec![0, 9]))
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();

        assert_eq!(
            vertices_place_city,
            vec![
                (CHINA_ID, [0, 8]),
                (BEIJING_ID, [0, 9]),
                (BEIJING_ID, [0, 9]), // beijing and shanghai will be included twice
                (SHANGHAI_ID, [0, 9]),
                (SHANGHAI_ID, [0, 9]),
            ],
        );
    }

    /*
    #[test]
    fn test_partition_load() {
        // with hierarchical vertex labels
        let data_dir = "data/small_data";
        let root_dir = "data/small_data";
        let schema_file = "data/ldbc_metadata.json";
        let mut loader1 =
            GraphLoader::<DefaultId, InternalId>::new(data_dir, root_dir, schema_file, 20, 0, 2);
        let mut loader2 =
            GraphLoader::<DefaultId, InternalId>::new(data_dir, root_dir, schema_file, 20, 1, 2);

        // Testing graph partition 1
        loader1.load().expect("Load ldbc data error!");
        let graphdb = loader1.into_graph();
        // Check the loaded properties
        let records: Vec<&str> = vec![
            "1|China|http://dbpedia.org/resource/China",
            "6455|Tsing_Hua_University|http://dbpedia.org/resource/Tsing_Hua_University",
        ];

        let vertices: Vec<LocalVertex<DefaultId>> =
            vec![graphdb.get_vertex(CHINA_ID).unwrap(), graphdb.get_vertex(TSINGHUA_ID).unwrap()];

        for index in 0..2 {
            check_properties(&graphdb, &vertices[index], records[index]);
        }

        // test get neighbors..
        let in_vertices: Vec<(DefaultId, Label)> = graphdb
            .get_adj_vertices(CHINA_ID, None, Direction::Incoming)
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();

        assert_eq!(
            in_vertices,
            vec![
                (BEIJING_ID, [0, INVALID_LABEL_ID]),  // corner vertices
                (SHANGHAI_ID, [0, INVALID_LABEL_ID]), // corner vertices
                (PDD_ID, [5, INVALID_LABEL_ID]),      // corner vertices
                (TSINGHUA_ID, [5, 12])
            ],
        );

        // Testing graph partition 2
        loader2.load().expect("Load ldbc data error!");
        let graphdb = loader2.into_graph();
        // Check the loaded properties
        let records: Vec<&str> = vec![
            "446|Beijing|http://dbpedia.org/resource/Beijing",
            "459|Shanghai|http://dbpedia.org/resource/Shanghai",
            "6000|PDD|http://dbpedia.org/resource/PDD_Company",
        ];

        let vertices: Vec<LocalVertex<DefaultId>> = vec![
            graphdb.get_vertex(BEIJING_ID).unwrap(),
            graphdb.get_vertex(SHANGHAI_ID).unwrap(),
            graphdb.get_vertex(PDD_ID).unwrap(),
        ];

        for index in 0..3 {
            check_properties(&graphdb, &vertices[index], records[index]);
        }

        // test neighbors..
        let out_vertices: Vec<(DefaultId, Label)> = graphdb
            .get_adj_vertices(PDD_ID, None, Direction::Outgoing)
            .map(|item| (item.get_id(), item.get_label()))
            .sorted()
            .collect();

        assert_eq!(
            out_vertices,
            vec![
                (CHINA_ID, [0, INVALID_LABEL_ID]), // corner vertex
                (SHANGHAI_ID, [0, 9]),
            ],
        );
    }
     */
}
