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

use super::graph_db::*;
use super::graph_db_impl::LargeGraphDB;
use crate::common::{DefaultId, InternalId, LabelId, INVALID_LABEL_ID};
use crate::config::{GraphDBConfig, JsonConf};
use crate::error::{GDBError, GDBResult};
use crate::graph_db_impl::MutableGraphDB;
use crate::parser::{parse_properties, EdgeMeta, ParserTrait, VertexMeta};
use crate::schema::{LDBCGraphSchema, Schema, ID_FIELD, LABEL_FIELD};
use csv::{Reader, ReaderBuilder};
use petgraph::graph::IndexType;
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

/// A ldbc raw file ends with "_0_0.csv"
pub static LDBC_SUFFIX: &'static str = "_0_0.csv";

/// A ldbc raw file uses | to split data fields
pub static SPLITTER: &'static str = "|";
/// A hdfs partitioned data starts wtih "part-"
pub static PARTITION_PREFIX: &'static str = "part-";

/// Given a worker of ID `worker`, identify the files it will processed
pub fn get_partition_names(
    prefix: &str, worker: usize, peers: usize, num_parts: usize,
) -> Vec<String> {
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

/// Verify if a given folder stores vertex or edge
pub fn is_vertex_file(fname: &str) -> bool {
    fname.find('_').is_none()
}

/// `LDBCParser` defines parsing from the original LDBC-generated raw files.
#[derive(Clone)]
pub enum LDBCParser<G = DefaultId> {
    Vertex(LDBCVertexParser<G>),
    Edge(LDBCEdgeParser<G>),
}

impl<G> LDBCParser<G> {
    pub fn vertex_parser(vertex_type_id: LabelId, schema: Arc<dyn Schema>) -> GDBResult<Self> {
        let header = schema.get_vertex_schema(vertex_type_id).ok_or(GDBError::InvalidTypeError)?;
        let id_field = header.get(ID_FIELD).ok_or(GDBError::FieldNotExistError)?;
        let label_field = header.get(LABEL_FIELD);

        Ok(LDBCParser::Vertex(LDBCVertexParser {
            vertex_type: vertex_type_id,
            id_index: id_field.1,
            label_index: label_field.map(|x| x.1),
            schema,
            ph: PhantomData,
        }))
    }

    pub fn edge_parser(
        src_vertex_type_id: LabelId, dst_vertex_type_id: LabelId, edge_type_id: LabelId,
        schema: Arc<dyn Schema>,
    ) -> GDBResult<Self> {
        Ok(LDBCParser::Edge(LDBCEdgeParser {
            src_vertex_type: src_vertex_type_id,
            dst_vertex_type: dst_vertex_type_id,
            edge_type: edge_type_id,
            schema,
            ph: PhantomData,
        }))
    }
}

impl<G: FromStr + PartialEq + Default + IndexType> ParserTrait<G> for LDBCParser<G> {
    fn parse_vertex_meta<'a, Iter: Iterator<Item = &'a str>>(
        &self, record_iter: Iter,
    ) -> GDBResult<VertexMeta<G>> {
        match &self {
            LDBCParser::Vertex(parser) => parser.parse_vertex_meta(record_iter),
            LDBCParser::Edge(parser) => parser.parse_vertex_meta(record_iter),
        }
    }

    fn parse_edge_meta<'a, Iter: Iterator<Item = &'a str>>(
        &self, record_iter: Iter,
    ) -> GDBResult<EdgeMeta<G>> {
        match &self {
            LDBCParser::Vertex(parser) => parser.parse_edge_meta(record_iter),
            LDBCParser::Edge(parser) => parser.parse_edge_meta(record_iter),
        }
    }
}

/// Define parsing a LDBC vertex
#[derive(Clone)]
pub struct LDBCVertexParser<G = DefaultId> {
    vertex_type: LabelId,
    id_index: usize,
    label_index: Option<usize>,
    schema: Arc<dyn Schema>,
    ph: PhantomData<G>,
}

/// In `LDBCVertexParser`, a vertex's global id is identified by:
///     `label_id << LABEL_SHIFT_BITS | ldbc_id`,
/// where label_id and ldbc_id are in the vertex's ldbc raw data
pub const LABEL_SHIFT_BITS: usize =
    8 * (std::mem::size_of::<DefaultId>() - std::mem::size_of::<LabelId>());

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
                    extra_label_id = self
                        .schema
                        .get_vertex_label_id(&record.to_uppercase())
                        .ok_or(GDBError::FieldNotExistError)?;
                    // can break here because id always presents before label
                    break;
                }
            } else {
                id = record.parse::<usize>()?;
                break;
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
}

/// Define parsing a LDBC edge
#[derive(Clone)]
pub struct LDBCEdgeParser<G = DefaultId> {
    src_vertex_type: LabelId,
    dst_vertex_type: LabelId,
    edge_type: LabelId,
    schema: Arc<dyn Schema>,
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
        let mut src_ldbc_id = 0_usize;
        let mut dst_ldbc_id = 0_usize;

        for (index, record) in record_iter.enumerate() {
            if index == 0 {
                src_ldbc_id = record.parse::<usize>()?;
            } else if index == 1 {
                dst_ldbc_id = record.parse::<usize>()?;
                break;
            }
        }

        let src_global_id = LDBCVertexParser::to_global_id(src_ldbc_id, self.src_vertex_type);
        let dst_global_id = LDBCVertexParser::to_global_id(dst_ldbc_id, self.dst_vertex_type);

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
    graph_schema: Arc<LDBCGraphSchema>,

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
    fn load_vertices_to_db<R: Read>(&mut self, vertex_type: LabelId, mut rdr: Reader<R>) -> usize {
        let mut num_vertices = 0_usize;
        let graph_db = &mut self.graph_builder;
        let schema = self.graph_schema.clone();
        let parser =
            LDBCParser::<G>::vertex_parser(vertex_type, schema).expect("Get vertex parser error!");
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
                        if let Ok(properties) = parse_properties(
                            record_iter_cloned,
                            self.graph_schema.get_vertex_header(vertex_type),
                        ) {
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
    fn load_edges_to_db<R: Read>(
        &mut self, src_vertex_type: LabelId, dst_vertex_type: LabelId, edge_type: LabelId,
        mut rdr: Reader<R>,
    ) -> usize {
        let mut num_edges = 0_usize;
        let graph_db = &mut self.graph_builder;
        let schema = self.graph_schema.clone();
        let parser =
            LDBCParser::<G>::edge_parser(src_vertex_type, dst_vertex_type, edge_type, schema)
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
                    if let Ok(properties) = parse_properties(
                        record_iter_cloned,
                        self.graph_schema.get_edge_header(edge_type),
                    ) {
                        end = timer.elapsed().as_secs_f64();
                        self.perf_metrics.edge_parse_time_s += end - start;
                        start = end;
                        // add edge
                        //TODO: in this part, we read all edges and add corner if not in current work_id
                        if keep_vertex(edge_meta.src_global_id, self.peers, self.work_id)
                            || keep_vertex(edge_meta.dst_global_id, self.peers, self.work_id)
                        {
                            if !graph_db.is_vertex_local(edge_meta.src_global_id) {
                                graph_db.add_corner_vertex(
                                    edge_meta.src_global_id,
                                    edge_meta.src_label_id,
                                );
                            }
                            if !graph_db.is_vertex_local(edge_meta.dst_global_id) {
                                graph_db.add_corner_vertex(
                                    edge_meta.dst_global_id,
                                    edge_meta.dst_label_id,
                                );
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
        let (vertex_files, edge_files) =
            split_vertex_edge_files(self.raw_data_dir.clone(), self.work_id, self.peers)?;

        for (vertex_type, vertex_file) in vertex_files {
            let rdr = ReaderBuilder::new()
                .delimiter(self.delim)
                .buffer_capacity(4096)
                .comment(Some(b'#'))
                .flexible(true)
                .has_headers(false)
                .from_reader(BufReader::new(File::open(&vertex_file).unwrap()));

            if let Some(vertex_type_id) = self.graph_schema.get_vertex_label_id(&vertex_type) {
                info!("Process vertex type & file {:?} {:?}", vertex_type, vertex_file);
                self.load_vertices_to_db(vertex_type_id, rdr);
            } else {
                debug!("Invalid vertex type: {}", vertex_type);
            }
        }

        for (edge_type, edge_file) in edge_files {
            if let Some(label_tuple) = self.graph_schema.get_edge_label_tuple(&edge_type) {
                info!("Process edge type & file {} {:?}", edge_type, edge_file);
                let rdr = ReaderBuilder::new()
                    .delimiter(self.delim)
                    .buffer_capacity(4096)
                    .comment(Some(b'#'))
                    .flexible(true)
                    .has_headers(false)
                    .from_reader(BufReader::new(File::open(&edge_file)?));

                self.load_edges_to_db(
                    label_tuple.src_vertex_label,
                    label_tuple.dst_vertex_label,
                    label_tuple.edge_label,
                    rdr,
                );
            } else {
                debug!("Invalid edge type: {}", edge_type);
            }
        }
        info!("Total time: {:?}", self.timer.elapsed().as_secs_f64());
        info!("Time in details: {:?}", self.perf_metrics);

        Ok(())
    }
}

impl<G: FromStr + Send + Sync + IndexType, I: Send + Sync + IndexType> GraphLoader<G, I> {
    pub fn new<D: AsRef<Path>>(
        raw_data_dir: D, graph_data_dir: D, schema_file: D, number_vertex_labels: usize,
        work_id: usize, peers: usize,
    ) -> GraphLoader<G, I> {
        let config = GraphDBConfig::default()
            .root_dir(graph_data_dir)
            .number_vertex_labels(number_vertex_labels)
            .partition(work_id);

        let schema =
            LDBCGraphSchema::from_json_file(schema_file).expect("Read graph schema error!");

        Self {
            raw_data_dir: raw_data_dir.as_ref().to_path_buf(),
            graph_builder: config.new(),
            graph_schema: Arc::new(schema),
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
        let mut schema = self.graph_schema.as_ref().clone();
        schema.trim();
        self.graph_builder.into_graph(schema)
    }
}

fn get_fname_from_path(path: &PathBuf) -> GDBResult<&str> {
    let fname =
        path.file_name().ok_or(GDBError::UnknownError)?.to_str().ok_or(GDBError::UnknownError)?;

    Ok(fname)
}

/// Recursively visit the directory in order to locate the input raw files.
fn visit_dirs(
    vertex_files: &mut Vec<(String, PathBuf)>, edge_files: &mut Vec<(String, PathBuf)>,
    raw_data_dir: PathBuf, work_id: usize, peers: usize,
) -> GDBResult<()> {
    if raw_data_dir.is_dir() {
        for _entry in read_dir(&raw_data_dir)? {
            let entry = _entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(vertex_files, edge_files, path, work_id, peers)?
            } else {
                let fname = get_fname_from_path(&path)?;
                if is_hidden_file(fname) {
                    continue;
                }
                let dname = if let Some(index) = fname.find(LDBC_SUFFIX) {
                    let (dname, _) = fname.split_at(index);
                    dname
                } else if let Some(index) = fname.find(".") {
                    let (dname, _) = fname.split_at(index);
                    dname
                } else {
                    fname
                };

                if is_vertex_file(dname) {
                    vertex_files.push((dname.to_uppercase(), path));
                } else {
                    edge_files.push((dname.to_uppercase(), path));
                }
            }
        }
    }
    Ok(())
}

fn split_vertex_edge_files(
    raw_data_dir: PathBuf, work_id: usize, peers: usize,
) -> GDBResult<(Vec<(String, PathBuf)>, Vec<(String, PathBuf)>)> {
    let mut vertex_files = Vec::new();
    let mut edge_files = Vec::new();
    visit_dirs(&mut vertex_files, &mut edge_files, raw_data_dir, work_id, peers)?;

    vertex_files.sort_by(|x, y| x.0.cmp(&y.0));
    edge_files.sort_by(|x, y| x.0.cmp(&y.0));

    Ok((vertex_files, edge_files))
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
    use super::*;
    use crate::common::Label;
    use crate::config::JsonConf;
    use crate::parser::DataType;
    use crate::table::{ItemType, Row};
    use itertools::Itertools;
    use std::collections::HashMap;

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
        for (name, dt) in graph.get_schema().get_vertex_header(vertex.get_label()[0]).unwrap() {
            // does not store LABEL as properties
            match dt {
                DataType::String => {
                    assert_eq!(
                        vertex.get_property(name).unwrap().as_str().unwrap(),
                        expected_results[index]
                    )
                }
                _ => assert_eq!(
                    vertex.get_property(name).unwrap().as_u64().unwrap(),
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
        let org_in_place_id: LabelId = 2;

        let ldbc_schema_file = "data/schema.json";
        let schema = Arc::new(LDBCGraphSchema::from_json_file(ldbc_schema_file).unwrap());

        // Test parsing an organisation record from LDBC
        let parser = LDBCParser::<DefaultId>::vertex_parser(org_id, schema.clone())
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
        let properties = parse_properties(record_iter, schema.get_vertex_header(org_id));
        assert!(properties.is_ok());
        assert_eq!(
            properties.unwrap(),
            Row::from(vec![
                object!(0),
                object!("Kam_Air"),
                object!("http://dbpedia.org/resource/Kam_Air")
            ])
        );

        // Test parsing a place record from LDBC
        let parser = LDBCParser::<DefaultId>::vertex_parser(place_id, schema.clone())
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
        let properties = parse_properties(record_iter, schema.get_vertex_header(place_id));
        assert!(properties.is_ok());
        // Not label field will be skipped in the properties
        assert_eq!(
            properties.unwrap(),
            Row::from(vec![
                object!(0),
                object!("India"),
                object!("http://dbpedia.org/resource/India"),
            ])
        );

        // Test edge parser
        let parser =
            LDBCParser::<DefaultId>::edge_parser(org_id, place_id, org_in_place_id, schema.clone())
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
        let properties = parse_properties(record_iter, schema.get_edge_header(org_in_place_id));

        assert!(properties.is_ok());
        assert!(properties.unwrap().is_empty());
    }

    #[test]
    fn test_load_graph() {
        // with hierarchical vertex labels
        let data_dir = "data/small_data";
        let root_dir = "data/small_data";
        let schema_file = "data/schema.json";
        let mut loader =
            GraphLoader::<DefaultId, InternalId>::new(data_dir, root_dir, schema_file, 20, 0, 1);
        // load whole graph
        loader.load().expect("Load ldbc data error!");
        let graphdb = loader.into_graph();

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

        let all_properties = graphdb.get_vertex(CHINA_ID).unwrap().clone_all_properties();

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
            vec![
                (BEIJING_ID, [0, 9]),
                (SHANGHAI_ID, [0, 9]),
                (PDD_ID, [5, 11]),
                (TSINGHUA_ID, [5, 12])
            ],
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

        assert_eq!(
            vertices_place,
            vec![(CHINA_ID, [0, 8]), (BEIJING_ID, [0, 9]), (SHANGHAI_ID, [0, 9]),],
        );

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
        let schema_file = "data/schema.json";
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
