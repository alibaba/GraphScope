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

use std::fs::{create_dir_all, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use csv::{Reader, ReaderBuilder, Writer, WriterBuilder};
use rust_htslib::bgzf::Reader as GzReader;

use crate::error::{GDBError, GDBResult};
use crate::graph::IndexType;
use crate::graph_loader::{get_files_list, keep_vertex};
use crate::ldbc_parser::{LDBCEdgeParser, LDBCVertexParser};
use crate::schema::{CsrGraphSchema, InputSchema, Schema};
use crate::types::{DefaultId, LabelId, DIR_SPLIT_RAW_DATA};

pub struct GraphPartitioner<G: FromStr + Send + Sync + IndexType = DefaultId> {
    input_dir: PathBuf,
    partition_dir: PathBuf,

    work_id: usize,
    peers: usize,
    delim: u8,
    input_schema: Arc<InputSchema>,
    graph_schema: Arc<CsrGraphSchema>,

    thread_id: usize,
    thread_num: usize,

    skip_header: bool,

    _marker: PhantomData<G>,
}

impl<G: FromStr + Send + Sync + IndexType + Eq> GraphPartitioner<G> {
    pub fn new<D: AsRef<Path>>(
        input_dir: D, output_path: &str, input_schema_file: D, graph_schema_file: D, work_id: usize,
        peers: usize, thread_id: usize, thread_num: usize,
    ) -> GraphPartitioner<G> {
        let graph_schema =
            CsrGraphSchema::from_json_file(graph_schema_file).expect("Read trim schema error!");
        let input_schema = InputSchema::from_json_file(input_schema_file, &graph_schema)
            .expect("Read graph schema error!");
        graph_schema.desc();

        let output_dir = PathBuf::from_str(output_path).unwrap();
        let partition_dir = output_dir
            .join(DIR_SPLIT_RAW_DATA)
            .join(format!("partition_{}", work_id));

        Self {
            input_dir: input_dir.as_ref().to_path_buf(),
            partition_dir,

            work_id,
            peers,
            delim: b'|',
            input_schema: Arc::new(input_schema),
            graph_schema: Arc::new(graph_schema),

            thread_id,
            thread_num,

            skip_header: false,

            _marker: PhantomData,
        }
    }

    /// For specifying a different delimiter
    pub fn with_delimiter(mut self, delim: u8) -> Self {
        self.delim = delim;
        self
    }

    pub fn skip_header(&mut self) {
        self.skip_header = true;
    }

    fn load_vertices<R: Read, W: Write>(
        &mut self, vertex_type: LabelId, mut rdr: Reader<R>, wtr: &mut Writer<W>, is_static_vertex: bool,
    ) {
        let header = self
            .input_schema
            .get_vertex_header(vertex_type)
            .ok_or(GDBError::InvalidTypeError)
            .unwrap();
        let id_field = header.iter().position(|x| x.0 == "id").unwrap();
        let parser = LDBCVertexParser::<G>::new(vertex_type, id_field);
        info!("loading vertex-{}", vertex_type);
        if is_static_vertex {
            for result in rdr.records() {
                if let Ok(record) = result {
                    wtr.write_record(record.iter()).unwrap();
                }
            }
        } else {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    if keep_vertex(vertex_meta.global_id, self.peers, self.work_id) {
                        wtr.write_record(record.iter()).unwrap();
                    }
                }
            }
        }
    }

    fn load_edges<R: Read, W: Write>(
        &mut self, src_vertex_type: LabelId, dst_vertex_type: LabelId, edge_type: LabelId,
        is_src_static: bool, is_dst_static: bool, mut rdr: Reader<R>, mut wtr: Writer<W>,
    ) {
        info!("loading edge-{}", edge_type);
        let header = self
            .input_schema
            .get_edge_header(src_vertex_type, edge_type, dst_vertex_type)
            .ok_or(GDBError::InvalidTypeError)
            .unwrap();
        let src_id_field = header
            .iter()
            .position(|x| x.0 == "start_id")
            .unwrap();
        let dst_id_field = header
            .iter()
            .position(|x| x.0 == "end_id")
            .unwrap();
        let mut parser = LDBCEdgeParser::<G>::new(src_vertex_type, dst_vertex_type, edge_type);
        parser.with_endpoint_col_id(src_id_field, dst_id_field);
        if is_src_static && is_dst_static {
            for result in rdr.records() {
                if let Ok(record) = result {
                    wtr.write_record(record.iter()).unwrap();
                }
            }
        } else if is_src_static && !is_dst_static {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if keep_vertex(edge_meta.dst_global_id, self.peers, self.work_id) {
                        wtr.write_record(record.iter()).unwrap();
                    }
                }
            }
        } else if !is_src_static && is_dst_static {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if keep_vertex(edge_meta.src_global_id, self.peers, self.work_id) {
                        wtr.write_record(record.iter()).unwrap();
                    }
                }
            }
        } else {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if keep_vertex(edge_meta.src_global_id, self.peers, self.work_id)
                        || keep_vertex(edge_meta.dst_global_id, self.peers, self.work_id)
                    {
                        wtr.write_record(record.iter()).unwrap();
                    }
                }
            }
        }
    }

    pub fn load(&mut self) -> GDBResult<()> {
        create_dir_all(&self.partition_dir)?;

        let v_label_num = self.graph_schema.vertex_type_to_id.len() as LabelId;
        let mut index = 0_usize;
        for v_label_i in 0..v_label_num {
            let cols = self
                .graph_schema
                .get_vertex_header(v_label_i)
                .unwrap();
            let mut header = vec![];
            for pair in cols.iter() {
                header.push((pair.1.clone(), pair.0.clone()));
            }
            let vertex_file_strings = self
                .input_schema
                .get_vertex_file(v_label_i)
                .unwrap();
            let vertex_files = get_files_list(&self.input_dir, vertex_file_strings).unwrap();

            for vertex_file in vertex_files.iter() {
                if index % self.thread_num != self.thread_id {
                    index += 1;
                    continue;
                }
                index += 1;
                if vertex_file
                    .clone()
                    .to_str()
                    .unwrap()
                    .ends_with(".csv")
                {
                    let input_path = vertex_file
                        .as_os_str()
                        .clone()
                        .to_str()
                        .unwrap();
                    let input_dir_path = self
                        .input_dir
                        .as_os_str()
                        .clone()
                        .to_str()
                        .unwrap();
                    let output_path = if let Some(pos) = input_path.find(input_dir_path) {
                        self.partition_dir.join(
                            input_path
                                .clone()
                                .split_at(pos + input_dir_path.len() + 1)
                                .1,
                        )
                    } else {
                        self.partition_dir.join("tmp")
                    };
                    let mut output_dir = output_path.clone();
                    output_dir.pop();
                    create_dir_all(output_dir)?;
                    let rdr = ReaderBuilder::new()
                        .delimiter(self.delim)
                        .buffer_capacity(4096)
                        .comment(Some(b'#'))
                        .flexible(true)
                        .has_headers(self.skip_header)
                        .from_reader(BufReader::new(File::open(&vertex_file).unwrap()));
                    let mut wtr = WriterBuilder::new()
                        .delimiter(self.delim)
                        .from_writer(BufWriter::new(File::create(&output_path).unwrap()));
                    self.load_vertices(
                        v_label_i,
                        rdr,
                        &mut wtr,
                        self.graph_schema.is_static_vertex(v_label_i),
                    );
                } else if vertex_file
                    .clone()
                    .to_str()
                    .unwrap()
                    .ends_with(".csv.gz")
                {
                    let input_path = vertex_file
                        .as_os_str()
                        .clone()
                        .to_str()
                        .unwrap();
                    let input_dir_path = self
                        .input_dir
                        .as_os_str()
                        .clone()
                        .to_str()
                        .unwrap();
                    let gz_loc = input_path.find(".gz").unwrap();
                    let input_path = input_path.split_at(gz_loc).0;
                    let output_path = if let Some(pos) = input_path.find(input_dir_path) {
                        self.partition_dir.join(
                            input_path
                                .clone()
                                .split_at(pos + input_dir_path.len() + 1)
                                .1,
                        )
                    } else {
                        self.partition_dir.join("tmp")
                    };
                    let mut output_dir = output_path.clone();
                    output_dir.pop();
                    create_dir_all(output_dir)?;
                    let rdr = ReaderBuilder::new()
                        .delimiter(self.delim)
                        .buffer_capacity(4096)
                        .comment(Some(b'#'))
                        .flexible(true)
                        .has_headers(self.skip_header)
                        .from_reader(BufReader::new(GzReader::from_path(&vertex_file).unwrap()));
                    let mut wtr = WriterBuilder::new()
                        .delimiter(self.delim)
                        .from_writer(BufWriter::new(File::create(&output_path).unwrap()));
                    self.load_vertices(
                        v_label_i,
                        rdr,
                        &mut wtr,
                        self.graph_schema.is_static_vertex(v_label_i),
                    );
                }
            }
        }

        index = 0;
        let e_label_num = self.graph_schema.edge_type_to_id.len() as LabelId;
        for e_label_i in 0..e_label_num {
            for src_label_i in 0..v_label_num {
                for dst_label_i in 0..v_label_num {
                    if let Some(edge_file_strings) =
                        self.input_schema
                            .get_edge_file(src_label_i, e_label_i, dst_label_i)
                    {
                        let edge_files = get_files_list(&self.input_dir, edge_file_strings).unwrap();
                        for edge_file in edge_files.iter() {
                            if index % self.thread_num != self.thread_id {
                                index += 1;
                                continue;
                            }
                            index += 1;
                            info!("reading from file: {}", edge_file.clone().to_str().unwrap());
                            if edge_file
                                .clone()
                                .to_str()
                                .unwrap()
                                .ends_with(".csv")
                            {
                                info!("{}", edge_file.as_os_str().clone().to_str().unwrap());
                                let input_path = edge_file.as_os_str().clone().to_str().unwrap();
                                let input_dir_path = self
                                    .input_dir
                                    .as_os_str()
                                    .clone()
                                    .to_str()
                                    .unwrap();
                                let output_path = if let Some(pos) = input_path.find(input_dir_path) {
                                    self.partition_dir.join(
                                        input_path
                                            .clone()
                                            .split_at(pos + input_dir_path.len() + 1)
                                            .1,
                                    )
                                } else {
                                    self.partition_dir.join("tmp")
                                };
                                let mut output_dir = output_path.clone();
                                output_dir.pop();
                                create_dir_all(output_dir)?;
                                let rdr = ReaderBuilder::new()
                                    .delimiter(self.delim)
                                    .buffer_capacity(4096)
                                    .comment(Some(b'#'))
                                    .flexible(true)
                                    .has_headers(self.skip_header)
                                    .from_reader(BufReader::new(File::open(&edge_file).unwrap()));
                                let wtr = WriterBuilder::new()
                                    .delimiter(self.delim)
                                    .from_writer(BufWriter::new(File::create(&output_path).unwrap()));
                                self.load_edges(
                                    src_label_i,
                                    dst_label_i,
                                    e_label_i,
                                    self.graph_schema.is_static_vertex(src_label_i),
                                    self.graph_schema.is_static_vertex(dst_label_i),
                                    rdr,
                                    wtr,
                                );
                            } else if edge_file
                                .clone()
                                .to_str()
                                .unwrap()
                                .ends_with(".csv.gz")
                            {
                                let input_path = edge_file.as_os_str().clone().to_str().unwrap();
                                let input_dir_path = self
                                    .input_dir
                                    .as_os_str()
                                    .clone()
                                    .to_str()
                                    .unwrap();
                                let gz_loc = input_path.find(".gz").unwrap();
                                let input_path = input_path.split_at(gz_loc).0;
                                let output_path = if let Some(pos) = input_path.find(input_dir_path) {
                                    self.partition_dir.join(
                                        input_path
                                            .clone()
                                            .split_at(pos + input_dir_path.len() + 1)
                                            .1,
                                    )
                                } else {
                                    self.partition_dir.join("tmp")
                                };
                                let mut output_dir = output_path.clone();
                                output_dir.pop();
                                create_dir_all(output_dir)?;
                                let rdr = ReaderBuilder::new()
                                    .delimiter(self.delim)
                                    .buffer_capacity(4096)
                                    .comment(Some(b'#'))
                                    .flexible(true)
                                    .has_headers(self.skip_header)
                                    .from_reader(BufReader::new(GzReader::from_path(&edge_file).unwrap()));
                                let wtr = WriterBuilder::new()
                                    .delimiter(self.delim)
                                    .from_writer(BufWriter::new(File::create(&output_path).unwrap()));
                                self.load_edges(
                                    src_label_i,
                                    dst_label_i,
                                    e_label_i,
                                    self.graph_schema.is_static_vertex(src_label_i),
                                    self.graph_schema.is_static_vertex(dst_label_i),
                                    rdr,
                                    wtr,
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
