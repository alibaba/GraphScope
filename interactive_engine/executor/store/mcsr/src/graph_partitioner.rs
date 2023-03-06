use std::fs::{create_dir_all, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use csv::{Reader, ReaderBuilder, Writer, WriterBuilder};
use rust_htslib::bgzf::Reader as GzReader;

use crate::columns::DataType;
use crate::error::{GDBError, GDBResult};
use crate::graph::IndexType;
use crate::graph_loader::{keep_vertex, split_vertex_edge_files};
use crate::ldbc_parser::{LDBCEdgeParser, LDBCVertexParser};
use crate::schema::{CsrGraphSchema, Schema, END_ID_FIELD, START_ID_FIELD};
use crate::types::{DefaultId, LabelId, DIR_SPLIT_RAW_DATA};

pub struct GraphPartitioner<G: FromStr + Send + Sync + IndexType = DefaultId> {
    input_dir: PathBuf,
    partition_dir: PathBuf,

    work_id: usize,
    peers: usize,
    delim: u8,
    graph_schema: Arc<CsrGraphSchema>,

    thread_id: usize,
    thread_num: usize,

    _marker: PhantomData<G>,
}

fn is_static_vertex(vertex_type: LabelId) -> bool {
    vertex_type == 0 || vertex_type == 5 || vertex_type == 6 || vertex_type == 7
}

impl<G: FromStr + Send + Sync + IndexType + Eq> GraphPartitioner<G> {
    pub fn new<D: AsRef<Path>>(
        input_dir: D, output_path: &str, schema_file: D, work_id: usize, peers: usize, thread_id: usize,
        thread_num: usize,
    ) -> GraphPartitioner<G> {
        let schema = CsrGraphSchema::from_json_file(schema_file).expect("Read graph schema error!");
        schema.desc();

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
            graph_schema: Arc::new(schema),

            thread_id,
            thread_num,

            _marker: PhantomData,
        }
    }

    /// For specifying a different delimiter
    pub fn with_delimiter(mut self, delim: u8) -> Self {
        self.delim = delim;
        self
    }

    fn load_vertices<R: Read, W: Write>(
        &mut self, vertex_type: LabelId, mut rdr: Reader<R>, wtr: &mut Writer<W>,
    ) {
        let header = self
            .graph_schema
            .get_vertex_schema(vertex_type)
            .ok_or(GDBError::InvalidTypeError)
            .unwrap();
        let id_field = header
            .get("id")
            .ok_or(GDBError::FieldNotExistError)
            .unwrap();
        let parser = LDBCVertexParser::<G>::new(vertex_type, id_field.1);
        println!("loading vertex-{}", vertex_type);
        if is_static_vertex(vertex_type) {
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
        mut rdr: Reader<R>, mut wtr: Writer<W>,
    ) {
        println!("loading edge-{}", edge_type);
        let header = self
            .graph_schema
            .get_edge_schema((src_vertex_type, edge_type, dst_vertex_type))
            .ok_or(GDBError::InvalidTypeError)
            .unwrap();
        let mut parser = LDBCEdgeParser::<G>::new(src_vertex_type, dst_vertex_type, edge_type);

        if is_static_vertex(src_vertex_type) && is_static_vertex(dst_vertex_type) {
            parser.with_endpoint_col_id(
                header.get("start_id").unwrap().1 - 1,
                header.get("end_id").unwrap().1 - 1,
            );
            for result in rdr.records() {
                if let Ok(record) = result {
                    wtr.write_record(record.iter()).unwrap();
                }
            }
        } else if is_static_vertex(src_vertex_type) && !is_static_vertex(dst_vertex_type) {
            parser.with_endpoint_col_id(header.get("start_id").unwrap().1, header.get("end_id").unwrap().1);
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if keep_vertex(edge_meta.dst_global_id, self.peers, self.work_id) {
                        wtr.write_record(record.iter()).unwrap();
                    }
                }
            }
        } else if !is_static_vertex(src_vertex_type) && is_static_vertex(dst_vertex_type) {
            parser.with_endpoint_col_id(header.get("start_id").unwrap().1, header.get("end_id").unwrap().1);
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if keep_vertex(edge_meta.src_global_id, self.peers, self.work_id) {
                        wtr.write_record(record.iter()).unwrap();
                    }
                }
            }
        } else {
            parser.with_endpoint_col_id(header.get("start_id").unwrap().1, header.get("end_id").unwrap().1);
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
        let (vertex_files, edge_files) =
            split_vertex_edge_files(self.input_dir.clone(), self.work_id, self.peers)?;
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

            for (vertex_type, vertex_file) in vertex_files.iter() {
                if let Some(vertex_type_id) = self
                    .graph_schema
                    .get_vertex_label_id(&vertex_type)
                {
                    if index % self.thread_num != self.thread_id {
                        index += 1;
                        continue;
                    }
                    index += 1;
                    if vertex_type_id == v_label_i {
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
                            let output_path = if let Some(pos) = input_path.find("static") {
                                self.partition_dir
                                    .join(input_path.clone().split_at(pos).1)
                            } else if let Some(pos) = input_path.find("dynamic") {
                                self.partition_dir
                                    .join(input_path.clone().split_at(pos).1)
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
                                .has_headers(false)
                                .from_reader(BufReader::new(File::open(&vertex_file).unwrap()));
                            let mut wtr = WriterBuilder::new()
                                .delimiter(self.delim)
                                .from_writer(BufWriter::new(File::create(&output_path).unwrap()));
                            self.load_vertices(vertex_type_id, rdr, &mut wtr);
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
                            let gz_loc = input_path.find(".gz").unwrap();
                            let input_path = input_path.split_at(gz_loc).0;
                            let output_path = if let Some(pos) = input_path.find("static") {
                                self.partition_dir
                                    .join(input_path.clone().split_at(pos).1)
                            } else if let Some(pos) = input_path.find("dynamic") {
                                self.partition_dir
                                    .join(input_path.clone().split_at(pos).1)
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
                                .has_headers(false)
                                .from_reader(BufReader::new(GzReader::from_path(&vertex_file).unwrap()));
                            let mut wtr = WriterBuilder::new()
                                .delimiter(self.delim)
                                .from_writer(BufWriter::new(File::create(&output_path).unwrap()));
                            self.load_vertices(vertex_type_id, rdr, &mut wtr);
                        }
                    }
                }
            }
        }

        index = 0;
        let e_label_num = self.graph_schema.edge_type_to_id.len() as LabelId;
        for e_label_i in 0..e_label_num {
            // let cols = self
            //     .graph_schema
            //     .get_edge_header(e_label_i)
            //     .unwrap();
            // let mut header = vec![];
            // for pair in cols.iter() {
            //     if pair.1 == DataType::ID {
            //         if pair.0 != START_ID_FIELD && pair.0 != END_ID_FIELD {
            //             header.push((pair.1.clone(), pair.0.clone()));
            //         }
            //     } else {
            //         header.push((pair.1.clone(), pair.0.clone()));
            //     }
            // }

            for src_label_i in 0..v_label_num {
                for dst_label_i in 0..v_label_num {
                    for (edge_type, edge_file) in edge_files.iter() {
                        if let Some(label_tuple) = self
                            .graph_schema
                            .get_edge_label_tuple(&edge_type)
                        {
                            if label_tuple.edge_label == e_label_i
                                && label_tuple.src_vertex_label == src_label_i
                                && label_tuple.dst_vertex_label == dst_label_i
                            {
                                if index % self.thread_num != self.thread_id {
                                    index += 1;
                                    continue;
                                }
                                index += 1;
                                println!("reading from file: {}", edge_file.clone().to_str().unwrap());
                                if edge_file
                                    .clone()
                                    .to_str()
                                    .unwrap()
                                    .ends_with(".csv")
                                {
                                    println!("{}", edge_file.as_os_str().clone().to_str().unwrap());
                                    let input_path = edge_file.as_os_str().clone().to_str().unwrap();
                                    let output_path = if let Some(pos) = input_path.find("static") {
                                        self.partition_dir
                                            .join(input_path.clone().split_at(pos).1)
                                    } else if let Some(pos) = input_path.find("dynamic") {
                                        self.partition_dir
                                            .join(input_path.clone().split_at(pos).1)
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
                                        .has_headers(false)
                                        .from_reader(BufReader::new(File::open(&edge_file).unwrap()));
                                    let wtr = WriterBuilder::new()
                                        .delimiter(self.delim)
                                        .from_writer(BufWriter::new(File::create(&output_path).unwrap()));
                                    self.load_edges(
                                        label_tuple.src_vertex_label,
                                        label_tuple.dst_vertex_label,
                                        label_tuple.edge_label,
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
                                    let gz_loc = input_path.find(".gz").unwrap();
                                    let input_path = input_path.split_at(gz_loc).0;
                                    let output_path = if let Some(pos) = input_path.find("static") {
                                        self.partition_dir
                                            .join(input_path.clone().split_at(pos).1)
                                    } else if let Some(pos) = input_path.find("dynamic") {
                                        self.partition_dir
                                            .join(input_path.clone().split_at(pos).1)
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
                                        .has_headers(false)
                                        .from_reader(BufReader::new(
                                            GzReader::from_path(&edge_file).unwrap(),
                                        ));
                                    let wtr = WriterBuilder::new()
                                        .delimiter(self.delim)
                                        .from_writer(BufWriter::new(File::create(&output_path).unwrap()));
                                    self.load_edges(
                                        label_tuple.src_vertex_label,
                                        label_tuple.dst_vertex_label,
                                        label_tuple.edge_label,
                                        rdr,
                                        wtr,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
