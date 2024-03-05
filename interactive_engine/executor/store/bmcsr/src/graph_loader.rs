use csv::{Reader, ReaderBuilder};
use glob::glob;
use rust_htslib::bgzf::Reader as GzReader;
use std::collections::HashSet;
use std::fs::{create_dir_all, read_dir, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use crate::bmcsr::BatchMutableCsrBuilder;
use crate::bmscsr::BatchMutableSingleCsrBuilder;
use crate::col_table::{parse_properties, ColTable};
use crate::columns::Item;
use crate::csr::CsrTrait;
use crate::error::{GDBError, GDBResult};
use crate::graph::IndexType;
use crate::ldbc_parser::{LDBCEdgeParser, LDBCVertexParser};
use crate::schema::{CsrGraphSchema, InputSchema, Schema};
use crate::types::{DefaultId, InternalId, LabelId, DIR_BINARY_DATA};
use crate::vertex_map::VertexMap;
use regex::Regex;

pub fn get_files_list_beta(prefix: &PathBuf, file_strings: &Vec<String>) -> Vec<PathBuf> {
    let mut ret = vec![];
    for suffix in file_strings.iter() {
        let path = prefix.to_str().unwrap().to_string() + "/" + suffix;
        for entry in glob(&path).unwrap() {
            match entry {
                Ok(p) => ret.push(p),
                Err(e) => warn!("parsing {} failed: {:?}", path, e),
            }
        }
    }
    ret
}

pub fn get_files_list(prefix: &PathBuf, file_strings: &Vec<String>) -> GDBResult<Vec<PathBuf>> {
    let mut path_lists = vec![];
    for file_string in file_strings {
        let temp_path = PathBuf::from(prefix.to_string_lossy().to_string() + "/" + file_string);
        let filename = temp_path
            .file_name()
            .ok_or(GDBError::UnknownError)?
            .to_str()
            .ok_or(GDBError::UnknownError)?;
        if filename.contains("*") {
            let re_string = "^".to_owned() + &filename.replace(".", "\\.").replace("*", ".*") + "$";
            let re = Regex::new(&re_string).unwrap();
            let parent_dir = temp_path.parent().unwrap();
            for _entry in read_dir(parent_dir)? {
                let entry = _entry?;
                let path = entry.path();
                let fname = path
                    .file_name()
                    .ok_or(GDBError::UnknownError)?
                    .to_str()
                    .ok_or(GDBError::UnknownError)?;
                if re.is_match(fname) {
                    path_lists.push(path);
                }
            }
        } else {
            path_lists.push(temp_path);
        }
    }
    Ok(path_lists)
}

pub(crate) fn keep_vertex<G: IndexType>(vid: G, peers: usize, work_id: usize) -> bool {
    vid.index() % peers == work_id
}

pub struct GraphLoader<
    G: FromStr + Send + Sync + IndexType = DefaultId,
    I: Send + Sync + IndexType = InternalId,
> {
    input_dir: PathBuf,
    partition_dir: PathBuf,

    work_id: usize,
    peers: usize,
    delim: u8,
    input_schema: Arc<InputSchema>,
    graph_schema: Arc<CsrGraphSchema>,
    skip_header: bool,
    vertex_map: VertexMap<G, I>,
}

impl<G: FromStr + Send + Sync + IndexType + Eq, I: Send + Sync + IndexType> GraphLoader<G, I> {
    pub fn new<D: AsRef<Path>>(
        input_dir: D, output_path: &str, input_schema_file: D, graph_schema_file: D, work_id: usize,
        peers: usize,
    ) -> GraphLoader<G, I> {
        let graph_schema =
            CsrGraphSchema::from_json_file(graph_schema_file).expect("Read trim schema error!");
        let input_schema = InputSchema::from_json_file(input_schema_file, &graph_schema)
            .expect("Read graph schema error!");
        graph_schema.desc();

        let vertex_label_num = graph_schema.vertex_type_to_id.len();
        let vertex_map = VertexMap::<G, I>::new(vertex_label_num);

        let output_dir = PathBuf::from_str(output_path).unwrap();
        let partition_dir = output_dir
            .join(DIR_BINARY_DATA)
            .join(format!("partition_{}", work_id));

        Self {
            input_dir: input_dir.as_ref().to_path_buf(),
            partition_dir,

            work_id,
            peers,
            delim: b'|',
            input_schema: Arc::new(input_schema),
            graph_schema: Arc::new(graph_schema),
            skip_header: false,

            vertex_map,
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

    fn load_vertices<R: Read>(
        &mut self, vertex_type: LabelId, mut rdr: Reader<R>, table: &mut ColTable, is_static: bool,
    ) {
        let input_header = self
            .input_schema
            .get_vertex_header(vertex_type)
            .unwrap();
        let graph_header = self
            .graph_schema
            .get_vertex_header(vertex_type)
            .unwrap();
        let mut keep_set = HashSet::new();
        for pair in graph_header {
            keep_set.insert(pair.0.clone());
        }
        let mut selected = vec![false; input_header.len()];
        let mut id_col_id = 0;
        for (index, (n, _)) in input_header.iter().enumerate() {
            if keep_set.contains(n) {
                selected[index] = true;
            }
            if n == "id" {
                id_col_id = index;
            }
        }
        let parser = LDBCVertexParser::new(vertex_type, id_col_id);
        info!("loading vertex-{}", vertex_type);
        if is_static {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    if let Ok(properties) = parse_properties(&record, input_header, selected.as_slice()) {
                        let vertex_index = self
                            .vertex_map
                            .add_vertex(vertex_meta.global_id, vertex_meta.label);
                        if properties.len() > 0 {
                            table.insert(vertex_index.index(), &properties);
                        }
                    }
                }
            }
        } else {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    if keep_vertex(vertex_meta.global_id, self.peers, self.work_id) {
                        if let Ok(properties) = parse_properties(&record, input_header, selected.as_slice())
                        {
                            let vertex_index = self
                                .vertex_map
                                .add_vertex(vertex_meta.global_id, vertex_meta.label);
                            if properties.len() > 0 {
                                table.insert(vertex_index.index(), &properties);
                            }
                        }
                    }
                }
            }
        }
    }

    fn load_edges<R: Read>(
        &mut self, src_vertex_type: LabelId, dst_vertex_type: LabelId, edge_type: LabelId,
        is_src_static: bool, is_dst_static: bool, mut rdr: Reader<R>, idegree: &mut Vec<i64>,
        odegree: &mut Vec<i64>, parsed_edges: &mut Vec<(I, I, Vec<Item>)>,
    ) {
        info!("loading edge-{}-{}-{}", src_vertex_type, edge_type, dst_vertex_type);
        let input_header = self
            .input_schema
            .get_edge_header(src_vertex_type, edge_type, dst_vertex_type)
            .unwrap();
        let graph_header = self
            .graph_schema
            .get_edge_header(src_vertex_type, edge_type, dst_vertex_type)
            .unwrap();
        let mut keep_set = HashSet::new();
        for pair in graph_header {
            keep_set.insert(pair.0.clone());
        }
        let mut selected = vec![false; input_header.len()];
        let mut src_col_id = 0;
        let mut dst_col_id = 1;
        for (index, (name, _)) in input_header.iter().enumerate() {
            if keep_set.contains(name) {
                selected[index] = true;
            }
            if name == "start_id" {
                src_col_id = index;
            } else if name == "end_id" {
                dst_col_id = index;
            }
        }

        let src_num = self.vertex_map.vertex_num(src_vertex_type);
        let dst_num = self.vertex_map.vertex_num(dst_vertex_type);
        let mut parser = LDBCEdgeParser::<G>::new(src_vertex_type, dst_vertex_type, edge_type);
        parser.with_endpoint_col_id(src_col_id, dst_col_id);

        if is_src_static && is_dst_static {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if let Ok(properties) = parse_properties(&record, input_header, selected.as_slice()) {
                        let src_lid = self
                            .vertex_map
                            .add_corner_vertex(edge_meta.src_global_id, src_vertex_type);
                        if src_lid.index() < src_num {
                            odegree[src_lid.index()] += 1;
                        }
                        let dst_lid = self
                            .vertex_map
                            .add_corner_vertex(edge_meta.dst_global_id, dst_vertex_type);
                        if dst_lid.index() < dst_num {
                            idegree[dst_lid.index()] += 1;
                        }
                        parsed_edges.push((src_lid, dst_lid, properties));
                    }
                }
            }
        } else if is_src_static && !is_dst_static {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if let Ok(properties) = parse_properties(&record, input_header, selected.as_slice()) {
                        if keep_vertex(edge_meta.src_global_id, self.peers, self.work_id)
                            || keep_vertex(edge_meta.dst_global_id, self.peers, self.work_id)
                        {
                            let src_lid = self
                                .vertex_map
                                .add_corner_vertex(edge_meta.src_global_id, src_vertex_type);
                            if src_lid.index() < src_num {
                                odegree[src_lid.index()] += 1;
                            }
                            let dst_lid = self
                                .vertex_map
                                .add_corner_vertex(edge_meta.dst_global_id, dst_vertex_type);
                            if dst_lid.index() < dst_num {
                                idegree[dst_lid.index()] += 1;
                            }
                            parsed_edges.push((src_lid, dst_lid, properties));
                        }
                    }
                }
            }
        } else if !is_src_static && is_dst_static {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if let Ok(properties) = parse_properties(&record, input_header, selected.as_slice()) {
                        if keep_vertex(edge_meta.src_global_id, self.peers, self.work_id) {
                            let src_lid = self
                                .vertex_map
                                .add_corner_vertex(edge_meta.src_global_id, src_vertex_type);
                            if src_lid.index() < src_num {
                                odegree[src_lid.index()] += 1;
                            }
                            let dst_lid = self
                                .vertex_map
                                .add_corner_vertex(edge_meta.dst_global_id, dst_vertex_type);
                            if dst_lid.index() < dst_num {
                                idegree[dst_lid.index()] += 1;
                            }
                            parsed_edges.push((src_lid, dst_lid, properties));
                        }
                    }
                }
            }
        } else {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if let Ok(properties) = parse_properties(&record, input_header, selected.as_slice()) {
                        if keep_vertex(edge_meta.src_global_id, self.peers, self.work_id)
                            || keep_vertex(edge_meta.dst_global_id, self.peers, self.work_id)
                        {
                            let src_lid = self
                                .vertex_map
                                .add_corner_vertex(edge_meta.src_global_id, src_vertex_type);
                            if src_lid.index() < src_num {
                                odegree[src_lid.index()] += 1;
                            }
                            let dst_lid = self
                                .vertex_map
                                .add_corner_vertex(edge_meta.dst_global_id, dst_vertex_type);
                            if dst_lid.index() < dst_num {
                                idegree[dst_lid.index()] += 1;
                            }
                            parsed_edges.push((src_lid, dst_lid, properties));
                        }
                    }
                }
            }
        }
    }

    pub fn load(&mut self) -> GDBResult<()> {
        create_dir_all(&self.partition_dir)?;

        let v_label_num = self.graph_schema.vertex_type_to_id.len() as LabelId;
        for v_label_i in 0..v_label_num {
            let cols = self
                .graph_schema
                .get_vertex_header(v_label_i)
                .unwrap();
            let mut header = vec![];
            for pair in cols.iter() {
                header.push((pair.1.clone(), pair.0.clone()));
            }
            let mut table = ColTable::new(header);
            let vertex_file_strings = self
                .input_schema
                .get_vertex_file(v_label_i)
                .unwrap();
            let vertex_files = get_files_list(&self.input_dir, vertex_file_strings).unwrap();

            for vertex_file in vertex_files.iter() {
                if vertex_file
                    .clone()
                    .to_str()
                    .unwrap()
                    .ends_with(".csv")
                {
                    let rdr = ReaderBuilder::new()
                        .delimiter(self.delim)
                        .buffer_capacity(4096)
                        .comment(Some(b'#'))
                        .flexible(true)
                        .has_headers(self.skip_header)
                        .from_reader(BufReader::new(File::open(&vertex_file).unwrap()));
                    self.load_vertices(
                        v_label_i,
                        rdr,
                        &mut table,
                        self.graph_schema.is_static_vertex(v_label_i),
                    );
                } else if vertex_file
                    .clone()
                    .to_str()
                    .unwrap()
                    .ends_with(".csv.gz")
                {
                    let rdr = ReaderBuilder::new()
                        .delimiter(self.delim)
                        .buffer_capacity(4096)
                        .comment(Some(b'#'))
                        .flexible(true)
                        .has_headers(self.skip_header)
                        .from_reader(BufReader::new(GzReader::from_path(&vertex_file).unwrap()));
                    self.load_vertices(
                        v_label_i,
                        rdr,
                        &mut table,
                        self.graph_schema.is_static_vertex(v_label_i),
                    );
                }
            }

            let table_path = self
                .partition_dir
                .join(format!("vp_{}", self.graph_schema.vertex_label_names()[v_label_i as usize]));
            let table_path_str = table_path.to_str().unwrap().to_string();
            info!(
                "vertex {}, size: {}",
                self.graph_schema.vertex_label_names()[v_label_i as usize],
                table.row_num()
            );
            table.serialize_table(&table_path_str);
        }

        let e_label_num = self.graph_schema.edge_type_to_id.len() as LabelId;
        for e_label_i in 0..e_label_num {
            let edge_label_name = self.graph_schema.edge_label_names()[e_label_i as usize].clone();

            for src_label_i in 0..v_label_num {
                for dst_label_i in 0..v_label_num {
                    let src_num = self.vertex_map.vertex_num(src_label_i);
                    let dst_num = self.vertex_map.vertex_num(dst_label_i);
                    let mut idegree = vec![0_i64; dst_num as usize];
                    let mut odegree = vec![0_i64; src_num as usize];
                    let mut parsed_edges = vec![];

                    if let Some(edge_file_strings) =
                        self.input_schema
                            .get_edge_file(src_label_i, e_label_i, dst_label_i)
                    {
                        for i in edge_file_strings {
                            info!("{}", i);
                        }
                        let edge_files = get_files_list(&self.input_dir, edge_file_strings).unwrap();
                        for edge_file in edge_files.iter() {
                            info!("reading from file: {}", edge_file.clone().to_str().unwrap());
                            if edge_file
                                .clone()
                                .to_str()
                                .unwrap()
                                .ends_with(".csv")
                            {
                                let rdr = ReaderBuilder::new()
                                    .delimiter(self.delim)
                                    .buffer_capacity(4096)
                                    .comment(Some(b'#'))
                                    .flexible(true)
                                    .has_headers(self.skip_header)
                                    .from_reader(BufReader::new(File::open(&edge_file).unwrap()));
                                self.load_edges(
                                    src_label_i,
                                    dst_label_i,
                                    e_label_i,
                                    self.graph_schema.is_static_vertex(src_label_i),
                                    self.graph_schema.is_static_vertex(dst_label_i),
                                    rdr,
                                    &mut idegree,
                                    &mut odegree,
                                    &mut parsed_edges,
                                );
                            } else if edge_file
                                .clone()
                                .to_str()
                                .unwrap()
                                .ends_with(".csv.gz")
                            {
                                let rdr = ReaderBuilder::new()
                                    .delimiter(self.delim)
                                    .buffer_capacity(4096)
                                    .comment(Some(b'#'))
                                    .flexible(true)
                                    .has_headers(self.skip_header)
                                    .from_reader(BufReader::new(GzReader::from_path(&edge_file).unwrap()));
                                self.load_edges(
                                    src_label_i,
                                    dst_label_i,
                                    e_label_i,
                                    self.graph_schema.is_static_vertex(src_label_i),
                                    self.graph_schema.is_static_vertex(dst_label_i),
                                    rdr,
                                    &mut idegree,
                                    &mut odegree,
                                    &mut parsed_edges,
                                );
                            }
                        }
                    }
                    if parsed_edges.is_empty() {
                        continue;
                    }
                    let src_label_name =
                        self.graph_schema.vertex_label_names()[src_label_i as usize].clone();
                    let dst_label_name =
                        self.graph_schema.vertex_label_names()[dst_label_i as usize].clone();
                    let cols = self
                        .graph_schema
                        .get_edge_header(src_label_i, e_label_i, dst_label_i)
                        .unwrap();
                    let mut header = vec![];
                    for pair in cols.iter() {
                        header.push((pair.1.clone(), pair.0.clone()));
                    }
                    let mut ie_edge_properties = ColTable::new(header.clone());
                    let mut oe_edge_properties = ColTable::new(header.clone());
                    if self
                        .graph_schema
                        .is_single_ie(src_label_i, e_label_i, dst_label_i)
                    {
                        let mut ie_csr_builder = BatchMutableSingleCsrBuilder::<I>::new();
                        let mut oe_csr_builder = BatchMutableCsrBuilder::<I>::new();
                        ie_csr_builder.init(&idegree, 1.2);
                        oe_csr_builder.init(&odegree, 1.2);
                        for e in parsed_edges.iter() {
                            let ie_offset = ie_csr_builder.put_edge(e.1, e.0).unwrap();
                            let oe_offset = oe_csr_builder.put_edge(e.0, e.1).unwrap();
                            if e.2.len() > 0 {
                                ie_edge_properties.insert(ie_offset, &e.2);
                                oe_edge_properties.insert(oe_offset, &e.2);
                            }
                        }

                        let ie_csr = ie_csr_builder.finish().unwrap();
                        let oe_csr = oe_csr_builder.finish().unwrap();

                        info!("start export ie");
                        let ie_path = self
                            .partition_dir
                            .join(format!("ie_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                        let ie_path_str = ie_path.to_str().unwrap().to_string();
                        ie_csr.serialize(&ie_path_str);
                        info!("start export oe");
                        let oe_path = self
                            .partition_dir
                            .join(format!("oe_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                        let oe_path_str = oe_path.to_str().unwrap().to_string();
                        oe_csr.serialize(&oe_path_str);
                        info!("finished export");
                    } else if self
                        .graph_schema
                        .is_single_oe(src_label_i, e_label_i, dst_label_i)
                    {
                        let mut ie_csr_builder = BatchMutableCsrBuilder::<I>::new();
                        let mut oe_csr_builder = BatchMutableSingleCsrBuilder::<I>::new();
                        ie_csr_builder.init(&idegree, 1.2);
                        oe_csr_builder.init(&odegree, 1.2);
                        for e in parsed_edges.iter() {
                            let ie_offset = ie_csr_builder.put_edge(e.1, e.0).unwrap();
                            let oe_offset = oe_csr_builder.put_edge(e.0, e.1).unwrap();
                            if e.2.len() > 0 {
                                ie_edge_properties.insert(ie_offset, &e.2);
                                oe_edge_properties.insert(oe_offset, &e.2);
                            }
                        }

                        let ie_csr = ie_csr_builder.finish().unwrap();
                        let oe_csr = oe_csr_builder.finish().unwrap();

                        info!("start export ie");
                        let ie_path = self
                            .partition_dir
                            .join(format!("ie_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                        let ie_path_str = ie_path.to_str().unwrap().to_string();
                        ie_csr.serialize(&ie_path_str);
                        info!("start export oe");
                        let oe_path = self
                            .partition_dir
                            .join(format!("oe_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                        let oe_path_str = oe_path.to_str().unwrap().to_string();
                        oe_csr.serialize(&oe_path_str);
                        info!("finished export");
                    } else {
                        let mut ie_csr_builder = BatchMutableCsrBuilder::<I>::new();
                        let mut oe_csr_builder = BatchMutableCsrBuilder::<I>::new();
                        ie_csr_builder.init(&idegree, 1.2);
                        oe_csr_builder.init(&odegree, 1.2);
                        for e in parsed_edges.iter() {
                            let ie_offset = ie_csr_builder.put_edge(e.1, e.0).unwrap();
                            let oe_offset = oe_csr_builder.put_edge(e.0, e.1).unwrap();
                            if e.2.len() > 0 {
                                ie_edge_properties.insert(ie_offset, &e.2);
                                oe_edge_properties.insert(oe_offset, &e.2);
                            }
                        }

                        let ie_csr = ie_csr_builder.finish().unwrap();
                        let oe_csr = oe_csr_builder.finish().unwrap();

                        info!("start export ie, edge size {}", ie_csr.edge_num());
                        let ie_path = self
                            .partition_dir
                            .join(format!("ie_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                        let ie_path_str = ie_path.to_str().unwrap().to_string();
                        ie_csr.serialize(&ie_path_str);
                        info!("start export oe");
                        let oe_path = self
                            .partition_dir
                            .join(format!("oe_{}_{}_{}", src_label_name, edge_label_name, dst_label_name));
                        let oe_path_str = oe_path.to_str().unwrap().to_string();
                        oe_csr.serialize(&oe_path_str);
                        info!("finished export");
                    }
                    if oe_edge_properties.row_num() > 0 {
                        let edge_property_path = self.partition_dir.join(format!(
                            "oep_{}_{}_{}",
                            self.graph_schema.vertex_label_names()[src_label_i as usize],
                            self.graph_schema.edge_label_names()[e_label_i as usize],
                            self.graph_schema.vertex_label_names()[dst_label_i as usize]
                        ));
                        let edge_property_path_str = edge_property_path.to_str().unwrap().to_string();
                        oe_edge_properties.serialize_table(&edge_property_path_str);
                    }
                    if ie_edge_properties.row_num() > 0 {
                        let edge_property_path = self.partition_dir.join(format!(
                            "iep_{}_{}_{}",
                            self.graph_schema.vertex_label_names()[src_label_i as usize],
                            self.graph_schema.edge_label_names()[e_label_i as usize],
                            self.graph_schema.vertex_label_names()[dst_label_i as usize]
                        ));
                        let edge_property_path_str = edge_property_path.to_str().unwrap().to_string();
                        ie_edge_properties.serialize_table(&edge_property_path_str);
                    }
                }
            }
        }

        let vm_path = self.partition_dir.join("vm");
        let vm_path_str = vm_path.to_str().unwrap().to_string();
        self.vertex_map.serialize(&vm_path_str);

        Ok(())
    }
}
