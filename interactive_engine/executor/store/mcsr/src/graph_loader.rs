use std::collections::HashSet;
use std::fs::{create_dir_all, read_dir, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use csv::{Reader, ReaderBuilder};
use rust_htslib::bgzf::Reader as GzReader;

use crate::col_table::{parse_properties_beta, ColTable};
use crate::columns::Item;
use crate::date_time::parse_datetime;
use crate::error::{GDBError, GDBResult};
use crate::graph::IndexType;
use crate::graph_db_impl::{is_single_ie_csr, is_single_oe_csr};
use crate::io::export;
use crate::ldbc_parser::{LDBCEdgeParser, LDBCVertexParser};
use crate::mcsr::MutableCsr;
use crate::schema::{LDBCGraphSchema, Schema};
use crate::scsr::SingleCsr;
use crate::types::{DefaultId, InternalId, LabelId, DIR_BINARY_DATA};
use crate::vertex_map::VertexMap;

/// Verify if a given file is a hidden file in Unix system.
pub fn is_hidden_file(fname: &str) -> bool {
    fname.starts_with('.')
}

/// Verify if a given folder stores vertex or edge
pub fn is_vertex_file(fname: &str) -> bool {
    fname.find('_').is_none()
}

pub fn replace_special_tag(dir_name: String) -> String {
    dir_name
        .replace("City", "Place")
        .replace("Country", "Place")
        .replace("University", "Organisation")
        .replace("Company", "Organisation")
}

fn get_fname_from_path(path: &PathBuf) -> GDBResult<&str> {
    let fname = path
        .file_name()
        .ok_or(GDBError::UnknownError)?
        .to_str()
        .ok_or(GDBError::UnknownError)?;

    Ok(fname)
}

fn visit_dirs_v2(
    vertex_files: &mut Vec<(String, PathBuf)>, edge_files: &mut Vec<(String, PathBuf)>,
    raw_data_dir: PathBuf, work_id: usize, peers: usize,
) -> GDBResult<()> {
    if raw_data_dir.is_dir() {
        for _entry in read_dir(&raw_data_dir)? {
            let entry = _entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_dirs_v2(vertex_files, edge_files, path, work_id, peers)?
            } else {
                let fname = get_fname_from_path(&path)?;
                if is_hidden_file(fname) {
                    continue;
                }
                if !(fname.ends_with(".csv") || fname.ends_with(".csv.gz")) {
                    continue;
                }
                let mut ancestors = path.ancestors();
                ancestors.next();
                let dir_name = ancestors
                    .next()
                    .unwrap()
                    .file_name()
                    .ok_or(GDBError::UnknownError)?
                    .to_str()
                    .ok_or(GDBError::UnknownError)?;

                let dir_name = replace_special_tag(dir_name.to_string());
                if is_vertex_file(&dir_name) {
                    vertex_files.push((dir_name.to_uppercase(), path));
                } else {
                    edge_files.push((dir_name.to_uppercase(), path));
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn keep_vertex<G: IndexType>(vid: G, peers: usize, work_id: usize) -> bool {
    vid.index() % peers == work_id
}

pub(crate) fn split_vertex_edge_files(
    raw_data_dir: PathBuf, work_id: usize, peers: usize,
) -> GDBResult<(Vec<(String, PathBuf)>, Vec<(String, PathBuf)>)> {
    let mut vertex_files = Vec::new();
    let mut edge_files = Vec::new();
    visit_dirs_v2(&mut vertex_files, &mut edge_files, raw_data_dir, work_id, peers)?;

    vertex_files.sort_by(|x, y| x.0.cmp(&y.0));
    edge_files.sort_by(|x, y| x.0.cmp(&y.0));

    Ok((vertex_files, edge_files))
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
    graph_schema: Arc<LDBCGraphSchema>,
    trim_schema: Arc<LDBCGraphSchema>,

    vertex_map: VertexMap<G, I>,
}

pub fn is_static_vertex(vertex_type: LabelId) -> bool {
    vertex_type == 0 || vertex_type == 5 || vertex_type == 6 || vertex_type == 7
}

fn encode_nbr_data<I: IndexType>(nbr: I, data: u32) -> I {
    let mut hi = data as u64;
    assert!(nbr.index() <= (u32::MAX as usize));
    hi = (hi << 32) | (nbr.index() as u64);
    I::new(hi as usize)
}

impl<G: FromStr + Send + Sync + IndexType + Eq, I: Send + Sync + IndexType> GraphLoader<G, I> {
    pub fn new<D: AsRef<Path>>(
        input_dir: D, output_path: &str, schema_file: D, trim_file: D, work_id: usize, peers: usize,
    ) -> GraphLoader<G, I> {
        let schema = LDBCGraphSchema::from_json_file(schema_file).expect("Read graph schema error!");
        let trim = LDBCGraphSchema::from_json_file(trim_file).expect("Read trim schema error!");
        schema.desc();

        let vertex_label_num = schema.vertex_type_to_id.len();
        let mut vertex_map = VertexMap::<G, I>::new(vertex_label_num);
        vertex_map.set_internal_id_mask(
            schema.get_vertex_label_id("PERSON").unwrap() as usize,
            I::new((1_usize << 32) - 1_usize),
        );
        vertex_map.set_internal_id_mask(
            schema
                .get_vertex_label_id("ORGANISATION")
                .unwrap() as usize,
            I::new((1_usize << 32) - 1_usize),
        );

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
            graph_schema: Arc::new(schema),
            trim_schema: Arc::new(trim),

            vertex_map,
        }
    }

    /// For specifying a different delimiter
    pub fn with_delimiter(mut self, delim: u8) -> Self {
        self.delim = delim;
        self
    }

    fn load_vertices<R: Read>(&mut self, vertex_type: LabelId, mut rdr: Reader<R>, table: &mut ColTable) {
        let header = self
            .graph_schema
            .get_vertex_header(vertex_type)
            .unwrap();
        let trim = self
            .trim_schema
            .get_vertex_header(vertex_type)
            .unwrap();
        let mut keep_set = HashSet::new();
        for pair in trim {
            keep_set.insert(pair.0.clone());
        }
        let mut selected = vec![false; header.len()];
        let mut id_col_id = 0;
        for (index, (n, _)) in header.iter().enumerate() {
            if keep_set.contains(n) {
                selected[index] = true;
            }
            if n == "id" {
                id_col_id = index;
            }
        }
        let parser = LDBCVertexParser::new(vertex_type, id_col_id);
        println!("loading vertex-{}", vertex_type);
        if is_static_vertex(vertex_type) {
            for result in rdr.records() {
                if let Ok(record) = result {
                    let vertex_meta = parser.parse_vertex_meta(&record);
                    if let Ok(properties) = parse_properties_beta(&record, header, selected.as_slice()) {
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
                        if let Ok(properties) = parse_properties_beta(&record, header, selected.as_slice())
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
        mut rdr: Reader<R>, idegree: &mut Vec<i64>, odegree: &mut Vec<i64>,
        parsed_edges: &mut Vec<(I, I, Vec<Item>)>,
    ) {
        println!("loading edge-{}", edge_type);
        let header = self
            .graph_schema
            .get_edge_header(edge_type)
            .unwrap();
        let trim = self
            .trim_schema
            .get_edge_header(edge_type)
            .unwrap();
        let mut keep_set = HashSet::new();
        for pair in trim {
            keep_set.insert(pair.0.clone());
        }
        let mut selected = vec![false; header.len()];
        let mut src_col_id = 0;
        let mut dst_col_id = 1;
        for (index, (name, _)) in header.iter().enumerate() {
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

        if is_static_vertex(src_vertex_type) && is_static_vertex(dst_vertex_type) {
            parser.with_endpoint_col_id(src_col_id - 1, dst_col_id - 1);
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if let Ok(properties) = parse_properties_beta(&record, header, selected.as_slice()) {
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
        } else if is_static_vertex(src_vertex_type) && !is_static_vertex(dst_vertex_type) {
            parser.with_endpoint_col_id(src_col_id - 1, dst_col_id - 1);
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if let Ok(properties) = parse_properties_beta(&record, header, selected.as_slice()) {
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
        // } else if !is_static_vertex(src_vertex_type) && is_static_vertex(dst_vertex_type) {
        //     parser.with_endpoint_col_id(src_col_id, dst_col_id);
        //     for result in rdr.records() {
        //         if let Ok(record) = result {
        //             let edge_meta = parser.parse_edge_meta(&record);
        //             if let Ok(properties) = parse_properties_beta(&record, header, selected.as_slice()) {
        //                 if keep_vertex(edge_meta.src_global_id, self.peers, self.work_id) {
        //                     let src_lid = self
        //                         .vertex_map
        //                         .add_corner_vertex(edge_meta.src_global_id, src_vertex_type);
        //                     if src_lid.index() < src_num {
        //                         odegree[src_lid.index()] += 1;
        //                     }
        //                     let dst_lid = self
        //                         .vertex_map
        //                         .add_corner_vertex(edge_meta.dst_global_id, dst_vertex_type);
        //                     if dst_lid.index() < dst_num {
        //                         idegree[dst_lid.index()] += 1;
        //                     }
        //                     parsed_edges.push((src_lid, dst_lid, properties));
        //                 }
        //             }
        //         }
        //     }
        } else {
            parser.with_endpoint_col_id(src_col_id, dst_col_id);
            for result in rdr.records() {
                if let Ok(record) = result {
                    let edge_meta = parser.parse_edge_meta(&record);
                    if let Ok(properties) = parse_properties_beta(&record, header, selected.as_slice()) {
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

    pub fn load_beta(&mut self) -> GDBResult<()> {
        let (vertex_files, edge_files) =
            split_vertex_edge_files(self.input_dir.clone(), self.work_id, self.peers)?;
        create_dir_all(&self.partition_dir)?;

        let v_label_num = self.graph_schema.vertex_type_to_id.len() as LabelId;
        for v_label_i in 0..v_label_num {
            let cols = self
                .trim_schema
                .get_vertex_header(v_label_i)
                .unwrap();
            let mut header = vec![];
            for pair in cols.iter() {
                header.push((pair.1.clone(), pair.0.clone()));
            }
            let mut table = ColTable::new(header);

            for (vertex_type, vertex_file) in vertex_files.iter() {
                if let Some(vertex_type_id) = self
                    .graph_schema
                    .get_vertex_label_id(&vertex_type)
                {
                    if vertex_type_id == v_label_i {
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
                                .has_headers(false)
                                .from_reader(BufReader::new(File::open(&vertex_file).unwrap()));
                            self.load_vertices(vertex_type_id, rdr, &mut table);
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
                                .has_headers(false)
                                .from_reader(BufReader::new(GzReader::from_path(&vertex_file).unwrap()));
                            self.load_vertices(vertex_type_id, rdr, &mut table);
                        }
                    }
                }
            }

            let table_path = self
                .partition_dir
                .join(format!("vp_{}", self.graph_schema.vertex_label_names()[v_label_i as usize]));
            let table_path_str = table_path.to_str().unwrap().to_string();
            println!(
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
                    for (edge_type, edge_file) in edge_files.iter() {
                        if let Some(label_tuple) = self
                            .graph_schema
                            .get_edge_label_tuple(&edge_type)
                        {
                            if label_tuple.edge_label == e_label_i
                                && label_tuple.src_vertex_label == src_label_i
                                && label_tuple.dst_vertex_label == dst_label_i
                            {
                                println!("reading from file: {}", edge_file.clone().to_str().unwrap());
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
                                        .has_headers(false)
                                        .from_reader(BufReader::new(File::open(&edge_file).unwrap()));
                                    self.load_edges(
                                        label_tuple.src_vertex_label,
                                        label_tuple.dst_vertex_label,
                                        label_tuple.edge_label,
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
                                        .has_headers(false)
                                        .from_reader(BufReader::new(
                                            GzReader::from_path(&edge_file).unwrap(),
                                        ));
                                    self.load_edges(
                                        label_tuple.src_vertex_label,
                                        label_tuple.dst_vertex_label,
                                        label_tuple.edge_label,
                                        rdr,
                                        &mut idegree,
                                        &mut odegree,
                                        &mut parsed_edges,
                                    );
                                }
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
                        .trim_schema
                        .get_edge_header(e_label_i)
                        .unwrap();
                    let mut header = vec![];
                    for pair in cols.iter() {
                        header.push((pair.1.clone(), pair.0.clone()));
                    }
                    let ie_table = ColTable::new(header.clone());
                    let oe_table = ColTable::new(header.clone());
                    if is_single_ie_csr(src_label_i, dst_label_i, e_label_i) {
                        let mut ie_csr = SingleCsr::<I>::new();
                        let mut oe_csr = MutableCsr::<I>::new();
                        info!(
                            "ie_{}_{}_{}: resize: {} vertices, {} edges",
                            src_label_name,
                            edge_label_name,
                            dst_label_name,
                            dst_num,
                            parsed_edges.len()
                        );
                        ie_csr.resize_vertices(I::new(dst_num));
                        ie_csr.put_col_table(ie_table);
                        info!(
                            "oe_{}_{}_{}: resize: {} vertices, {} edges",
                            src_label_name,
                            edge_label_name,
                            dst_label_name,
                            src_num,
                            parsed_edges.len()
                        );
                        oe_csr.resize_vertices(I::new(src_num));
                        oe_csr.reserve_edges_dense(&odegree);
                        oe_csr.put_col_table(oe_table);
                        info!("start put edges");
                        for e in parsed_edges.iter() {
                            ie_csr.put_edge_properties(e.1, e.0, &e.2);
                            oe_csr.put_edge_properties(e.0, e.1, &e.2);
                        }

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
                    } else if is_single_oe_csr(src_label_i, dst_label_i, e_label_i) {
                        let mut ie_csr = MutableCsr::<I>::new();
                        let mut oe_csr = SingleCsr::<I>::new();
                        info!(
                            "ie_{}_{}_{}: resize: {} vertices, {} edges",
                            src_label_name,
                            edge_label_name,
                            dst_label_name,
                            dst_num,
                            parsed_edges.len()
                        );
                        ie_csr.resize_vertices(I::new(dst_num));
                        ie_csr.reserve_edges_dense(&idegree);
                        ie_csr.put_col_table(ie_table);
                        info!(
                            "oe_{}_{}_{}: resize: {} vertices, {} edges",
                            src_label_name,
                            edge_label_name,
                            dst_label_name,
                            src_num,
                            parsed_edges.len()
                        );
                        oe_csr.resize_vertices(I::new(src_num));
                        oe_csr.put_col_table(oe_table);
                        info!("start put edges");
                        for e in parsed_edges.iter() {
                            ie_csr.put_edge_properties(e.1, e.0, &e.2);
                            oe_csr.put_edge_properties(e.0, e.1, &e.2);
                        }

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
                        let mut ie_csr = MutableCsr::<I>::new();
                        let mut oe_csr = MutableCsr::<I>::new();
                        info!(
                            "ie_{}_{}_{}: resize: {} vertices, {} edges",
                            src_label_name,
                            edge_label_name,
                            dst_label_name,
                            dst_num,
                            parsed_edges.len()
                        );
                        ie_csr.resize_vertices(I::new(dst_num));
                        ie_csr.reserve_edges_dense(&idegree);
                        ie_csr.put_col_table(ie_table);
                        info!(
                            "oe_{}_{}_{}: resize: {} vertices, {} edges",
                            src_label_name,
                            edge_label_name,
                            dst_label_name,
                            src_num,
                            parsed_edges.len()
                        );
                        oe_csr.resize_vertices(I::new(src_num));
                        oe_csr.reserve_edges_dense(&odegree);
                        oe_csr.put_col_table(oe_table);
                        info!("start put edges");
                        for e in parsed_edges.iter() {
                            ie_csr.put_edge_properties(e.1, e.0, &e.2);
                            oe_csr.put_edge_properties(e.0, e.1, &e.2);
                        }

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
