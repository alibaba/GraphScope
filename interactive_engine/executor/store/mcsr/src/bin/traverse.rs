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

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use clap::{App, Arg};
use mcsr::columns::DataType;
use mcsr::graph_db::GlobalCsrTrait;
use mcsr::graph_db_impl::{CsrDB, SingleSubGraph, SubGraph};
use mcsr::ldbc_parser::LDBCVertexParser;
use mcsr::schema::Schema;
use mcsr::types::{DefaultId, LabelId, DIR_BINARY_DATA, NAME, VERSION};

fn get_partition_num(graph_data_dir: &String) -> usize {
    let root_dir = PathBuf::from_str(graph_data_dir.as_str()).unwrap();
    let partitions_dir = root_dir.join(DIR_BINARY_DATA);
    let mut index = 0_usize;
    loop {
        let partition_dir = partitions_dir.join(format!("partition_{}", index));
        let b = Path::new(partition_dir.to_str().unwrap()).is_dir();
        if b {
            index += 1;
        } else {
            return index;
        }
    }
}

fn output_vertices(graph: &CsrDB, output_dir: &String, files: &mut HashMap<LabelId, File>) {
    let vertex_label_names = graph.graph_schema.vertex_label_names();
    let output_dir_path = PathBuf::from_str(output_dir.as_str()).unwrap();
    for n in vertex_label_names.iter() {
        if let Some(v_label) = graph
            .graph_schema
            .get_vertex_label_id(n.as_str())
        {
            println!("outputing vertex-{}, size {}", n, graph.get_vertices_num(v_label));
            let header = graph
                .graph_schema
                .get_vertex_header(v_label)
                .unwrap();
            if !files.contains_key(&v_label) {
                let file = File::create(output_dir_path.join(n.as_str())).unwrap();
                files.insert(v_label.clone(), file);
            }
            let file = files.get_mut(&v_label).unwrap();

            let v_labels = vec![v_label];
            for v in graph.get_all_vertices(Some(&v_labels)) {
                let id = LDBCVertexParser::<DefaultId>::get_original_id(v.get_id());
                write!(file, "\"{}\"", id.to_string()).unwrap();
                for c in header {
                    if c.1 != DataType::ID {
                        write!(
                            file,
                            "|\"{}\"",
                            v.get_property(c.0.as_str())
                                .unwrap()
                                .to_string()
                        )
                        .unwrap();
                    }
                }
                writeln!(file).unwrap();
            }
        }
    }
}

fn output_edges(
    graph: &CsrDB, output_dir: &String, files: &mut HashMap<(LabelId, LabelId, LabelId), File>,
) {
    let output_dir_path = PathBuf::from_str(output_dir.as_str()).unwrap();
    let vertex_label_num = graph.vertex_label_num;
    let edge_label_num = graph.edge_label_num;
    for src_label in 0..vertex_label_num {
        for edge_label in 0..edge_label_num {
            for dst_label in 0..vertex_label_num {
                if let Some(header) = graph.graph_schema.get_edge_header(
                    src_label as LabelId,
                    edge_label as LabelId,
                    dst_label as LabelId,
                ) {
                    println!("{}_{}_{}", src_label, edge_label, dst_label);
                    let src_label_name =
                        graph.graph_schema.vertex_label_names()[src_label as usize].clone();
                    let dst_label_name =
                        graph.graph_schema.vertex_label_names()[dst_label as usize].clone();
                    let edge_label_name =
                        graph.graph_schema.edge_label_names()[edge_label as usize].clone();
                    let filename = src_label_name.clone()
                        + "_"
                        + &*edge_label_name.clone()
                        + "_"
                        + &*dst_label_name.clone();
                    let mut file = File::create(output_dir_path.join(filename.as_str())).unwrap();
                    if !graph.graph_schema.is_single_oe(
                        src_label as LabelId,
                        edge_label as LabelId,
                        dst_label as LabelId,
                    ) {
                        let subgraph = graph.get_sub_graph(
                            src_label as LabelId,
                            edge_label as LabelId,
                            dst_label as LabelId,
                            mcsr::graph::Direction::Outgoing,
                        );
                        for vertex_id in 0..subgraph.get_vertex_num() {
                            let src_global_id = graph
                                .get_global_id(vertex_id, src_label as LabelId)
                                .unwrap();
                            let src_oid =
                                LDBCVertexParser::<usize>::get_original_id(src_global_id as usize) as u64;
                            if let Some(edges) = subgraph.get_adj_list_with_offset(vertex_id) {
                                for e in edges {
                                    let dst_global_id = graph
                                        .get_global_id(e.0.neighbor, dst_label as LabelId)
                                        .unwrap();
                                    let dst_oid =
                                        LDBCVertexParser::<usize>::get_original_id(dst_global_id as usize)
                                            as u64;
                                    write!(file, "\"{}\"|\"{}\"", src_oid, dst_oid).unwrap();
                                    if let Some(properties) = subgraph.get_properties() {
                                        for c in header {
                                            if c.1 != DataType::ID {
                                                write!(
                                                    file,
                                                    "|\"{}\"",
                                                    properties
                                                        .get_column_by_name(c.0.as_str())
                                                        .get(*e.1.unwrap())
                                                        .unwrap()
                                                        .to_string()
                                                )
                                                .unwrap();
                                            }
                                        }
                                    }
                                    writeln!(file).unwrap();
                                }
                            }
                        }
                    } else {
                        let subgraph = graph.get_single_sub_graph(
                            src_label as LabelId,
                            edge_label as LabelId,
                            dst_label as LabelId,
                            mcsr::graph::Direction::Outgoing,
                        );
                        for vertex_id in 0..subgraph.get_vertex_num() {
                            let src_global_id = graph
                                .get_global_id(vertex_id, src_label as LabelId)
                                .unwrap();
                            let src_oid =
                                LDBCVertexParser::<usize>::get_original_id(src_global_id as usize) as u64;
                            if let Some(edges) = subgraph.get_adj_list_with_offset(vertex_id) {
                                for e in edges {
                                    let dst_global_id = graph
                                        .get_global_id(e.0.neighbor, dst_label as LabelId)
                                        .unwrap();
                                    let dst_oid =
                                        LDBCVertexParser::<usize>::get_original_id(dst_global_id as usize)
                                            as u64;
                                    write!(file, "\"{}\"|\"{}\"", src_oid, dst_oid).unwrap();
                                    if let Some(properties) = subgraph.get_properties() {
                                        for c in header {
                                            if c.1 != DataType::ID {
                                                write!(
                                                    file,
                                                    "|\"{}\"",
                                                    properties
                                                        .get_column_by_name(c.0.as_str())
                                                        .get(*e.1.unwrap())
                                                        .unwrap()
                                                        .to_string()
                                                )
                                                .unwrap();
                                            }
                                        }
                                    }
                                    writeln!(file).unwrap();
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn traverse_partition(
    graph_data_dir: &String, output_dir: &String, partition: usize, v_files: &mut HashMap<LabelId, File>,
    e_files: &mut HashMap<(LabelId, LabelId, LabelId), File>,
) {
    let graph = CsrDB::deserialize(graph_data_dir.as_str(), partition).unwrap();

    // output_vertices(&graph, output_dir, v_files);
    println!("start output edges");
    output_edges(&graph, output_dir, e_files);
}

fn main() {
    env_logger::init();
    let matches = App::new(NAME)
        .version(VERSION)
        .about("Build graph storage on single machine.")
        .args(&[
            Arg::with_name("graph_data_dir")
                .short("g")
                .long_help("The directory to graph store")
                .required(true)
                .takes_value(true)
                .index(1),
            Arg::with_name("output_dir")
                .short("o")
                .long_help("The directory to place output files")
                .required(true)
                .takes_value(true)
                .index(2),
        ])
        .get_matches();

    let graph_data_dir = matches
        .value_of("graph_data_dir")
        .unwrap()
        .to_string();
    let output_dir = matches
        .value_of("output_dir")
        .unwrap()
        .to_string();

    let partition_num = get_partition_num(&graph_data_dir);

    let mut v_files = HashMap::<LabelId, File>::new();
    let mut e_files = HashMap::<(LabelId, LabelId, LabelId), File>::new();
    for i in 0..partition_num {
        traverse_partition(&graph_data_dir, &output_dir, i, &mut v_files, &mut e_files);
    }
}
