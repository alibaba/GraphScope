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

#[macro_use]
extern crate abomonation_derive;
extern crate clap;
extern crate env_logger;
extern crate timely;
extern crate walkdir;

#[macro_use]
extern crate log;

use clap::{App, Arg};

use timely::dataflow::channels::pact::Exchange as PactExchange;
use timely::dataflow::operators::{Exchange, Input, Inspect, Operator, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};

use std::cell::RefCell;
use std::fmt::Debug;
use std::fs::{read_dir, File};
use std::io::prelude::*;
use std::io::BufReader;
use std::path::PathBuf;
use std::rc::Rc;

use serde::{Deserialize, Serialize};

use graph_store::config::{DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::ldbc::{
    get_partition_names, is_hidden_file, is_vertex_file, LDBCParser, SPLITTER,
};
use graph_store::parser::{parse_properties, EdgeMeta, ParserTrait, VertexMeta};
use graph_store::prelude::*;
use std::str::FromStr;
use std::sync::Arc;

pub static DEFAULT_BATCH: usize = 100_000;

#[derive(Abomonation, Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
enum Direction {
    InEdge,
    OutEdge,
}

fn which_part(id: DefaultId, peers: usize) -> usize {
    id as usize % peers
}

macro_rules! run_dataflow {
    ($worker:expr, $ntable:ty, $etable:ty, $graph_dir:expr, $num_vlabels:expr,
            $input_vertexs:expr, $input_edges:expr, $probe:expr, $count:expr) => {{
        let index = $worker.index();
        let peers = $worker.peers();

        $worker.dataflow::<u32, _, _>(|scope| {
            let vertex_stream = scope.input_from($input_vertexs);
            let edge_stream = scope.input_from($input_edges);

            let config = GraphDBConfig::default()
                .root_dir($graph_dir)
                .number_vertex_labels($num_vlabels)
                .partition(index)
                .schema_file(&$graph_dir.join(DIR_GRAPH_SCHEMA).join(FILE_SCHEMA));

            let mut graph: MutableGraphDB<DefaultId, InternalId, $ntable, $etable> = config.new();
            let schema = Arc::new(config.schema().expect("Read graph schema error!"));

            let mut vertices_data: Vec<(DefaultId, Label, Row)> = Vec::with_capacity(DEFAULT_BATCH);
            let mut edges_data: Vec<(DefaultId, DefaultId, LabelId, Row)> =
                Vec::with_capacity(DEFAULT_BATCH);

            vertex_stream
                .binary_notify(
                    &edge_stream,
                    PactExchange::new(|vertex: &(VertexMeta<DefaultId>, String)| {
                        vertex.0.global_id as u64
                    }),
                    PactExchange::new(
                        |edge: &(DefaultId, Direction, EdgeMeta<DefaultId>, String)| edge.0 as u64,
                    ),
                    "Partition Graph Data",
                    None,
                    move |input1, input2, output, notificator| {
                        input1.for_each(|time, data| {
                            notificator.notify_at(time.retain());
                            for (vertex_meta, content) in data.replace(Vec::new()) {
                                let header = schema.get_vertex_header(vertex_meta.label[0]);
                                let record_iter = content.split(SPLITTER);
                                if let Ok(properties) = parse_properties::<_>(record_iter, header) {
                                    vertices_data.push((
                                        vertex_meta.global_id,
                                        vertex_meta.label,
                                        properties,
                                    ));
                                } else {
                                    error!("Prop parse error: {:?}", vertex_meta);
                                }
                            }

                            if vertices_data.len() >= DEFAULT_BATCH {
                                let rst = graph.add_vertex_batches(vertices_data.drain(..));
                                if rst.is_err() {
                                    error!("Add vertices error: {:?}", rst);
                                }
                            }
                        });

                        input2.for_each(|time, data| {
                            notificator.notify_at(time.retain());
                            for (_, edge_type, edge_meta, content) in data.replace(Vec::new()) {
                                let header = schema.get_edge_header(edge_meta.label_id);
                                let record_iter = content.split(SPLITTER);
                                if let Ok(properties) = parse_properties::<_>(record_iter, header) {
                                    if edge_type == Direction::OutEdge {
                                        graph.add_vertex(
                                            edge_meta.src_global_id,
                                            [edge_meta.src_label_id, INVALID_LABEL_ID],
                                        );
                                        if which_part(edge_meta.dst_global_id, peers) == index {
                                            graph.add_vertex(
                                                edge_meta.dst_global_id,
                                                [edge_meta.dst_label_id, INVALID_LABEL_ID],
                                            );
                                        } else {
                                            graph.add_corner_vertex(
                                                edge_meta.dst_global_id,
                                                edge_meta.dst_label_id,
                                            );
                                        }
                                    } else if edge_type == Direction::InEdge {
                                        graph.add_vertex(
                                            edge_meta.dst_global_id,
                                            [edge_meta.dst_label_id, INVALID_LABEL_ID],
                                        );
                                        if which_part(edge_meta.src_global_id, peers) == index {
                                            graph.add_vertex(
                                                edge_meta.src_global_id,
                                                [edge_meta.src_label_id, INVALID_LABEL_ID],
                                            );
                                        } else {
                                            graph.add_corner_vertex(
                                                edge_meta.src_global_id,
                                                edge_meta.src_label_id,
                                            );
                                        }
                                    }

                                    edges_data.push((
                                        edge_meta.src_global_id,
                                        edge_meta.dst_global_id,
                                        edge_meta.label_id,
                                        properties,
                                    ));
                                } else {
                                    error!("Prop parse error: {:?}", edge_meta);
                                }
                            }

                            if edges_data.len() >= DEFAULT_BATCH {
                                let rst = graph.add_edge_batches(edges_data.drain(..));
                                if rst.is_err() {
                                    error!("Add edges error: {:?}", rst);
                                }
                            }
                        });

                        notificator.for_each(|time, _, _| {
                            let mut session = output.session(&time);

                            if !vertices_data.is_empty() {
                                let rst = graph.add_vertex_batches(vertices_data.drain(..));
                                if rst.is_err() {
                                    error!("Add vertices error: {:?}", rst);
                                }
                            }

                            if !edges_data.is_empty() {
                                let rst = graph.add_edge_batches(edges_data.drain(..));
                                if rst.is_err() {
                                    error!("Add edges error: {:?}", rst);
                                }
                            }

                            vertices_data.shrink_to_fit();
                            edges_data.shrink_to_fit();
                            graph.shrink_to_fit();
                            let num_vertices = graph.node_count();
                            let num_edges = graph.edge_count();

                            println!(
                                "Worker {}: received {} vertices, {} edges",
                                index, num_vertices, num_edges
                            );

                            let rst = graph.export();
                            if rst.is_err() {
                                println!("Worker {} write graph error: {:?}", index, rst);
                            }

                            session.give((num_vertices, num_edges));
                        });
                    },
                )
                .exchange(|_| 0_u64)
                .inspect(move |(x, y)| {
                    let mut c = $count.borrow_mut();
                    c.0 += x;
                    c.1 += y;
                })
                .probe_with($probe);
        });
    }};
}

/// The option for vertex/edge's property storage
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
enum PropertyStorageOpt {
    /// In memory: Store the vertex in `PropertyTable`, and edge in `SingleValueTable`
    SimpleLDBC,
}

impl FromStr for PropertyStorageOpt {
    type Err = GDBError;

    fn from_str(_opt: &str) -> Result<Self, Self::Err> {
        let opt = _opt.to_uppercase();
        match opt.as_str() {
            "SIMPLELDBC" => Ok(PropertyStorageOpt::SimpleLDBC),
            _ => GDBResult::Err(GDBError::ParseError),
        }
    }
}

/// Load LDBC data from raw file (preprocessed by including the id), reshuffle the data according
/// the cluster (with certain worker + machine settings), and construct the graph binary file
/// for each worker in each machine.
fn main() {
    env_logger::init();
    let matches = App::new(NAME)
        .version(VERSION)
        .about("Parse the LDBC raw data and maintain them in a distributed graph storage.")
        .args(&[
            Arg::with_name("ldbc_dir")
                .short("d")
                .long_help("Root directory to LDBC data.")
                .required(true)
                .takes_value(true)
                .index(1),
            Arg::with_name("graph_db_dir")
                .short("g")
                .long_help("The root directory to store the graph db.")
                .required(true)
                .takes_value(true)
                .index(2),
            Arg::with_name("raw_partitions")
                .long_help("The default number of partitions of raw data.")
                .required(true)
                .takes_value(true)
                .index(3),
            Arg::with_name("is_whole_data")
                .short("a")
                .long_help("Specify whether the first machine contains the whole data.")
                .required(false)
                .takes_value(false),
            Arg::with_name("is_only_out_edges")
                .short("o")
                .long_help("Specify whether the vertex only maintains the outgoing edges.")
                .required(false),
            Arg::with_name("num_vertex_labels")
                .short("l")
                .long_help("Specify the number of vertex labels")
                .required(false)
                .takes_value(true)
                .default_value("13"),
            Arg::with_name("ppt_store_opt")
                .short("s")
                .long_help("Specify the storage option for property data, supported [simpleLDBC].")
                .required(false)
                .default_value("simpleLDBC")
                .takes_value(true),
            Arg::with_name("workers")
                .short("w")
                .help("Number of workers for Timely")
                .takes_value(true),
            Arg::with_name("machines")
                .short("n")
                .help("Number of machines for Timely")
                .takes_value(true),
            Arg::with_name("processor")
                .short("p")
                .help("The index of the processor for Timely")
                .takes_value(true),
            Arg::with_name("host_file")
                .short("h")
                .help("The path to the host file for Timely")
                .takes_value(true),
        ])
        .get_matches();

    // Fill the timely arguments
    let mut timely_args = Vec::new();
    if matches.is_present("workers") {
        timely_args.push("-w".to_string());
        timely_args.push(matches.value_of("workers").unwrap().to_string());
    }

    let mut num_machines = 1;
    if matches.is_present("machines") {
        timely_args.push("-n".to_string());
        let machines = matches.value_of("machines").unwrap().to_string();
        num_machines = machines.parse().unwrap();

        timely_args.push(machines);
    }

    if matches.is_present("processor") {
        timely_args.push("-p".to_string());
        timely_args.push(matches.value_of("processor").unwrap().to_string());
    }

    if matches.is_present("host_file") {
        timely_args.push("-h".to_string());
        timely_args.push(matches.value_of("host_file").unwrap().to_string());
    }

    let ldbc_part_prefix = "part-";

    // The ldbc data will be stored according to each type of vertex/edge,
    // each vertex/edge has a directory, and inside the directory it maintains the original
    // partition of the data in the name of "part_00000, part_00001"
    let ldbc_data_dir = matches.value_of("ldbc_dir").map(PathBuf::from).unwrap();
    let graph_dir = matches.value_of("graph_db_dir").map(PathBuf::from).unwrap();
    let raw_partitions: usize = matches.value_of("raw_partitions").unwrap().parse().unwrap();
    let is_whole_data = matches.is_present("is_whole_data");
    let is_only_out_edges = matches.is_present("is_only_out_edges");
    let ppt_store_opt: PropertyStorageOpt =
        matches.value_of("ppt_store_opt").unwrap().parse().unwrap();
    let num_vlabels: usize = matches.value_of("num_vertex_labels").unwrap().parse().unwrap();

    let config = GraphDBConfig::default()
        .root_dir(&graph_dir)
        .number_vertex_labels(num_vlabels)
        .schema_file(&graph_dir.join(DIR_GRAPH_SCHEMA).join(FILE_SCHEMA));

    let mut vertex_dirs = Vec::new();
    let mut edge_dirs = Vec::new();

    for _entry in read_dir(&ldbc_data_dir).unwrap() {
        let entry = _entry.unwrap();
        let fname = entry.file_name().to_str().unwrap().to_string();
        // start with '.'
        if !is_hidden_file(&fname) {
            if is_vertex_file(&fname) {
                vertex_dirs.push((fname.to_uppercase(), entry.path()));
            } else {
                edge_dirs.push((fname.to_uppercase(), entry.path()));
            }
        }
    }

    let schema = Arc::new(config.schema().expect("Read schema error!"));
    let schema_clone = schema.clone();

    timely::execute_from_args(timely_args.into_iter(), move |worker| {
        let index = worker.index();
        let peers = worker.peers();
        let mut input_vertices = InputHandle::new();
        let mut input_edges = InputHandle::new();
        let mut probe = ProbeHandle::new();
        // Counter of nodes and edges
        let count = Rc::new(RefCell::new((0_usize, 0_usize)));
        let _count = count.clone();

        match ppt_store_opt {
            PropertyStorageOpt::SimpleLDBC => run_dataflow!(
                worker,
                PropertyTable,
                SingleValueTable,
                &graph_dir,
                num_vlabels,
                &mut input_vertices,
                &mut input_edges,
                &mut probe,
                _count
            ),
        };

        // Either there is one machine (all its workers) handling the whole data read
        // or all workers process all data together
        if (is_whole_data && index < peers / num_machines) | !is_whole_data {
            let num_readers = if is_whole_data { peers / num_machines } else { peers };

            // Get the partitions that are meant to be read by current worker
            let partitions =
                get_partition_names(ldbc_part_prefix, index, num_readers, raw_partitions);

            // Send vertex_data
            for (vertex_type, vertex_dir) in &vertex_dirs {
                let _vertex_type_id = schema_clone.get_vertex_label_id(&vertex_type);
                if _vertex_type_id.is_none() {
                    continue;
                }
                let vertex_type_id = _vertex_type_id.unwrap();

                let parser = LDBCParser::vertex_parser(vertex_type_id, schema_clone.clone())
                    .expect("Get vertex parser error");

                let mut count = 0;
                for partition in &partitions {
                    info!(
                        "Worker {} process vertex: {:?}, partition {}",
                        index, vertex_dir, partition
                    );
                    let _file = File::open(vertex_dir.join(&partition));
                    if _file.is_err() {
                        // Log and skip processing current file
                        error!("Open {:?} error - {:?}", vertex_dir.join(&partition), _file);
                        continue;
                    }

                    let reader = BufReader::new(_file.unwrap());
                    for _line in reader.lines() {
                        if let Ok(line) = _line {
                            let splitter = line.split(SPLITTER);
                            if let Ok(vertex_meta) = parser.parse_vertex_meta(splitter) {
                                input_vertices.send((vertex_meta, line));
                                count += 1;
                                if count == DEFAULT_BATCH {
                                    worker.step();
                                    count = 0;
                                }
                            }
                        }
                    }
                }
            }

            // Send edge_data
            for (edge_type, edge_dir) in &edge_dirs {
                let _label_tuple = schema_clone.get_edge_label_tuple(&edge_type);
                if _label_tuple.is_none() {
                    continue;
                }
                let label_tuple = _label_tuple.unwrap();

                let parser = LDBCParser::edge_parser(
                    label_tuple.src_vertex_label,
                    label_tuple.dst_vertex_label,
                    label_tuple.edge_label,
                    schema_clone.clone(),
                )
                .expect("Get edge parser error!");

                let mut count = 0;
                for partition in &partitions {
                    info!("Worker {} process edge: {:?}, partition {}", index, edge_dir, partition);
                    let _file = File::open(edge_dir.join(&partition));
                    if _file.is_err() {
                        // Log and skip processing current file
                        error!("Open {:?} error - {:?}", edge_dir.join(&partition), _file);
                        continue;
                    }
                    let reader = BufReader::new(_file.unwrap());
                    for _line in reader.lines() {
                        if let Ok(line) = _line {
                            let splitter = line.split(SPLITTER);
                            if let Ok(edge_meta) = parser.parse_edge_meta(splitter) {
                                input_edges.send((
                                    edge_meta.src_global_id,
                                    Direction::OutEdge,
                                    edge_meta.clone(),
                                    line.clone(),
                                ));

                                if !is_only_out_edges {
                                    // Edge will be duplicated on both vertices
                                    // That means the two end vertices of this edge do not locate
                                    // in the same partition
                                    if which_part(edge_meta.src_global_id, peers)
                                        != which_part(edge_meta.dst_global_id, peers)
                                    {
                                        input_edges.send((
                                            edge_meta.dst_global_id,
                                            Direction::InEdge,
                                            edge_meta,
                                            line,
                                        ));
                                        count += 2;
                                    } else {
                                        count += 1;
                                    }
                                } else {
                                    // Edge will only go to the source vertex
                                    count += 1;
                                }
                                if count == DEFAULT_BATCH {
                                    worker.step();
                                    count = 0;
                                }
                            }
                        }
                    }
                }
            }

            input_vertices.advance_to(1_u32);
            input_edges.advance_to(1_u32);
            worker.step_while(|| probe.less_than(input_edges.time()));

            if index == 0 {
                info!(
                    r#"
                        **************************************
                                Total (#nodes, #edges): {:?}
                        **************************************
                     "#,
                    *count.borrow()
                );
            }
        }
    })
    .unwrap();
}
