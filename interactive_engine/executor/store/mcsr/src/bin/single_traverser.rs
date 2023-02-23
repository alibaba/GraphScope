use clap::{App, Arg};
use log::info;
use mcsr::date::Date;
use mcsr::graph::IndexType;
use mcsr::graph_db::GlobalCsrTrait;
use mcsr::graph_db_impl::{is_single_ie_csr, is_single_oe_csr, CsrDB, SingleSubGraph, SubGraph};
use mcsr::ldbc_parser::LDBCVertexParser;
use mcsr::schema::{LDBCGraphSchema, Schema};
use mcsr::types::{DefaultId, InternalId, LabelId, NAME, VERSION};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::{thread, time};

fn output_vertices<
    G: IndexType + std::marker::Sync + std::marker::Send,
    I: IndexType + std::marker::Sync + std::marker::Send,
>(
    graph: &CsrDB<G, I>,
    output_dir: &String,
    files: &mut HashMap<LabelId, File>,
) {
    let vertex_label_names = graph.graph_schema.vertex_label_names();
    let output_dir_path = PathBuf::from_str(output_dir.as_str()).unwrap();
    for n in vertex_label_names.iter() {
        if let Some(v_label) = graph.graph_schema.get_vertex_label_id(n.as_str()) {
            println!("outputing vertex-{}", n);
            let header = graph.graph_schema.get_vertex_header(v_label).unwrap();
            if !files.contains_key(&v_label) {
                let file = File::create(output_dir_path.join(n.as_str())).unwrap();
                files.insert(v_label.clone(), file);
            }
            let file = files.get_mut(&v_label).unwrap();

            let v_labels = vec![v_label];
            for v in graph.get_all_vertices(Some(&v_labels)) {
                let id = LDBCVertexParser::<G>::get_original_id(v.get_id());
                write!(file, "\"{}\"", id.index()).unwrap();
                for c in header {
                    write!(
                        file,
                        "|\"{}\"",
                        v.get_property(c.0.as_str()).unwrap().to_string()
                    )
                    .unwrap();
                }
                writeln!(file).unwrap();
            }
        }
    }
}

fn output_edges<
    G: IndexType + std::marker::Sync + std::marker::Send,
    I: IndexType + std::marker::Sync + std::marker::Send,
>(
    graph: &CsrDB<G, I>,
    output_dir: &String,
    files: &mut HashMap<(LabelId, LabelId, LabelId), File>,
) {
    let output_dir_path = PathBuf::from_str(output_dir.as_str()).unwrap();
    for e in graph.get_all_edges(None) {
        let src = e.get_src_id();
        let dst = e.get_dst_id();
        let src_label = LDBCVertexParser::<G>::get_label_id(src);
        let src_oid = LDBCVertexParser::<G>::get_original_id(src);
        let dst_label = LDBCVertexParser::<G>::get_label_id(dst);
        let dst_oid = LDBCVertexParser::<G>::get_original_id(dst);
        let e_label = e.get_label();
        let label_tuple = (src_label, e_label, dst_label);
        if !files.contains_key(&label_tuple) {
            let src_label_name =
                graph.graph_schema.vertex_label_names()[src_label as usize].clone();
            let dst_label_name =
                graph.graph_schema.vertex_label_names()[dst_label as usize].clone();
            let edge_label_name = graph.graph_schema.edge_label_names()[e_label as usize].clone();
            let filename = src_label_name.clone()
                + "_"
                + &*edge_label_name.clone()
                + "_"
                + &*dst_label_name.clone();
            let file = File::create(output_dir_path.join(filename.as_str())).unwrap();
            files.insert(label_tuple.clone(), file);
        }
        let file = files.get_mut(&label_tuple).unwrap();
        writeln!(file, "\"{}\"|\"{}\"", src_oid.index(), dst_oid.index()).unwrap();
    }
}

fn output_graph<
    G: IndexType + std::marker::Sync + std::marker::Send,
    I: IndexType + std::marker::Sync + std::marker::Send,
>(
    graph: SubGraph<'_, G, I>,
    schema: Arc<LDBCGraphSchema>,
    output_dir: &String,
    files: &mut HashMap<(LabelId, LabelId, LabelId), File>,
) {
    if graph.get_edge_num() == 0 {
        return;
    }
    let output_dir_path = PathBuf::from_str(output_dir.as_str()).unwrap();
    let vnum = graph.get_vertex_num().index();
    let src_label = graph.get_src_label();
    let dst_label = graph.get_dst_label();
    let edge_label = graph.get_edge_label();
    println!(
        "outputing graph: {}, {}, {}",
        src_label, edge_label, dst_label
    );
    let label_tuple = (src_label, edge_label, dst_label);
    if !files.contains_key(&label_tuple) {
        let src_label_name = schema.vertex_label_names()[src_label as usize].clone();
        let dst_label_name = schema.vertex_label_names()[dst_label as usize].clone();
        let edge_label_name = schema.edge_label_names()[edge_label as usize].clone();
        let filename = src_label_name.clone()
            + "_"
            + &*edge_label_name.clone()
            + "_"
            + &*dst_label_name.clone();
        let file = File::create(output_dir_path.join(filename.as_str())).unwrap();
        files.insert(label_tuple.clone(), file);
    }
    let file = files.get_mut(&label_tuple).unwrap();
    for i in 0..vnum {
        let v = I::new(i);
        if let Some(edges) = graph.get_adj_list(v) {
            let src = graph.get_src_global_id(v).unwrap();
            let src_oid = LDBCVertexParser::<G>::get_original_id(src);
            for e in edges {
                let dst = graph.get_dst_global_id(e.neighbor).unwrap();
                let dst_oid = LDBCVertexParser::<G>::get_original_id(dst);
                writeln!(file, "\"{}\"|\"{}\"", src_oid.index(), dst_oid.index()).unwrap();
            }
        }
    }
}

fn output_knows_graph<
    G: IndexType + std::marker::Sync + std::marker::Send,
    I: IndexType + std::marker::Sync + std::marker::Send,
>(
    graph: SubGraph<'_, G, I>,
    schema: Arc<LDBCGraphSchema>,
    output_dir: &String,
    files: &mut HashMap<(LabelId, LabelId, LabelId), File>,
) {
    if graph.get_edge_num() == 0 {
        return;
    }
    let output_dir_path = PathBuf::from_str(output_dir.as_str()).unwrap();
    let vnum = graph.get_vertex_num().index();
    let src_label = graph.get_src_label();
    let dst_label = graph.get_dst_label();
    let edge_label = graph.get_edge_label();
    println!(
        "outputing graph: {}, {}, {}",
        src_label, edge_label, dst_label
    );
    let label_tuple = (src_label, edge_label, dst_label);
    if !files.contains_key(&label_tuple) {
        let src_label_name = schema.vertex_label_names()[src_label as usize].clone();
        let dst_label_name = schema.vertex_label_names()[dst_label as usize].clone();
        let edge_label_name = schema.edge_label_names()[edge_label as usize].clone();
        let filename = src_label_name.clone()
            + "_"
            + &*edge_label_name.clone()
            + "_"
            + &*dst_label_name.clone();
        let file = File::create(output_dir_path.join(filename.as_str())).unwrap();
        files.insert(label_tuple.clone(), file);
    }
    let file = files.get_mut(&label_tuple).unwrap();
    for i in 0..vnum {
        let v = I::new(i);
        if let Some(edges) = graph.get_adj_list(v) {
            let src = graph.get_src_global_id(v).unwrap();
            let src_oid = LDBCVertexParser::<G>::get_original_id(src);
            for e in edges {
                let dst = graph.get_dst_global_id(e.neighbor).unwrap();
                let date = Date::from_u32(e.neighbor.hi().index() as u32);
                let dst_oid = LDBCVertexParser::<G>::get_original_id(dst);
                writeln!(
                    file,
                    "\"{}\"|\"{}\"|\"{}\"",
                    src_oid.index(),
                    dst_oid.index(),
                    date
                )
                .unwrap();
            }
        }
    }
}

fn output_graph_in<
    G: IndexType + std::marker::Sync + std::marker::Send,
    I: IndexType + std::marker::Sync + std::marker::Send,
>(
    graph: SubGraph<'_, G, I>,
    schema: Arc<LDBCGraphSchema>,
    output_dir: &String,
    files: &mut HashMap<(LabelId, LabelId, LabelId), File>,
) {
    if graph.get_edge_num() == 0 {
        return;
    }
    let output_dir_path = PathBuf::from_str(output_dir.as_str()).unwrap();
    let vnum = graph.get_vertex_num().index();
    let src_label = graph.get_src_label();
    let dst_label = graph.get_dst_label();
    let edge_label = graph.get_edge_label();
    let label_tuple = (src_label, edge_label, dst_label);
    if !files.contains_key(&label_tuple) {
        let src_label_name = schema.vertex_label_names()[src_label as usize].clone();
        let dst_label_name = schema.vertex_label_names()[dst_label as usize].clone();
        let edge_label_name = schema.edge_label_names()[edge_label as usize].clone();
        let filename = dst_label_name.clone()
            + "_"
            + &*edge_label_name.clone()
            + "_"
            + &*src_label_name.clone();
        let file = File::create(output_dir_path.join(filename.as_str())).unwrap();
        files.insert(label_tuple.clone(), file);
    }
    let file = files.get_mut(&label_tuple).unwrap();
    for i in 0..vnum {
        let v = I::new(i);
        if let Some(edges) = graph.get_adj_list(v) {
            let src = graph.get_src_global_id(v).unwrap();
            let src_oid = LDBCVertexParser::<G>::get_original_id(src);
            for e in edges {
                let dst = graph.get_dst_global_id(e.neighbor).unwrap();
                let dst_oid = LDBCVertexParser::<G>::get_original_id(dst);
                writeln!(file, "\"{}\"|\"{}\"", dst_oid.index(), src_oid.index()).unwrap();
            }
        }
    }
}

fn output_single_graph<
    G: IndexType + std::marker::Sync + std::marker::Send,
    I: IndexType + std::marker::Sync + std::marker::Send,
>(
    graph: SingleSubGraph<'_, G, I>,
    schema: Arc<LDBCGraphSchema>,
    output_dir: &String,
    files: &mut HashMap<(LabelId, LabelId, LabelId), File>,
) {
    if graph.get_edge_num() == 0 {
        return;
    }
    let output_dir_path = PathBuf::from_str(output_dir.as_str()).unwrap();
    let vnum = graph.get_vertex_num().index();
    let src_label = graph.get_src_label();
    let dst_label = graph.get_dst_label();
    let edge_label = graph.get_edge_label();
    println!(
        "outputing single graph: {}, {}, {}",
        src_label, edge_label, dst_label
    );
    let label_tuple = (src_label, edge_label, dst_label);
    if !files.contains_key(&label_tuple) {
        let src_label_name = schema.vertex_label_names()[src_label as usize].clone();
        let dst_label_name = schema.vertex_label_names()[dst_label as usize].clone();
        let edge_label_name = schema.edge_label_names()[edge_label as usize].clone();
        let filename = src_label_name.clone()
            + "_"
            + &*edge_label_name.clone()
            + "_"
            + &*dst_label_name.clone();
        let file = File::create(output_dir_path.join(filename.as_str())).unwrap();
        files.insert(label_tuple.clone(), file);
    }
    let file = files.get_mut(&label_tuple).unwrap();
    for i in 0..vnum {
        let v = I::new(i);
        if let Some(edge) = graph.get_edge(v) {
            let src = graph.get_src_global_id(v).unwrap();
            let src_oid = LDBCVertexParser::<G>::get_original_id(src);
            let dst = graph.get_dst_global_id(edge.neighbor).unwrap();
            let dst_oid = LDBCVertexParser::<G>::get_original_id(dst);
            writeln!(file, "\"{}\"|\"{}\"", src_oid.index(), dst_oid.index()).unwrap();
        }
    }
}

fn output_single_graph_in<
    G: IndexType + std::marker::Sync + std::marker::Send,
    I: IndexType + std::marker::Sync + std::marker::Send,
>(
    graph: SingleSubGraph<'_, G, I>,
    schema: Arc<LDBCGraphSchema>,
    output_dir: &String,
    files: &mut HashMap<(LabelId, LabelId, LabelId), File>,
) {
    if graph.get_edge_num() == 0 {
        return;
    }
    let output_dir_path = PathBuf::from_str(output_dir.as_str()).unwrap();
    let vnum = graph.get_vertex_num().index();
    let src_label = graph.get_src_label();
    let dst_label = graph.get_dst_label();
    let edge_label = graph.get_edge_label();
    let label_tuple = (src_label, edge_label, dst_label);
    if !files.contains_key(&label_tuple) {
        let src_label_name = schema.vertex_label_names()[src_label as usize].clone();
        let dst_label_name = schema.vertex_label_names()[dst_label as usize].clone();
        let edge_label_name = schema.edge_label_names()[edge_label as usize].clone();
        let filename = dst_label_name.clone()
            + "_"
            + &*edge_label_name.clone()
            + "_"
            + &*src_label_name.clone();
        let file = File::create(output_dir_path.join(filename.as_str())).unwrap();
        files.insert(label_tuple.clone(), file);
    }
    let file = files.get_mut(&label_tuple).unwrap();
    for i in 0..vnum {
        let v = I::new(i);
        if let Some(edge) = graph.get_edge(v) {
            let src = graph.get_src_global_id(v).unwrap();
            let src_oid = LDBCVertexParser::<G>::get_original_id(src);
            let dst = graph.get_dst_global_id(edge.neighbor).unwrap();
            let dst_oid = LDBCVertexParser::<G>::get_original_id(dst);
            writeln!(file, "\"{}\"|\"{}\"", dst_oid.index(), src_oid.index()).unwrap();
        }
    }
}

fn output_outgoing_edges(
    graph: &CsrDB,
    output_dir: &String,
    files: &mut HashMap<(LabelId, LabelId, LabelId), File>,
) {
    let v_label_num = graph.vertex_label_num as LabelId;
    let e_label_num = graph.edge_label_num as LabelId;
    for src_label in 0..v_label_num {
        for dst_label in 0..v_label_num {
            for edge_label in 0..e_label_num {
                // if edge_label == 1 as LabelId {
                if false {
                    output_knows_graph(
                        graph.get_sub_graph(
                            src_label,
                            edge_label,
                            dst_label,
                            mcsr::graph::Direction::Outgoing,
                        ),
                        graph.graph_schema.clone(),
                        output_dir,
                        files,
                    )
                } else {
                    if is_single_oe_csr(src_label, dst_label, edge_label) {
                        output_single_graph(
                            graph.get_single_sub_graph(
                                src_label,
                                edge_label,
                                dst_label,
                                mcsr::graph::Direction::Outgoing,
                            ),
                            graph.graph_schema.clone(),
                            output_dir,
                            files,
                        );
                    } else {
                        output_graph(
                            graph.get_sub_graph(
                                src_label,
                                edge_label,
                                dst_label,
                                mcsr::graph::Direction::Outgoing,
                            ),
                            graph.graph_schema.clone(),
                            output_dir,
                            files,
                        );
                    }
                }
            }
        }
    }
}

fn output_incoming_edges(
    graph: &CsrDB,
    output_dir: &String,
    files: &mut HashMap<(LabelId, LabelId, LabelId), File>,
) {
    let v_label_num = graph.vertex_label_num as LabelId;
    let e_label_num = graph.edge_label_num as LabelId;
    for src_label in 0..v_label_num {
        for dst_label in 0..v_label_num {
            for edge_label in 0..e_label_num {
                // if is_single_ie_csr(src_label, dst_label, edge_label) {
                if is_single_ie_csr(dst_label, src_label, edge_label) {
                    output_single_graph_in(
                        graph.get_single_sub_graph(
                            src_label,
                            edge_label,
                            dst_label,
                            mcsr::graph::Direction::Incoming,
                        ),
                        graph.graph_schema.clone(),
                        output_dir,
                        files,
                    );
                } else {
                    output_graph_in(
                        graph.get_sub_graph(
                            src_label,
                            edge_label,
                            dst_label,
                            mcsr::graph::Direction::Incoming,
                        ),
                        graph.graph_schema.clone(),
                        output_dir,
                        files,
                    );
                }
            }
        }
    }
}

fn traverse_partition(
    graph_data_dir: &String,
    output_dir: &String,
    partition: usize,
    v_files: &mut HashMap<LabelId, File>,
    e_files: &mut HashMap<(LabelId, LabelId, LabelId), File>,
    t: i32,
) {
    info!("start import graph");
    let graph = CsrDB::<DefaultId, InternalId>::import(graph_data_dir.as_str(), partition).unwrap();
    info!("finished import graph");

    if t == 0 {
        loop {
            let ten_millis = time::Duration::from_millis(10);
            thread::sleep(ten_millis);
        }
    } else if t == 1 {
        println!("start output vertices");
        output_vertices(&graph, output_dir, v_files);
        println!("start output edges");
        output_edges(&graph, output_dir, e_files);
    } else if t == 2 {
        println!("start output vertices");
        output_vertices(&graph, output_dir, v_files);
        println!("start output outgoing edges");
        output_outgoing_edges(&graph, output_dir, e_files);
    } else if t == 3 {
        println!("start output vertices");
        output_vertices(&graph, output_dir, v_files);
        println!("start output incoming edges");
        output_incoming_edges(&graph, output_dir, e_files);
    }
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
            Arg::with_name("index")
                .short("i")
                .long_help("The partition to traverse")
                .required(true)
                .takes_value(true)
                .index(3),
            Arg::with_name("type")
                .short("t")
                .long_help("The operaton type")
                .required(true)
                .takes_value(true)
                .index(4),
        ])
        .get_matches();

    let graph_data_dir = matches.value_of("graph_data_dir").unwrap().to_string();
    let output_dir = matches.value_of("output_dir").unwrap().to_string();

    let partition_id = matches.value_of("index").unwrap().parse::<usize>().unwrap();
    let t = matches.value_of("type").unwrap().parse::<i32>().unwrap();

    let mut v_files = HashMap::<LabelId, File>::new();
    let mut e_files = HashMap::<(LabelId, LabelId, LabelId), File>::new();
    traverse_partition(
        &graph_data_dir,
        &output_dir,
        partition_id,
        &mut v_files,
        &mut e_files,
        t,
    );
}
