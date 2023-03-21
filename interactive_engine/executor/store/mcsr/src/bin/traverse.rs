use clap::{App, Arg};
use mcsr::columns::DataType;
use mcsr::date::Date;
use mcsr::graph::IndexType;
use mcsr::graph_db::GlobalCsrTrait;
use mcsr::graph_db_impl::CsrDB;
use mcsr::graph_las::is_static_vertex;
use mcsr::ldbc_parser::LDBCVertexParser;
use mcsr::schema::Schema;
use mcsr::types::{DefaultId, InternalId, LabelId, DIR_BINARY_DATA, NAME, VERSION};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;

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
        if let Some(v_label) = graph.graph_schema.get_vertex_label_id(n.as_str()) {
            if is_static_vertex(v_label) && graph.partition != 0 {
                continue;
            }
            println!("outputing vertex-{}", n);
            let header = graph.graph_schema.get_vertex_header(v_label).unwrap();
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
                            v.get_property(c.0.as_str()).unwrap().to_string()
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
    graph: &CsrDB,
    output_dir: &String,
    files: &mut HashMap<(LabelId, LabelId, LabelId), File>,
) {
    let output_dir_path = PathBuf::from_str(output_dir.as_str()).unwrap();
    let study_at_e_label = graph.graph_schema.get_edge_label_id("STUDYAT").unwrap();
    let knows_e_label = graph.graph_schema.get_edge_label_id("KNOWS").unwrap();
    for e in graph.get_all_edges(None) {
        let src = e.get_src_id();
        let dst = e.get_dst_id();
        let src_label = LDBCVertexParser::<DefaultId>::get_label_id(src);
        let dst_label = LDBCVertexParser::<DefaultId>::get_label_id(dst);
        if is_static_vertex(src_label) && is_static_vertex(dst_label) && graph.partition != 0 {
            continue;
        }
        let src_oid = LDBCVertexParser::<DefaultId>::get_original_id(src);
        let dst_oid = LDBCVertexParser::<DefaultId>::get_original_id(dst);
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
        if e_label == study_at_e_label {
            writeln!(
                file,
                "\"{}\"|\"{}\"|\"{}\"",
                src_oid,
                dst_oid,
                e.get_encoded_data().index(),
            )
            .unwrap();
        } else if e_label == knows_e_label {
            writeln!(
                file,
                "\"{}\"|\"{}\"|\"{}\"",
                Date::from_u32(e.get_encoded_data().index() as u32),
                src_oid,
                dst_oid,
            )
            .unwrap();
        } else {
            writeln!(file, "\"{}\"|\"{}\"", src_oid, dst_oid).unwrap();
        }
    }
}

fn traverse_partition(
    graph_data_dir: &String,
    output_dir: &String,
    partition: usize,
    v_files: &mut HashMap<LabelId, File>,
    e_files: &mut HashMap<(LabelId, LabelId, LabelId), File>,
) {
    let graph = CsrDB::<DefaultId, InternalId>::import(graph_data_dir.as_str(), partition).unwrap();

    output_vertices(&graph, output_dir, v_files);
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

    let graph_data_dir = matches.value_of("graph_data_dir").unwrap().to_string();
    let output_dir = matches.value_of("output_dir").unwrap().to_string();

    let partition_num = get_partition_num(&graph_data_dir);

    let mut v_files = HashMap::<LabelId, File>::new();
    let mut e_files = HashMap::<(LabelId, LabelId, LabelId), File>::new();
    for i in 0..partition_num {
        traverse_partition(&graph_data_dir, &output_dir, i, &mut v_files, &mut e_files);
    }
}
