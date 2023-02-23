use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use chrono::{TimeZone, Utc};
use clap::{App, Arg};
use mcsr::columns::DataType;
use mcsr::graph::Direction;
use mcsr::graph_db_impl::CsrDB;
use mcsr::ldbc_parser::LDBCVertexParser;
use mcsr::schema::Schema;
use mcsr::types::{DefaultId, InternalId, DIR_BINARY_DATA, NAME, VERSION};

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

fn get_millis(t: u64) -> u64 {
    let mut dt = t;
    let ss = dt % 1000;
    dt = dt / 1000;
    let s = dt % 100;
    dt = dt / 100;
    let m = dt % 100;
    dt = dt / 100;
    let h = dt % 100;
    dt = dt / 100;
    let d = dt % 100;
    dt = dt / 100;
    let mm = dt % 100;
    dt = dt / 100;
    let y = dt % 10000;

    Utc.ymd(y as i32, mm as u32, d as u32)
        .and_hms_milli(h as u32, m as u32, s as u32, ss as u32)
        .timestamp_millis() as u64
}

fn traverse_partition(graph_data_dir: &String, output_dir: &String, partition: usize) {
    let graph = CsrDB::<DefaultId, InternalId>::import(graph_data_dir.as_str(), partition).unwrap();

    let src_label = graph
        .graph_schema
        .get_vertex_label_id("PERSON")
        .unwrap();
    let dst_label = graph
        .graph_schema
        .get_vertex_label_id("COMMENT")
        .unwrap();
    let edge_label = graph
        .graph_schema
        .get_edge_label_id("LIKES")
        .unwrap();
    let sub_graph = graph.get_sub_graph(src_label, edge_label, dst_label, Direction::Outgoing);

    let vnum = sub_graph.get_vertex_num();
    let output_dir_path = PathBuf::from_str(output_dir.as_str()).unwrap();
    let mut vfile = File::create(output_dir_path.join("PERSON")).unwrap();
    let header = graph
        .graph_schema
        .get_vertex_header(src_label)
        .unwrap();
    let col_num = header.len();
    for i in 0..vnum {
        let id = sub_graph.get_src_global_id(i).unwrap();
        let oid = LDBCVertexParser::<DefaultId>::get_original_id(id);
        write!(vfile, "{}", oid).unwrap();
        for k in 1..col_num {
            let item = sub_graph
                .vertex_data
                .get_item_by_index(k, i as usize)
                .unwrap();
            if header[k].1 == DataType::DateTime {
                write!(vfile, "|{}", get_millis(item.as_u64().unwrap())).unwrap();
            } else {
                write!(vfile, "|{}", item.to_string()).unwrap();
            }
        }
        writeln!(vfile).unwrap();
    }

    let mut efile = File::create(output_dir_path.join("PERSON_LIKES_COMMENT")).unwrap();
    for i in 0..vnum {
        let src = sub_graph.get_src_global_id(i).unwrap();
        let src_oid = LDBCVertexParser::<DefaultId>::get_original_id(src);
        for e in sub_graph.get_adj_list(i).unwrap() {
            let dst = sub_graph.get_dst_global_id(e.neighbor).unwrap();
            let dst_oid = LDBCVertexParser::<DefaultId>::get_original_id(dst);
            writeln!(efile, "{}|{}", src_oid, dst_oid).unwrap();
        }
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

    for i in 0..partition_num {
        traverse_partition(&graph_data_dir, &output_dir, i);
    }
}
