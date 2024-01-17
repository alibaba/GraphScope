use std::path::PathBuf;

use clap::{App, Arg};
use env_logger;
use mcsr::graph_loader::get_files_list;
use mcsr::schema::{CsrGraphSchema, InputSchema};
use mcsr::types::*;

fn main() {
    env_logger::init();
    let matches = App::new(NAME)
        .version(VERSION)
        .about("Build graph storage on single machine.")
        .args(&[
            Arg::with_name("input_dir")
                .short("d")
                .long_help("The filepath of input")
                .required(true)
                .takes_value(true)
                .index(1),
            Arg::with_name("input_schema_path")
                .short("i")
                .long_help("The filepath of input schema")
                .required(true)
                .takes_value(true)
                .index(2),
            Arg::with_name("graph_schema_path")
                .short("r")
                .long_help("The filepath of graph schema")
                .required(true)
                .takes_value(true)
                .index(3),
            Arg::with_name("graph_data_dir")
                .short("g")
                .long_help("The directory to graph store")
                .required(true)
                .takes_value(true)
                .index(4),
        ])
        .get_matches();

    let input_dir = PathBuf::from(
        matches
            .value_of("input_dir")
            .unwrap()
            .to_string(),
    );
    let input_schema_path = matches
        .value_of("input_schema_path")
        .unwrap()
        .to_string();
    let graph_schema_path = matches
        .value_of("graph_schema_path")
        .unwrap()
        .to_string();
    let graph_data_dir = matches
        .value_of("graph_data_dir")
        .unwrap()
        .to_string();
    let graph_schema =
        CsrGraphSchema::from_json_file(&graph_schema_path).expect("Read graph schema error!");
    let input_schema =
        InputSchema::from_json_file(&input_schema_path, &graph_schema).expect("Read input schema error!");
    let file_strings = input_schema.get_vertex_file(0u8);
    for i in get_files_list(&input_dir, file_strings.unwrap()) {
        println!("{:?}", i);
    }
    let out_dir = PathBuf::from(format!("{}/{}", graph_data_dir, DIR_GRAPH_SCHEMA));
    if !out_dir.exists() {
        std::fs::create_dir_all(&out_dir).expect("Create graph schema directory error");
    }
    graph_schema
        .to_json_file(&out_dir.join(FILE_SCHEMA))
        .expect("Write graph schema error!");
}
