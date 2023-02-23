use std::path::PathBuf;

use clap::{App, Arg};
use env_logger;

use mcsr::graph_las::GraphLAS;
use mcsr::schema::LDBCGraphSchema;
use mcsr::types::*;
use mpi::traits::*;

fn main() {
    env_logger::init();
    let universe = mpi::initialize().unwrap();
    let matches = App::new(NAME)
        .version(VERSION)
        .about("Build graph storage on single machine.")
        .args(&[
            Arg::with_name("raw_data_dir")
                .short("r")
                .long_help("The directory to the raw data")
                .required(true)
                .takes_value(true)
                .index(1),
            Arg::with_name("graph_data_dir")
                .short("g")
                .long_help("The directory to graph store")
                .required(true)
                .takes_value(true)
                .index(2),
            Arg::with_name("schema_file")
                .short("s")
                .long_help("The schema file")
                .required(true)
                .takes_value(true)
                .index(3),
            Arg::with_name("trimed_schema_file")
                .short("m")
                .long_help("The trimed schema file")
                .required(true)
                .takes_value(true)
                .index(4),
            Arg::with_name("delimiter")
                .short("t")
                .long_help("The delimiter of the raw data [comma|semicolon|pipe]. pipe (|) is the default option")
                .takes_value(true),
        ]).get_matches();

    let raw_data_dir = matches.value_of("raw_data_dir").unwrap().to_string();
    let graph_data_dir = matches.value_of("graph_data_dir").unwrap().to_string();
    let schema_file = matches.value_of("schema_file").unwrap().to_string();
    let trimed_schema_file = matches.value_of("trimed_schema_file").unwrap().to_string();
    let world = universe.world();
    let partition_num = world.size() as usize;
    let partition_index = world.rank() as usize;
    let delimiter_str = matches
        .value_of("delimiter")
        .unwrap_or("pipe")
        .to_uppercase();

    let delimiter = if delimiter_str.as_str() == "COMMA" {
        b','
    } else if delimiter_str.as_str() == "SEMICOLON" {
        b';'
    } else {
        b'|'
    };

    // Copy graph schema to graph_data_dir/graph_schema/schema.json if no there
    let out_dir = PathBuf::from(format!("{}/{}", graph_data_dir, DIR_GRAPH_SCHEMA));
    if !out_dir.exists() {
        std::fs::create_dir_all(&out_dir).expect("Create graph schema directory error");
    }
    let trim =
        LDBCGraphSchema::from_json_file(&trimed_schema_file).expect("Read trimed schema error!");
    trim.to_json_file(&out_dir.join(FILE_SCHEMA))
        .expect("Write graph schema error!");

    let mut handles = Vec::with_capacity(partition_num);
    let raw_dir = raw_data_dir.clone();
    // let graph_dir = graph_data_dir.clone();
    let schema_f = schema_file.clone();
    let trim_f = trimed_schema_file.clone();

    let cur_out_dir = graph_data_dir.clone();

    let handle = std::thread::spawn(move || {
        let mut laser: GraphLAS = GraphLAS::new(
            raw_dir,
            cur_out_dir.as_str(),
            schema_f,
            trim_f,
            partition_index,
            partition_num,
        );
        laser = laser.with_delimiter(delimiter);

        laser.load_beta().expect("Load error");
    });

    handles.push(handle);

    for handle in handles {
        handle.join().unwrap();
    }
}
