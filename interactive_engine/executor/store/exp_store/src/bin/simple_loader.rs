use std::path::PathBuf;

use clap::{App, Arg};
use graph_store::config::{JsonConf, DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::ldbc::GraphLoader;
use graph_store::prelude::{DefaultId, GraphDBConfig, InternalId, LargeGraphDB, NAME, VERSION};

fn main() {
    env_logger::init();
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
            Arg::with_name("meta")
                .short("m")
                .long_help("The metadata file")
                .required(true)
                .takes_value(true)
                .index(3),
            Arg::with_name("partition")
                .short("p")
                .long_help("The number of partitions")
                .takes_value(true),
            Arg::with_name("delimiter")
                .short("t")
                .long_help(
                    "The delimiter of the raw data [comma|semicolon|pipe]. pipe (|) is the default option",
                )
                .takes_value(true),
        ])
        .get_matches();

    let raw_data_dir = matches
        .value_of("raw_data_dir")
        .unwrap()
        .to_string();
    let graph_data_dir = matches
        .value_of("graph_data_dir")
        .unwrap()
        .to_string();
    let meta_file = matches.value_of("meta").unwrap().to_string();
    let partition_num = matches
        .value_of("partition")
        .unwrap_or("1")
        .parse::<usize>()
        .expect(&format!("Specify invalid partition number"));

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
    let out_data_dir = PathBuf::from(format!("{}/{}", graph_data_dir, DIR_GRAPH_SCHEMA));
    if !out_data_dir.exists() {
        std::fs::create_dir_all(&out_data_dir).expect("Create graph schema directory error");
    }

    let mut handles = Vec::with_capacity(partition_num);
    for i in 0..partition_num {
        let raw_dir = raw_data_dir.clone();
        let graph_dir = graph_data_dir.clone();
        let out_dir = out_data_dir.clone();
        let meta_f = meta_file.clone();

        let handle = std::thread::spawn(move || {
            let mut loader: GraphLoader = GraphLoader::new(raw_dir, graph_dir, meta_f, i, partition_num);
            loader = loader.with_delimiter(delimiter);
            if i == 0 {
                // the first thread
                let schema = loader.get_ldbc_parser().get_graph_schema();
                schema
                    .to_json_file(&out_dir.join(FILE_SCHEMA))
                    .expect("Write graph schema error!");
            }

            loader.load().expect("Load error");

            let graph = loader.into_mutable_graph();
            graph.export().expect("Export error!");
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for i in 0..partition_num {
        println!("Test opening graph with partition {}", i);
        let config = GraphDBConfig::default()
            .root_dir(&graph_data_dir)
            .partition(i)
            .schema_file(&format!("{}/{}/{}", graph_data_dir, DIR_GRAPH_SCHEMA, FILE_SCHEMA));
        let graph: LargeGraphDB<DefaultId, InternalId> = config.open().expect("Open graph error");
        graph.print_statistics();
    }
}
