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

use std::path::PathBuf;

use bmcsr::graph_loader::GraphLoader;
use bmcsr::schema::CsrGraphSchema;
use bmcsr::types::*;
use clap::{App, Arg};
use env_logger;

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
            Arg::with_name("input_schema_file")
                .long_help("The input schema file")
                .required(true)
                .takes_value(true)
                .index(3),
            Arg::with_name("graph_schema_file")
                .long_help("The graph schema file")
                .required(true)
                .takes_value(true)
                .index(4),
            Arg::with_name("partition")
                .short("p")
                .long_help("The number of partitions")
                .takes_value(true),
            Arg::with_name("index")
                .short("i")
                .long_help("The index of partitions")
                .takes_value(true),
            Arg::with_name("delimiter")
                .short("t")
                .long_help(
                    "The delimiter of the raw data [comma|semicolon|pipe]. pipe (|) is the default option",
                )
                .takes_value(true),
            Arg::with_name("skip_header")
                .long("skip_header")
                .long_help("Whether skip the first line in input file")
                .takes_value(false),
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
    let input_schema_file = matches
        .value_of("input_schema_file")
        .unwrap()
        .to_string();
    let graph_schema_file = matches
        .value_of("graph_schema_file")
        .unwrap()
        .to_string();
    let partition_num = matches
        .value_of("partition")
        .unwrap_or("1")
        .parse::<usize>()
        .expect(&format!("Specify invalid partition number"));
    let partition_index = matches
        .value_of("index")
        .unwrap_or("0")
        .parse::<usize>()
        .expect(&format!("Specify invalid partition number"));

    let delimiter_str = matches
        .value_of("delimiter")
        .unwrap_or("pipe")
        .to_uppercase();

    let skip_header = matches.is_present("skip_header");

    let delimiter = if delimiter_str.as_str() == "COMMA" {
        b','
    } else if delimiter_str.as_str() == "SEMICOLON" {
        b';'
    } else {
        b'|'
    };

    let out_dir = PathBuf::from(format!("{}/{}", graph_data_dir, DIR_GRAPH_SCHEMA));
    if !out_dir.exists() {
        std::fs::create_dir_all(&out_dir).expect("Create graph schema directory error");
    }
    let graph_schema =
        CsrGraphSchema::from_json_file(&graph_schema_file).expect("Read graph schema error!");
    graph_schema
        .to_json_file(&out_dir.join(FILE_SCHEMA))
        .expect("Write graph schema error!");

    let mut handles = Vec::with_capacity(partition_num);
    let raw_dir = raw_data_dir.clone();
    let graph_schema_f = graph_schema_file.clone();
    let input_schema_f = input_schema_file.clone();

    let cur_out_dir = graph_data_dir.clone();

    let handle = std::thread::spawn(move || {
        let mut loader: GraphLoader<usize, usize> = GraphLoader::<usize, usize>::new(
            raw_dir,
            cur_out_dir.as_str(),
            input_schema_f,
            graph_schema_f,
            partition_index,
            partition_num,
        );
        loader = loader.with_delimiter(delimiter);
        if skip_header {
            loader.skip_header();
        }

        loader.load().expect("Load error");
    });

    handles.push(handle);

    for handle in handles {
        handle.join().unwrap();
    }
}
