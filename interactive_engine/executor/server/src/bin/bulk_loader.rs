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
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "r", long = "raw_data_dir")]
    raw_data_dir: PathBuf,
    #[structopt(short = "g", long = "graph_data_dir")]
    graph_data_dir: PathBuf,
    #[structopt(short = "i", long = "input_schema_file")]
    input_schema_path: PathBuf,
    #[structopt(short = "s", long = "graph_schema_file")]
    graph_schema_path: PathBuf,
    #[structopt(short = "n", long = "partition_num", default_value = "1")]
    partition_num: u32,
    #[structopt(short = "p", long = "partition_index", default_value = "0")]
    partition_index: u32,
    #[structopt(short = "d", long = "delimiter", default_value = "PIPE")]
    delimiter: String,
    #[structopt(long = "skip_header")]
    skip_header: bool,
}

fn main() {
    let config: Config = Config::from_args();
    let raw_data_dir = config.raw_data_dir.clone();
    let graph_data_dir = config.graph_data_dir.clone();
    let input_schema_path = config.input_schema_path.clone();
    let graph_schema_path = config.graph_schema_path.clone();
    let partition_num = config.partition_num;
    let partition_index = config.partition_index;
    let delimiter_str = config.delimiter.clone();
    let skip_header = config.skip_header;

    let delimiter = if delimiter_str.as_str() == "COMMA" {
        b','
    } else if delimiter_str.as_str() == "SEMICOLON" {
        b';'
    } else {
        b'|'
    };

    let out_dir = graph_data_dir.clone().join(DIR_GRAPH_SCHEMA);
    if !out_dir.exists() {
        std::fs::create_dir_all(&out_dir).expect("Create graph schema directory error");
    }
    let graph_schema =
        CsrGraphSchema::from_json_file(&graph_schema_path).expect("Read graph schema error!");
    graph_schema
        .to_json_file(&out_dir.join(FILE_SCHEMA))
        .expect("Write graph schema error!");

    let mut handles = Vec::with_capacity(partition_num as usize);
    let raw_dir = raw_data_dir.clone();
    let graph_schema_f = graph_schema_path.clone();
    let input_schema_f = input_schema_path.clone();

    let cur_out_dir = graph_data_dir.clone();

    let handle = std::thread::spawn(move || {
        let mut loader: GraphLoader<usize, usize> = GraphLoader::<usize, usize>::new(
            raw_dir,
            cur_out_dir,
            input_schema_f,
            graph_schema_f,
            partition_index as usize,
            partition_num as usize,
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
