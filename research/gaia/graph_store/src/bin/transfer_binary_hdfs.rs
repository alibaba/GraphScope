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

extern crate clap;
extern crate timely;

#[macro_use]
extern crate log;

use clap::{App, Arg};

use graph_store::common::*;
use graph_store::utils::*;
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::Mutex;

fn main() {
    let matches = App::new(NAME)
        .version(VERSION)
        .about("Load LDBC raw data and partition the graph.")
        .args(&[
            Arg::with_name("hadoop_home")
                .short("h")
                .long_help("The $HADOOP_HOME.")
                .required(true)
                .takes_value(true)
                .index(1),
            Arg::with_name("hdfs_dir")
                .short("f")
                .long_help("The HDFS directory to the graph's binary data.")
                .required(true)
                .takes_value(true)
                .index(2),
            Arg::with_name("local_dir")
                .short("d")
                .long_help("The local directory to store the binary data.")
                .required(true)
                .takes_value(true)
                .index(3),
            Arg::with_name("up_or_down")
                .short("x")
                .long_help("Upload to or download from HDFS the binary files [upload|download]")
                .default_value("upload")
                .takes_value(true)
                .required(false),
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

    if matches.is_present("machines") {
        timely_args.push("-n".to_string());
        let machines = matches.value_of("machines").unwrap().to_string();
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

    let part_prefix = "partition_";
    let graph_binary = "graph_data_bin";
    let graph_schema = "graph_schema";
    let hdfs_bin = format!("{}/bin/hdfs", matches.value_of("hadoop_home").unwrap());
    let hdfs_dir = matches.value_of("hdfs_dir").unwrap().to_string();
    let local_dir = matches.value_of("local_dir").map(PathBuf::from).unwrap();
    let is_upload = matches.value_of("up_or_down").unwrap() == "upload";
    let num_workers = matches.value_of("workers").unwrap().parse::<usize>().unwrap();

    let lock = Mutex::new(0_u32);

    timely::execute_from_args(timely_args.into_iter(), move |root| {
        let worker = root.index();

        let partition_name = format!("{}{}", part_prefix, worker);
        let hdfs_schema_path = format!("{}/{}", hdfs_dir, graph_schema);
        let hdfs_partition_path = format!("{}/{}", hdfs_dir, partition_name);

        let local_schema_path = local_dir.join(graph_schema);
        let local_partition_dir = local_dir.join(graph_binary);
        let local_partition_path = local_partition_dir.join(&partition_name);

        if is_upload {
            if worker == 0 {
                // Upload the schema, only the first work do this
                let status = upload_to_hdfs(&hdfs_bin, &hdfs_schema_path, &local_schema_path);

                info!("Worker {}: Upload {:?}, Status: {:?}", worker, local_schema_path, status);
            }

            let status = upload_to_hdfs(&hdfs_bin, &hdfs_partition_path, &local_partition_path);

            info!("Worker {}: Upload {:?}, Status: {:?}", worker, local_partition_path, status);
        } else {
            {
                let _hold_lock = lock.lock();
                if !local_partition_dir.as_path().exists() {
                    create_dir_all(&local_partition_dir).expect("Create local_dir error!");
                }
            }

            // mean the first worker in every machine
            if worker % num_workers == 0 {
                let status = download_from_hdfs(&hdfs_bin, &hdfs_schema_path, &local_schema_path);

                info!("Worker {}: Download {:?}, Status: {:?}", worker, local_schema_path, status);
            }

            let status = download_from_hdfs(&hdfs_bin, &hdfs_partition_path, &local_partition_path);

            info!("Worker {}: Upload {:?}, Status: {:?}", worker, local_partition_path, status);
        }
    })
    .unwrap();
}
