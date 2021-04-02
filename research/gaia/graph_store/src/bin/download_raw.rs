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
use graph_store::ldbc::{get_partition_names, is_hidden_file};
use graph_store::utils::*;
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Mutex;

fn main() {
    env_logger::init();
    let matches = App::new(NAME)
        .version(VERSION)
        .about("Download raw data from hdfs.")
        .args(&[
            Arg::with_name("hadoop_home")
                .short("h")
                .long_help("The $HADOOP_HOME.")
                .required(true)
                .takes_value(true)
                .index(1),
            Arg::with_name("hdfs_dir")
                .short("f")
                .long_help("The HDFS directory to LDBC data.")
                .required(true)
                .takes_value(true)
                .index(2),
            Arg::with_name("local_dir")
                .short("d")
                .long_help("The local directory to store the LDBC data.")
                .required(true)
                .takes_value(true)
                .index(3),
            Arg::with_name("raw_partitions")
                .long_help("The default number of partitions of raw data.")
                .required(true)
                .takes_value(true)
                .index(4),
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

    let ldbc_part_prefix = "part-";
    let hdfs_bin = format!("{}/bin/hdfs", matches.value_of("hadoop_home").unwrap());
    let hdfs_dir = matches.value_of("hdfs_dir").unwrap().to_string();
    let local_dir = matches.value_of("local_dir").map(PathBuf::from).unwrap();

    let raw_partitions: usize = matches.value_of("raw_partitions").unwrap().parse().unwrap();
    let lock = Mutex::new(0_u32);

    timely::execute_from_args(timely_args.into_iter(), move |root| {
        let worker = root.index();
        let peers = root.peers();

        {
            let _hold_lock = lock.lock();
            if !local_dir.as_path().exists() {
                create_dir_all(&local_dir).expect("Create local_dir error!");
            }
        }

        let mut cmd = Command::new("sh");
        let list_str = format!("{} dfs -ls {}", hdfs_bin, hdfs_dir);
        cmd.arg("-c").arg(&list_str);

        if let Ok(output) = cmd.output() {
            let stdout = String::from_utf8(output.stdout).unwrap();
            let items = stdout.split("\n");

            for item in items {
                if let Some(index) = item.rfind("hdfs") {
                    let hdfs_path = item.chars().skip(index).collect::<String>();
                    if let Some(index2) = hdfs_path.rfind("/") {
                        let fname = hdfs_path.chars().skip(index2 + 1).collect::<String>();
                        let local_path = local_dir.join(&fname);
                        {
                            let _hold_lock = lock.lock();
                            if !local_path.as_path().exists() {
                                create_dir_all(&local_path).expect("Create local_path error!");
                            }
                        }

                        if !is_hidden_file(&fname) {
                            let partitions = get_partition_names(
                                ldbc_part_prefix,
                                worker,
                                peers,
                                raw_partitions,
                            );
                            for partition in partitions {
                                let local_file = local_path.join(&partition);

                                if !local_file.exists() {
                                    let status = download_from_hdfs(
                                        &hdfs_bin,
                                        &format!("{}/{}", hdfs_path, partition),
                                        &local_file,
                                    );

                                    info!(
                                        "Worker {}: Download {:?}, Status: {:?}",
                                        worker, local_file, status
                                    );
                                } else {
                                    info!(
                                        "Worker {}: {:?} already exists. Skipped!",
                                        worker, local_file,
                                    )
                                }
                            }
                        }
                    }
                }
            }
        }
    })
    .unwrap();
}
