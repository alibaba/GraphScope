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
extern crate dlopen;
#[macro_use]
extern crate dlopen_derive;

use std::any::TypeId;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::ops::Add;
use std::path::PathBuf;
use std::time::Instant;

use codegen_benchmark::queries;
use dlopen::wrapper::{Container, WrapperApi};
use itertools::Itertools;
use mcsr::graph_db::GlobalCsrTrait;
use mcsr::graph_db_impl::{CsrDB, SingleSubGraph, SubGraph};
use pegasus::api::*;
use pegasus::errors::BuildJobError;
use pegasus::result::ResultSink;
use pegasus::result::ResultStream;
use pegasus::{Configuration, JobConf, ServerConf};
use pegasus_network::config::ServerAddr;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

#[derive(WrapperApi)]
struct QueryApi {
    Query: fn(
        conf: JobConf,
        graph: &'static CsrDB<usize, usize>,
        input_params: Vec<String>,
    )
        -> Box<dyn Fn(&mut Source<i32>, ResultSink<String>) -> Result<(), BuildJobError>>,
}

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "m", long = "mode", default_value = "codegen")]
    mode: String,
    #[structopt(short = "w", long = "workers", default_value = "2")]
    workers: u32,
    #[structopt(short = "q", long = "query")]
    query_path: String,
    #[structopt(short = "p", long = "print")]
    print_result: bool,
    #[structopt(short = "s", long = "servers")]
    servers: Option<PathBuf>,
    #[structopt(short = "l", long = "dylib")]
    lib_path: String,
}

fn main() {
    pegasus_common::logs::init_log();

    let config: Config = Config::from_args();

    queries::graph::CSR.get_current_partition();

    let mut server_conf = if let Some(ref servers) = config.servers {
        let servers = std::fs::read_to_string(servers).unwrap();
        Configuration::parse(&servers).unwrap()
    } else {
        Configuration::singleton()
    };

    let mut servers = vec![];
    if let Some(network) = &server_conf.network {
        for i in 0..network.servers_size {
            servers.push(i as u64);
        }
    }
    pegasus::startup(server_conf).ok();
    pegasus::wait_servers_ready(&ServerConf::All);

    let mut query_map = HashMap::new();
    let libs_path = config.lib_path;
    let file = File::open(libs_path).unwrap();
    let lines = io::BufReader::new(file).lines();
    for line in lines {
        let line = line.unwrap();
        let split = line.trim().split("|").collect::<Vec<&str>>();
        let query_name = split[0].clone().to_string();
        let lib_path = split[1].clone().to_string();
        let libc: Container<QueryApi> = unsafe { Container::load(lib_path.clone()) }
            .expect("Could not open library or load symbols");
        query_map.insert(query_name, libc);
    }

    let query_start = Instant::now();
    if config.mode == "handwriting" {
        let query_path = config.query_path;
        let mut queries = vec![];
        let file = File::open(query_path).unwrap();
        let lines = io::BufReader::new(file).lines();
        for line in lines {
            queries.push(line.unwrap());
        }
        let mut index = 0i32;
        for query in queries {
            let split = query.trim().split("|").collect::<Vec<&str>>();
            let query_name = split[0].clone();
            let mut conf = JobConf::new(query_name.clone().to_owned() + "-" + &index.to_string());
            conf.set_workers(config.workers);
            conf.reset_servers(ServerConf::Partial(servers.clone()));
            match split[0] {
                _ => println!("Unknown query"),
            }
            index += 1;
        }
    } else if config.mode == "codegen" {
        let query_path = config.query_path;
        let mut queries = vec![];
        let file = File::open(query_path).unwrap();
        let lines = io::BufReader::new(file).lines();
        for line in lines {
            queries.push(line.unwrap());
        }
        let mut index = 0i32;
        for query in queries {
            let split = query.trim().split("|").collect::<Vec<&str>>();
            let query_name = split[0].to_string();
            let mut input_params = vec![];
            for i in 1..split.len() {
                input_params.push(split[i].to_string());
            }
            println!("Start run query {}", query_name);
            let mut conf = JobConf::new(query_name.clone().to_owned() + "-" + &index.to_string());
            conf.set_workers(config.workers);
            conf.reset_servers(ServerConf::Partial(servers.clone()));
            if let Some(libc) = query_map.get(&query_name) {
                let result = pegasus::run(conf.clone(), || {
                    libc.Query(conf.clone(), &queries::graph::CSR, input_params.clone())
                })
                .expect("submit Query0 failure");
                for x in result {
                    println!("{:?}", x.unwrap());
                }
            }
            index += 1;
        }
    }
    pegasus::shutdown_all();
    println!("Finished query, elapsed time: {:?}", query_start.elapsed())
}
