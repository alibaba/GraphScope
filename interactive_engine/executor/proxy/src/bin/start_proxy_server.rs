use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use rpc_proxy::proxy::*;
use rpc_proxy::request::JobClient;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "e", long = "endpoint")]
    endpoint: String,
    #[structopt(short = "w", long = "workers")]
    workers: u32,
    #[structopt(short = "r", long = "rpc_server")]
    rpc_server: String,
    #[structopt(short = "c", long = "query_config")]
    query_config: String,
}

#[actix_rt::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let config: Config = Config::from_args();
    let endpoint = config.endpoint;
    let rpc_endpoint = config.rpc_server;
    let query_config = config.query_config;
    let file = File::open(query_config).unwrap();
    let queries_config: rpc_proxy::request::QueriesConfig =
        serde_yaml::from_reader(file).expect("Could not read values");
    let mut input_info = HashMap::<String, Vec<String>>::new();
    if let Some(queries) = queries_config.queries {
        for query in queries {
            let query_name = query.name;
            if let Some(params) = query.params {
                let mut inputs = vec![];
                for param in params {
                    inputs.push(param.name);
                }
                input_info.insert(query_name, inputs);
            }
        }
    }
    let mut rpc_client = JobClient::new(rpc_endpoint, endpoint.clone(), input_info, config.workers).await?;
    let shared_client = web::Data::new(Mutex::new(rpc_client));
    let index = web::Data::new(AtomicU64::new(0));

    println!("Start http proxy");
    HttpServer::new(move || {
        App::new()
            .app_data(index.clone())
            .app_data(shared_client.clone())
            .service(get_status)
            .service(submit_query)
    })
    .bind(&endpoint)?
    .run()
    .await;
    Ok(())
}
