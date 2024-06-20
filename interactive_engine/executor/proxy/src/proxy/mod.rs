use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};

use crate::request::JobClient;

#[derive(Deserialize)]
struct Type {
    primitive_type: Option<String>,
}

#[derive(Deserialize)]
struct Argument {
    #[serde(rename = "type")]
    type_field: Type,
    value: String,
}

#[derive(Deserialize)]
struct Query {
    query_name: String,
    arguments: Vec<Argument>,
}

#[derive(Serialize)]
struct ServiceStatus {
    status: String,
    graph: Option<String>,
    bolt_port: i32,
    hqps_port: i32,
    gremlin_port: i32,
    start_time: i32,
}

#[post("/v1/graph/{graph_id}/query")]
async fn submit_query(
    path: web::Path<(String,)>, bytes: web::Bytes, index: web::Data<AtomicU64>,
    client: web::Data<Mutex<JobClient>>,
) -> impl Responder {
    let byte_slice: &[u8] = &bytes;
    let arguments: Vec<String> = serde_json::from_slice(byte_slice).unwrap();
    let job_id = index.fetch_add(1, Ordering::SeqCst);
    let query_name = arguments[0].clone();
    let mut client_lock = client.lock().unwrap();
    let queries_config = client_lock.get_input_info().await;
    let mut args = HashMap::<String, String>::new();
    if let Some(params) = queries_config.get(&query_name) {
        for (index, param_name) in params.iter().enumerate() {
            args.insert(param_name.clone(), arguments[index + 1].clone());
        }
    }
    drop(queries_config);
    let result = client_lock
        .submitProcedure(job_id, query_name, args)
        .await
        .unwrap()
        .unwrap();
    HttpResponse::Ok().body(web::Bytes::from(result))
}

#[get("/v1/service/status")]
async fn get_status(data: web::Data<Mutex<JobClient>>) -> impl Responder {
    let status = "Running".to_string();
    let port = data.lock().unwrap().get_port().await;
    let service_status = ServiceStatus {
        status,
        graph: None,
        bolt_port: port,
        hqps_port: port,
        gremlin_port: port,
        start_time: 0,
    };
    HttpResponse::Ok().json(service_status)
}
