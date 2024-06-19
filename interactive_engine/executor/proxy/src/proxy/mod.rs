use std::collections::HashMap;
use std::sync::atomic::AtomicU32;

use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

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
    path: web::Path<(u32, )>, query: web::Json<Query>, index: web::Data<AtomicU32>,
    data: web::Data<Mutex<JobClient>>,
) -> impl Responder {
    let graph_id = path.into_inner().0;
    let query_name = query.query_name.clone();
    let mut arguments = HashMap::<String, String>::new();
    HttpResponse::Ok().body(format!("graph_id: {}", graph_id))
}

#[get("/v1/service/status")]
async fn get_status(data: web::Data<Mutex<JobClient>>) -> impl Responder {
    let status = "Running";
    let port = data.lock().expect("JobClient poisoned").get_port();
    HttpResponse::Ok().body(format!("graph_idc"))
}
