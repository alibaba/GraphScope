use std::collections::HashMap;
use std::env;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::net::SocketAddr;
use std::str::FromStr;

use byteorder::{LittleEndian, ReadBytesExt};
use lazy_static::lazy_static;
use pb::job_service_client::JobServiceClient;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Response, Streaming};

use crate::generated::common;
use crate::generated::procedure;
use crate::generated::protocol as pb;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Param {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueryConfig {
    pub name: String,
    pub description: String,
    pub mode: String,
    pub extension: String,
    pub library: String,
    pub params: Option<Vec<Param>>,
    pub returns: Option<Vec<Param>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueriesConfig {
    pub queries: Option<Vec<QueryConfig>>,
}

pub struct JobClient {
    client: JobServiceClient<Channel>,
    socket_addr: SocketAddr,
    input_info: HashMap<String, Vec<String>>,
    workers: u32,
}

impl JobClient {
    pub async fn new(
        endpoint: String, self_endpoint: String, input_info: HashMap<String, Vec<String>>, workers: u32,
    ) -> Result<JobClient, Box<dyn std::error::Error>> {
        let trimmed_endpoint = self_endpoint.trim_start_matches("http://");
        let socket_addr = SocketAddr::from_str(trimmed_endpoint).unwrap();
        let channel = Channel::from_shared(endpoint)
            .unwrap()
            .connect() // 连接到服务端
            .await?;
        let client = JobServiceClient::new(channel);
        Ok(JobClient { client, socket_addr, input_info, workers })
    }

    pub async fn get_port(&self) -> i32 {
        self.socket_addr.port() as i32
    }

    pub async fn get_input_info(&self) -> &HashMap<String, Vec<String>> {
        &self.input_info
    }

    pub async fn submitProcedure(
        &mut self, job_id: u64, query_name: String, arguments: HashMap<String, String>,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        println!("Try to submit query {}", query_name);
        let mut index = 0;
        let mut inputs = vec![];
        for (param_name, value) in arguments {
            let argument = procedure::Argument {
                param_name,
                param_ind: index,
                value: Some(common::Value { item: Some(common::value::Item::Str(value)) }),
            };
            inputs.push(argument);
            index += 1;
        }
        let query = procedure::Query {
            query_name: Some(common::NameOrId {
                item: Some(common::name_or_id::Item::Name(query_name.clone())),
            }),
            arguments: inputs,
        };
        let mut plan = Vec::new();
        query
            .encode(&mut plan)
            .expect("Failed to encode query");

        let job_config = pb::JobConfig {
            job_id,
            job_name: query_name.clone(),
            workers: self.workers,
            time_limit: 0,
            batch_size: 0,
            batch_capacity: 0,
            memory_limit: 0,
            trace_enable: false,
            servers: None,
        };
        let job_request = pb::JobRequest { conf: Some(job_config), source: vec![], plan, resource: vec![] };

        let file_path = "query_result.csv";
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)?;
        writeln!(file, "{}", query_name)?;
        let mut result = vec![];
        if let Ok(response) = self.client.submit(job_request).await {
            let mut stream = response.into_inner();
            while let Some(message) = stream.next().await {
                match message {
                    Ok(job_response) => {
                        let job_id: u64 = job_response.job_id;
                        let resp_bytes: Vec<u8> = job_response.resp;
                        result = resp_bytes.clone();
                        let mut reader = io::Cursor::new(resp_bytes);
                        let mut buf = [0u8; 8];
                        while reader.read_exact(&mut buf).await.is_ok() {
                            let mut cursor = io::Cursor::new(buf);
                            let length = ReadBytesExt::read_u64::<LittleEndian>(&mut cursor).unwrap();
                            let mut buffer = vec![0; length as usize];
                            if reader.read_exact(&mut buffer).await.is_err() {
                                break;
                            }
                            let result = match String::from_utf8(buffer) {
                                Ok(s) => s,
                                Err(_) => {
                                    panic!("Invalid result")
                                }
                            };
                            writeln!(file, "{}", result)?;
                        }
                    }
                    Err(e) => {
                        eprintln!("Stream error: {}", e);
                        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "oh no!")));
                    }
                }
            }
        }
        Ok(Some(result))
    }
}
