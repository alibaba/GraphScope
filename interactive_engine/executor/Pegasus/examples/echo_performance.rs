//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::io;
use log::*;
use structopt::StructOpt;
use pegasus::{Pegasus, try_init_logger, ConfigArgs};
use pegasus::server::*;
use pegasus::common::{Bytes, BytesSlab};
use pegasus::serialize::{CPSerialize, CPDeserialize, write_binary};
use std::io::BufRead;
use std::net::SocketAddr;
use std::time::Instant;
use pegasus::operator::sink::BinarySinker;

/// To run echo server:
/// echo_performance -s
/// To run echo client :
/// echo_performance -a server_address_file
#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    /// config run echo server or echo client;
    #[structopt(short = "s", long = "server")]
    pub server: bool,
    #[structopt(short = "g", long = "generators", default_value = "2")]
    pub generators: usize,
    #[structopt(short = "a", long = "address", default_value = "")]
    pub address: String,
    #[structopt(short = "n", long = "number", default_value = "10000")]
    pub number: u64,
    #[structopt(short = "c", long = "clients", default_value = "1")]
    pub clients: u64,
}

pub struct Echo {
    pub seq: u64
}

impl Echo {
    pub fn new(seq: u64) -> Self {
        Echo { seq }
    }
}

impl CPSerialize for Echo {
    fn serialize_len(&self) -> usize {
        8
    }

    fn write_to(&self, write: &mut BytesSlab) -> Result<(), io::Error> {
        write.write_u64(self.seq)
    }
}

impl CPDeserialize for Echo {
    fn read_from(mut bytes: Bytes) -> Result<Self, io::Error> {
        let seq = bytes.read_u64()?;
        Ok(Echo::new(seq))
    }
}

pub struct EchoCallback {
    expect: u64,
    peers: u32,
    count: u32,
}

impl EchoCallback {
    pub fn new(expect: u64, peers: u32) -> Self {
        EchoCallback {
            expect,
            peers,
            count: 0
        }
    }
}

impl Callback for EchoCallback {
    fn accept(&mut self, mut res: BinaryMessage<TaskResponseHeader>) -> bool {
        let body = res.take_body().unwrap();
        let echo = Echo::read_from(body).unwrap();
        assert_eq!(echo.seq, self.expect);
        self.count += 1;
        self.count == self.peers
    }
}

pub struct EchoService;

impl TaskGenerator for EchoService {
    fn create_task(&self, task: TaskRequest<Option<Bytes>>, _runtime: &Pegasus, sink: &mut Sink) -> Result<(), String> {
        let task_id = task.header.task_id;
        let body = task.take_body().unwrap();
        let echo = Echo::read_from(body).unwrap();
        let res = TaskResponse::new(task_id, ResponseType::OK, echo);
        let res = write_binary(&res).unwrap();
        sink.sink(res).unwrap();
        Ok(())
    }
}

fn main() {
    try_init_logger().ok();
    let config: Config = Config::from_args();
    info!("Config: {:?}", config);
    if config.server {
        let mut service = Service::new(ConfigArgs::singleton(0).build(), EchoService);
        service.bind().unwrap();
        service.start(config.generators)
    } else {
        let reader = ::std::io::BufReader::new(::std::fs::File::open(config.address).unwrap());
        let mut addrs = Vec::new();
        for line in reader.lines() {
            let addr = line.unwrap().parse::<SocketAddr>().unwrap();
            addrs.push(addr);
        }
        info!("Server address: {:?}", addrs);
        let peers = addrs.len() as u32;
        let mut guards = Vec::new();
        let start = Instant::now();
        for i in 0..config.clients {
            let addrs = addrs.clone();
            let number = config.number;
            let guard = ::std::thread::spawn(move || {
               let mut client_s = ClientService::new();
               client_s.start_service(&addrs).unwrap();
               let client = client_s.new_async_client();
               for j in 0..number {
                   client.new_task(i * number + j, 1, peers, Echo::new(j),
                                   EchoCallback::new(j, peers)).unwrap();
               }

               client.waiting_tasks_done();
            });
            guards.push(guard);
        }
        for g in guards {
            g.join().unwrap()
        }

        let elapsed = start.elapsed();
        let elapsed = elapsed.as_millis() as f64 / 1000.0;
        let number = config.number * config.clients;
        info!("Echo qps: {}/s", number as f64 / elapsed);

    }
}
