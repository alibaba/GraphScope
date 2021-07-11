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

#[macro_use]
extern crate pegasus;

use pegasus::api::function::*;
use pegasus::codec::{Decode, Encode};
use pegasus::Configuration;
use pegasus_common::collections::{Collection, CollectionFactory, Set};
use pegasus_common::io::{ReadExt, WriteExt};
use pegasus_server::config::combine_config;
use pegasus_server::factory::{CompileResult, FoldFunction, GroupFunction, JobCompiler};
use pegasus_server::rpc::start_rpc_server;
use pegasus_server::service::Service;
use pegasus_server::{AnyData, CommonConfig, HostsConfig};
use std::net::SocketAddr;
use structopt::StructOpt;

pub struct SimpleJobFactory;

impl JobCompiler<Message> for SimpleJobFactory {
    fn shuffle(&self, _res: &[u8]) -> CompileResult<Box<dyn RouteFunction<Message>>> {
        Ok(box_route!(|item: &Message| -> u64 { item.0 }))
    }

    fn broadcast(&self, _: &[u8]) -> CompileResult<Box<dyn MultiRouteFunction<Message>>> {
        unimplemented!()
    }

    fn source(&self, src: &[u8]) -> CompileResult<Box<dyn Iterator<Item = Message> + Send>> {
        if let Some(worker_id) = pegasus::get_current_worker() {
            let src = if worker_id.index == 0 {
                let seed = from_resource(src);
                vec![Message(seed), Message(seed + 1), Message(seed + 2), Message(seed + 3)]
            } else {
                vec![]
            };
            Ok(Box::new(src.into_iter()))
        } else {
            Err("worker id not found")?
        }
    }

    fn map(&self, res: &[u8]) -> CompileResult<Box<dyn MapFunction<Message, Message>>> {
        let add = from_resource(res);
        Ok(Box::new(map!(move |item: Message| { Ok(Message(item.0 + add)) })))
    }

    fn flat_map(
        &self, res: &[u8],
    ) -> CompileResult<Box<dyn FlatMapFunction<Message, Message, Target = DynIter<Message>>>> {
        let copy = from_resource(res) as usize + 1;
        let func = move |item: Message| {
            Ok(Box::new(vec![item; copy].into_iter().map(|item| Ok(item))) as DynIter<Message>)
        };
        Ok(Box::new(flat_map!(func)))
    }

    fn filter(&self, _: &[u8]) -> CompileResult<Box<dyn FilterFunction<Message>>> {
        unimplemented!()
    }

    fn left_join(&self, _: &[u8]) -> CompileResult<Box<dyn LeftJoinFunction<Message>>> {
        unimplemented!()
    }

    fn compare(&self, _: &[u8]) -> CompileResult<Box<dyn CompareFunction<Message>>> {
        unimplemented!()
    }

    fn group(
        &self, _: &[u8], _: &[u8], _: &[u8],
    ) -> CompileResult<Box<dyn GroupFunction<Message>>> {
        unimplemented!()
    }

    fn fold(&self, _: &[u8], _: &[u8], _: &[u8]) -> CompileResult<Box<dyn FoldFunction<Message>>> {
        unimplemented!()
    }

    fn collection_factory(
        &self, _: &[u8],
    ) -> CompileResult<Box<dyn CollectionFactory<Message, Target = Box<dyn Collection<Message>>>>>
    {
        unimplemented!()
    }

    fn set_factory(
        &self, _: &[u8],
    ) -> CompileResult<Box<dyn CollectionFactory<Message, Target = Box<dyn Set<Message>>>>> {
        unimplemented!()
    }

    fn sink(&self, _: &[u8]) -> CompileResult<Box<dyn EncodeFunction<Message>>> {
        let func = |batch: Vec<Message>| {
            let len = batch.len();
            let mut buf = Vec::with_capacity(len * std::mem::size_of::<u64>());
            for item in batch {
                buf.extend_from_slice(&item.0.to_le_bytes());
            }
            buf
        };
        Ok(Box::new(encode!(func)))
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Message(pub u64);

impl Encode for Message {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64(self.0)
    }
}

impl Decode for Message {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let value = reader.read_u64()?;
        Ok(Message(value))
    }
}

impl Partition for Message {
    fn get_partition(&self) -> FnResult<u64> {
        Ok(self.0)
    }
}

impl AnyData for Message {}

#[derive(Debug, Clone, StructOpt)]
#[structopt(about = "An RPC ping-pong server for accepting jobs.")]
pub struct ServerConfig {
    #[structopt(
        long = "port",
        short = "p",
        default_value = "1234",
        help = "the port to accept RPC connections"
    )]
    pub rpc_port: u16,
    #[structopt(
        long = "index",
        short = "i",
        default_value = "0",
        help = "the current server id among all servers"
    )]
    pub server_id: u64,
    #[structopt(
        long = "hosts",
        short = "h",
        default_value = "",
        help = "the path of hosts file for pegasus communication"
    )]
    pub hosts: String,
    #[structopt(
        long = "config",
        short = "c",
        default_value = "",
        help = "the path of config file for pegasus"
    )]
    pub config: String,
    #[structopt(long = "report", help = "the option to report the job latency and memory usage")]
    pub report: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_config: ServerConfig = ServerConfig::from_args();
    println!("Server Config: {:?}", server_config);
    let host_config = if server_config.hosts.is_empty() {
        None
    } else {
        Some(HostsConfig::read_from(server_config.hosts)?)
    };
    let common_config = if server_config.config.is_empty() {
        None
    } else {
        Some(CommonConfig::read_from(server_config.config)?)
    };
    let config = combine_config(server_config.server_id, host_config, common_config);
    pegasus_common::logs::init_log();
    if let Some(engine_config) = config {
        println!("Engine config: {:?}", engine_config);
        pegasus::startup(engine_config).unwrap();
    } else {
        pegasus::startup(Configuration::singleton()).unwrap();
    }
    println!("try to start rpc server;");
    let service = Service::new(SimpleJobFactory);
    let addr = format!("{}:{}", "0.0.0.0", server_config.rpc_port);
    println!("pegasus service start at {}", addr);
    let socket_addr: SocketAddr = addr.parse().unwrap();
    start_rpc_server(socket_addr, service, server_config.report, true).await?;
    Ok(())
}

#[inline]
fn from_resource(bytes: &[u8]) -> u64 {
    let len = bytes.len();
    if len < std::mem::size_of::<u64>() {
        0
    } else {
        let mut buf = [0u8; std::mem::size_of::<u64>()];
        buf.copy_from_slice(bytes);
        u64::from_le_bytes(buf)
    }
}
