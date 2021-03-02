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

use pegasus::api::accum::{AccumFactory, Accumulator};
use pegasus::api::function::*;
use pegasus::codec::{Decode, Encode};
use pegasus::{Configuration, Data};
use pegasus_common::collections::{Collection, CollectionFactory, DrainSet, DrainSetFactory};
use pegasus_common::io::{ReadExt, WriteExt};
use pegasus_server::desc::Resource;
use pegasus_server::factory::{CompileResult, HashKey, JobCompiler};
use pegasus_server::rpc::{start_rpc_server, RpcServer};
use pegasus_server::service::Service;
use pegasus_server::AnyData;
use std::net::SocketAddr;

pub struct SimpleJobFactory;

impl JobCompiler<Message> for SimpleJobFactory {
    fn shuffle(&self, _res: &dyn Resource) -> CompileResult<Box<dyn RouteFunction<Message>>> {
        Ok(box_route!(|item: &Message| -> u64 { item.0 }))
    }

    fn broadcast(&self, _: &dyn Resource) -> CompileResult<Box<dyn MultiRouteFunction<Message>>> {
        unimplemented!()
    }

    fn source(
        &self, worker_index: u32, src: &dyn Resource,
    ) -> CompileResult<Box<dyn Iterator<Item = Message> + Send>> {
        let src = if worker_index == 0 {
            let seed = from_resource(src);
            vec![Message(seed)]
        } else {
            vec![]
        };
        Ok(Box::new(src.into_iter()))
    }

    fn map(&self, res: &dyn Resource) -> CompileResult<Box<dyn MapFunction<Message, Message>>> {
        let add = from_resource(res);
        Ok(Box::new(map!(move |item: Message| Ok(Message(item.0 + add)))))
    }

    fn flat_map(
        &self, res: &dyn Resource,
    ) -> CompileResult<Box<dyn FlatMapFunction<Message, Message, Target = DynIter<Message>>>> {
        let copy = from_resource(res) as usize + 1;
        let func = move |item: Message| {
            Box::new(vec![item; copy].into_iter().map(|item| Ok(item))) as DynIter<Message>
        };
        Ok(Box::new(flat_map!(func)))
    }

    fn filter(&self, _: &dyn Resource) -> CompileResult<Box<dyn FilterFunction<Message>>> {
        unimplemented!()
    }

    fn left_join(&self, _: &dyn Resource) -> CompileResult<Box<dyn LeftJoinFunction<Message>>> {
        unimplemented!()
    }

    fn compare(&self, _: &dyn Resource) -> CompileResult<Box<dyn CompareFunction<Message>>> {
        unimplemented!()
    }

    fn key(
        &self, _: &dyn Resource,
    ) -> CompileResult<Box<dyn KeyFunction<Message, Target = HashKey<Message>>>> {
        unimplemented!()
    }

    fn accumulate(
        &self, _: &dyn Resource,
    ) -> CompileResult<
        Box<dyn AccumFactory<Message, Message, Target = Box<dyn Accumulator<Message, Message>>>>,
    > {
        unimplemented!()
    }

    fn collect(
        &self, _: &dyn Resource,
    ) -> CompileResult<Box<dyn CollectionFactory<Message, Target = Box<dyn Collection<Message>>>>>
    {
        unimplemented!()
    }

    fn set(
        &self, _: &dyn Resource,
    ) -> CompileResult<
        Box<
            dyn DrainSetFactory<
                Message,
                Target = Box<
                    dyn DrainSet<Message, Target = Box<dyn Iterator<Item = Message> + Send>>,
                >,
            >,
        >,
    > {
        unimplemented!()
    }

    fn sink(&self, _: &dyn Resource) -> CompileResult<Box<dyn EncodeFunction<Message>>> {
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

impl AnyData for Message {
    fn with<T: Data + Eq>(_: T) -> Self {
        unimplemented!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).unwrap();
    println!("try to start rpc server;");
    let service = Service::new(SimpleJobFactory);
    let addr: SocketAddr = "0.0.0.0:1234".parse().unwrap();
    start_rpc_server(addr, service);
    Ok(())
}

#[inline]
fn from_resource(bytes: &dyn Resource) -> u64 {
    let empty = vec![];
    let res = bytes.as_any_ref().downcast_ref::<Vec<u8>>().unwrap_or(&empty);
    let len = res.len();
    if len < std::mem::size_of::<u64>() {
        0
    } else {
        let mut buf = [0u8; std::mem::size_of::<u64>()];
        let len = std::mem::size_of::<u64>();
        buf.copy_from_slice(&res[0..len]);
        u64::from_le_bytes(buf)
    }
}
