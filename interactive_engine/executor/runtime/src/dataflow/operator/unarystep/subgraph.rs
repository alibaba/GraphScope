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

use dataflow::operator::shuffle::StreamShuffleType;
use dataflow::message::subgraph::SubGraph;
use dataflow::message::{RawMessage, RawMessageType};
use dataflow::builder::{Operator, UnaryOperator, InputStreamShuffle, MessageCollector};

use maxgraph_store::api::*;
use maxgraph_common::proto::query_flow::OperatorBase;
use maxgraph_common::util::time::duration_to_millis;

use std::time::Instant;
use std::sync::Arc;
use dataflow::store::cache::CacheStore;

pub struct SubGraphOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    subgraph: Arc<SubGraph>,
    now: Instant,
    vertex_flag: bool,
    debug_log_flag: bool,
}

impl<F> SubGraphOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase,
               subgraph: Arc<SubGraph>,
               vertex_flag: bool,
               debug_log_flag: bool, ) -> Self {
        *subgraph.enable.borrow_mut() = true;
        return SubGraphOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            subgraph,
            now: Instant::now(),
            vertex_flag,
            debug_log_flag,
        };
    }
}

impl<F> Operator for SubGraphOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for SubGraphOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_input_id(&self) -> i32 {
        self.input_id
    }

    fn get_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.shuffle_type.clone())
    }

    fn get_stream_index(&self) -> i32 {
        self.stream_index
    }

    fn execute<'a>(&mut self, data: Vec<RawMessage>, _collector: &mut Box<'a + MessageCollector>) {
        for mut message in data.into_iter() {
            match message.get_message_type() {
                RawMessageType::EDGE => {
                    if let Some(edge) = message.take_edge() {
                        let src_id = (edge.get_src_id(), edge.get_src_label() as u32);
                        let dst_id = (edge.get_dst_id(), edge.get_dst_label() as u32);
                        _info!("src {:?}, dst: {:?}", src_id, dst_id);
                        let mut e = self.subgraph.edges.borrow_mut();
                        e.entry(src_id).or_insert(Vec::new()).push(dst_id);

                        let mut eprop = self.subgraph.edge_prop_list.borrow_mut();
                        if let Some(prop_list) = message.take_prop_entity_list() {
                            _info!("add edge {:?} with id {:?}.{:?} prop list {:?}", &edge, message.get_id(), message.get_label_id(), &prop_list);
                            eprop.push(((message.get_id(), message.get_label_id() as u32), edge, prop_list));
                        } else {
                            _info!("add edge {:?} with id {:?}.{:?}", &edge, message.get_id(), message.get_label_id());
                            eprop.push(((message.get_id(), message.get_label_id() as u32), edge, vec![]));
                        }
                    }
                }
                _ => {
                    error!("Only support to build sub graph from edge yet.");
                }
            }
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        let duration = self.now.elapsed();
        info!("finish graph building {:?}", duration_to_millis(&duration));
        let e = self.subgraph.edges.borrow();
        let mut result_list = Vec::with_capacity(e.len());
        for k in e.keys() {
            result_list.push(RawMessage::from_vertex_id(k.1 as i32, k.0));
        }
        if self.vertex_flag {
            for vlist in e.values() {
                for (vid, vlabel) in vlist.iter() {
                    result_list.push(RawMessage::from_vertex_id(*vlabel as i32, *vid as i64));
                }
            }
        }
        let duration = self.now.elapsed();
        info!("send out the intital data, cost(ms): {:?}", duration_to_millis(&duration));
        if self.debug_log_flag {
            info!("send subgraph vertex list {:?}", &result_list);
        }
        return Box::new(result_list.into_iter());
    }
}

pub struct CacheOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleType<F>,
    cache_store: Arc<CacheStore>,
    property_ids: Vec<i32>,
}

impl<F> CacheOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleType<F>,
               base: &OperatorBase,
               cache_store: Arc<CacheStore>) -> Self {
        let property_ids = base.get_argument().get_int_value_list().to_vec();
        return CacheOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            cache_store,
            property_ids,
        };
    }
}

impl<F> Operator for CacheOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for CacheOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_input_id(&self) -> i32 {
        self.input_id
    }

    fn get_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.shuffle_type.clone())
    }

    fn get_stream_index(&self) -> i32 {
        self.stream_index
    }

    fn execute<'a>(&mut self, data: Vec<RawMessage>, collector: &mut Box<'a + MessageCollector>) {
        for message in data.iter() {
            let message_type = message.get_message_type();
            match message_type {
                RawMessageType::VERTEX => {
                    unimplemented!();
                }
                _ => {
                    warn!("cache {:?} not supported yet", &message_type);
                }
            }
        }
        collector.collect_iterator(Box::new(data.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        return Box::new(None.into_iter());
    }
}

