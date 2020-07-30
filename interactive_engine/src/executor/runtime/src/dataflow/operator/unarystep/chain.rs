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

use maxgraph_common::proto::query_flow::{OperatorBase, OperatorType, InputShuffleType};
use maxgraph_common::proto::message::Value;
use maxgraph_store::config::StoreConfig;

use dataflow::operator::shuffle::*;
use dataflow::builder::*;
use dataflow::common::iterator::{UnaryIteratorNode, UnaryIteratorNodeBox};
use dataflow::manager::context::RuntimeContext;
use dataflow::message::{RawMessage, RawMessageType, IdMessage};
use dataflow::operator::unary::build_unary_operator_node;
use std::sync::Arc;
use crossbeam_channel::{Receiver, Sender};
use crossbeam_queue::ArrayQueue;
use dataflow::operator::collector::MessageLocalCollector;

pub struct UnaryChain {
    current: Option<Box<UnaryIteratorNodeBox>>,
    backend: Arc<ArrayQueue<Box<UnaryIteratorNodeBox>>>,
}

impl UnaryChain {
    pub fn new(backend: Arc<ArrayQueue<Box<UnaryIteratorNodeBox>>>) -> Self {
        UnaryChain {
            current: None,
            backend,
        }
    }
}

impl Iterator for UnaryChain {
    type Item = RawMessage;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut iter) = self.current {
            iter.next()
        } else {
            match self.backend.pop() {
                Ok(mut iter) => {
                    let next = iter.next();
                    self.current.replace(iter);
                    next
                }
                Err(_) => None
            }
        }
    }
}

impl Drop for UnaryChain {
    fn drop(&mut self) {
        if let Some(node) = self.current.take() {
            self.backend.push(node).unwrap();
        }
    }
}

pub struct UnaryChainOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    stream_index: i32,
    shuffle_type: StreamShuffleCompositeType<F>,
    tx: Sender<Vec<RawMessage>>,
    chain: Option<Arc<ArrayQueue<Box<UnaryIteratorNodeBox>>>>,
    //chain_operator_list: Vec<Box<UnaryOperator>>,
}

pub struct ChainFirstOperator {
    rx: Receiver<Vec<RawMessage>>,
    inner: Box<dyn UnaryOperator>,
    output: Option<Box<dyn Iterator<Item=RawMessage>>>,
}

impl ChainFirstOperator {
    pub fn new(rx: Receiver<Vec<RawMessage>>, op: Box<dyn UnaryOperator>) -> Self {
        ChainFirstOperator {
            rx,
            inner: op,
            output: None,
        }
    }
}

impl Iterator for ChainFirstOperator {
    type Item = RawMessage;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref mut iter) = self.output {
                if let Some(m) = iter.next() {
                    return Some(m);
                } else {
                    match self.rx.try_recv() {
                        Ok(msg) => {
                            let mut data = Vec::with_capacity(10);
                            {
                                let mut local_collector: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data));
                                self.inner.execute(msg, &mut local_collector);
                            }
                            self.output.replace(Box::new(data.into_iter()));
                        }
                        Err(_) => return None
                    }
                }
            } else {
                match self.rx.try_recv() {
                    Ok(msg) => {
                        let mut data = Vec::with_capacity(10);
                        {
                            let mut local_collector: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data));
                            self.inner.execute(msg, &mut local_collector);
                        }
                        self.output.replace(Box::new(data.into_iter()));
                    }
                    Err(_) => return None
                }
            }
        }
    }
}


impl<F> UnaryChainOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(input_id: i32,
               stream_index: i32,
               shuffle_type: StreamShuffleCompositeType<F>,
               base: &OperatorBase,
               mut chain_operator_list: Vec<Box<dyn UnaryOperator>>) -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        let chain = if !chain_operator_list.is_empty() {
            let first = chain_operator_list.remove(0);
            let mut iterator_node = Some(Box::new(ChainFirstOperator::new(rx, first)) as Box<dyn Iterator<Item=RawMessage>>);
            let last = chain_operator_list.remove(chain_operator_list.len() - 1);
            for next_op in chain_operator_list.into_iter() {
                let curr_iter_node = Box::new(UnaryIteratorNodeBox::new(iterator_node,
                                                                        None,
                                                                        next_op));
                iterator_node = Some(curr_iter_node);
            }

            let last = UnaryIteratorNodeBox::new(iterator_node, None, last);

            let sequence = ArrayQueue::new(1);
            sequence.push(Box::new(last)).unwrap();
            Some(Arc::new(sequence))
        } else {
            None
        };

        UnaryChainOperator {
            id: base.get_id(),
            input_id,
            stream_index,
            shuffle_type,
            tx,
            chain,
        }
    }
}

impl<F> Operator for UnaryChainOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<F> UnaryOperator for UnaryChainOperator<F>
    where F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_input_id(&self) -> i32 {
        self.input_id
    }

    fn get_input_shuffle(&self) -> Box<dyn InputStreamShuffle> {
        Box::new(self.shuffle_type.clone())
    }

    fn get_stream_index(&self) -> i32 {
        self.stream_index
    }


    fn execute<'a>(&mut self, data: Vec<RawMessage>, collector: &mut Box<'a + MessageCollector>) {
        if let Some(ref chain) = self.chain {
            self.tx.send(data).unwrap();
            collector.collect_iterator(Box::new(UnaryChain::new(chain.clone())));
        } else {
            collector.collect_iterator(Box::new(data.into_iter()));
        }
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        Box::new(None.into_iter())
    }
}

