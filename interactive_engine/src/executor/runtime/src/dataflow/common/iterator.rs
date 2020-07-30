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

use dataflow::builder::{UnaryOperator, Operator, InputStreamShuffle, MessageCollector};
use dataflow::message::RawMessage;
use itertools::Itertools;
use dataflow::operator::collector::MessageLocalCollector;
use dataflow::manager::filter::FilterManager;
use std::sync::Arc;
use dataflow::manager::dedup::DedupManager;
use dataflow::manager::requirement::RequirementManager;
use maxgraph_store::api::{Vertex, VertexId, PartitionVertexIds, Edge};
use std::collections::HashMap;
use store::{LocalStoreVertex, LocalStoreEdge};
use maxgraph_common::proto::message::EdgeDirection;
use alloc::vec::IntoIter;


pub struct IteratorList<T, I> where T: Iterator<Item=I> {
    iters: Vec<T>,
    curr_iter: Option<T>,
}

unsafe impl<T, I> Send for IteratorList<T, I> where T: Iterator<Item=I> {}

impl<T, I> IteratorList<T, I> where T: Iterator<Item=I> {
    pub fn new(iters: Vec<T>) -> Self {
        IteratorList {
            iters,
            curr_iter: None,
        }
    }
}

impl<T, I> Iterator for IteratorList<T, I> where T: Iterator<Item=I> {
    type Item = I;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        loop {
            if let Some(ref mut iter) = self.curr_iter {
                if let Some(x) = iter.next() {
                    return Some(x);
                } else {
                    if let Some(iter_val) = self.iters.pop() {
                        *iter = iter_val;
                    } else {
                        return None;
                    }
                }
            } else {
                if let Some(iter_val) = self.iters.pop() {
                    self.curr_iter = Some(iter_val);
                } else {
                    return None;
                }
            }
        }
    }
}

pub struct UnaryIteratorNode {
    parent: Option<Box<dyn Iterator<Item=RawMessage> + Send>>,
    input_iter: Option<Box<dyn Iterator<Item=RawMessage>>>,
    operator: Box<dyn UnaryOperator>,
}

unsafe impl Send for UnaryIteratorNode {}

impl UnaryIteratorNode {
    pub fn new(parent: Option<Box<Iterator<Item=RawMessage> + Send>>,
               input_iter: Option<Box<Iterator<Item=RawMessage>>>,
               operator: Box<UnaryOperator>) -> Self {
        UnaryIteratorNode {
            parent,
            input_iter,
            operator,
        }
    }
}

impl Iterator for UnaryIteratorNode {
    type Item = RawMessage;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref mut iter) = self.input_iter {
                if let Some(m) = iter.next() {
                    return Some(m);
                } else {
                    if let Some(ref mut unary) = self.parent {
                        let input = unary.collect_vec();
                        if input.is_empty() {
                            return None;
                        } else {
                            let mut data = Vec::with_capacity(10);
                            {
                                let mut local_collector: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data));
                                self.operator.execute(unary.collect_vec(), &mut local_collector);
                            }
                            self.input_iter = Some(Box::new(data.into_iter()));
                        }
                    } else {
                        return None;
                    }
                }
            } else {
                if let Some(ref mut unary) = self.parent {
                    let input = unary.collect_vec();
                    if input.is_empty() {
                        return None;
                    } else {
                        let mut data = Vec::with_capacity(10);
                        {
                            let mut local_collector: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data));
                            self.operator.execute(input, &mut local_collector);
                        }
                        self.input_iter = Some(Box::new(data.into_iter()));
                    }
                } else {
                    return None;
                }
            }
        }
    }
}

pub struct UnaryIteratorNodeBox {
    parent: Option<Box<Iterator<Item=RawMessage>>>,
    input_iter: Option<Box<Iterator<Item=RawMessage>>>,
    operator: Box<dyn UnaryOperator>,
}

unsafe impl Send for UnaryIteratorNodeBox {}

impl UnaryIteratorNodeBox {
    pub fn new(parent: Option<Box<Iterator<Item=RawMessage>>>,
               input_iter: Option<Box<Iterator<Item=RawMessage>>>,
               operator: Box<dyn UnaryOperator>) -> Self {
        UnaryIteratorNodeBox {
            parent,
            input_iter,
            operator,
        }
    }
}

impl Iterator for UnaryIteratorNodeBox {
    type Item = RawMessage;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref mut iter) = self.input_iter {
                if let Some(m) = iter.next() {
                    return Some(m);
                } else {
                    if let Some(ref mut unary) = self.parent {
                        let input = unary.collect_vec();
                        if input.is_empty() {
                            return None;
                        } else {
                            let mut data = Vec::with_capacity(10);
                            {
                                let mut local_collector: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data));
                                self.operator.execute(input, &mut local_collector);
                            }
                            self.input_iter = Some(Box::new(data.into_iter()));
                        }
                    } else {
                        return None;
                    }
                }
            } else {
                if let Some(ref mut unary) = self.parent {
                    let input = unary.collect_vec();
                    if input.is_empty() {
                        return None;
                    } else {
                        let mut data = Vec::with_capacity(10);
                        {
                            let mut local_collector: Box<MessageCollector> = Box::new(MessageLocalCollector::new(&mut data));
                            self.operator.execute(input, &mut local_collector);
                        }
                        self.input_iter = Some(Box::new(data.into_iter()));
                    }
                } else {
                    return None;
                }
            }
        }
    }
}

// just for test
pub struct EmptyUnaryOperator {}

impl Operator for EmptyUnaryOperator {
    fn get_id(&self) -> i32 {
        0
    }
}

impl UnaryOperator for EmptyUnaryOperator {
    fn get_input_id(&self) -> i32 {
        0
    }

    fn get_input_shuffle(&self) -> Box<InputStreamShuffle> {
        unimplemented!()
    }

    fn get_stream_index(&self) -> i32 {
        0
    }

    fn execute<'a>(&mut self, data: Vec<RawMessage>, collector: &mut Box<'a + MessageCollector>) {
        println!("receive input and generate 3 result messages");
        let mut list = vec![];
        for message in data.into_iter() {
            list.push(message.clone());
            list.push(message.clone());
            list.push(message);
        }
        collector.collect_iterator(Box::new(list.into_iter()));
    }

    fn finish(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        return Box::new(None.into_iter());
    }
}

pub struct VertexDirectionResultIterator<V, VI>
    where V: Vertex,
          VI: Iterator<Item=V> {
    filter_manager: Arc<FilterManager>,
    range_limit: usize,
    dedup_manager: Option<Arc<DedupManager>>,
    after_requirement: Arc<RequirementManager>,
    vertex_list: VI,
    message_list: Vec<RawMessage>,
    result_list: Vec<RawMessage>,
    curr_count: usize,
}

impl<V, VI> VertexDirectionResultIterator<V, VI>
    where V: Vertex,
          VI: Iterator<Item=V> {
    pub fn new(filter_manager: Arc<FilterManager>,
               range_limit: usize,
               dedup_manager: Option<Arc<DedupManager>>,
               after_requirement: Arc<RequirementManager>,
               vertex_list: VI,
               message_list: Vec<RawMessage>) -> Self {
        let count = message_list.len();
        VertexDirectionResultIterator {
            filter_manager,
            range_limit,
            dedup_manager,
            after_requirement,
            vertex_list,
            message_list,
            result_list: Vec::with_capacity(count),
            curr_count: 0,
        }
    }
}

impl<V, VI> Iterator for VertexDirectionResultIterator<V, VI>
    where V: Vertex,
          VI: Iterator<Item=V> {
    type Item = RawMessage;

    fn next(&mut self) -> Option<Self::Item> {
        if self.range_limit > 0 && self.curr_count >= self.range_limit {
            return None;
        }
        if let Some(result) = self.result_list.pop() {
            return Some(result);
        }

        while let Some(v) = self.vertex_list.next() {
            if self.filter_manager.filter_native_vertex(&v) {
                if self.dedup_manager.is_none() || self.dedup_manager.as_ref().unwrap().check_dedup(v.get_id()) {
                    self.curr_count += 1;
                    for message in self.message_list.iter() {
                        let mut result = RawMessage::from_vertex_id(v.get_label_id() as i32, v.get_id());
                        self.after_requirement.as_ref().process_extend_entity(message, &mut result);
                        self.result_list.push(self.after_requirement.as_ref().process_requirement(result));
                    }
                    break;
                }
            }
        }
        return self.result_list.pop();
    }
}

// for get_out_vertex_ids & get_in_vertex_ids
pub struct GlobalStoreAdjVertexIdResultIterator<V, VI, E, EI>
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    adj_list: IntoIter<(VertexId, LocalStoreVertexIterator<V, VI, E, EI>)>,
    result_list: Vec<(VertexId, LocalStoreVertexIterator<V, VI, E, EI>)>,
}

impl<V, VI, E, EI> GlobalStoreAdjVertexIdResultIterator<V, VI, E, EI>
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    pub fn new(adj_list: IntoIter<(VertexId, LocalStoreVertexIterator<V, VI, E, EI>)>) -> Self {
        GlobalStoreAdjVertexIdResultIterator {
            adj_list,
            result_list: vec![],
        }
    }
}

impl<V, VI, E, EI> Iterator for GlobalStoreAdjVertexIdResultIterator<V, VI, E, EI>
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    type Item = (VertexId, LocalStoreVertexIterator<V, VI, E, EI>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.result_list.pop() {
            return Some(result);
        }
        while let Some(adj) = self.adj_list.next() {
            self.result_list.push(adj);
            break;
        }
        return self.result_list.pop();
    }
}

pub struct LocalStoreVertexIterator<V, VI, E, EI>
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    filter_manager: Option<Arc<FilterManager>>,
    range_limit: usize,
    dedup_manager: Option<Arc<DedupManager>>,
    edge_iter_list: IteratorList<EI, E>,
    direction: Option<EdgeDirection>,
    vertex_list: IntoIter<V>,
    output_prop_ids: Option<Vec<u32>>,
    vertex_iter_list: IteratorList<VI, V>,
    adj_vertex_flag: bool,
    all_vertex_flag: bool,
    result_list: Vec<LocalStoreVertex>,
    curr_count: usize,
    // TODO: add a local_flag, only local needs filtering
}

impl<V, VI, E, EI> LocalStoreVertexIterator<V, VI, E, EI>
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    pub fn new(filter_manager: Option<Arc<FilterManager>>,
               range_limit: usize,
               dedup_manager: Option<Arc<DedupManager>>,
               edge_list: IteratorList<EI, E>,
               direction: Option<EdgeDirection>,
               vertex_list: IntoIter<V>,
               output_prop_ids: Option<Vec<u32>>,
               vertex_iter_list: IteratorList<VI, V>,
               adj_vertex_flag: bool,
               all_vertex_flag: bool,
    ) -> Self {
        LocalStoreVertexIterator {
            filter_manager,
            range_limit,
            dedup_manager,
            edge_iter_list: edge_list,
            direction,
            vertex_list,
            output_prop_ids,
            vertex_iter_list,
            adj_vertex_flag,
            all_vertex_flag,
            result_list: vec![],
            curr_count: 0,
        }
    }

    pub fn from_getting_adj_vertex_ids(
        filter_manager: Option<Arc<FilterManager>>,
        range_limit: usize,
        dedup_manager: Option<Arc<DedupManager>>,
        edge_iter_list: IteratorList<EI, E>,
        direction: Option<EdgeDirection>,
    ) -> Self {
        LocalStoreVertexIterator {
            filter_manager,
            range_limit,
            dedup_manager,
            edge_iter_list,
            direction,
            vertex_list: vec![].into_iter(),
            output_prop_ids: None,
            vertex_iter_list: IteratorList::new(vec![]),
            adj_vertex_flag: true,
            all_vertex_flag: false,
            result_list: vec![],
            curr_count: 0,
        }
    }

    pub fn from_getting_vertex_properties(
        vertex_list: IntoIter<V>,
        output_prop_ids: Option<Vec<u32>>,
    ) -> Self {
        LocalStoreVertexIterator {
            filter_manager: None,
            range_limit: 0,
            dedup_manager: None,
            edge_iter_list: IteratorList::new(vec![]),
            direction: None,
            vertex_list,
            output_prop_ids,
            vertex_iter_list: IteratorList::new(vec![]),
            adj_vertex_flag: false,
            all_vertex_flag: false,
            result_list: vec![],
            curr_count: 0,
        }
    }

    pub fn from_getting_all_vertices(filter_manager: Option<Arc<FilterManager>>,
                                     range_limit: usize,
                                     dedup_manager: Option<Arc<DedupManager>>,
                                     output_prop_ids: Option<Vec<u32>>,
                                     vertex_iter_list: IteratorList<VI, V>,
    ) -> Self {
        LocalStoreVertexIterator {
            filter_manager,
            range_limit,
            dedup_manager,
            edge_iter_list: IteratorList::new(vec![]),
            direction: None,
            vertex_list: vec![].into_iter(),
            output_prop_ids,
            vertex_iter_list,
            adj_vertex_flag: false,
            all_vertex_flag: true,
            result_list: vec![],
            curr_count: 0,
        }
    }
}

impl<V, VI, E, EI> Iterator for LocalStoreVertexIterator<V, VI, E, EI>
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    type Item = LocalStoreVertex;

    fn next(&mut self) -> Option<Self::Item> {
        if self.range_limit > 0 && self.curr_count >= self.range_limit {
            return None;
        }
        if let Some(result) = self.result_list.pop() {
            return Some(result);
        }
        if self.adj_vertex_flag {
            // for VI in get_out_vertex_ids && get_in_vertex_ids
            while let Some(e) = self.edge_iter_list.next() {
                if self.filter_manager.is_none() || self.filter_manager.as_ref().unwrap().filter_native_edge(&e) {
                    if let Some(v) = {
                        match self.direction {
                            Some(EdgeDirection::DIR_OUT) => Some(LocalStoreVertex::new(e.get_dst_id(), e.get_dst_label_id())),
                            Some(EdgeDirection::DIR_IN) => Some(LocalStoreVertex::new(e.get_src_id(), e.get_src_label_id())),
                            _ => None,
                        }
                    } {
                        if self.dedup_manager.is_none() || self.dedup_manager.as_ref().unwrap().check_dedup(v.get_id()) {
                            self.curr_count += 1;
                            self.result_list.push(v);
                            break;
                        }
                    }
                }
            }
        } else if self.all_vertex_flag {
            // for get_all_vertices
            while let Some(v) = self.vertex_iter_list.next() {
                if self.dedup_manager.is_none() || self.dedup_manager.as_ref().unwrap().check_dedup(v.get_id()) {
                    if self.filter_manager.is_none() || self.filter_manager.as_ref().unwrap().filter_native_vertex(&v) {
                        self.curr_count += 1;
                        let mut local_vertex = LocalStoreVertex::new(v.get_id(), v.get_label_id());
                        if let Some(prop_ids) = &self.output_prop_ids {
                            for prop_id in prop_ids {
                                if let Some(prop) = v.get_property(*prop_id as u32) {
                                    local_vertex.add_property(*prop_id as u32, prop);
                                }
                            }
                        } else {
                            for (pid, pval) in v.get_properties() {
                                local_vertex.add_property(pid, pval);
                            }
                        }
                        self.result_list.push(local_vertex);
                        break;
                    }
                }
            }
        } else {
            // for get_vertex_properties
            while let Some(v) = self.vertex_list.next() {
                let mut local_vertex = LocalStoreVertex::new(v.get_id(), v.get_label_id());
                if let Some(prop_ids) = &self.output_prop_ids {
                    for prop_id in prop_ids {
                        if let Some(prop) = v.get_property(*prop_id as u32) {
                            local_vertex.add_property(*prop_id as u32, prop);
                        }
                    }
                } else {
                    for (pid, pval) in v.get_properties() {
                        local_vertex.add_property(pid, pval);
                    }
                }
                self.result_list.push(local_vertex);
                break;
            }
        }
        return self.result_list.pop();
    }
}

/// for get_out_edges and get_in_edges
pub struct GlobalStoreAdjEdgeResultIterator<E, EI>
    where E: Edge,
          EI: Iterator<Item=E> {
    adj_list: IntoIter<(VertexId, LocalStoreEdgeIterator<E, EI>)>,
    result_list: Vec<(VertexId, LocalStoreEdgeIterator<E, EI>)>,
}

impl<E, EI> GlobalStoreAdjEdgeResultIterator<E, EI>
    where E: Edge,
          EI: Iterator<Item=E> {
    pub fn new(adj_list: IntoIter<(VertexId, LocalStoreEdgeIterator<E, EI>)>) -> Self {
        GlobalStoreAdjEdgeResultIterator {
            adj_list,
            result_list: vec![],
        }
    }
}

impl<E, EI> Iterator for GlobalStoreAdjEdgeResultIterator<E, EI>
    where E: Edge,
          EI: Iterator<Item=E> {
    type Item = (VertexId, LocalStoreEdgeIterator<E, EI>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.result_list.pop() {
            return Some(result);
        }
        while let Some(adj) = self.adj_list.next() {
            self.result_list.push(adj);
            break;
        }
        return self.result_list.pop();
    }
}

pub struct LocalStoreEdgeIterator<E, EI>
    where E: Edge,
          EI: Iterator<Item=E> {
    filter_manager: Option<Arc<FilterManager>>,
    range_limit: usize,
    dedup_manager: Option<Arc<DedupManager>>,
    output_prop_ids: Option<Vec<u32>>,
    edge_list: IteratorList<EI, E>,
    result_list: Vec<LocalStoreEdge>,
    curr_count: usize,
}

impl<E, EI> LocalStoreEdgeIterator<E, EI>
    where E: Edge,
          EI: Iterator<Item=E> {
    pub fn new(filter_manager: Option<Arc<FilterManager>>,
               range_limit: usize,
               dedup_manager: Option<Arc<DedupManager>>,
               output_prop_ids: Option<Vec<u32>>,
               edge_list: IteratorList<EI, E>,
    ) -> Self {
        LocalStoreEdgeIterator {
            filter_manager,
            range_limit,
            dedup_manager,
            output_prop_ids,
            edge_list,
            result_list: vec![],
            curr_count: 0,
        }
    }
}

impl<E, EI> Iterator for LocalStoreEdgeIterator<E, EI>
    where E: Edge,
          EI: Iterator<Item=E> {
    type Item = LocalStoreEdge;

    fn next(&mut self) -> Option<Self::Item> {
        if self.range_limit > 0 && self.curr_count >= self.range_limit {
            return None;
        }
        if let Some(result) = self.result_list.pop() {
            return Some(result);
        }

        while let Some(e) = self.edge_list.next() {
            if self.dedup_manager.is_none() || self.dedup_manager.as_ref().unwrap().check_dedup(e.get_edge_id()) {
                if self.filter_manager.is_none() || self.filter_manager.as_ref().unwrap().filter_native_edge(&e) {
                    self.curr_count += 1;
                    let src = LocalStoreVertex::new(e.get_src_id(), e.get_src_label_id());
                    let dst = LocalStoreVertex::new(e.get_dst_id(), e.get_dst_label_id());
                    let mut local_edge = LocalStoreEdge::new(src, dst, e.get_label_id(), e.get_edge_id());
                    if let Some(prop_ids) = &self.output_prop_ids {
                        for prop_id in prop_ids {
                            if let Some(prop) = e.get_property(*prop_id as u32) {
                                local_edge.add_property(*prop_id as u32, prop);
                            }
                        }
                    } else {
                        for (pid, pval) in e.get_properties() {
                            local_edge.add_property(pid, pval);
                        }
                    }
                    self.result_list.push(local_edge);
                    break;
                }
            }
        }
        return self.result_list.pop();
    }
}

pub struct EdgeDirectionResultIterator<E, EI>
    where E: Edge,
          EI: Iterator<Item=E> {
    filter_manager: Arc<FilterManager>,
    range_limit: usize,
    dedup_manager: Option<Arc<DedupManager>>,
    after_requirement: Arc<RequirementManager>,
    edge_list: EI,
    message_list: Vec<RawMessage>,
    result_list: Vec<RawMessage>,
    curr_count: usize,
    is_out: bool,
}

impl<E, EI> EdgeDirectionResultIterator<E, EI>
    where E: Edge,
          EI: Iterator<Item=E> {
    pub fn new(filter_manager: Arc<FilterManager>,
               range_limit: usize,
               dedup_manager: Option<Arc<DedupManager>>,
               after_requirement: Arc<RequirementManager>,
               edge_list: EI,
               message_list: Vec<RawMessage>,
               is_out: bool, ) -> Self {
        let count = message_list.len();
        EdgeDirectionResultIterator {
            filter_manager,
            range_limit,
            dedup_manager,
            after_requirement,
            edge_list,
            message_list,
            result_list: Vec::with_capacity(count),
            curr_count: 0,
            is_out,
        }
    }
}

impl<E, EI> Iterator for EdgeDirectionResultIterator<E, EI>
    where E: Edge,
          EI: Iterator<Item=E> {
    type Item = RawMessage;

    fn next(&mut self) -> Option<Self::Item> {
        if self.range_limit > 0 && self.curr_count >= self.range_limit {
            return None;
        }
        if let Some(result) = self.result_list.pop() {
            return Some(result);
        }

        while let Some(e) = self.edge_list.next() {
            if self.filter_manager.filter_native_edge(&e) {
                if self.dedup_manager.is_none() || self.dedup_manager.as_ref().unwrap().check_dedup(e.get_edge_id()) {
                    self.curr_count += 1;
                    for message in self.message_list.iter() {
                        let mut result = RawMessage::from_edge_id(e.get_edge_id(),
                                                                  e.get_label_id() as i32,
                                                                  self.is_out,
                                                                  e.get_src_id(),
                                                                  e.get_src_label_id() as i32,
                                                                  e.get_dst_id(),
                                                                  e.get_dst_label_id() as i32);
                        for (propid, prop) in e.get_properties() {
                            result.add_native_property(propid as i32, prop);
                        }
                        self.after_requirement.as_ref().process_extend_entity(message, &mut result);
                        self.result_list.push(self.after_requirement.as_ref().process_requirement(result));
                    }
                    break;
                }
            }
        }
        return self.result_list.pop();
    }
}

#[test]
fn test_iterator_node() {
    use dataflow::message::*;

    let source_list = Box::new(vec![RawMessage::new(RawMessageType::VERTEX),
                                    RawMessage::new(RawMessageType::VERTEX),
                                    RawMessage::new(RawMessageType::VERTEX)].into_iter());
    let mut iter_node = UnaryIteratorNode::new(Some(source_list), None, Box::new(EmptyUnaryOperator {}));
    let mut count = 0_i32;
    while let Some(v) = iter_node.next() {
        println!("value => {:?}", &v);
        count += 1;
    }
    assert_eq!(count, 9);
}
