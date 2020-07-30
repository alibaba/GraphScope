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
use dataflow::builder::{Operator, UnaryOperator, InputStreamShuffle, MessageCollector};
use dataflow::message::{RawMessage, RawMessageType, ValuePayload};
use maxgraph_store::api::{LabelId, SnapshotId, MVGraph, Vertex, Edge, VertexId, PropId, EdgeId, GlobalGraphQuery};
use store::ffi::WriteNativeProperty;
use std::sync::Arc;
use dataflow::manager::context::{RuntimeContext, TaskContext};
use store::graph_builder_ffi::VineyardGraphWriter;
use maxgraph_store::api::prelude::Property;
use dataflow::message::subgraph::SubGraph;
use std::collections::{HashMap, HashSet};

pub struct VineyardWriteVertexOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    shuffle_type: StreamShuffleType<F>,
    stream_index: i32,
    graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
    writer: VineyardGraphWriter,
    context: TaskContext,
    vertex_id_set: HashSet<i64>,
}

impl<V, VI, E, EI, F> VineyardWriteVertexOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(id: i32,
               input_id: i32,
               shuffle_type: StreamShuffleType<F>,
               stream_index: i32,
               graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
               graph_name: String,
               context: TaskContext) -> Self {
        let writer = VineyardGraphWriter::new(graph_name, context.get_worker_index() as i32);
        VineyardWriteVertexOperator {
            id,
            input_id,
            shuffle_type,
            stream_index,
            graph,
            writer,
            context,
            vertex_id_set: HashSet::new(),
        }
    }
}

impl<V, VI, E, EI, F> Operator for VineyardWriteVertexOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<V, VI, E, EI, F> UnaryOperator for VineyardWriteVertexOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_input_id(&self) -> i32 {
        self.input_id
    }

    fn get_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.shuffle_type.clone())
    }

    fn get_stream_index(&self) -> i32 {
        self.stream_index
    }

    fn execute<'a>(&mut self, message: Vec<RawMessage>, collector: &mut Box<'a + MessageCollector>) {
        let mut partition_vertex_list = Vec::with_capacity(message.len());
        for m in message.into_iter() {
            match m.get_message_type() {
                RawMessageType::VERTEX => {
                    if self.vertex_id_set.contains(&m.get_id()) {
                        continue;
                    }
                    self.vertex_id_set.insert(m.get_id());
                    self.context.assign_prop_vertex_partition(Some(m.get_label_id() as u32),
                                                              m.get_id(),
                                                              &mut partition_vertex_list);
                }
                _ => {}
            }
        }
        if self.context.get_debug_flag() {
            info!("start to add partition vertex list {:?} to vineyard", &partition_vertex_list);
        }
        if !partition_vertex_list.is_empty() {
            let mut vertex_ids_map = HashSet::new();
            let mut vertex_ids = Vec::with_capacity(partition_vertex_list.len());
            let mut label_ids = Vec::with_capacity(partition_vertex_list.len());
            let mut properties = Vec::with_capacity(partition_vertex_list.len());
            let mut vi = self.graph.as_ref().get_vertex_properties(self.context.get_si(),
                                                                   partition_vertex_list,
                                                                   None);
            while let Some(v) = vi.next() {
                if !vertex_ids_map.contains(&v.get_id()) {
                    vertex_ids_map.insert(v.get_id());
                    let mut local_propertices = vec![];

                    vertex_ids.push(v.get_id());
                    label_ids.push(v.get_label_id());
                    for (propid, property) in v.get_properties() {
                        local_propertices.push(WriteNativeProperty::from_property(propid, property));
                    }

                    properties.push(local_propertices);
                }
            }
            if self.context.get_debug_flag() {
                info!("start to add vertex ids {:?} label ids {:?}", &vertex_ids, &label_ids);
            }

            let mut vertex_outer_ids = Vec::with_capacity(vertex_ids.len());
            for vid in vertex_ids.drain(..) {
                vertex_outer_ids.push(self.graph.as_ref().translate_vertex_id(vid));
            }
            self.writer.add_vertices(vertex_outer_ids, label_ids, properties);
        }
    }

    fn finish(&mut self) -> Box<Iterator<Item=RawMessage> + Send> {
        self.writer.finish_vertice();
        Box::new(Some(RawMessage::from_vertex_id(0, 0)).into_iter())
    }
}

pub struct VineyardWriteEdgeOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    id: i32,
    input_id: i32,
    shuffle_type: StreamShuffleType<F>,
    stream_index: i32,
    graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
    writer: VineyardGraphWriter,
    subgraph: Arc<SubGraph>,
    debug_log_flag: bool,
}

impl<V, VI, E, EI, F> VineyardWriteEdgeOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    pub fn new(id: i32,
               input_id: i32,
               shuffle_type: StreamShuffleType<F>,
               stream_index: i32,
               graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
               graph_name: String,
               index: i32,
               subgraph: Arc<SubGraph>,
               debug_log_flag: bool, ) -> Self {
        let writer = VineyardGraphWriter::new(graph_name, index);
        VineyardWriteEdgeOperator {
            id,
            input_id,
            shuffle_type,
            stream_index,
            graph,
            writer,
            subgraph,
            debug_log_flag,
        }
    }
}

impl<V, VI, E, EI, F> Operator for VineyardWriteEdgeOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<V, VI, E, EI, F> UnaryOperator for VineyardWriteEdgeOperator<V, VI, E, EI, F>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E>,
          F: Fn(&i64) -> u64 + 'static + Send + Sync {
    fn get_input_id(&self) -> i32 {
        self.input_id
    }

    fn get_input_shuffle(&self) -> Box<InputStreamShuffle> {
        Box::new(self.shuffle_type.clone())
    }

    fn get_stream_index(&self) -> i32 {
        self.stream_index
    }

    fn execute<'a>(&mut self, mut message: Vec<RawMessage>, collector: &mut Box<'a + MessageCollector>) {
        message.clear();
    }

    fn finish(&mut self) -> Box<Iterator<Item=RawMessage> + Send> {
        let edge_prop_list = self.subgraph.edge_prop_list.borrow();
        let mut edge_ids = Vec::with_capacity(edge_prop_list.len());
        let mut src_ids = Vec::with_capacity(edge_prop_list.len());
        let mut dst_ids = Vec::with_capacity(edge_prop_list.len());
        let mut edge_labels = Vec::with_capacity(edge_prop_list.len());
        let mut src_labels = Vec::with_capacity(edge_prop_list.len());
        let mut dst_labels = Vec::with_capacity(edge_prop_list.len());
        let mut properties = Vec::with_capacity(edge_prop_list.len());

        for ((id, labelid), e, eprop) in edge_prop_list.iter() {
            edge_ids.push(*id);
            edge_labels.push(*labelid);
            src_ids.push(e.get_src_id());
            src_labels.push(e.get_src_label() as u32);
            dst_ids.push(e.get_dst_id());
            dst_labels.push(e.get_dst_label() as u32);

            let mut local_prop_list = Vec::with_capacity(eprop.len());
            for prop in eprop.iter() {
                local_prop_list.push(WriteNativeProperty::from_prop_entity(prop));
            }
            properties.push(local_prop_list);
        }
        if self.debug_log_flag {
            info!("start to add edge ids {:?} src ids {:?} dst ids {:?} edge labels {:?} src labels {:?} dst labels {:?}",
                  &edge_ids, &src_ids, &dst_ids, &edge_labels, &src_labels, &dst_labels);
        }

        let mut src_outer_ids = Vec::with_capacity(src_ids.len());
        let mut dst_outer_ids = Vec::with_capacity(dst_ids.len());
        for src_vid in src_ids.drain(..) {
            src_outer_ids.push(self.graph.as_ref().translate_vertex_id(src_vid));
        }
        for dst_vid in dst_ids.drain(..) {
            dst_outer_ids.push(self.graph.as_ref().translate_vertex_id(dst_vid));
        }
        self.writer.add_edges(edge_ids, src_outer_ids, dst_outer_ids, edge_labels, src_labels, dst_labels, properties);
        self.writer.finish_edge();

        return Box::new(Some(RawMessage::from_value(ValuePayload::Long(edge_prop_list.len() as i64))).into_iter());
    }
}
