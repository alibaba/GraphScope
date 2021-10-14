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

use maxgraph_store::api::{Vertex, Edge, GlobalGraphQuery};
use dataflow::common::iterator::IteratorList;
use dataflow::manager::filter::FilterManager;
use dataflow::manager::range::RangeManager;
use dataflow::manager::requirement::RequirementManager;
use dataflow::message::RawMessage;
use dataflow::manager::context::TaskContext;
use dataflow::builder::{Operator, SourceOperator};
use dataflow::operator::sourcestep::edge::SourceEdgeOperator;
use std::collections::HashMap;
use itertools::Itertools;
use std::sync::Arc;

// source vertex iterator
pub struct SourceVertexIterator<V, VI>
    where V: Vertex + 'static, VI: Iterator<Item=V> + 'static {
    iter_list: IteratorList<VI, V>,
    filter_manager: FilterManager,
    range_manager: Option<RangeManager>,
    after_requirement: RequirementManager,
}

impl<V, VI> SourceVertexIterator<V, VI>
    where V: Vertex + 'static + Send, VI: Iterator<Item=V> + 'static {
    pub fn new(iter_list: IteratorList<VI, V>,
               filter_manager: FilterManager,
               range_manager: Option<RangeManager>,
               after_requirement: RequirementManager) -> Self {
        SourceVertexIterator {
            iter_list,
            filter_manager,
            range_manager,
            after_requirement,
        }
    }
}

impl<V, VI> Iterator for SourceVertexIterator<V, VI>
    where V: Vertex + Send + 'static, VI: Iterator<Item=V> + Send + 'static {
    type Item = RawMessage;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(v) = self.iter_list.next() {
                if self.filter_manager.filter_native_vertex(&v) {
                    if let Some(ref mut range) = self.range_manager {
                        if range.range_finish() {
                            return None;
                        } else if !range.range_filter() {
                            return Some(self.after_requirement.process_requirement(RawMessage::from_vertex(v)));
                        }
                    } else {
                        return Some(self.after_requirement.process_requirement(RawMessage::from_vertex(v)));
                    }
                }
            } else {
                return None;
            }
        }
    }
}

// source of vertex for scan
pub struct SourceVertexOperator<V, VI>
    where V: Vertex + Send + 'static, VI: Iterator<Item=V> + Send + 'static {
    id: i32,
    iter: Option<SourceVertexIterator<V, VI>>,
}

impl<V, VI> SourceVertexOperator<V, VI>
    where V: Vertex + Send + 'static,
          VI: Iterator<Item=V> + Send + 'static
{
    pub fn new<E, EI>(id: i32,
                      context: TaskContext,
                      label_list: Vec<u32>,
                      filter_manager: FilterManager,
                      range_manager: Option<RangeManager>,
                      after_requirement: RequirementManager,
                      global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>) -> Self
        where E: Edge + Send + 'static,
              EI: Iterator<Item=E> + Send + 'static {
        let si = context.get_si();
        let partition_list = context.get_partition_list();
        if context.get_debug_flag() {
            info!("Scan vertex from partition list {:?} for worker {:?}", partition_list, context.get_worker_index());
        }
        if partition_list.is_empty() {
            SourceVertexOperator {
                id,
                iter: None,
            }
        } else {
            let vi = global_graph.as_ref().get_all_vertices(si,
                                                            &label_list,
                                                            None,
                                                            None,
                                                            None,
                                                            0,
                                                            partition_list);

            let vertex_iter = SourceVertexIterator::new(
                IteratorList::new(vec![vi]),
                filter_manager,
                range_manager,
                after_requirement);
            SourceVertexOperator {
                id,
                iter: Some(vertex_iter),
            }
        }
    }
}

impl<V, VI> Operator for SourceVertexOperator<V, VI>
    where V: Vertex + 'static, VI: Iterator<Item=V> + Send + 'static {
    #[inline]
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<V, VI> SourceOperator for SourceVertexOperator<V, VI>
    where V: Vertex + 'static, VI: Iterator<Item=V> + Send + 'static {
    #[inline]
    fn execute(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        if self.iter.is_some() {
            return Box::new(self.iter.take().unwrap());
        } else {
            return Box::new(None.into_iter());
        }
    }
}

// source vertex with id list operator
pub struct SourceVertexIdOperator {
    id: i32,
    vertex_list: Option<Vec<RawMessage>>,
}

impl Operator for SourceVertexIdOperator {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl SourceOperator for SourceVertexIdOperator {
    fn execute(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        if self.vertex_list.is_some() {
            return Box::new(self.vertex_list.take().unwrap().into_iter());
        } else {
            return Box::new(None.into_iter());
        }
    }
}

impl SourceVertexIdOperator {
    pub fn new<V, VI, E, EI>(id: i32,
                             context: TaskContext,
                             label_list: Vec<u32>,
                             id_list: Vec<i64>,
                             filter_manager: FilterManager,
                             mut range_manager: Option<RangeManager>,
                             after_requirement: RequirementManager,
                             global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>) -> Self
        where V: Vertex + 'static,
              VI: Iterator<Item=V> + 'static,
              E: Edge + 'static,
              EI: Iterator<Item=E> + 'static {
        let si = context.get_si();
        let partition_list = context.get_partition_list();
        let mut partition_label_vid_map = HashMap::new();
        for vid in id_list.into_iter() {
            if let Some(partition_id) = context.get_partition_id(vid) {
                if partition_list.contains(&partition_id) {
                    let label_vid_list = partition_label_vid_map.entry(partition_id).or_insert(HashMap::new());
                    if label_list.is_empty() {
                        label_vid_list.entry(None).or_insert(vec![]).push(vid);
                    } else {
                        for label_id in label_list.iter() {
                            label_vid_list.entry(Some(*label_id)).or_insert(vec![]).push(vid);
                        }
                    }
                }
            }
        }
        let partition_label_vid_list = partition_label_vid_map
            .into_iter()
            .map(|(k, v)|
                (k, v.into_iter().map(|(kk, vv)| (kk, vv)).collect_vec())
            ).collect_vec();
        if context.get_debug_flag() {
            info!("Fetch vertex list with {:?}", &partition_label_vid_list);
        }
        let mut vi = global_graph.as_ref().get_vertex_properties(si, partition_label_vid_list, None);
        let mut vertex_list = vec![];
        while let Some(vertex) = vi.next() {
            if context.get_debug_flag() {
                info!("Fetch vertex {:?}", vertex.get_id());
            }
            if label_list.is_empty() || label_list.contains(&vertex.get_label_id()) {
                if filter_manager.filter_native_vertex(&vertex) {
                    if let Some(ref mut range) = range_manager {
                        if range.range_finish() {
                            break;
                        }
                        if !range.range_filter() {
                            vertex_list.push(after_requirement.process_requirement(RawMessage::from_vertex(vertex)));
                        }
                    } else {
                        vertex_list.push(after_requirement.process_requirement(RawMessage::from_vertex(vertex)));
                    }
                }
            }
        }
        if context.get_debug_flag() {
            info!("Fetch vertex result list {:?}", &vertex_list);
        }

        SourceVertexIdOperator {
            id,
            vertex_list: Some(vertex_list),
        }
    }
}
