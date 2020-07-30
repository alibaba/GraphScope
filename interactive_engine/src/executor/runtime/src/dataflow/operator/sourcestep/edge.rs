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

use maxgraph_store::api::{Edge, Vertex, GlobalGraphQuery};
use dataflow::manager::filter::FilterManager;
use dataflow::manager::range::RangeManager;
use dataflow::manager::requirement::RequirementManager;
use dataflow::message::RawMessage;
use dataflow::manager::context::TaskContext;
use dataflow::builder::{SourceOperator, Operator};
use std::sync::Arc;

// source edge iterator
pub struct SourceEdgeIterator<E, EI>
    where E: Edge + Send + 'static,
          EI: Iterator<Item=E> + Send + 'static {
    iter: EI,
    id_list: Vec<i64>,
    filter_manager: FilterManager,
    range_manager: Option<RangeManager>,
    after_requirement: RequirementManager,
    fetch_prop_flag: bool,
}

unsafe impl<E, EI> Send for SourceEdgeIterator<E, EI>
    where E: Edge + Send + 'static,
          EI: Iterator<Item=E> + Send + 'static {}

impl<E, EI> SourceEdgeIterator<E, EI>
    where E: Edge + Send + 'static,
          EI: Iterator<Item=E> + Send + 'static {
    pub fn new(iter: EI,
               id_list: Vec<i64>,
               filter_manager: FilterManager,
               range_manager: Option<RangeManager>,
               after_requirement: RequirementManager,
               fetch_prop_flag: bool) -> Self {
        SourceEdgeIterator {
            iter,
            id_list,
            filter_manager,
            range_manager,
            after_requirement,
            fetch_prop_flag,
        }
    }
}

impl<E, EI> Iterator for SourceEdgeIterator<E, EI>
    where E: Edge + Send + 'static,
          EI: Iterator<Item=E> + Send + 'static {
    type Item = RawMessage;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(v) = self.iter.next() {
                if self.filter_manager.filter_native_edge(&v) &&
                    (self.id_list.is_empty() || self.id_list.contains(&v.get_edge_id())) {
                    if let Some(ref mut range) = self.range_manager {
                        if range.range_finish() {
                            return None;
                        } else if !range.range_filter() {
                            return Some(self.after_requirement.process_requirement(RawMessage::from_edge(v, true, self.fetch_prop_flag)));
                        }
                    } else {
                        return Some(self.after_requirement.process_requirement(RawMessage::from_edge(v, true, self.fetch_prop_flag)));
                    }
                }
            } else {
                return None;
            }
        }
    }
}

// source edge operator
pub struct SourceEdgeOperator<E, EI>
    where E: Edge + Send + 'static,
          EI: Iterator<Item=E> + Send + 'static {
    id: i32,
    iter: Option<SourceEdgeIterator<E, EI>>,
}

impl<E, EI> SourceEdgeOperator<E, EI>
    where E: Edge + Send + 'static,
          EI: Iterator<Item=E> + Send + 'static {
    pub fn new<V, VI>(id: i32,
                      context: TaskContext,
                      label_list: Vec<u32>,
                      filter_manager: FilterManager,
                      range_manager: Option<RangeManager>,
                      after_requirement: RequirementManager,
                      id_list: Vec<i64>,
                      fetch_prop_flag: bool,
                      global_graph: Arc<GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>) -> Self
        where V: Vertex + Send + 'static,
              VI: Iterator<Item=V> + Send + 'static {
        let si = context.get_si();
        let partition_list = context.get_partition_list();
        if partition_list.is_empty() {
            SourceEdgeOperator {
                id,
                iter: None,
            }
        } else {
            let ei = global_graph.as_ref().get_all_edges(si,
                                                         &label_list,
                                                         None,
                                                         None,
                                                         None,
                                                         0,
                                                         partition_list);

            let vertex_iter = SourceEdgeIterator::new(
                ei,
                id_list,
                filter_manager,
                range_manager,
                after_requirement,
                fetch_prop_flag);
            SourceEdgeOperator {
                id,
                iter: Some(vertex_iter),
            }
        }
    }
}

impl<E, EI> Operator for SourceEdgeOperator<E, EI>
    where E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static {
    fn get_id(&self) -> i32 {
        self.id
    }
}

impl<E, EI> SourceOperator for SourceEdgeOperator<E, EI>
    where E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static {
    fn execute(&mut self) -> Box<dyn Iterator<Item=RawMessage> + Send> {
        if self.iter.is_some() {
            return Box::new(self.iter.take().unwrap());
        } else {
            return Box::new(None.into_iter());
        }
    }
}
