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

use crate::api::{GlobalGraphQuery, SnapshotId, PartitionVertexIds, LabelId, Vertex, Edge, VertexId, PropId, PartitionLabeledVertexIds, PartitionId};
use std::sync::Arc;
use std::collections::HashMap;
use crate::api::prelude::Property;


fn check_list_equal<T: PartialEq>(list1: &Vec<T>, list2: &Vec<T>) -> bool {
    if list1.len() == list2.len() {
        for val1 in list1.iter() {
            if !list2.contains(val1) {
                return false;
            }
        }
        return true;
    } else {
        return false;
    }
}

pub fn test_get_out_vertex_ids<V, VI, E, EI>(global_graph: Arc<dyn GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
                                             si: SnapshotId,
                                             src_ids: Vec<PartitionVertexIds>,
                                             edge_labels: &Vec<LabelId>,
                                             result_list: Vec<(VertexId, Vec<(LabelId, VertexId)>)>)
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    let mut out_result_list = global_graph.as_ref().get_out_vertex_ids(si,
                                                                       src_ids,
                                                                       edge_labels,
                                                                       None,
                                                                       None,
                                                                       0);
    let mut out_result_vertex_list = HashMap::new();
    while let Some((src_id, mut vidlist)) = out_result_list.next() {
        let result_vid_list = out_result_vertex_list.entry(src_id).or_insert(vec![]);
        while let Some(vid) = vidlist.next() {
            result_vid_list.push((vid.get_label_id(), vid.get_id()));
        }
    }
    for (src_id, expect_out_vid_list) in result_list.into_iter() {
        if let Some(result_out_vid_list) = out_result_vertex_list.get(&src_id) {
            if !check_list_equal(&expect_out_vid_list, result_out_vid_list) {
                panic!("expect result list is {:?} while result out vid list is {:?}", &expect_out_vid_list, result_out_vid_list);
            }
        } else {
            panic!("expect result list is {:?} while result out vid list is none", &expect_out_vid_list);
        }
    }
}

pub fn test_get_in_vertex_ids<V, VI, E, EI>(global_graph: Arc<dyn GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
                                            si: SnapshotId,
                                            src_ids: Vec<PartitionVertexIds>,
                                            edge_labels: &Vec<LabelId>,
                                            result_list: Vec<(VertexId, Vec<(LabelId, VertexId)>)>)
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    let mut in_result_list = global_graph.as_ref().get_in_vertex_ids(si,
                                                                     src_ids,
                                                                     edge_labels,
                                                                     None,
                                                                     None,
                                                                     0);
    let mut in_result_vertex_list = HashMap::new();
    while let Some((src_id, mut vidlist)) = in_result_list.next() {
        let result_vid_list = in_result_vertex_list.entry(src_id).or_insert(vec![]);
        while let Some(vid) = vidlist.next() {
            result_vid_list.push((vid.get_label_id(), vid.get_id()));
        }
        println!("Get result vid list {:?} for dst id {:?}", result_vid_list, src_id);
    }
    for (src_id, expect_in_vid_list) in result_list.into_iter() {
        if let Some(result_in_vid_list) = in_result_vertex_list.get(&src_id) {
            if !check_list_equal(&expect_in_vid_list, result_in_vid_list) {
                panic!("expect result list is {:?} while result out vid list is {:?}", &expect_in_vid_list, result_in_vid_list);
            }
        } else {
            panic!("expect result list is {:?} while result out vid list is none", &expect_in_vid_list);
        }
    }
}

pub fn test_get_out_edges<V, VI, E, EI>(global_graph: Arc<dyn GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
                                        si: SnapshotId,
                                        src_ids: Vec<PartitionVertexIds>,
                                        edge_labels: &Vec<LabelId>,
                                        output_prop_ids: Option<&Vec<PropId>>,
                                        result_list: Vec<(VertexId, Vec<E>)>)
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    let mut out_result_list = global_graph.as_ref().get_out_edges(si,
                                                                  src_ids,
                                                                  edge_labels,
                                                                  None,
                                                                  None,
                                                                  output_prop_ids,
                                                                  0);
    let mut out_result_edge_list = HashMap::new();
    while let Some((src_id, mut edgelist)) = out_result_list.next() {
        let result_edge_list = out_result_edge_list.entry(src_id).or_insert(vec![]);
        while let Some(edge) = edgelist.next() {
            result_edge_list.push(edge);
        }
    }
    for (src_id, expect_out_edge_list) in result_list.into_iter() {
        if let Some(result_out_edge_list) = out_result_edge_list.get(&src_id) {
            if expect_out_edge_list.len() != result_out_edge_list.len() {
                panic!("expect result list len is {:?} while result out edge list len is {:?}", &expect_out_edge_list.len(), result_out_edge_list.len());
            }
            for out_edge in expect_out_edge_list.into_iter() {
                let mut found = false;
                for edge in result_out_edge_list.iter() {
                    if edge.get_edge_id() == out_edge.get_edge_id() &&
                        edge.get_label_id() == out_edge.get_label_id() &&
                        edge.get_src_id() == out_edge.get_src_id() &&
                        edge.get_src_label_id() == out_edge.get_src_label_id() &&
                        edge.get_dst_id() == out_edge.get_dst_id() &&
                        edge.get_dst_label_id() == out_edge.get_dst_label_id() {
                        let mut out_edge_prop_vec = vec![];
                        let mut out_edge_prop_list = out_edge.get_properties();
                        while let Some(prop) = out_edge_prop_list.next() {
                            out_edge_prop_vec.push(prop);
                        }

                        let mut edge_prop_vec = vec![];
                        let mut edge_prop_list = edge.get_properties();
                        while let Some(prop) = edge_prop_list.next() {
                            edge_prop_vec.push(prop);
                        }

                        if !check_list_equal(&out_edge_prop_vec, &edge_prop_vec) {
                            panic!("expect edge prop list is {:?} while result edge prop list len is {:?}", &out_edge_prop_vec, edge_prop_vec);
                        }

                        found = true;
                    }
                }
                if !found {
                    panic!("Cant find edge {:?}:{:?}->{:?}:{:?}->{:?}:{:?} in result",
                           out_edge.get_src_label_id(),
                           out_edge.get_src_id(),
                           out_edge.get_label_id(),
                           out_edge.get_edge_id(),
                           out_edge.get_dst_label_id(),
                           out_edge.get_dst_id());
                }
            }
        } else {
            panic!("Cant find result edge list for {:?}", src_id);
        }
    }
}

pub fn test_get_in_edges<V, VI, E, EI>(global_graph: Arc<dyn GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
                                       si: SnapshotId,
                                       src_ids: Vec<PartitionVertexIds>,
                                       edge_labels: &Vec<LabelId>,
                                       output_prop_ids: Option<&Vec<PropId>>,
                                       result_list: Vec<(VertexId, Vec<E>)>)
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    let mut in_result_list = global_graph.as_ref().get_in_edges(si,
                                                                src_ids,
                                                                edge_labels,
                                                                None,
                                                                None,
                                                                output_prop_ids,
                                                                0);
    let mut in_result_edge_list = HashMap::new();
    while let Some((src_id, mut edgelist)) = in_result_list.next() {
        let result_edge_list = in_result_edge_list.entry(src_id).or_insert(vec![]);
        while let Some(edge) = edgelist.next() {
            result_edge_list.push(edge);
        }
    }
    for (src_id, expect_in_edge_list) in result_list.into_iter() {
        if let Some(result_in_edge_list) = in_result_edge_list.get(&src_id) {
            if expect_in_edge_list.len() != result_in_edge_list.len() {
                panic!("expect result list len is {:?} while result out edge list len is {:?}", &expect_in_edge_list.len(), result_in_edge_list.len());
            }
            for in_edge in expect_in_edge_list.into_iter() {
                let mut found = false;
                for edge in result_in_edge_list.iter() {
                    if edge.get_edge_id() == in_edge.get_edge_id() &&
                        edge.get_label_id() == in_edge.get_label_id() &&
                        edge.get_src_id() == in_edge.get_src_id() &&
                        edge.get_src_label_id() == in_edge.get_src_label_id() &&
                        edge.get_dst_id() == in_edge.get_dst_id() &&
                        edge.get_dst_label_id() == in_edge.get_dst_label_id() {
                        let mut in_edge_prop_vec = vec![];
                        let mut in_edge_prop_list = in_edge.get_properties();
                        while let Some(prop) = in_edge_prop_list.next() {
                            in_edge_prop_vec.push(prop);
                        }

                        let mut edge_prop_vec = vec![];
                        let mut edge_prop_list = edge.get_properties();
                        while let Some(prop) = edge_prop_list.next() {
                            edge_prop_vec.push(prop);
                        }

                        if !check_list_equal(&in_edge_prop_vec, &edge_prop_vec) {
                            panic!("expect edge prop list is {:?} while result edge prop list len is {:?}", &in_edge_prop_vec, edge_prop_vec);
                        }

                        found = true;
                    }
                }
                if !found {
                    panic!("Cant find edge {:?}:{:?}->{:?}:{:?}->{:?}:{:?} in result",
                           in_edge.get_src_label_id(),
                           in_edge.get_src_id(),
                           in_edge.get_label_id(),
                           in_edge.get_edge_id(),
                           in_edge.get_dst_label_id(),
                           in_edge.get_dst_id());
                }
            }
        } else {
            panic!("Cant find result edge list for {:?}", src_id);
        }
    }
}

pub fn test_count_out_edges<V, VI, E, EI>(global_graph: Arc<dyn GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
                                          si: SnapshotId,
                                          src_ids: Vec<PartitionVertexIds>,
                                          edge_labels: &Vec<LabelId>,
                                          result_list: Vec<(VertexId, usize)>)
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    let mut out_result_list = global_graph.as_ref().count_out_edges(si,
                                                                  src_ids,
                                                                  edge_labels,
                                                                  None);
    let mut out_count_list = Vec::new();
    while let Some((src_id, size)) = out_result_list.next() {
        out_count_list.push((src_id,size));
    }
    if !check_list_equal(&result_list, &out_count_list) {
        panic!("expect edge prop list is {:?} while result edge prop list len is {:?}", &result_list, out_count_list);
    }
}

pub fn test_count_in_edges<V, VI, E, EI>(global_graph: Arc<dyn GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
                                         si: SnapshotId,
                                         dst_ids: Vec<PartitionVertexIds>,
                                         edge_labels: &Vec<LabelId>,
                                         result_list: Vec<(VertexId, usize)>)
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    let mut in_result_list = global_graph.as_ref().count_in_edges(si,
                                                                    dst_ids,
                                                                    edge_labels,
                                                                    None);
    let mut in_count_list = Vec::new();
    while let Some((src_id, size)) = in_result_list.next() {
        in_count_list.push((src_id,size));
    }
    if !check_list_equal(&result_list, &in_count_list) {
        panic!("expect edge prop list is {:?} while result edge prop list len is {:?}", &result_list, in_count_list);
    }
}

pub fn test_get_vertex_properties<V, VI, E, EI>(global_graph: Arc<dyn GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
                                                si: SnapshotId,
                                                ids: Vec<PartitionLabeledVertexIds>,
                                                output_prop_ids: Option<&Vec<PropId>>,
                                                result_list: HashMap<VertexId, Vec<(PropId, Property)>>)
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    let mut vertex_list = global_graph.as_ref().get_vertex_properties(si, ids, output_prop_ids);
    let mut result_vertex_prop_list = HashMap::new();
    while let Some(vertex) = vertex_list.next() {
        let mut prop_list = vec![];
        let pi = vertex.get_properties();
        for prop in pi {
            prop_list.push(prop);
        }
        result_vertex_prop_list.insert(vertex.get_id(), prop_list);
    }
    if result_vertex_prop_list.len() != result_list.len() {
        panic!("Expect vertex count {:?} != result vertex count {:?}", result_list.len(), result_vertex_prop_list.len());
    }
    for (vid, prop_list) in result_vertex_prop_list.drain() {
        if let Some(expect_prop_list) = result_list.get(&vid) {
            if !check_list_equal(expect_prop_list, &prop_list) {
                panic!("expect property list is {:?} while result property list is {:?} for vertex {:?}", expect_prop_list, &prop_list, vid);
            }
        } else {
            panic!("Cant found vertex {:?} in expect list while it exists in result list", vid);
        }
    }
}

pub fn test_get_edge_properties<V, VI, E, EI>(_global_graph: Arc<dyn GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
                                              _si: SnapshotId,
                                              _ids: Vec<PartitionLabeledVertexIds>,
                                              _output_prop_ids: Option<&Vec<PropId>>,
                                              _result_list: HashMap<VertexId, Vec<(PropId, Property)>>)
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    unimplemented!()
}

pub fn test_get_all_vertices<V, VI, E, EI>(global_graph: Arc<dyn GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
                                           si: SnapshotId,
                                           labels: &Vec<LabelId>,
                                           partition_ids: &Vec<PartitionId>,
                                           result_list: Vec<V>)
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    let mut vertex_list = global_graph.as_ref().get_all_vertices(si,
                                                                 labels,
                                                                 None,
                                                                 None,
                                                                 None,
                                                                 0,
                                                                 partition_ids);
    let mut result_vertex_list = HashMap::new();
    while let Some(v) = vertex_list.next() {
        result_vertex_list.insert(v.get_id(), v);
    }
    if result_vertex_list.len() != result_list.len() {
        panic!("Expect vertex count is {:?} while result vertex count is {:?}", result_list.len(), result_vertex_list.len());
    }
    for vertex in result_list.iter() {
        if let Some(result_vertex) = result_vertex_list.get(&vertex.get_id()) {
            if !check_vertex_equals(vertex, result_vertex) {
                panic!("get expect vertex {:?} fail", vertex.get_id());
            }
        } else {
            panic!("Cant find vertex {:?} in result list", vertex.get_id());
        }
    }
}

pub fn test_get_all_edges<V, VI, E, EI>(global_graph: Arc<dyn GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
                                        si: SnapshotId,
                                        labels: &Vec<LabelId>,
                                        partition_ids: &Vec<PartitionId>,
                                        result_list: Vec<E>)
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    let mut edge_list = global_graph.as_ref().get_all_edges(si,
                                                            labels,
                                                            None,
                                                            None,
                                                            None,
                                                            0,
                                                            partition_ids);
    let mut result_edge_list = HashMap::new();
    while let Some(e) = edge_list.next() {
        result_edge_list.insert(e.get_edge_id(), e);
    }
    if result_edge_list.len() != result_list.len() {
        panic!("Expect vertex count is {:?} while result vertex count is {:?}", result_list.len(), result_edge_list.len());
    }
    for e in result_list.iter() {
        if let Some(result_edge) = result_edge_list.get(&e.get_edge_id()) {
            if !check_edge_equals(e, result_edge) {
                panic!("get expect edge {:?} fail", e.get_edge_id());
            }
        } else {
            panic!("Cant find edge {:?} in result list", e.get_edge_id());
        }
    }
}

pub fn test_count_all_vertices<V, VI, E, EI>(global_graph: Arc<dyn GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
                                             si: SnapshotId,
                                             labels: &Vec<LabelId>,
                                             partition_ids: &Vec<PartitionId>,
                                             result_count: u64)
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    let count_value = global_graph.count_all_vertices(si, labels, None, partition_ids);
    assert_eq!(result_count, count_value, "expect vertex count {} not equals to result count {}", result_count, count_value);
}

pub fn test_count_all_edges<V, VI, E, EI>(global_graph: Arc<dyn GlobalGraphQuery<V=V, VI=VI, E=E, EI=EI>>,
                                             si: SnapshotId,
                                             labels: &Vec<LabelId>,
                                             partition_ids: &Vec<PartitionId>,
                                             result_count: u64)
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    let count_value = global_graph.count_all_edges(si, labels, None, partition_ids);
    assert_eq!(result_count, count_value, "expect edge count {} not equals to result count {}", result_count, count_value);
}

fn check_vertex_equals<V>(v1: &V, v2: &V) -> bool
    where V: Vertex {
    if v1.get_id() == v2.get_id() && v1.get_label_id() == v2.get_label_id() {
        let mut list1 = vec![];
        let mut pi1 = v1.get_properties();
        while let Some(prop) = pi1.next() {
            list1.push(prop);
        }

        let mut pi2 = v2.get_properties();
        let mut list2 = vec![];
        while let Some(prop) = pi2.next() {
            list2.push(prop);
        }

        if !check_list_equal(&list1, &list2) {
            panic!("Expect vertex {:?}:{:?} property list {:?} while find property list {:?}", v1.get_label_id(), v1.get_id(), &list1, &list2);
        }
        return true;
    } else {
        panic!("Expect vertex {:?}:{:?} != result vertex {:?}:{:?}", v1.get_label_id(), v1.get_id(), v2.get_label_id(), v2.get_id());
    }
}

fn check_edge_equals<E>(v1: &E, v2: &E) -> bool
    where E: Edge {
    if v1.get_edge_id() == v2.get_edge_id() &&
        v1.get_label_id() == v2.get_label_id() &&
        v1.get_src_label_id() == v2.get_src_label_id() &&
        v1.get_src_id() == v2.get_src_id() &&
        v1.get_dst_label_id() == v2.get_dst_label_id() &&
        v1.get_dst_id() == v2.get_dst_id() {
        let mut list1 = vec![];
        let mut pi1 = v1.get_properties();
        while let Some(prop) = pi1.next() {
            list1.push(prop);
        }

        let mut pi2 = v2.get_properties();
        let mut list2 = vec![];
        while let Some(prop) = pi2.next() {
            list2.push(prop);
        }

        if !check_list_equal(&list1, &list2) {
            panic!("Expect vertex {:?}:{:?} property list {:?} while find property list {:?}", v1.get_label_id(), v1.get_edge_id(), &list1, &list2);
        }
        return true;
    } else {
        panic!("Expect vertex {:?}:{:?} != result vertex {:?}:{:?}", v1.get_label_id(), v1.get_edge_id(), v2.get_label_id(), v2.get_edge_id());
    }
}
