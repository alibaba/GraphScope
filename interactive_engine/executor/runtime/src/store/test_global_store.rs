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

use store::remote_store_service::RemoteStoreServiceManager;
use dataflow::test::{MockGraph, build_modern_mock_graph, MockGraphPartition};
use store::global_store::GlobalStore;
use std::sync::Arc;
use maxgraph_store::test::global_graph_test_fn::{test_get_out_vertex_ids, test_get_in_vertex_ids, test_get_out_edges, test_get_in_edges, test_get_all_vertices, test_get_all_edges, test_get_vertex_properties, test_count_out_edges, test_count_in_edges, test_count_all_vertices, test_count_all_edges};
use maxgraph_store::api::{MVGraphQuery, Edge, Vertex, GlobalGraphQuery};
use itertools::Itertools;
use store::{LocalStoreEdge, LocalStoreVertex};
use std::collections::HashMap;


#[test]
fn test_global_store_local() {
    let remote_service_manager = Arc::new(RemoteStoreServiceManager::empty());
    let graph = Arc::new(build_modern_mock_graph());
    let global_store = Arc::new(GlobalStore::new(remote_service_manager,
                                                 graph.clone(),
                                                 true));

    // test out/in vids
    test_global_store_local_out_vids(graph.clone(), global_store.clone());
    test_global_store_local_in_vids(graph.clone(), global_store.clone());

    // test out/in edges
    test_global_store_local_out_edges(graph.clone(), global_store.clone());
    test_global_store_local_in_edges(graph.clone(), global_store.clone());

    // test scan vertices/edges
    test_global_store_local_scan_vertexes(graph.clone(), global_store.clone());
    test_global_store_local_scan_edges(graph.clone(), global_store.clone());

    // test get vertex
    test_global_store_local_get_vertex(graph.clone(), global_store.clone());

    // test count
    test_global_store_count_out_edges(graph.clone(),global_store.clone());
    test_global_store_count_in_edges(graph.clone(), global_store.clone());
    test_global_store_count_all_vertices(graph.clone(), global_store.clone());
    test_global_store_count_all_edges(graph.clone(),global_store.clone());
}

fn test_global_store_local_out_vids<V, VI, E, EI>(graph: Arc<MockGraph>,
                                                  global_store: Arc<GlobalStore<V, VI, E, EI>>)
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> {
    let edge_label_all = vec![];
    let p1_out_list = graph.get_out_edges(0, 1, None).collect_vec();
    let p1_out_vid_list = p1_out_list.iter().map(|e| (e.get_dst_label_id(), e.get_dst_id())).collect_vec();
    println!("p1 all out vid list {:?}", &p1_out_vid_list);
    test_get_out_vertex_ids(global_store.clone(), 0, vec![(1, vec![1])], &edge_label_all, vec![(1, p1_out_vid_list)]);

    let edge_label_knows = vec![3];
    let p1_knows_out_list = graph.get_out_edges(0, 1, Some(3)).collect_vec();
    let p1_knows_out_vid_list = p1_knows_out_list.iter().map(|e| (e.get_dst_label_id(), e.get_dst_id())).collect_vec();
    println!("p1 knows out vid list {:?}", &p1_knows_out_vid_list);
    test_get_out_vertex_ids(global_store.clone(), 0, vec![(1, vec![1]), (0, vec![1])], &edge_label_knows, vec![(1, p1_knows_out_vid_list)]);
}

fn test_global_store_local_in_vids<V, VI, E, EI>(graph: Arc<MockGraph>,
                                                 global_store: Arc<GlobalStore<V, VI, E, EI>>)
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> {
    let edge_label_all = vec![];
    let s3_in_list = graph.get_in_edges(0, 3, None).collect_vec();
    let s3_in_vid_list = s3_in_list.iter().map(|e| (e.get_src_label_id(), e.get_src_id())).collect_vec();
    println!("s3 all in vid list {:?}", &s3_in_vid_list);
    test_get_in_vertex_ids(global_store.clone(), 0, vec![(1, vec![3]), (0, vec![3])], &edge_label_all, vec![(3, s3_in_vid_list)]);

    let edge_label_created = vec![4];
    let s3_created_in_list = graph.get_in_edges(0, 3, Some(4)).collect_vec();
    let s3_created_in_vid_list = s3_created_in_list.iter().map(|e| (e.get_src_label_id(), e.get_src_id())).collect_vec();
    println!("s3 created in vid list {:?}", &s3_created_in_vid_list);
    test_get_in_vertex_ids(global_store.clone(), 0, vec![(1, vec![3]), (0, vec![3])], &edge_label_created, vec![(3, s3_created_in_vid_list)]);
}

fn test_global_store_local_out_edges<V, VI, E, EI>(graph: Arc<MockGraph>,
                                                   global_store: Arc<GlobalStore<V, VI, E, EI>>)
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> {
    let edge_label_all = vec![];
    let p1_out_list = graph.get_out_edges(0, 1, None).collect_vec();
    println!("p1 all out edge list {:?}", &p1_out_list);
    test_get_out_edges(global_store.clone(),
                       0,
                       vec![(1, vec![1])],
                       &edge_label_all,
                       None,
                       vec![(1, p1_out_list.clone())]);

    let prop_empty = vec![];
    let p1_out_prop_empty_list = p1_out_list.iter()
        .map(|e| LocalStoreEdge::new(e.src.clone(), e.dst.clone(), e.get_label_id(), e.get_edge_id()))
        .collect_vec();
    test_get_out_edges(global_store.clone(),
                       0,
                       vec![(1, vec![1])],
                       &edge_label_all,
                       Some(&prop_empty),
                       vec![(1, p1_out_prop_empty_list)]);

    let edge_label_knows = vec![3];
    let p1_knows_out_list = graph.get_out_edges(0, 1, Some(3)).collect_vec();
    println!("p1 knows out edge list {:?}", &p1_knows_out_list);
    test_get_out_edges(global_store.clone(),
                       0,
                       vec![(1, vec![1])],
                       &edge_label_knows,
                       None,
                       vec![(1, p1_knows_out_list.clone())]);

    let p1_knows_out_prop_empty_list = p1_knows_out_list.iter()
        .map(|e| LocalStoreEdge::new(e.src.clone(), e.dst.clone(), e.get_label_id(), e.get_edge_id()))
        .collect_vec();
    test_get_out_edges(global_store.clone(),
                       0,
                       vec![(1, vec![1])],
                       &edge_label_knows,
                       Some(&prop_empty),
                       vec![(1, p1_knows_out_prop_empty_list)]);
}

fn test_global_store_local_in_edges<V, VI, E, EI>(graph: Arc<MockGraph>,
                                                  global_store: Arc<GlobalStore<V, VI, E, EI>>)
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> {
    let edge_label_all = vec![];
    let s3_in_list = graph.get_in_edges(0, 3, None).collect_vec();
    println!("s3 all in edge list {:?}", &s3_in_list);
    test_get_in_edges(global_store.clone(),
                      0,
                      vec![(1, vec![3]), (0, vec![3])],
                      &edge_label_all,
                      None,
                      vec![(3, s3_in_list.clone())]);

    let prop_empty = vec![];
    let s3_in_prop_empty_list = s3_in_list.iter()
        .map(|e| LocalStoreEdge::new(e.src.clone(), e.dst.clone(), e.get_label_id(), e.get_edge_id()))
        .collect_vec();
    test_get_in_edges(global_store.clone(),
                      0,
                      vec![(1, vec![3]), (0, vec![3])],
                      &edge_label_all,
                      Some(&prop_empty),
                      vec![(3, s3_in_prop_empty_list)]);

    let edge_label_created = vec![4];
    let s3_created_in_list = graph.get_in_edges(0, 3, Some(4)).collect_vec();
    println!("s3 created in edge list {:?}", &s3_created_in_list);
    test_get_in_edges(global_store.clone(),
                      0,
                      vec![(1, vec![3]), (0, vec![3])],
                      &edge_label_created,
                      None,
                      vec![(3, s3_created_in_list.clone())]);

    let s3_created_in_prop_empty_list = s3_created_in_list.iter()
        .map(|e| LocalStoreEdge::new(e.src.clone(), e.dst.clone(), e.get_label_id(), e.get_edge_id()))
        .collect_vec();
    test_get_in_edges(global_store.clone(),
                      0,
                      vec![(1, vec![3]), (0, vec![3])],
                      &edge_label_created,
                      Some(&prop_empty),
                      vec![(3, s3_created_in_prop_empty_list)]);
}

fn test_global_store_local_scan_vertexes<V, VI, E, EI>(graph: Arc<MockGraph>,
                                                       global_store: Arc<GlobalStore<V, VI, E, EI>>)
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> {
    let edge_label_all = vec![];
    let partition_all = vec![0, 1];
    let all_vertex_list = graph.scan(0, None).collect_vec();
    println!("all vertex list {:?}", &all_vertex_list);
    test_get_all_vertices(global_store.clone(),
                          0,
                          &edge_label_all,
                          &partition_all,
                          all_vertex_list.clone());

    let edge_label_person = vec![1];
    let all_person_vertex_list = graph.scan(0, Some(1)).collect_vec();
    println!("all person vertex list {:?}", &all_person_vertex_list);
    test_get_all_vertices(global_store.clone(),
                          0,
                          &edge_label_person,
                          &partition_all,
                          all_person_vertex_list.clone());
}

fn test_global_store_local_scan_edges<V, VI, E, EI>(graph: Arc<MockGraph>,
                                                    global_store: Arc<GlobalStore<V, VI, E, EI>>)
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> {
    let edge_label_all = vec![];
    let partition_all = vec![0, 1];
    let all_edge_list = graph.scan_edges(0, None).collect_vec();
    println!("all edge list {:?}", &all_edge_list);
    test_get_all_edges(global_store.clone(),
                       0,
                       &edge_label_all,
                       &partition_all,
                       all_edge_list.clone());

    let edge_label_knows = vec![3];
    let all_knows_edges_list = graph.scan_edges(0, Some(3)).collect_vec();
    println!("all knows edge list {:?}", &all_knows_edges_list);
    test_get_all_edges(global_store.clone(),
                       0,
                       &edge_label_knows,
                       &partition_all,
                       all_knows_edges_list.clone());
}

fn test_global_store_local_get_vertex<V, VI, E, EI>(graph: Arc<MockGraph>,
                                                    global_store: Arc<GlobalStore<V, VI, E, EI>>)
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> {
    let mut result_list = HashMap::new();
    let p1 = graph.get_vertex(0, 1, None).unwrap();
    let p4 = graph.get_vertex(0, 4, None).unwrap();
    result_list.insert(p1.get_id(), p1.get_properties().collect_vec());
    result_list.insert(p4.get_id(), p4.get_properties().collect_vec());
    test_get_vertex_properties(global_store.clone(),
                               0,
                               vec![(1, vec![(None, vec![1])]), (0, vec![(None, vec![4])])],
                               None,
                               result_list);

    let output_prop_ids = vec![1, 2];
    let mut p1_prop = LocalStoreVertex::new(p1.get_id(), p1.get_label_id());
    p1_prop.add_property(1, p1.get_property(1).unwrap());
    p1_prop.add_property(2, p1.get_property(2).unwrap());
    let mut p4_prop = LocalStoreVertex::new(p4.get_id(), p4.get_label_id());
    p4_prop.add_property(1, p4.get_property(1).unwrap());
    p4_prop.add_property(2, p4.get_property(2).unwrap());
    let mut result_prop_list = HashMap::new();
    result_prop_list.insert(p1_prop.get_id(), p1_prop.get_properties().collect_vec());
    result_prop_list.insert(p4_prop.get_id(), p4_prop.get_properties().collect_vec());
    test_get_vertex_properties(global_store.clone(),
                               0,
                               vec![(1, vec![(None, vec![1])]), (0, vec![(None, vec![4])])],
                               Some(&output_prop_ids),
                               result_prop_list);
}

fn test_global_store_count_out_edges<V, VI, E, EI>(graph: Arc<MockGraph>,
                                                    global_store: Arc<GlobalStore<V, VI, E, EI>>)
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> {
    let edge_label_all = vec![];
    let p1_out_count = graph.count_out_edges(0, 1, None);
    println!("p1 all out count {:?}", &p1_out_count);
    test_count_out_edges(global_store.clone(), 0, vec![(1, vec![1])], &edge_label_all, vec![(1, p1_out_count)]);

    let edge_label_knows = vec![3];
    let p1_knows_out_count = graph.count_out_edges(0, 1, Some(3));
    println!("p1 knows out count {:?}", &p1_knows_out_count);
    test_count_out_edges(global_store.clone(), 0, vec![(1, vec![1]), (0, vec![1])], &edge_label_knows, vec![(1, p1_knows_out_count)]);
}

fn test_global_store_count_in_edges<V, VI, E, EI>(graph: Arc<MockGraph>,
                                                   global_store: Arc<GlobalStore<V, VI, E, EI>>)
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> {
    let edge_label_all = vec![];
    let s3_in_count = graph.count_in_edges(0, 3, None);
    println!("s3 all in count {:?}", &s3_in_count);
    test_count_in_edges(global_store.clone(), 0, vec![(1, vec![3]), (0, vec![3])], &edge_label_all, vec![(3, s3_in_count)]);

    let edge_label_knows = vec![4];
    let s3_created_in_count = graph.count_in_edges(0, 3, Some(4));
    println!("s3 created in count {:?}", &s3_created_in_count);
    test_count_in_edges(global_store.clone(), 0, vec![(1, vec![3]), (0, vec![3])], &edge_label_knows, vec![(3, s3_created_in_count)]);
}

fn test_global_store_count_all_vertices<V, VI, E, EI>(graph: Arc<MockGraph>,
                                                  global_store: Arc<GlobalStore<V, VI, E, EI>>)
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> {
    let edge_label_all = vec![];
    let partition_all = vec![0, 1];
    let all_vertex_count = graph.vertex_count();
    println!("all vertex count {:?}", &all_vertex_count);
    test_count_all_vertices(global_store.clone(),
                          0,
                          &edge_label_all,
                          &partition_all,
                            all_vertex_count.clone());

    let vertex_label_person = vec![1];
    let all_person_vertex_count = graph.estimate_vertex_count(Some(1));
    println!("all person vertex count {:?}", &all_person_vertex_count);
    test_count_all_vertices(global_store.clone(),
                          0,
                          &vertex_label_person,
                          &partition_all,
                          all_person_vertex_count.clone());
}

fn test_global_store_count_all_edges<V, VI, E, EI>(graph: Arc<MockGraph>,
                                                  global_store: Arc<GlobalStore<V, VI, E, EI>>)
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> {
    let edge_label_all = vec![];
    let partition_all = vec![0, 1];
    let all_edge_count = graph.edge_count();
    println!("all edge count {:?}", &all_edge_count);
    test_count_all_edges(global_store.clone(),
                            0,
                            &edge_label_all,
                            &partition_all,
                         all_edge_count.clone());

    let edge_label_knows = vec![3];
    let all_knows_edge_count = graph.estimate_edge_count(Some(3));
    println!("all knows edge count {:?}", &all_knows_edge_count);
    test_count_all_edges(global_store.clone(),
                            0,
                            &edge_label_knows,
                            &partition_all,
                            all_knows_edge_count.clone());
}

#[test]
fn test_global_store_remote() {}
