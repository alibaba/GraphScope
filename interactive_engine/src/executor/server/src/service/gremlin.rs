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

use maxgraph_common::proto::gremlin_query_grpc::*;
use grpcio::*;
use maxgraph_common::proto::gremlin_query::*;
use std::sync::Arc;
use ::protobuf::RepeatedField;
use maxgraph_common::proto::gremlin_query::VertexId as PB_VertexId;
use ::futures::{Future, stream, Sink};
use ::grpcio;
use maxgraph_store::api::prelude::*;
use byteorder::{BigEndian, WriteBytesExt};
use std::cmp::{min, Ordering};
use std::time::Instant;
use maxgraph_store::schema::prelude::*;
use crate::StoreContext;
use maxgraph_common::util::partition::{assign_single_partition, assign_empty_partition, assign_all_partition, assign_vertex_label_partition};

pub struct GremlinRpcService<V, VI, E, EI> {
    store: Arc<StoreContext<V, VI, E, EI>>,
}

impl<V, VI, E, EI> GremlinRpcService<V, VI, E, EI> {
    pub fn new(store: Arc<StoreContext<V, VI, E, EI>>) -> Self {
        GremlinRpcService {
            store,
        }
    }
}

impl<V, VI, E, EI> Clone for GremlinRpcService<V, VI, E, EI> {
    fn clone(&self) -> Self {
        GremlinRpcService {
            store: self.store.clone(),
        }
    }
}

impl<V, VI, E, EI> GremlinService for GremlinRpcService<V, VI, E, EI>
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    fn get_edges(&mut self, ctx: RpcContext, req: EdgesRequest, sink: ServerStreamingSink<EdgesReponse>) {
        // get out edges
        _debug!("get_edges: {:?}", req);
        let src_ids = req.get_srcId();
        let labels = req.get_label();
        let mut responses = vec![];

        let graph = self.store.get_graph();
        let partition_manager = self.store.get_partition_manager();
        let partition_id_list = partition_manager.get_process_partition_list();
        let si = SnapshotId::max_value() - 1;
        let schema = graph.get_schema(si).unwrap();

        let mut partition_vertex_id_list = Vec::new();
        if req.get_d() {
            for id in src_ids.iter() {
                let partition_id = partition_manager.as_ref().get_partition_id(*id) as u32;
                assign_single_partition(*id, partition_id, &mut partition_vertex_id_list);
            }
        } else {
            assign_empty_partition(&partition_id_list, &mut partition_vertex_id_list);
            for id in src_ids.iter() {
                assign_all_partition(*id, &mut partition_vertex_id_list);
            }
        }

        for label in labels.iter() {
            if let Some(label_id) = schema.get_label_id(label) {
                let label_id_list = vec![label_id];
                let mut edges = {
                    if req.get_d() {
                        graph.get_out_edges(si, partition_vertex_id_list.clone(), &label_id_list, None, None, None, 0)
                    } else {
                        graph.get_in_edges(si, partition_vertex_id_list.clone(), &label_id_list, None, None, None, 0)
                    }
                };
                while let Some((id, mut ei)) = edges.next() {
                    let mut response = EdgesReponse::new();
                    response.set_srcid(id.clone());
                    response.set_label(label.clone());
                    let max_batch_size = 256;
                    let mut dsts = Vec::with_capacity(max_batch_size);
                    let mut pros = Vec::with_capacity(max_batch_size);
                    for _ in 0..max_batch_size {
                        if let Some(e) = ei.next() {
                            let mut v_id = PB_VertexId::new();
                            if req.get_d() {
                                v_id.set_id(e.get_dst_id() as i64);
                                v_id.set_typeId(e.get_dst_label_id() as i32);
                            } else {
                                v_id.set_id(e.get_src_id() as i64);
                                v_id.set_typeId(e.get_src_label_id() as i32);
                            }
                            dsts.push(v_id);
                            if !req.idOnly {
                                pros.push(properties_to_bytes(e.get_properties()));
                            }
                        } else {
                            break;
                        }
                    }
                    response.set_dstids(RepeatedField::from_vec(dsts));
                    response.set_pros(RepeatedField::from_vec(pros));
                    responses.push((response, WriteFlags::default()));
                }
            }
        }


        let f = sink.send_all(stream::iter_ok::<_, grpcio::Error>(responses))
            .map(|_| {})
            .map_err(|e| println!("failed to query: {:?}", e));
        ctx.spawn(f);
    }

    fn get_vertexs(&mut self, ctx: RpcContext, req: VertexRequest, sink: ServerStreamingSink<VertexResponse>) {
        _debug!("get_vertexs: {:?}", req);
        let now = Instant::now();

        let graph = self.store.get_graph();
        let partition_manager = self.store.get_partition_manager();
        let si = SnapshotId::max_value() - 1;
        let mut partition_label_vertex_list = Vec::new();
        let mut res = Vec::with_capacity(req.ids.len());
        for id in req.get_ids() {
            assign_vertex_label_partition(Some(id.get_typeId() as u32),
                                          id.get_id(),
                                          partition_manager.get_partition_id(id.get_id()) as u32,
                                          &mut partition_label_vertex_list);
        }
        let mut vertex_list = graph.get_vertex_properties(si, partition_label_vertex_list, None);
        while let Some(v) = vertex_list.next() {
            res.push(v);
        }

        if !req.get_orderKey().is_empty() {
            let p_name = req.get_orderKey();
            let p_id = self.store.get_graph().get_schema(SnapshotId::max_value() - 1).unwrap().get_prop_id(p_name).unwrap().clone();
            res.sort_by(|a, b| {
                let a_p = a.get_property(p_id);
                let b_p = b.get_property(p_id);
                let order = a_p.partial_cmp(&b_p).unwrap();
                if req.get_order() {
                    order
                } else {
                    order.reverse()
                }
            })
        }
        let mut res_len = res.len();
        if req.get_limit() > 0 {
            res_len = min(req.get_limit() as usize, res.len());
        }
        let mut response = Vec::with_capacity(res_len);

        let mut count = 0;
        for v in res {
            let mut v_id = PB_VertexId::new();
            v_id.set_id(v.get_id());
            v_id.set_typeId(v.get_label_id() as i32);
            let mut r = VertexResponse::new();
            r.set_id(v_id);
            r.set_pros(properties_to_bytes(v.get_properties()));
            response.push((r, WriteFlags::default()));
            count += 1;
            if count >= res_len {
                break;
            }
        }

        let f = sink.send_all(stream::iter_ok::<_, grpcio::Error>(response))
            .map(|_| {})
            .map_err(|e| println!("failed to query: {:?}", e));
        ctx.spawn(f);
        println!("get vertexs cost {:?}", now.elapsed());
    }

    fn get_limit_edges(&mut self, ctx: RpcContext, req: LimitEdgeRequest, sink: ServerStreamingSink<LimitEdgesReponse>) {
        _debug!("get_limit_edges: {:?}", req);
        let query_req = req.get_req();
        let src_ids = query_req.get_srcId();
        let labels = query_req.get_label();

        let graph = self.store.get_graph();
        let partition_manager = self.store.get_partition_manager();
        let partition_id_list = partition_manager.get_process_partition_list();
        let si = SnapshotId::max_value() - 1;
        let schema = graph.get_schema(si).unwrap();

        let mut edge_list = Vec::with_capacity(src_ids.len());
        let label_id_list = labels.iter()
            .map(|l| schema.get_label_id(l).unwrap())
            .collect();
        let mut partition_vertex_id_list = Vec::new();
        if query_req.get_d() {
            for id in src_ids.iter() {
                let partition_id = partition_manager.as_ref().get_partition_id(*id) as u32;
                assign_single_partition(*id, partition_id, &mut partition_vertex_id_list);
            }
        } else {
            assign_empty_partition(&partition_id_list, &mut partition_vertex_id_list);
            for id in src_ids.iter() {
                assign_all_partition(*id, &mut partition_vertex_id_list);
            }
        }
        let mut edges = {
            if query_req.get_d() {
                graph.get_out_edges(si, partition_vertex_id_list, &label_id_list, None, None, None, 0)
            } else {
                graph.get_in_edges(si, partition_vertex_id_list, &label_id_list, None, None, None, 0)
            }
        };
        while let Some((_, mut ei)) = edges.next() {
            while let Some(e) = ei.next() {
                edge_list.push(e);
            }
        }

        if req.get_orderKey().len() > 0 {
            edge_list.sort_by(|a, b| {
                for k in req.get_orderKey() {
                    let p_id = schema.get_prop_id(k.as_str()).unwrap().clone();
                    let a_p = a.get_property(p_id);
                    let b_p = b.get_property(p_id);
                    if let Some(ord) = a_p.partial_cmp(&b_p) {
                        match ord {
                            Ordering::Equal => {
                                continue;
                            }
                            _ => {
                                if req.get_order() {
                                    return ord;
                                } else {
                                    return ord.reverse();
                                }
                            }
                        }
                    } else {
                        println!("can't compare {:?} vs {:?}", &a_p, &b_p)
                    }
                }
                Ordering::Equal
            })
        }

        let mut len = edge_list.len();
        if req.get_limit() > 0 {
            len = min(req.get_limit() as usize, edge_list.len());
        }

        let edge_slice = &edge_list[0..len];
        let mut response = Vec::with_capacity(len);
        for e in edge_slice {
            let mut s_id = PB_VertexId::new();
            s_id.set_id(e.get_src_id());
            s_id.set_typeId(e.get_src_label_id() as i32);
            let mut d_id = PB_VertexId::new();
            d_id.set_id(e.get_dst_id());
            d_id.set_typeId(e.get_dst_label_id() as i32);

            let mut r = LimitEdgesReponse::new();
            r.set_source(s_id);
            r.set_destin(d_id);
            r.set_typeId(e.get_label_id() as i32);
            if !req.idOnly {
                r.set_pros(properties_to_bytes(e.get_properties()));
            }
            response.push((r, WriteFlags::default()));
        }

        let f = sink.send_all(stream::iter_ok::<_, grpcio::Error>(response))
            .map(|_| {})
            .map_err(|e| println!("failed to query: {:?}", e));
        ctx.spawn(f);
    }

    fn scan(&mut self, ctx: RpcContext, req: VertexScanRequest, sink: ServerStreamingSink<VertexResponse>) {
        _debug!("scan: {:?}", req);

        let graph = self.store.get_graph();
        let partition_manager = self.store.get_partition_manager();
        let partition_id_list = partition_manager.get_process_partition_list();
        let si = SnapshotId::max_value() - 1;

        let mut scanned = {
            if req.get_typeId() >= 0 {
                let label_list = vec![req.get_typeId() as u32];
                graph.get_all_vertices(si, &label_list, None, None, None, 0, &partition_id_list)
            } else {
                graph.get_all_vertices(si, &vec![], None, None, None, 0, &partition_id_list)
            }
        };

        let mut response;
        if req.get_orderKey().is_empty() && req.get_limit() > 0 {
            response = Vec::with_capacity(req.get_limit() as usize);
            let mut count = 0;
            loop {
                if count >= req.get_limit() {
                    break;
                }
                if let Some(v) = scanned.next() {
                    let mut v_id = PB_VertexId::new();
                    v_id.set_id(v.get_id());
                    v_id.set_typeId(v.get_label_id() as i32);
                    let mut r = VertexResponse::new();
                    r.set_id(v_id);
                    r.set_pros(properties_to_bytes(v.get_properties()));
                    response.push((r, WriteFlags::default()));
                } else {
                    break;
                }
                count += 1;
            }
        } else {
            let mut res = Vec::with_capacity(1024);
            for s in scanned {
                res.push(s);
            }
            _debug!("size: {}", res.len());
            if !req.get_orderKey().is_empty() {
                let p_name = req.get_orderKey();
                let p_id = self.store.get_graph().get_schema(SnapshotId::max_value() - 1).unwrap().get_prop_id(p_name).unwrap().clone();
                res.sort_by(|a, b| {
                    let a_p = a.get_property(p_id);
                    let b_p = b.get_property(p_id);
                    let order = a_p.partial_cmp(&b_p).unwrap();
                    if req.get_order() {
                        order
                    } else {
                        order.reverse()
                    }
                })
            }
            let mut res_len = res.len();
            if req.get_limit() > 0 {
                res_len = min(req.get_limit() as usize, res.len());
            }
            _debug!("yyyyy, {}", 1);
            response = Vec::with_capacity(res_len);
            for v in &res[0..res_len] {
                let mut v_id = PB_VertexId::new();
                v_id.set_id(v.get_id());
                v_id.set_typeId(v.get_label_id() as i32);
                let mut r = VertexResponse::new();
                r.set_id(v_id);
                r.set_pros(properties_to_bytes(v.get_properties()));
                response.push((r, WriteFlags::default()));
            }
            _debug!("kkkk, {}", 1);
        }

        let f = sink.send_all(stream::iter_ok::<_, grpcio::Error>(response))
            .map(|_| {})
            .map_err(|e| println!("failed to query: {:?}", e));
        ctx.spawn(f);
    }
}

fn properties_to_bytes<I: Iterator<Item=(PropId, Property)>>(iter: I) -> Vec<u8> {
    let mut data = vec![];
    let props: Vec<(PropId, Property)> = iter.collect();
    data.write_i32::<BigEndian>(props.len() as i32).unwrap();
    for (pid, p) in props {
        _info!("{:?} {:?}", pid, p);
        data.write_i32::<BigEndian>(pid as i32).unwrap();
        data.extend(p.to_bytes().iter());
    }
    data
}
