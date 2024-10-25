//
//! Copyright 2021 Alibaba Group Holding Limited.
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
//!
//!

mod common;

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use dyn_type::Object;
    use graph_proxy::apis::{register_graph, GraphElement};
    use graph_proxy::create_exp_store;
    use graph_store::ldbc::LDBCVertexParser;
    use graph_store::prelude::DefaultId;
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::physical as pb;
    use ir_common::KeyId;
    use pegasus::api::{Map, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;
    use runtime::process::entry::Entry;
    use runtime::process::operator::flatmap::FlatMapFuncGen;
    use runtime::process::operator::map::FilterMapFuncGen;
    use runtime::process::operator::source::SourceOperator;
    use runtime::process::record::Record;

    use crate::common::test::*;

    // g.V()
    fn source_gen(alias: Option<KeyId>) -> Box<dyn Iterator<Item = Record> + Send> {
        source_gen_with_scan_opr(pb::Scan {
            scan_opt: 0,
            alias,
            params: None,
            idx_predicate: None,
            is_count_only: false,
        })
    }

    fn source_gen_with_scan_opr(scan_opr_pb: pb::Scan) -> Box<dyn Iterator<Item = Record> + Send> {
        let graph = create_exp_store(Arc::new(TestCluster {}));
        register_graph(graph);
        let source = SourceOperator::new(scan_opr_pb.into(), Arc::new(TestRouter::default())).unwrap();
        source.gen_source(0).unwrap()
    }

    fn expand_test(expand: pb::EdgeExpand) -> ResultStream<Record> {
        let conf = JobConf::new("expand_test");
        let result = pegasus::run(conf, || {
            let expand = expand.clone();
            |input, output| {
                let mut stream = input.input_from(source_gen(Some(TAG_A)))?;
                let flatmap_func = expand.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");
        result
    }

    // g.V().out() with optional out
    // v1, v4, v6 have out-neighbors; while v2, v3, v5 do not
    #[test]
    fn optional_expand_outv_test() {
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: None,
            expand_opt: 0,
            alias: None,
            is_optional: true,
        };
        let mut result = expand_test(expand_opr_pb);
        let mut result_ids = vec![];
        let mut none_cnt = 0;
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let mut expected_ids = vec![v2, v3, v3, v3, v4, v5];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_vertex() {
                result_ids.push(element.id() as usize)
            } else if let Some(obj) = record.get(None).unwrap().as_object() {
                assert_eq!(obj, &Object::None);
                none_cnt += 1;
            }
        }
        result_ids.sort();
        expected_ids.sort();
        assert_eq!(result_ids, expected_ids);
        // v2, v3, v5 does not have out edges
        assert_eq!(none_cnt, 3);
    }

    // g.V().out('knows') with optional out
    // v1 has out knows neighbors of v2, v4; while other vertices do not have out knows neighbors
    #[test]
    fn optional_expand_outv_test_2() {
        let query_param = query_params(vec![KNOWS_LABEL.into()], vec![], None);
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_param),
            expand_opt: 0,
            alias: None,
            is_optional: true,
        };
        let mut result = expand_test(expand_opr_pb);
        let mut result_ids = vec![];
        let mut none_cnt = 0;
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let mut expected_ids = vec![v2, v4];
        while let Some(Ok(record)) = result.next() {
            println!("record: {:?}", record);
            if let Some(element) = record.get(None).unwrap().as_vertex() {
                result_ids.push(element.id() as usize)
            } else if let Some(obj) = record.get(None).unwrap().as_object() {
                assert_eq!(obj, &Object::None);
                none_cnt += 1;
            }
        }
        result_ids.sort();
        expected_ids.sort();
        assert_eq!(result_ids, expected_ids);
        assert_eq!(none_cnt, 5);
    }

    // g.V().outE('knows', 'created') with optional out
    #[test]
    fn optional_expand_oute_with_many_labels_test() {
        let query_param = query_params(vec![KNOWS_LABEL.into(), CREATED_LABEL.into()], vec![], None);
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_param),
            expand_opt: 1,
            alias: None,
            is_optional: true,
        };
        let mut result = expand_test(expand_opr_pb);
        let mut result_edges = vec![];
        let mut none_cnt = 0;
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let v6: DefaultId = LDBCVertexParser::to_global_id(6, 0);
        let mut expected_edges = vec![(v1, v2), (v1, v3), (v1, v4), (v4, v3), (v4, v5), (v6, v3)];
        expected_edges.sort();
        while let Some(Ok(record)) = result.next() {
            if let Some(e) = record.get(None).unwrap().as_edge() {
                result_edges.push((e.src_id as usize, e.dst_id as usize));
            } else if let Some(obj) = record.get(None).unwrap().as_object() {
                assert_eq!(obj, &Object::None);
                none_cnt += 1;
            }
        }
        result_edges.sort();
        assert_eq!(result_edges, expected_edges);
        assert_eq!(none_cnt, 3);
    }

    // g.V().out('knows').where(@ isnull) with optional out
    // in this case, for the vertices v2, v3, v4, v5, v6, that do not have out knows edges.
    #[test]
    fn optional_expand_outv_filter_test() {
        let query_param = query_params(vec![KNOWS_LABEL.into()], vec![], None);
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_param),
            expand_opt: 0,
            alias: None,
            is_optional: true,
        };
        let vertex_query_param = query_params(vec![], vec![], str_to_expr_pb("isnull @".to_string()).ok());
        let auxilia_opr_pb = pb::GetV { tag: None, opt: 4, params: Some(vertex_query_param), alias: None };

        let conf = JobConf::new("optional_expand_outv_filter_test");
        let mut result = pegasus::run(conf, || {
            let expand = expand_opr_pb.clone();
            let auxilia = auxilia_opr_pb.clone();
            |input, output| {
                let mut stream = input.input_from(source_gen(Some(TAG_A)))?;
                let flatmap_func = expand.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func.exec(input))?;
                let filter_map_func = auxilia.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| filter_map_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut result_ids: Vec<usize> = vec![];
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let v6: DefaultId = LDBCVertexParser::to_global_id(6, 0);
        let expected_ids = vec![v2, v3, v4, v5, v6];
        while let Some(Ok(record)) = result.next() {
            let vertex = record
                .get(Some(TAG_A))
                .unwrap()
                .as_vertex()
                .unwrap();
            result_ids.push(vertex.id() as usize);
        }
        assert_eq!(result_ids, expected_ids)
    }

    // g.V().outE('knows').inV() with optional outE
    // in this case, for the vertices, e.g., v2, v3, v4, v5, v6, that do not have out knows edges, the result of inV() would also be regarded as None.
    #[test]
    fn optional_expand_oute_inv_test() {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 1,
            alias: None,
            is_optional: true,
        };

        let getv_opr = pb::GetV {
            tag: None,
            opt: 1, // EndV
            params: Some(query_params(vec![], vec![], None)),
            alias: None,
        };

        let conf = JobConf::new("expand_oute_inv_test");
        let mut result = pegasus::run(conf, || {
            let expand = expand_opr.clone();
            let getv = getv_opr.clone();
            |input, output| {
                let mut stream = input.input_from(source_gen(None))?;
                let flatmap_func = expand.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func.exec(input))?;
                let filter_map_func = getv.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| filter_map_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let expected_ids = vec![2, 4];
        let mut result_ids = vec![];
        let mut none_cnt = 0;
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_vertex() {
                result_ids.push(element.id() as usize);
            } else if let Some(obj) = record.get(None).unwrap().as_object() {
                assert_eq!(obj, &Object::None);
                none_cnt += 1;
            } else {
                unreachable!()
            }
        }
        result_ids.sort();
        assert_eq!(result_ids, expected_ids);
        assert_eq!(none_cnt, 5);
    }

    // g.V().as(0).select(0).by(out().count().as(1)) with optional out
    // in this case, the vertices that do not have out edges would also be taken into account in the count()
    #[test]
    fn optional_expand_out_degree_test() {
        let conf = JobConf::new("expand_degree_fused_test");
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: None,
            expand_opt: 2,
            alias: Some(1.into()),
            is_optional: true,
        };
        let getv = pb::GetV { tag: None, opt: 4, params: None, alias: Some(TAG_A) };
        let project = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("@0".to_string()).ok(),
                alias: None,
            }],
            is_append: true,
        };

        let mut pegasus_result = pegasus::run(conf, || {
            let getv = getv.clone();
            let expand = expand_opr_pb.clone();
            let project = project.clone();
            |input, output| {
                let mut stream = input.input_from(source_gen(None))?;
                let filter_map_func = getv.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| filter_map_func.exec(input))?;
                let flat_map_func = expand.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flat_map_func.exec(input))?;
                let filter_map_func = project.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| filter_map_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut results = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let v6: DefaultId = LDBCVertexParser::to_global_id(6, 0);
        let mut expected_results = vec![(v1, 3), (v2, 0), (v3, 0), (v4, 2), (v5, 0), (v6, 1)];
        while let Some(Ok(record)) = pegasus_result.next() {
            if let Some(v) = record.get(None).unwrap().as_vertex() {
                if let Some(degree_obj) = record.get(Some(1)).unwrap().as_object() {
                    results.push((v.id() as DefaultId, degree_obj.as_u64().unwrap()));
                }
            }
        }
        results.sort();
        expected_results.sort();

        assert_eq!(results, expected_results)
    }
}
