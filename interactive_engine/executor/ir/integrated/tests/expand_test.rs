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

    use dyn_type::object;
    use graph_proxy::apis::GraphElement;
    use graph_proxy::create_exp_store;
    use graph_store::ldbc::LDBCVertexParser;
    use graph_store::prelude::DefaultId;
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::physical as pb;
    use ir_common::KeyId;
    use pegasus::api::{Map, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;
    use pegasus_common::downcast::AsAny;
    use runtime::process::entry::Entry;
    use runtime::process::operator::flatmap::FlatMapFuncGen;
    use runtime::process::operator::map::{FilterMapFuncGen, IntersectionEntry};
    use runtime::process::operator::source::SourceOperator;
    use runtime::process::record::Record;

    use crate::common::test::*;

    // g.V()
    fn source_gen(alias: Option<KeyId>) -> Box<dyn Iterator<Item = Record> + Send> {
        source_gen_with_scan_opr(pb::Scan { scan_opt: 0, alias, params: None, idx_predicate: None })
    }

    fn source_gen_with_scan_opr(scan_opr_pb: pb::Scan) -> Box<dyn Iterator<Item = Record> + Send> {
        create_exp_store();
        let source = SourceOperator::new(scan_opr_pb.into(), Arc::new(TestRouter::default())).unwrap();
        source.gen_source(0).unwrap()
    }

    fn expand_test(expand: pb::EdgeExpand) -> ResultStream<Record> {
        let conf = JobConf::new("expand_test");
        let result = pegasus::run(conf, || {
            let expand = expand.clone();
            |input, output| {
                let mut stream = input.input_from(source_gen(None))?;
                let flatmap_func = expand.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");
        result
    }

    fn expand_degree_opt_test(expand: pb::EdgeExpand) -> ResultStream<Record> {
        let conf = JobConf::new("expand_degree_fused_test");
        let getv = pb::GetV { tag: None, opt: 4, params: None, alias: Some(TAG_A) };
        let expand = expand.clone();
        let project = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("@0".to_string()).ok(),
                alias: None,
            }],
            is_append: true,
        };

        let result = pegasus::run(conf, || {
            let getv = getv.clone();
            let expand = expand.clone();
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

        result
    }

    fn expand_test_with_source_tag(source_tag: KeyId, expand: pb::EdgeExpand) -> ResultStream<Record> {
        let conf = JobConf::new("expand_test");
        let result = pegasus::run(conf, || {
            let source_tag = source_tag.clone();
            let expand = expand.clone();
            move |input, output| {
                let mut stream = input.input_from(source_gen(Some(source_tag)))?;
                let flatmap_func = expand.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");
        result
    }

    // g.V().out()
    #[test]
    fn expand_outv_test() {
        let expand_opr_pb =
            pb::EdgeExpand { v_tag: None, direction: 0, params: None, expand_opt: 0, alias: None };
        let mut result = expand_test(expand_opr_pb);
        let mut result_ids = vec![];
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let mut expected_ids = vec![v2, v3, v3, v3, v4, v5];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_vertex() {
                result_ids.push(element.id() as usize)
            }
        }
        result_ids.sort();
        expected_ids.sort();
        assert_eq!(result_ids, expected_ids)
    }

    // g.V().outE().hasLabel("knows")
    #[test]
    fn expand_oute_with_label_test() {
        let query_param = query_params(vec![KNOWS_LABEL.into()], vec![], None);
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_param),
            expand_opt: 1,
            alias: None,
        };
        let mut result = expand_test(expand_opr_pb);
        let mut result_edges = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let expected_edges = vec![(v1, v2), (v1, v4)];
        while let Some(Ok(record)) = result.next() {
            if let Some(e) = record.get(None).unwrap().as_edge() {
                result_edges.push((e.src_id as usize, e.dst_id as usize));
            }
        }
        result_edges.sort();
        assert_eq!(result_edges, expected_edges)
    }

    // g.V().outE('knows', 'created')
    #[test]
    fn expand_oute_with_many_labels_test() {
        let query_param = query_params(vec![KNOWS_LABEL.into(), CREATED_LABEL.into()], vec![], None);
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_param),
            expand_opt: 1,
            alias: None,
        };
        let mut result = expand_test(expand_opr_pb);
        let mut result_edges = vec![];
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
            }
        }
        result_edges.sort();
        assert_eq!(result_edges, expected_edges)
    }

    // g.V().inE('knows') with required properties
    #[test]
    fn expand_ine_with_label_property_test() {
        let query_param = query_params(vec![KNOWS_LABEL.into()], vec!["weight".into()], None);
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 1,
            params: Some(query_param),
            expand_opt: 1,
            alias: None,
        };
        let mut result = expand_test(expand_opr_pb);
        let mut result_ids_with_prop = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let expected_dst_ids_with_prop = vec![(v1, object!(0.5)), (v1, object!(1.0))];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_edge() {
                result_ids_with_prop.push((
                    element.get_other_id() as usize,
                    element
                        .get_property(&"weight".into())
                        .unwrap()
                        .try_to_owned()
                        .unwrap(),
                ))
            }
        }

        assert_eq!(result_ids_with_prop, expected_dst_ids_with_prop)
    }

    // g.V().both()
    #[test]
    fn expand_bothv_test() {
        let query_param = query_params(vec![], vec![], None);
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_param),
            expand_opt: 0,
            alias: None,
        };
        let mut result = expand_test(expand_opr_pb);
        let mut cnt = 0;
        let expected_result_num = 12;
        while let Some(_) = result.next() {
            cnt += 1;
        }
        assert_eq!(cnt, expected_result_num)
    }

    // g.V().as('a').out('knows').as('b')
    #[test]
    fn expand_outv_from_tag_as_tag_test() {
        let query_param = query_params(vec![KNOWS_LABEL.into()], vec![], None);
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 0,
            params: Some(query_param),
            expand_opt: 0,
            alias: Some(TAG_B.into()),
        };
        let mut result = expand_test_with_source_tag(TAG_A.into(), expand_opr_pb);
        let mut result_ids = vec![];
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let mut expected_ids = vec![v2, v4];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(Some(TAG_B)).unwrap().as_vertex() {
                result_ids.push(element.id() as usize)
            }
        }
        result_ids.sort();
        expected_ids.sort();
        assert_eq!(result_ids, expected_ids)
    }

    // g.V().as("a").select('a').out("knows")
    #[test]
    fn expand_outv_from_select_tag_test() {
        let query_param = query_params(vec![KNOWS_LABEL.into()], vec![], None);
        let project = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_var_pb(Some(TAG_A.into()), None)),
                alias: None,
            }],
            is_append: false,
        };
        let expand = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_param),
            expand_opt: 0,
            alias: None,
        };

        let conf = JobConf::new("expand_test");
        let mut result = pegasus::run(conf, || {
            let project = project.clone();
            let expand = expand.clone();
            |input, output| {
                let mut stream = input.input_from(source_gen(Some(TAG_A.into())))?;
                let filter_map_func = project.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| filter_map_func.exec(input))?;
                let flatmap_func = expand.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut result_ids = vec![];
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let mut expected_ids = vec![v2, v4];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_vertex() {
                result_ids.push(element.id() as usize)
            }
        }
        result_ids.sort();
        expected_ids.sort();
        assert_eq!(result_ids, expected_ids)
    }

    // g.V().out('knows').has('id',2)
    #[test]
    fn expand_outv_filter_test() {
        let edge_query_param = query_params(vec![KNOWS_LABEL.into()], vec![], None);
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(edge_query_param),
            expand_opt: 0,
            alias: None,
        };
        let vertex_query_param = query_params(vec![], vec![], str_to_expr_pb("@.id == 2".to_string()).ok());
        let auxilia_opr_pb = pb::GetV { tag: None, opt: 4, params: Some(vertex_query_param), alias: None };

        let conf = JobConf::new("expand_getv_test");
        let mut result = pegasus::run(conf, || {
            let expand = expand_opr_pb.clone();
            let auxilia = auxilia_opr_pb.clone();
            |input, output| {
                let mut stream = input.input_from(source_gen(None))?;
                let flatmap_func = expand.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func.exec(input))?;
                let filter_map_func = auxilia.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| filter_map_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut result_ids = vec![];
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let expected_ids = vec![v2];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_vertex() {
                result_ids.push(element.id() as usize)
            }
        }
        assert_eq!(result_ids, expected_ids)
    }

    // g.V().out('knows').has('id',2), this is an error case, since we cannot filter on vertices in an EdgeExpand opr.
    #[test]
    fn expand_outv_filter_error_test() {
        let query_param =
            query_params(vec![KNOWS_LABEL.into()], vec![], str_to_expr_pb("@.id == 2".to_string()).ok());
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_param),
            expand_opt: 0,
            alias: None,
        };
        let mut result = expand_test(expand_opr_pb);
        let mut result_ids = vec![];
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let expected_ids = vec![v2];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_vertex() {
                result_ids.push(element.id() as usize)
            }
        }
        assert_ne!(result_ids, expected_ids)
    }

    // g.V().outE('knows').inV()
    #[test]
    fn expand_oute_inv_test() {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 1,
            alias: None,
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
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None) {
                result_ids.push(element.id() as usize);
            }
        }
        result_ids.sort();
        assert_eq!(result_ids, expected_ids)
    }

    // g.V().inE('created').outV()
    #[test]
    fn expand_ine_outv_test() {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 1,
            params: Some(query_params(vec![CREATED_LABEL.into()], vec![], None)),
            expand_opt: 1,
            alias: None,
        };

        let getv_opr = pb::GetV {
            tag: None,
            opt: 0, // StartV
            params: Some(query_params(vec![], vec![], None)),
            alias: None,
        };

        let conf = JobConf::new("expand_ine_outv_test");
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

        let expected_ids = vec![1, 4, 4, 6];
        let mut result_ids = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None) {
                result_ids.push(element.id() as usize);
            }
        }
        result_ids.sort();
        assert_eq!(result_ids, expected_ids)
    }

    // g.V().bothE('knows').otherV()
    #[test]
    fn expand_bothe_otherv_test() {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 1,
            alias: None,
        };

        let getv_opr = pb::GetV {
            tag: None,
            opt: 2, // OtherV
            params: Some(query_params(vec![], vec![], None)),
            alias: None,
        };

        let conf = JobConf::new("expand_bothe_otherv_test");
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

        let expected_ids = vec![1, 1, 2, 4];
        let mut result_ids = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None) {
                result_ids.push(element.id() as usize);
            }
        }
        result_ids.sort();
        assert_eq!(result_ids, expected_ids)
    }

    // g.V().outE('knows').bothV()
    #[test]
    fn expand_oute_bothv_test() {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 1,
            alias: None,
        };

        let getv_opr = pb::GetV {
            tag: None,
            opt: 3, // BothV
            params: Some(query_params(vec![], vec![], None)),
            alias: None,
        };

        let conf = JobConf::new("expand_oute_bothv_test");
        let mut result = pegasus::run(conf, || {
            let expand = expand_opr.clone();
            let getv = getv_opr.clone();
            |input, output| {
                let mut stream = input.input_from(source_gen(None))?;
                let flatmap_func = expand.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func.exec(input))?;
                let flatmap_func = getv.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let expected_ids = vec![1, 1, 2, 4];
        let mut result_ids = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None) {
                result_ids.push(element.id() as usize);
            }
        }
        result_ids.sort();
        assert_eq!(result_ids, expected_ids)
    }

    // g.V().as(0).select(0).by(out().count().as(1))
    #[test]
    fn expand_out_degree_test() {
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: None,
            expand_opt: 2,
            alias: Some(1.into()),
        };
        let mut pegasus_result = expand_degree_opt_test(expand_opr_pb);
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

    // g.V().as(0).select(0).by(in().count().as(1))
    #[test]
    fn expand_in_degree_test() {
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 1,
            params: None,
            expand_opt: 2,
            alias: Some(1.into()),
        };
        let mut pegasus_result = expand_degree_opt_test(expand_opr_pb);
        let mut results = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let v6: DefaultId = LDBCVertexParser::to_global_id(6, 0);
        let mut expected_results = vec![(v1, 0), (v2, 1), (v3, 3), (v4, 1), (v5, 1), (v6, 0)];
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

    // g.V().as(0).select(0).by(both().count().as(1))
    #[test]
    fn expand_both_degree_test() {
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: None,
            expand_opt: 2,
            alias: Some(1.into()),
        };
        let mut pegasus_result = expand_degree_opt_test(expand_opr_pb);
        let mut results = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let v6: DefaultId = LDBCVertexParser::to_global_id(6, 0);
        let mut expected_results = vec![(v1, 3), (v2, 1), (v3, 3), (v4, 3), (v5, 1), (v6, 1)];
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

    // marko (A) -> lop (B); marko (A) -> josh (C); lop (B) <- josh (C)
    // test the expand phase of A -> C
    #[test]
    fn expand_and_intersection_expand_test() {
        // marko (A) -> lop (B);
        let expand_opr1 = pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 0, // out
            params: Some(query_params(vec![KNOWS_LABEL.into(), CREATED_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: Some(TAG_B.into()),
        };

        // marko (A) -> josh (C): expand C;
        let expand_opr2 = pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 0, // out
            params: Some(query_params(vec![KNOWS_LABEL.into(), CREATED_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: Some(TAG_C.into()),
        };

        let conf = JobConf::new("expand_and_intersection_expand_test");
        let mut result = pegasus::run(conf, || {
            let expand1 = expand_opr1.clone();
            let expand2 = expand_opr2.clone();
            |input, output| {
                // source vertex: marko
                let source_iter = source_gen_with_scan_opr(pb::Scan {
                    scan_opt: 0,
                    alias: Some(TAG_A.into()),
                    params: None,
                    idx_predicate: Some(vec![1].into()),
                });
                let mut stream = input.input_from(source_iter)?;
                let flatmap_func1 = expand1.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func1.exec(input))?;
                let map_func2 = expand2.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| map_func2.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let mut expected_collection = vec![v2, v3, v4];
        expected_collection.sort();
        let expected_collections =
            vec![expected_collection.clone(), expected_collection.clone(), expected_collection];
        let mut result_collections: Vec<Vec<usize>> = vec![];
        while let Some(Ok(record)) = result.next() {
            let intersection = record
                .get(Some(TAG_C))
                .unwrap()
                .as_any_ref()
                .downcast_ref::<IntersectionEntry>()
                .unwrap();
            let mut result_collection: Vec<usize> = intersection
                .clone()
                .iter()
                .map(|r| *r as usize)
                .collect();
            result_collection.sort();
            result_collections.push(result_collection);
        }
        assert_eq!(result_collections, expected_collections)
    }

    // marko (A) -> lop (B); marko (A) -> josh (C); lop (B) <- josh (C)
    // test the intersection phase of B <- C after expand A -> C
    #[test]
    fn expand_and_intersection_intersect_test() {
        // marko (A) -> lop (B);
        let expand_opr1 = pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 0, // out
            params: Some(query_params(vec![KNOWS_LABEL.into(), CREATED_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: Some(TAG_B.into()),
        };

        // marko (A) -> josh (C): expand C;
        let expand_opr2 = pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 0, // out
            params: Some(query_params(vec![KNOWS_LABEL.into(), CREATED_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: Some(TAG_C.into()),
        };

        // lop (B) <- josh (C): expand C and intersect on C;
        let expand_opr3 = pb::EdgeExpand {
            v_tag: Some(TAG_B.into()),
            direction: 1, // in
            params: Some(query_params(vec![KNOWS_LABEL.into(), CREATED_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: Some(TAG_C.into()),
        };

        let conf = JobConf::new("expand_and_intersection_intersect_test");
        let mut result = pegasus::run(conf, || {
            let expand1 = expand_opr1.clone();
            let expand2 = expand_opr2.clone();
            let expand3 = expand_opr3.clone();
            |input, output| {
                // source vertex: marko
                let source_iter = source_gen_with_scan_opr(pb::Scan {
                    scan_opt: 0,
                    alias: Some(TAG_A.into()),
                    params: None,
                    idx_predicate: Some(vec![1].into()),
                });
                let mut stream = input.input_from(source_iter)?;
                let flatmap_func1 = expand1.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func1.exec(input))?;
                let map_func2 = expand2.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| map_func2.exec(input))?;
                let map_func3 = expand3.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| map_func3.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let expected_collections = vec![vec![v4]];
        let mut result_collections = vec![];
        while let Some(Ok(record)) = result.next() {
            let intersection = record
                .get(Some(TAG_C))
                .unwrap()
                .as_any_ref()
                .downcast_ref::<IntersectionEntry>()
                .unwrap();

            let mut result_collection: Vec<DefaultId> = intersection
                .clone()
                .iter()
                .map(|r| *r as DefaultId)
                .collect();
            result_collection.sort();
            result_collections.push(result_collection);
        }
        assert_eq!(result_collections, expected_collections)
    }

    // marko (A) -> lop (B); marko (A) -> josh (C); lop (B) <- josh (C)
    #[test]
    fn expand_and_intersection_unfold_test() {
        // marko (A) -> lop (B);
        let expand_opr1 = pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 0, // out
            params: Some(query_params(vec![KNOWS_LABEL.into(), CREATED_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: Some(TAG_B.into()),
        };

        // marko (A) -> josh (C): expand C;
        let expand_opr2 = pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 0, // out
            params: Some(query_params(vec![KNOWS_LABEL.into(), CREATED_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: Some(TAG_C.into()),
        };

        // lop (B) <- josh (C): expand C and intersect on C;
        let expand_opr3 = pb::EdgeExpand {
            v_tag: Some(TAG_B.into()),
            direction: 1, // in
            params: Some(query_params(vec![KNOWS_LABEL.into(), CREATED_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: Some(TAG_C.into()),
        };

        // unfold tag C
        let unfold_opr = pb::Unfold { tag: Some(TAG_C.into()), alias: Some(TAG_C.into()) };

        let conf = JobConf::new("expand_and_intersection_unfold_test");
        let mut result = pegasus::run(conf, || {
            let expand1 = expand_opr1.clone();
            let expand2 = expand_opr2.clone();
            let expand3 = expand_opr3.clone();
            let unfold = unfold_opr.clone();
            |input, output| {
                // source vertex: marko
                let source_iter = source_gen_with_scan_opr(pb::Scan {
                    scan_opt: 0,
                    alias: Some(TAG_A.into()),
                    params: None,
                    idx_predicate: Some(vec![1].into()),
                });
                let mut stream = input.input_from(source_iter)?;
                let flatmap_func1 = expand1.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func1.exec(input))?;
                let map_func2 = expand2.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| map_func2.exec(input))?;
                let map_func3 = expand3.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| map_func3.exec(input))?;
                let unfold_func = unfold.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| unfold_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let expected_ids = vec![v4];
        let mut result_ids = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None) {
                result_ids.push(element.id() as usize);
            }
        }
        assert_eq!(result_ids, expected_ids)
    }

    // A <-> B; A <-> C; B <-> C
    #[test]
    fn expand_and_intersection_unfold_test_02() {
        // A <-> B;
        let expand_opr1 = pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 2, // both
            params: Some(query_params(vec![KNOWS_LABEL.into(), CREATED_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: Some(TAG_B.into()),
        };

        // A <-> C: expand C;
        let expand_opr2 = pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 2, // both
            params: Some(query_params(vec![KNOWS_LABEL.into(), CREATED_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: Some(TAG_C.into()),
        };

        // B <-> C: expand C and intersect on C;
        let expand_opr3 = pb::EdgeExpand {
            v_tag: Some(TAG_B.into()),
            direction: 2, // both
            params: Some(query_params(vec![KNOWS_LABEL.into(), CREATED_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: Some(TAG_C.into()),
        };

        // unfold tag C
        let unfold_opr = pb::Unfold { tag: Some(TAG_C.into()), alias: Some(TAG_C.into()) };

        let conf = JobConf::new("expand_and_intersection_unfold_multiv_test");
        let mut result = pegasus::run(conf, || {
            let expand1 = expand_opr1.clone();
            let expand2 = expand_opr2.clone();
            let expand3 = expand_opr3.clone();
            let unfold = unfold_opr.clone();
            |input, output| {
                let source_iter = source_gen(Some(TAG_A.into()));
                let mut stream = input.input_from(source_iter)?;
                let flatmap_func1 = expand1.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func1.exec(input))?;
                let map_func2 = expand2.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| map_func2.exec(input))?;
                let map_func3 = expand3.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| map_func3.exec(input))?;
                let unfold_func = unfold.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| unfold_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let mut expected_ids = vec![v1, v1, v3, v3, v4, v4];
        let mut result_ids = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None) {
                result_ids.push(element.id() as usize);
            }
        }
        result_ids.sort();
        expected_ids.sort();
        assert_eq!(result_ids, expected_ids)
    }

    // A <-> B; A <-> C; B <-> C; with filters of 'weight > 0.5' on edge A<->C
    #[test]
    fn expand_and_intersection_unfold_test_03() {
        // A <-> B;
        let expand_opr1 = pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 2, // both
            params: Some(query_params(vec![KNOWS_LABEL.into(), CREATED_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: Some(TAG_B.into()),
        };

        // A <-> C: expand C;
        let expand_opr2 = pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 2, // both
            params: Some(query_params(
                vec![KNOWS_LABEL.into(), CREATED_LABEL.into()],
                vec![],
                str_to_expr_pb("@.weight > 0.5".to_string()).ok(),
            )),
            expand_opt: 0,
            alias: Some(TAG_C.into()),
        };

        // B <-> C: expand C and intersect on C;
        let expand_opr3 = pb::EdgeExpand {
            v_tag: Some(TAG_B.into()),
            direction: 2, // both
            params: Some(query_params(vec![KNOWS_LABEL.into(), CREATED_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: Some(TAG_C.into()),
        };

        // unfold tag C
        let unfold_opr = pb::Unfold { tag: Some(TAG_C.into()), alias: Some(TAG_C.into()) };

        let conf = JobConf::new("expand_filter_and_intersection_unfold_test");
        let mut result = pegasus::run(conf, || {
            let expand1 = expand_opr1.clone();
            let expand2 = expand_opr2.clone();
            let expand3 = expand_opr3.clone();
            let unfold = unfold_opr.clone();
            |input, output| {
                // source vertex: marko
                let source_iter = source_gen_with_scan_opr(pb::Scan {
                    scan_opt: 0,
                    alias: Some(TAG_A.into()),
                    params: None,
                    idx_predicate: Some(vec![1].into()),
                });
                let mut stream = input.input_from(source_iter)?;
                let flatmap_func1 = expand1.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func1.exec(input))?;
                let map_func2 = expand2.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| map_func2.exec(input))?;
                let map_func3 = expand3.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| map_func3.exec(input))?;
                let unfold_func = unfold.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| unfold_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let mut expected_ids = vec![v4];
        let mut result_ids = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None) {
                result_ids.push(element.id() as usize);
            }
        }
        result_ids.sort();
        expected_ids.sort();
        assert_eq!(result_ids, expected_ids)
    }

    // g.V().outE().inV().hasLabel('person')
    #[test]
    fn expand_ine_outv_label_filter_test() {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // OutE
            params: Some(query_params(vec![], vec![], None)),
            expand_opt: 1,
            alias: None,
        };

        let getv_opr = pb::GetV {
            tag: None,
            opt: 1, // EndV
            params: Some(query_params(vec![PERSON_LABEL.into()], vec![], None)),
            alias: None,
        };

        let conf = JobConf::new("expand_ine_outv_haslabel_test");
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
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None) {
                result_ids.push(element.id() as usize);
            }
        }
        result_ids.sort();
        assert_eq!(result_ids, expected_ids)
    }
}
