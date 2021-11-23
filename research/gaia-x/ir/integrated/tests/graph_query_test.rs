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
    use graph_proxy::{create_demo_graph, SimplePartition};
    use graph_store::ldbc::LDBCVertexParser;
    use graph_store::prelude::DefaultId;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_common::NameOrId;
    use pegasus::api::{Map, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;
    use runtime::expr::str_to_expr_pb;
    use runtime::graph::element::{Element, GraphElement, VertexOrEdge};
    use runtime::graph::property::Details;
    use runtime::process::operator::flatmap::FlatMapFuncGen;
    use runtime::process::operator::source::SourceOperator;
    use runtime::process::record::Record;
    use std::sync::Arc;

    // g.V()
    fn source_gen(alias: Option<common_pb::NameOrId>) -> Box<dyn Iterator<Item = Record> + Send> {
        create_demo_graph();
        let scan_opr_pb = pb::Scan {
            scan_opt: 0,
            alias,
            params: None,
        };
        let mut source_opr_pb = pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Scan(scan_opr_pb)),
        };
        let source = SourceOperator::new(
            &mut source_opr_pb,
            1,
            1,
            Arc::new(SimplePartition { num_servers: 1 }),
        )
        .unwrap();
        source.gen_source(0).unwrap()
    }

    // g.V()
    #[test]
    fn scan_test() {
        let source_iter = source_gen(None);
        let mut result_ids = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let v6: DefaultId = LDBCVertexParser::to_global_id(6, 0);
        let mut expected_ids = vec![v1, v2, v3, v4, v5, v6];
        for record in source_iter {
            if let Some(element) = record.get(None).unwrap().as_graph_element() {
                result_ids.push(element.id() as usize)
            }
        }
        result_ids.sort();
        expected_ids.sort();
        assert_eq!(result_ids, expected_ids)
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

    fn expand_test_with_source_tag(
        source_tag: common_pb::NameOrId,
        expand: pb::EdgeExpand,
    ) -> ResultStream<Record> {
        let conf = JobConf::new("expand_test");
        let result = pegasus::run(conf, || {
            let source_tag = source_tag.clone();
            let expand = expand.clone();
            |input, output| {
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
    fn expand_test_01() {
        let edge_expand_base = pb::ExpandBase {
            v_tag: None,
            direction: 0,
            params: None,
        };
        let expand_opr_pb = pb::EdgeExpand {
            base: Some(edge_expand_base),
            is_edge: false,
            alias: None,
        };
        let mut result = expand_test(expand_opr_pb);
        let mut result_ids = vec![];
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let mut expected_ids = vec![v2, v3, v3, v3, v4, v5];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_element() {
                result_ids.push(element.id() as usize)
            }
        }
        result_ids.sort();
        expected_ids.sort();
        assert_eq!(result_ids, expected_ids)
    }

    // g.V().outE().hasLabel("knows")
    #[test]
    fn expand_test_02() {
        let query_param = pb::QueryParams {
            table_names: vec![common_pb::NameOrId::from("knows".to_string())],
            columns: vec![],
            limit: None,
            predicate: None,
            requirements: vec![],
        };
        let edge_expand_base = pb::ExpandBase {
            v_tag: None,
            direction: 0,
            params: Some(query_param),
        };
        let expand_opr_pb = pb::EdgeExpand {
            base: Some(edge_expand_base),
            is_edge: true,
            alias: None,
        };
        let mut result = expand_test(expand_opr_pb);
        let mut result_edges = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let expected_edges = vec![(v1, v4), (v1, v2)];
        while let Some(Ok(record)) = result.next() {
            if let Some(VertexOrEdge::E(e)) = record.get(None).unwrap().as_graph_element() {
                result_edges.push((e.src_id as usize, e.dst_id as usize));
            }
        }
        assert_eq!(result_edges, expected_edges)
    }

    // g.V().in('knows') with required properties
    #[test]
    fn expand_test_03() {
        let query_param = pb::QueryParams {
            table_names: vec![common_pb::NameOrId::from("knows".to_string())],
            columns: vec![common_pb::NameOrId::from("name".to_string())],
            limit: None,
            predicate: None,
            requirements: vec![],
        };
        let edge_expand_base = pb::ExpandBase {
            v_tag: None,
            direction: 1,
            params: Some(query_param),
        };
        let expand_opr_pb = pb::EdgeExpand {
            base: Some(edge_expand_base),
            is_edge: false,
            alias: None,
        };
        let mut result = expand_test(expand_opr_pb);
        let mut result_ids_with_prop = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let expected_ids_with_prop = vec![
            (v1, "marko".to_string().into()),
            (v1, "marko".to_string().into()),
        ];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_element() {
                result_ids_with_prop.push((
                    element.id() as usize,
                    element
                        .details()
                        .unwrap()
                        .get_property(&NameOrId::Str("name".to_string()))
                        .unwrap()
                        .try_to_owned()
                        .unwrap(),
                ))
            }
        }

        assert_eq!(result_ids_with_prop, expected_ids_with_prop)
    }

    // g.V().both()
    #[test]
    fn expand_test_04() {
        let query_param = pb::QueryParams {
            table_names: vec![],
            columns: vec![],
            limit: None,
            predicate: None,
            requirements: vec![],
        };
        let edge_expand_base = pb::ExpandBase {
            v_tag: None,
            direction: 2,
            params: Some(query_param),
        };
        let expand_opr_pb = pb::EdgeExpand {
            base: Some(edge_expand_base),
            is_edge: false,
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
    fn expand_test_05() {
        let query_param = pb::QueryParams {
            table_names: vec![common_pb::NameOrId::from("knows".to_string())],
            columns: vec![],
            limit: None,
            predicate: None,
            requirements: vec![],
        };
        let edge_expand_base = pb::ExpandBase {
            v_tag: Some(common_pb::NameOrId::from("a".to_string())),
            direction: 0,
            params: Some(query_param),
        };
        let expand_opr_pb = pb::EdgeExpand {
            base: Some(edge_expand_base),
            is_edge: false,
            alias: Some(common_pb::NameOrId::from("b".to_string())),
        };
        let mut result =
            expand_test_with_source_tag(common_pb::NameOrId::from("a".to_string()), expand_opr_pb);
        let mut result_ids = vec![];
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let mut expected_ids = vec![v2, v4];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record
                .get(Some(&NameOrId::Str("b".to_string())))
                .unwrap()
                .as_graph_element()
            {
                result_ids.push(element.id() as usize)
            }
        }
        result_ids.sort();
        expected_ids.sort();
        assert_eq!(result_ids, expected_ids)
    }

    // g.V().out('knows').has('id',2)
    #[test]
    fn expand_test_06() {
        let query_param = pb::QueryParams {
            table_names: vec![common_pb::NameOrId::from("knows".to_string())],
            columns: vec![],
            limit: None,
            predicate: Some(str_to_expr_pb("@.id == 2".to_string()).unwrap()),
            requirements: vec![],
        };
        let edge_expand_base = pb::ExpandBase {
            v_tag: None,
            direction: 0,
            params: Some(query_param),
        };
        let expand_opr_pb = pb::EdgeExpand {
            base: Some(edge_expand_base),
            is_edge: false,
            alias: None,
        };
        let mut result = expand_test(expand_opr_pb);
        let mut result_ids = vec![];
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let expected_ids = vec![v2];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_element() {
                result_ids.push(element.id() as usize)
            }
        }
        assert_eq!(result_ids, expected_ids)
    }
}
