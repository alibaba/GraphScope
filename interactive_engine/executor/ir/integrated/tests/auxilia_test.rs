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
    use dyn_type::Object;
    use graph_proxy::apis::{Details, GraphElement};
    use graph_proxy::{create_exp_store, SimplePartition};
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_common::NameOrId;
    use pegasus::api::{Map, Sink};
    use pegasus::JobConf;
    use runtime::process::operator::flatmap::FlatMapFuncGen;
    use runtime::process::operator::map::FilterMapFuncGen;
    use runtime::process::operator::source::SourceOperator;
    use runtime::process::record::Record;

    use crate::common::test::*;

    // g.V()
    fn source_gen(alias: Option<common_pb::NameOrId>) -> Box<dyn Iterator<Item = Record> + Send> {
        create_exp_store();
        let scan_opr_pb = pb::Scan { scan_opt: 0, alias, params: None, idx_predicate: None };
        let source_opr_pb = pb::logical_plan::operator::Opr::Scan(scan_opr_pb);
        let source =
            SourceOperator::new(source_opr_pb, 1, 1, Arc::new(SimplePartition { num_servers: 1 })).unwrap();
        source.gen_source(0).unwrap()
    }

    // g.V().out('knows').as('a')
    #[test]
    fn auxilia_simple_alias_test() {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
        };

        let auxilia_opr = pb::Auxilia {
            tag: None,
            params: Some(query_params(vec![], vec![], None)),
            alias: Some(TAG_A.into()),
            remove_tags: vec![],
        };

        let conf = JobConf::new("auxilia_simple_alias_test");
        let mut result = pegasus::run(conf, || {
            let expand = expand_opr.clone();
            let auxilia = auxilia_opr.clone();
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

        let expected_ids = vec![2, 4];
        let mut result_ids = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record
                .get(Some(TAG_A))
                .unwrap()
                .as_graph_vertex()
            {
                result_ids.push(element.id());
            }
        }
        result_ids.sort();
        assert_eq!(result_ids, expected_ids)
    }

    // g.V().out('knows').values('name')  // must obtain the name property
    #[test]
    fn auxilia_get_property_test() {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
        };

        let auxilia_opr = pb::Auxilia {
            tag: None,
            params: Some(query_params(vec![], vec!["name".into()], None)),
            alias: None,
            remove_tags: vec![],
        };

        let conf = JobConf::new("auxilia_get_property_test");
        let mut result = pegasus::run(conf, || {
            let expand = expand_opr.clone();
            let auxilia = auxilia_opr.clone();
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

        let expected_ids_with_prop = vec![(2, "vadas".to_string().into()), (4, "josh".to_string().into())];
        let mut result_ids_with_prop = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                result_ids_with_prop.push((
                    element.id(),
                    element
                        .details()
                        .unwrap()
                        .get_property(&NameOrId::Str("name".to_string()))
                        .unwrap()
                        .try_to_owned()
                        .unwrap(),
                ));
            }
        }
        result_ids_with_prop.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        assert_eq!(result_ids_with_prop, expected_ids_with_prop)
    }

    // g.V().out('knows').as("a").values('name')  // we give alias "a" in auxilia
    #[test]
    fn auxilia_get_property_with_none_tag_input_test() {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
        };

        let auxilia_opr = pb::Auxilia {
            tag: None,
            params: Some(query_params(vec![], vec!["name".into()], None)),
            alias: Some(TAG_A.into()),
            remove_tags: vec![],
        };

        let conf = JobConf::new("auxilia_get_property_with_none_tag_input_test");
        let mut result = pegasus::run(conf, || {
            let expand = expand_opr.clone();
            let auxilia = auxilia_opr.clone();
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

        let expected_ids_with_prop = vec![(2, "vadas".to_string().into()), (4, "josh".to_string().into())];
        let mut result_ids_with_prop = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record
                .get(Some(TAG_A))
                .unwrap()
                .as_graph_vertex()
            {
                result_ids_with_prop.push((
                    element.id(),
                    element
                        .details()
                        .unwrap()
                        .get_property(&NameOrId::Str("name".to_string()))
                        .unwrap()
                        .try_to_owned()
                        .unwrap(),
                ));
            }
        }
        result_ids_with_prop.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        assert_eq!(result_ids_with_prop, expected_ids_with_prop)
    }

    // g.V().out('knows').has('name', "vadas")
    #[test]
    fn auxilia_filter_test() {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
        };

        let auxilia_opr = pb::Auxilia {
            tag: None,
            params: Some(query_params(
                vec![],
                vec![],
                str_to_expr_pb("@.name==\"vadas\"".to_string()).ok(),
            )),
            alias: None,
            remove_tags: vec![],
        };

        let conf = JobConf::new("auxilia_filter_test");
        let mut result = pegasus::run(conf, || {
            let expand = expand_opr.clone();
            let auxilia = auxilia_opr.clone();
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

        let expected_ids_with_prop = vec![(2, "vadas".to_string().into())];
        let mut result_ids_with_prop = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                result_ids_with_prop.push((
                    element.id(),
                    element
                        .details()
                        .unwrap()
                        .get_property(&NameOrId::Str("name".to_string()))
                        .unwrap()
                        .try_to_owned()
                        .unwrap(),
                ));
            }
        }
        result_ids_with_prop.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        assert_eq!(result_ids_with_prop, expected_ids_with_prop)
    }

    // g.V().out('knows').has("name", "vadas").as('a')
    #[test]
    fn auxilia_alias_test() {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
        };

        let auxilia_opr = pb::Auxilia {
            tag: None,
            params: Some(query_params(
                vec![],
                vec!["name".into()],
                str_to_expr_pb("@.name==\"vadas\"".to_string()).ok(),
            )),
            alias: Some(TAG_A.into()),
            remove_tags: vec![],
        };

        let conf = JobConf::new("auxilia_alias_test");
        let mut result = pegasus::run(conf, || {
            let expand = expand_opr.clone();
            let auxilia = auxilia_opr.clone();
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

        let expected_ids = vec![2];
        let mut result_ids = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record
                .get(Some(TAG_A))
                .unwrap()
                .as_graph_vertex()
            {
                result_ids.push(element.id());
            }
        }
        result_ids.sort();
        assert_eq!(result_ids, expected_ids)
    }

    // g.V() with two auxilia ops to test updating with new properties
    #[test]
    fn auxilia_update_test() {
        let auxilia_opr_1 = pb::Auxilia {
            tag: None,
            params: Some(query_params(vec![], vec!["id".into()], None)),
            alias: None,
            remove_tags: vec![],
        };
        let auxilia_opr_2 = pb::Auxilia {
            tag: None,
            params: Some(query_params(vec![], vec!["name".into()], None)),
            alias: None,
            remove_tags: vec![],
        };

        let conf = JobConf::new("auxilia_update_test");
        let mut result = pegasus::run(conf, || {
            let auxilia_1 = auxilia_opr_1.clone();
            let auxilia_2 = auxilia_opr_2.clone();
            |input, output| {
                let mut stream = input.input_from(source_gen(None))?;
                let filter_map_func_1 = auxilia_1.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| filter_map_func_1.exec(input))?;
                let filter_map_func_2 = auxilia_2.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| filter_map_func_2.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut expected_id_names: Vec<(Object, Object)> = vec![
            (object!(1), object!("marko")),
            (object!(2), object!("vadas")),
            (object!(3), object!("lop")),
            (object!(4), object!("josh")),
            (object!(5), object!("ripple")),
            (object!(6), object!("peter")),
        ];

        let mut results: Vec<(Object, Object)> = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                let details = element.details().unwrap();
                results.push((
                    details
                        .get_property(&"id".to_string().into())
                        .unwrap()
                        .try_to_owned()
                        .unwrap(),
                    details
                        .get_property(&"name".to_string().into())
                        .unwrap()
                        .try_to_owned()
                        .unwrap(),
                ));
            }
        }
        expected_id_names.sort_by(|e1, e2| e1.0.cmp(&e2.0));
        results.sort_by(|e1, e2| e1.0.cmp(&e2.0));
        assert_eq!(expected_id_names, results)
    }

    // g.V() with an auxilia to update lazy properties
    #[test]
    fn auxilia_update_on_lazy_vertex_test() {
        let auxilia_opr = pb::Auxilia {
            tag: None,
            params: Some(query_params(vec![], vec!["name".into()], None)),
            alias: None,
            remove_tags: vec![],
        };

        let conf = JobConf::new("auxilia_update_on_lazy_vertex_test");
        let mut result = pegasus::run(conf, || {
            let auxilia = auxilia_opr.clone();
            |input, output| {
                // input vertex with DynDetails of LazyDetails
                let mut stream = input.input_from(source_gen(None))?;
                // update vertex properties
                let filter_map_func = auxilia.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| filter_map_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut expected_id_names: Vec<Object> = vec![
            object!("marko"),
            object!("vadas"),
            object!("lop"),
            object!("josh"),
            object!("ripple"),
            object!("peter"),
        ];

        let mut results: Vec<Object> = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                let details = element.details().unwrap();
                results.push(
                    details
                        .get_property(&"name".to_string().into())
                        .unwrap()
                        .try_to_owned()
                        .unwrap(),
                );
            }
        }
        expected_id_names.sort();
        results.sort();
        assert_eq!(expected_id_names, results)
    }

    // g.V().out("knows") with an auxilia to update empty properties
    #[test]
    fn auxilia_update_on_empty_vertex_test() {
        let query_param = query_params(vec![KNOWS_LABEL.into()], vec![], None);

        // expand vertex without any properties, i.e., Vertex with None details
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_param),
            expand_opt: 0,
            alias: None,
        };

        let auxilia_opr = pb::Auxilia {
            tag: None,
            params: Some(query_params(vec![], vec!["name".into()], None)),
            alias: None,
            remove_tags: vec![],
        };

        let conf = JobConf::new("auxilia_update_on_empty_vertex_test");
        let mut result = pegasus::run(conf, || {
            let expand = expand_opr_pb.clone();
            let auxilia = auxilia_opr.clone();
            |input, output| {
                let mut stream = input.input_from(source_gen(None))?;
                let flatmap_func = expand.gen_flat_map().unwrap();
                // expand vertex with DynDetails::default()
                stream = stream.flat_map(move |input| flatmap_func.exec(input))?;
                // update vertex properties
                let filter_map_func = auxilia.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| filter_map_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut expected_id_names: Vec<Object> = vec![object!("vadas"), object!("josh")];

        let mut results: Vec<Object> = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                let details = element.details().unwrap();
                results.push(
                    details
                        .get_property(&"name".to_string().into())
                        .unwrap()
                        .try_to_owned()
                        .unwrap(),
                );
            }
        }
        expected_id_names.sort();
        results.sort();
        assert_eq!(expected_id_names, results)
    }

    // g.V().as('a').remove('a') via auxilia with remove_tag
    #[test]
    fn auxilia_remove_tag_test() {
        let auxilia_opr =
            pb::Auxilia { tag: None, params: None, alias: None, remove_tags: vec![TAG_A.into()] };

        let conf = JobConf::new("auxilia_remove_tag_test");
        let mut result = pegasus::run(conf, || {
            let auxilia = auxilia_opr.clone();
            |input, output| {
                let mut stream = input.input_from(source_gen(Some(TAG_A.into())))?;
                let filter_map_func = auxilia.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| filter_map_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut result_count = 0;
        while let Some(Ok(record)) = result.next() {
            assert!(record.get(Some(TAG_A)).is_none());
            result_count += 1;
        }
        assert_eq!(result_count, 6)
    }

    // g.V().as('a').out().as('b').remove('a', 'b') via auxilia with remove_tag
    #[test]
    fn auxilia_remove_tags_test() {
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: None,
            expand_opt: 0,
            alias: Some(TAG_B.into()),
        };

        let auxilia_opr = pb::Auxilia {
            tag: None,
            params: None,
            alias: None,
            remove_tags: vec![TAG_A.into(), TAG_B.into()],
        };

        let conf = JobConf::new("auxilia_remove_tags_test");
        let mut result = pegasus::run(conf, || {
            let auxilia = auxilia_opr.clone();
            let expand = expand_opr_pb.clone();
            |input, output| {
                let mut stream = input.input_from(source_gen(Some(TAG_A.into())))?;
                let flatmap_func = expand.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func.exec(input))?;
                let filter_map_func = auxilia.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| filter_map_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut result_count = 0;
        while let Some(Ok(record)) = result.next() {
            assert!(record.get(Some(TAG_A)).is_none());
            assert!(record.get(Some(TAG_B)).is_none());
            result_count += 1;
        }
        assert_eq!(result_count, 6)
    }

    // g.V().as('a').remove('a') via auxilia with tag='a', remove_tag='a'
    #[test]
    fn auxilia_tag_remove_tag_test() {
        let auxilia_opr = pb::Auxilia {
            tag: Some(TAG_A.into()),
            params: None,
            alias: None,
            remove_tags: vec![TAG_A.into()],
        };

        let conf = JobConf::new("auxilia_tag_remove_tag_test");
        let mut result = pegasus::run(conf, || {
            let auxilia = auxilia_opr.clone();
            |input, output| {
                let mut stream = input.input_from(source_gen(Some(TAG_A.into())))?;
                let filter_map_func = auxilia.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| filter_map_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut result_count = 0;
        while let Some(Ok(record)) = result.next() {
            assert!(record.get(Some(TAG_A)).is_none());
            result_count += 1;
        }
        assert_eq!(result_count, 6)
    }

    // g.V().as('a').out().as('b').remove('a') via auxilia with remove_tag
    #[test]
    fn auxilia_remove_some_tag_test() {
        let expand_opr_pb = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: None,
            expand_opt: 0,
            alias: Some(TAG_B.into()),
        };

        let auxilia_opr =
            pb::Auxilia { tag: None, params: None, alias: None, remove_tags: vec![TAG_A.into()] };

        let conf = JobConf::new("auxilia_remove_some_tag_test");
        let mut result = pegasus::run(conf, || {
            let auxilia = auxilia_opr.clone();
            let expand = expand_opr_pb.clone();
            |input, output| {
                let mut stream = input.input_from(source_gen(Some(TAG_A.into())))?;
                let flatmap_func = expand.gen_flat_map().unwrap();
                stream = stream.flat_map(move |input| flatmap_func.exec(input))?;
                let filter_map_func = auxilia.gen_filter_map().unwrap();
                stream = stream.filter_map(move |input| filter_map_func.exec(input))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut result_count = 0;
        while let Some(Ok(record)) = result.next() {
            println!("record {:?}", record);
            assert!(record.get(Some(TAG_A)).is_none());
            assert!(record.get(Some(TAG_B)).is_some());
            result_count += 1;
        }
        assert_eq!(result_count, 6)
    }
}
