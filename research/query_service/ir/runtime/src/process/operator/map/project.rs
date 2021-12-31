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

use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::NameOrId;
use pegasus::api::function::{FnResult, MapFunction};

use crate::error::{FnExecError, FnGenResult};
use crate::expr::eval::{Evaluate, Evaluator};
use crate::process::operator::map::MapFuncGen;
use crate::process::record::{ObjectElement, Record};

#[derive(Debug)]
struct ProjectOperator {
    is_append: bool,
    projected_columns: Vec<(Evaluator, Option<NameOrId>)>,
}

impl MapFunction<Record, Record> for ProjectOperator {
    fn exec(&self, mut input: Record) -> FnResult<Record> {
        if self.is_append {
            for (evaluator, alias) in self.projected_columns.iter() {
                let entry = if let Some(single_tag) = evaluator.extract_single_tag() {
                    input
                        .get(Some(&single_tag))
                        .ok_or(FnExecError::get_tag_error(format!("{:?}", single_tag).as_str()))?
                        .clone()
                } else {
                    let projected_result = evaluator
                        .eval(Some(&input))
                        .map_err(|e| FnExecError::from(e))?;
                    Arc::new(
                        projected_result
                            .map_or(ObjectElement::None, |prop| ObjectElement::Prop(prop))
                            .into(),
                    )
                };
                input.append_arc_entry(entry, alias.clone());
            }

            Ok(input)
        } else {
            let mut new_record = Record::default();
            for (evaluator, alias) in self.projected_columns.iter() {
                let entry = if let Some(single_tag) = evaluator.extract_single_tag() {
                    input
                        .get(Some(&single_tag))
                        .ok_or(FnExecError::get_tag_error(format!("{:?}", single_tag).as_str()))?
                        .clone()
                } else {
                    let projected_result = evaluator
                        .eval(Some(&input))
                        .map_err(|e| FnExecError::from(e))?;
                    Arc::new(
                        projected_result
                            .map_or(ObjectElement::None, |prop| ObjectElement::Prop(prop))
                            .into(),
                    )
                };
                new_record.append_arc_entry(entry, alias.clone());
            }

            Ok(new_record)
        }
    }
}

impl MapFuncGen for algebra_pb::Project {
    fn gen_map(self) -> FnGenResult<Box<dyn MapFunction<Record, Record>>> {
        let mut projected_columns = Vec::with_capacity(self.mappings.len());
        for expr_alias in self.mappings.into_iter() {
            let alias = expr_alias
                .alias
                .clone()
                .map(|alias| alias.try_into())
                .transpose()?;
            let expr = expr_alias
                .expr
                .ok_or(ParsePbError::from("expr eval is missing in project"))?;
            let evaluator = Evaluator::try_from(expr)?;
            projected_columns.push((evaluator, alias));
        }
        let project_operator = ProjectOperator { is_append: self.is_append, projected_columns };
        debug!("Runtime project operator {:?}", project_operator);
        Ok(Box::new(project_operator))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dyn_type::Object;
    use ir_common::expr_parse::str_to_suffix_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_common::NameOrId;
    use pegasus::api::{Map, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;

    use crate::graph::element::{GraphElement, Vertex};
    use crate::graph::property::{DefaultDetails, DynDetails};
    use crate::process::operator::map::MapFuncGen;
    use crate::process::operator::tests::{init_source, init_source_with_tag, init_vertex1, init_vertex2};
    use crate::process::record::{Entry, ObjectElement, Record, RecordElement};

    fn project_test(source: Vec<Record>, project_opr_pb: pb::Project) -> ResultStream<Record> {
        let conf = JobConf::new("project_test");
        let result = pegasus::run(conf, || {
            let source = source.clone();
            let project_opr_pb = project_opr_pb.clone();
            |input, output| {
                let mut stream = input.input_from(source.into_iter())?;
                let project_func = project_opr_pb.gen_map().unwrap();
                stream = stream.map(move |i| project_func.exec(i))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        result
    }

    // g.V().valueMap("id")
    #[test]
    fn project_single_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_suffix_expr_pb("@.id".to_string()).unwrap()),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            match res.get(None).unwrap().as_ref() {
                Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(val))) => {
                    object_result.push(val.clone());
                }
                _ => {}
            }
        }
        let expected_result = vec![object!(1), object!(2)];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as("a").select("a").by("name").as("b")
    #[test]
    fn project_tag_single_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_suffix_expr_pb("@a.name".to_string()).unwrap()),
                alias: Some("b".into()),
            }],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);

        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let a_entry = res.get(Some(&"a".into()));
            assert_eq!(a_entry, None);
            match res.get(Some(&"b".into())).unwrap().as_ref() {
                Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(val))) => {
                    object_result.push(val.clone());
                }
                _ => {}
            }
        }
        let expected_result = vec![object!("marko"), object!("vadas")];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as("a").valueMap("id")
    #[test]
    fn project_none_tag_single_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_suffix_expr_pb("@.id".to_string()).unwrap()),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            match res.get(None).unwrap().as_ref() {
                Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(val))) => {
                    object_result.push(val.clone());
                }
                _ => {}
            }
        }
        let expected_result = vec![object!(1), object!(2)];
        assert_eq!(object_result, expected_result);
    }

    // g.V().valueMap('age', 'name') with alias of 'age' as 'b' and 'name' as 'c'
    #[test]
    fn project_multi_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias {
                    expr: Some(str_to_suffix_expr_pb("@.age".to_string()).unwrap()),
                    alias: Some(NameOrId::Str("b".to_string()).into()),
                },
                pb::project::ExprAlias {
                    expr: Some(str_to_suffix_expr_pb("@.name".to_string()).unwrap()),
                    alias: Some(NameOrId::Str("c".to_string()).into()),
                },
            ],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let age_val = res.get(Some(&"b".into())).unwrap();
            let name_val = res.get(Some(&"c".into())).unwrap();
            match (age_val.as_ref(), name_val.as_ref()) {
                (
                    Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(age))),
                    Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(name))),
                ) => {
                    object_result.push((age.clone(), name.clone()));
                }
                _ => {}
            }
        }
        let expected_result = vec![(object!(29), object!("marko")), (object!(27), object!("vadas"))];
        assert_eq!(object_result, expected_result);
    }

    // g.V().valueMap('age', 'name') with alias of 'age' as 'b' and 'name' as 'None' (head)
    #[test]
    fn project_multi_mapping_with_head_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias {
                    expr: Some(str_to_suffix_expr_pb("@.age".to_string()).unwrap()),
                    alias: Some(NameOrId::Str("b".to_string()).into()),
                },
                pb::project::ExprAlias {
                    expr: Some(str_to_suffix_expr_pb("@.name".to_string()).unwrap()),
                    alias: None,
                },
            ],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let age_val = res.get(Some(&"b".into())).unwrap();
            let name_val = res.get(None).unwrap();
            match (age_val.as_ref(), name_val.as_ref()) {
                (
                    Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(age))),
                    Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(name))),
                ) => {
                    object_result.push((age.clone(), name.clone()));
                }
                _ => {}
            }
        }
        let expected_result = vec![(object!(29), object!("marko")), (object!(27), object!("vadas"))];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as('a').select('a').by(valueMap('age', 'name')) with alias of 'age' as 'b' and 'name' as 'c'
    #[test]
    fn project_tag_multi_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias {
                    expr: Some(str_to_suffix_expr_pb("@a.age".to_string()).unwrap()),
                    alias: Some(NameOrId::Str("b".to_string()).into()),
                },
                pb::project::ExprAlias {
                    expr: Some(str_to_suffix_expr_pb("@a.name".to_string()).unwrap()),
                    alias: Some(NameOrId::Str("c".to_string()).into()),
                },
            ],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);

        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let age_val = res.get(Some(&"b".into())).unwrap();
            let name_val = res.get(Some(&"c".into())).unwrap();
            match (age_val.as_ref(), name_val.as_ref()) {
                (
                    Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(age))),
                    Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(name))),
                ) => {
                    object_result.push((age.clone(), name.clone()));
                }
                _ => {}
            }
        }
        let expected_result = vec![(object!(29), object!("marko")), (object!(27), object!("vadas"))];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as('a').out().as('b').select('a', 'b').by(valueMap('age', 'name')).by('name'),
    // with alias of 'a.age' as 'c', 'a.name' as 'd' and 'b.name' as 'e'
    #[test]
    fn project_multi_tag_multi_mapping() {
        // 1->3
        // 2->3
        let v1 = init_vertex1();
        let v2 = init_vertex2();
        let map3: HashMap<NameOrId, Object> =
            vec![("id".into(), object!(3)), ("age".into(), object!(32)), ("name".into(), object!("josh"))]
                .into_iter()
                .collect();
        let v3 = Vertex::new(DynDetails::new(DefaultDetails::with_property(3, "person".into(), map3)));
        let mut r1 = Record::new(v1, Some("a".into()));
        r1.append(v3.clone(), Some("b".into()));
        let mut r2 = Record::new(v2, Some("a".into()));
        r2.append(v3.clone(), Some("b".into()));
        let source = vec![r1, r2];

        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias {
                    expr: Some(str_to_suffix_expr_pb("@a.age".to_string()).unwrap()),
                    alias: Some(NameOrId::Str("c".to_string()).into()),
                },
                pb::project::ExprAlias {
                    expr: Some(str_to_suffix_expr_pb("@a.name".to_string()).unwrap()),
                    alias: Some(NameOrId::Str("d".to_string()).into()),
                },
                pb::project::ExprAlias {
                    expr: Some(str_to_suffix_expr_pb("@b.name".to_string()).unwrap()),
                    alias: Some(NameOrId::Str("e".to_string()).into()),
                },
            ],
            is_append: false,
        };
        let mut result = project_test(source, project_opr_pb);

        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let a_age_val = res.get(Some(&"c".into())).unwrap();
            let a_name_val = res.get(Some(&"d".into())).unwrap();
            let b_name_val = res.get(Some(&"e".into())).unwrap();
            match (a_age_val.as_ref(), a_name_val.as_ref(), b_name_val.as_ref()) {
                (
                    Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(a_age))),
                    Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(a_name))),
                    Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(b_name))),
                ) => {
                    object_result.push((a_age.clone(), a_name.clone(), b_name.clone()));
                }
                _ => {}
            }
        }
        let expected_result = vec![
            (object!(29), object!("marko"), object!("josh")),
            (object!(27), object!("vadas"), object!("josh")),
        ];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as('a').select('a').by(valueMap('age')) with 'age' as 'b' and append 'b'
    #[test]
    fn project_single_mapping_appended_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_suffix_expr_pb("@a.age".to_string()).unwrap()),
                alias: Some(NameOrId::Str("b".to_string()).into()),
            }],
            is_append: true,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);
        let mut a_results = vec![];
        let mut b_results = vec![];
        while let Some(Ok(res)) = result.next() {
            let a_entry = res.get(Some(&"a".into())).unwrap().as_ref();
            let b_entry = res.get(Some(&"b".into())).unwrap().as_ref();
            match (a_entry, b_entry) {
                (
                    Entry::Element(RecordElement::OnGraph(v)),
                    Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(val))),
                ) => {
                    a_results.push(v.id());
                    b_results.push(val.clone());
                }
                _ => {}
            }
        }
        let expected_a_result = vec![1, 2];
        let expected_b_result = vec![object!(29), object!(27)];
        assert_eq!(a_results, expected_a_result);
        assert_eq!(b_results, expected_b_result);
    }

    // None expr is not allowed
    #[test]
    fn project_empty_mapping_expr_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias { expr: None, alias: None }],
            is_append: false,
        };
        let project_func = project_opr_pb.gen_map();
        if let Err(_) = project_func {
            assert!(true)
        }
    }

    // None alias is not allowed.
    #[test]
    fn project_empty_mapping_alias_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_suffix_expr_pb("@.id".to_string()).unwrap()),
                alias: None,
            }],
            is_append: true,
        };
        let project_func = project_opr_pb.gen_map();
        if let Err(_) = project_func {
            assert!(true)
        }
    }

    // g.V().valueMap("age", "name") // by vec
    #[test]
    fn project_vec_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_suffix_expr_pb("[@.age,@.name]".to_string()).unwrap()),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            match res.get(None).unwrap().as_ref() {
                Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(val))) => {
                    object_result.push(val.clone());
                }
                _ => {}
            }
        }
        let expected_result = vec![
            object!(vec![object!(29), object!("marko")]),
            object!(vec![object!(27), object!("vadas")]),
        ];
        assert_eq!(object_result, expected_result);
    }

    // g.V().valueMap("age", "name") // by map
    #[test]
    fn project_map_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_suffix_expr_pb("{@.age,@.name}".to_string()).unwrap()),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            match res.get(None).unwrap().as_ref() {
                Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(val))) => {
                    object_result.push(val.clone());
                }
                _ => {}
            }
        }
        let expected_result = vec![
            Object::KV(
                vec![
                    (object!(vec![object!(""), object!("age")]), object!(29)),
                    (object!(vec![object!(""), object!("name")]), object!("marko")),
                ]
                .into_iter()
                .collect(),
            ),
            Object::KV(
                vec![
                    (object!(vec![object!(""), object!("age")]), object!(27)),
                    (object!(vec![object!(""), object!("name")]), object!("vadas")),
                ]
                .into_iter()
                .collect(),
            ),
        ];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as("a").select("a").by(valueMap("age", "name")) // by map
    #[test]
    fn project_tag_map_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_suffix_expr_pb("{@a.age,@a.name}".to_string()).unwrap()),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            match res.get(None).unwrap().as_ref() {
                Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(val))) => {
                    object_result.push(val.clone());
                }
                _ => {}
            }
        }

        let expected_result = vec![
            Object::KV(
                vec![
                    (object!(vec![object!("a"), object!("age")]), object!(29)),
                    (object!(vec![object!("a"), object!("name")]), object!("marko")),
                ]
                .into_iter()
                .collect(),
            ),
            Object::KV(
                vec![
                    (object!(vec![object!("a"), object!("age")]), object!(27)),
                    (object!(vec![object!("a"), object!("name")]), object!("vadas")),
                ]
                .into_iter()
                .collect(),
            ),
        ];
        assert_eq!(object_result, expected_result);
    }
}
