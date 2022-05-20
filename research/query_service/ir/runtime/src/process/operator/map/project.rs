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

use dyn_type::Object;
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::common as common_pb;
use ir_common::KeyId;
use pegasus::api::function::{FnResult, MapFunction};

use crate::error::{FnExecError, FnGenResult};
use crate::expr::eval::{Evaluate, Evaluator};
use crate::process::operator::map::MapFuncGen;
use crate::process::operator::TagKey;
use crate::process::record::{CommonObject, Entry, Record, RecordElement};

#[derive(Debug)]
struct ProjectOperator {
    is_append: bool,
    projected_columns: Vec<(Projector, Option<KeyId>)>,
}

#[derive(Debug)]
pub enum Projector {
    ExprProjector(Evaluator),
    GraphElementProjector(TagKey),
}

fn exec_projector(input: &Record, projector: &Projector) -> FnResult<Arc<Entry>> {
    let entry = match projector {
        Projector::ExprProjector(evaluator) => {
            let projected_result = evaluator
                .eval::<RecordElement, Record>(Some(&input))
                .map_err(|e| FnExecError::from(e))?;
            Arc::new(
                match projected_result {
                    Object::None => CommonObject::None,
                    _ => CommonObject::Prop(projected_result),
                }
                .into(),
            )
        }
        Projector::GraphElementProjector(tag_key) => tag_key.get_arc_entry(input)?,
    };
    Ok(entry)
}

impl MapFunction<Record, Record> for ProjectOperator {
    fn exec(&self, mut input: Record) -> FnResult<Record> {
        if self.is_append {
            if self.projected_columns.len() == 1 {
                let (projector, alias) = self.projected_columns.get(0).unwrap();
                let entry = exec_projector(&input, &projector)?;
                input.append_arc_entry(entry, alias.clone());
            } else {
                for (projector, alias) in self.projected_columns.iter() {
                    let entry = exec_projector(&input, projector)?;
                    // Notice that if multiple columns, alias cannot be None
                    if let Some(alias) = alias {
                        let columns = input.get_columns_mut();
                        columns.insert(*alias as usize, entry);
                    }
                }
                // set head as None when the last column is appended
                let curr = input.get_curr_mut();
                *curr = None;
            }

            Ok(input)
        } else {
            let mut new_record = Record::default();
            if self.projected_columns.len() == 1 {
                let (projector, alias) = self.projected_columns.get(0).unwrap();
                let entry = exec_projector(&input, &projector)?;
                new_record.append_arc_entry(entry, alias.clone());
            } else {
                for (projector, alias) in self.projected_columns.iter() {
                    let entry = exec_projector(&input, &projector)?;
                    // Notice that if multiple columns, alias cannot be None
                    if let Some(alias) = alias {
                        let columns = new_record.get_columns_mut();
                        columns.insert(*alias as usize, entry);
                    }
                }
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
            let projector = if expr.operators.len() == 1 {
                if let Some(common_pb::ExprOpr { item: Some(common_pb::expr_opr::Item::Var(var)) }) =
                    expr.operators.get(0)
                {
                    let tag_key = TagKey::try_from(var.clone())?;
                    Projector::GraphElementProjector(tag_key)
                } else {
                    let evaluator = Evaluator::try_from(expr)?;
                    Projector::ExprProjector(evaluator)
                }
            } else {
                let evaluator = Evaluator::try_from(expr)?;
                Projector::ExprProjector(evaluator)
            };
            projected_columns.push((projector, alias));
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
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_common::NameOrId;
    use pegasus::api::{Map, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;

    use crate::graph::element::{GraphElement, Vertex};
    use crate::graph::property::{DefaultDetails, DynDetails};
    use crate::process::operator::map::MapFuncGen;
    use crate::process::operator::tests::{
        init_source, init_source_with_multi_tags, init_source_with_tag, init_vertex1, init_vertex2,
        to_expr_var_pb, to_expr_vars_pb, TAG_A, TAG_B, TAG_C, TAG_D, TAG_E,
    };
    use crate::process::record::{CommonObject, Entry, Record, RecordElement};

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
                expr: Some(str_to_expr_pb("@.id".to_string()).unwrap()),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            match res.get(None).unwrap().as_ref() {
                Entry::Element(RecordElement::OffGraph(CommonObject::Prop(val))) => {
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
                expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("name".into()))),
                alias: Some(TAG_B.into()),
            }],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);

        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let a_entry = res.get(Some(&TAG_A.into()));
            assert_eq!(a_entry, None);
            match res.get(Some(&TAG_B.into())).unwrap().as_ref() {
                Entry::Element(RecordElement::OffGraph(CommonObject::Prop(val))) => {
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
                expr: Some(str_to_expr_pb("@.id".to_string()).unwrap()),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            match res.get(None).unwrap().as_ref() {
                Entry::Element(RecordElement::OffGraph(CommonObject::Prop(val))) => {
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
                    expr: Some(str_to_expr_pb("@.age".to_string()).unwrap()),
                    alias: Some(TAG_B.into()),
                },
                pb::project::ExprAlias {
                    expr: Some(str_to_expr_pb("@.name".to_string()).unwrap()),
                    alias: Some(TAG_C.into()),
                },
            ],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            // head should be None
            assert_eq!(res.get(None), None);
            let age_val = res.get(Some(&TAG_B.into())).unwrap();
            let name_val = res.get(Some(&TAG_C.into())).unwrap();
            match (age_val.as_ref(), name_val.as_ref()) {
                (
                    Entry::Element(RecordElement::OffGraph(CommonObject::Prop(age))),
                    Entry::Element(RecordElement::OffGraph(CommonObject::Prop(name))),
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
                    expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("age".into()))),
                    alias: Some(TAG_B.into()),
                },
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("name".into()))),
                    alias: Some(TAG_C.into()),
                },
            ],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);

        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let age_val = res.get(Some(&TAG_B.into())).unwrap();
            let name_val = res.get(Some(&TAG_C.into())).unwrap();
            match (age_val.as_ref(), name_val.as_ref()) {
                (
                    Entry::Element(RecordElement::OffGraph(CommonObject::Prop(age))),
                    Entry::Element(RecordElement::OffGraph(CommonObject::Prop(name))),
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
        let v3 = Vertex::new(3, Some("person".into()), DynDetails::new(DefaultDetails::new(map3)));
        let mut r1 = Record::new(v1, Some(TAG_A.into()));
        r1.append(v3.clone(), Some(TAG_B.into()));
        let mut r2 = Record::new(v2, Some(TAG_A.into()));
        r2.append(v3.clone(), Some(TAG_B.into()));
        let source = vec![r1, r2];

        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("age".into()))),
                    alias: Some(TAG_C.into()),
                },
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("name".into()))),
                    alias: Some(TAG_D.into()),
                },
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_B.into()), Some("name".into()))),
                    alias: Some(TAG_E.into()),
                },
            ],
            is_append: false,
        };
        let mut result = project_test(source, project_opr_pb);

        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let a_age_val = res.get(Some(&TAG_C.into())).unwrap();
            let a_name_val = res.get(Some(&TAG_D.into())).unwrap();
            let b_name_val = res.get(Some(&TAG_E.into())).unwrap();
            match (a_age_val.as_ref(), a_name_val.as_ref(), b_name_val.as_ref()) {
                (
                    Entry::Element(RecordElement::OffGraph(CommonObject::Prop(a_age))),
                    Entry::Element(RecordElement::OffGraph(CommonObject::Prop(a_name))),
                    Entry::Element(RecordElement::OffGraph(CommonObject::Prop(b_name))),
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
                expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("age".into()))),
                alias: Some(TAG_B.into()),
            }],
            is_append: true,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);
        let mut a_results = vec![];
        let mut b_results = vec![];
        while let Some(Ok(res)) = result.next() {
            let v = res
                .get(Some(&TAG_A.into()))
                .unwrap()
                .as_graph_vertex()
                .unwrap();
            let b_entry = res.get(Some(&TAG_B.into())).unwrap().as_ref();
            match b_entry {
                Entry::Element(RecordElement::OffGraph(CommonObject::Prop(val))) => {
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

    // g.V().valueMap('age', 'name') with alias of 'age' as 'b' and 'name' as 'c'
    #[test]
    fn project_multi_mapping_appended_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias {
                    expr: Some(str_to_expr_pb("@.age".to_string()).unwrap()),
                    alias: Some(TAG_B.into()),
                },
                pb::project::ExprAlias {
                    expr: Some(str_to_expr_pb("@.name".to_string()).unwrap()),
                    alias: Some(TAG_C.into()),
                },
            ],
            is_append: true,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let age_val = res.get(Some(&TAG_B.into())).unwrap();
            let name_val = res.get(Some(&TAG_C.into())).unwrap();
            match (age_val.as_ref(), name_val.as_ref()) {
                (
                    Entry::Element(RecordElement::OffGraph(CommonObject::Prop(age))),
                    Entry::Element(RecordElement::OffGraph(CommonObject::Prop(name))),
                ) => {
                    object_result.push((age.clone(), name.clone()));
                }
                _ => {}
            }
        }
        let expected_result = vec![(object!(29), object!("marko")), (object!(27), object!("vadas"))];
        assert_eq!(object_result, expected_result);
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
                expr: Some(str_to_expr_pb("@.id".to_string()).unwrap()),
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
                expr: Some(str_to_expr_pb("[@.age,@.name]".to_string()).unwrap()),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            match res.get(None).unwrap().as_ref() {
                Entry::Element(RecordElement::OffGraph(CommonObject::Prop(val))) => {
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
                expr: Some(str_to_expr_pb("{@.age,@.name}".to_string()).unwrap()),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            match res.get(None).unwrap().as_ref() {
                Entry::Element(RecordElement::OffGraph(CommonObject::Prop(val))) => {
                    object_result.push(val.clone());
                }
                _ => {}
            }
        }
        let expected_result = vec![
            Object::KV(
                vec![
                    (object!(vec![Object::None, object!("age")]), object!(29)),
                    (object!(vec![Object::None, object!("name")]), object!("marko")),
                ]
                .into_iter()
                .collect(),
            ),
            Object::KV(
                vec![
                    (object!(vec![Object::None, object!("age")]), object!(27)),
                    (object!(vec![Object::None, object!("name")]), object!("vadas")),
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
                expr: Some(to_expr_vars_pb(
                    vec![
                        (Some(TAG_A.into()), Some("age".into())),
                        (Some(TAG_A.into()), Some("name".into())),
                    ],
                    true,
                )),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            match res.get(None).unwrap().as_ref() {
                Entry::Element(RecordElement::OffGraph(CommonObject::Prop(val))) => {
                    object_result.push(val.clone());
                }
                _ => {}
            }
        }

        let expected_result = vec![
            Object::KV(
                vec![
                    (object!(vec![object!(TAG_A), object!("age")]), object!(29)),
                    (object!(vec![object!(TAG_A), object!("name")]), object!("marko")),
                ]
                .into_iter()
                .collect(),
            ),
            Object::KV(
                vec![
                    (object!(vec![object!(TAG_A), object!("age")]), object!(27)),
                    (object!(vec![object!(TAG_A), object!("name")]), object!("vadas")),
                ]
                .into_iter()
                .collect(),
            ),
        ];
        assert_eq!(object_result, expected_result);
    }

    #[test]
    fn project_multi_mapping_tags() {
        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_A.into()), None)),
                    alias: Some(TAG_C.into()),
                },
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_B.into()), None)),
                    alias: Some(TAG_D.into()),
                },
            ],
            is_append: true,
        };
        let mut result = project_test(init_source_with_multi_tags(), project_opr_pb);
        let mut results = vec![];
        while let Some(Ok(res)) = result.next() {
            let a_entry = res.get(Some(&TAG_C.into())).unwrap().as_ref();
            let b_entry = res.get(Some(&TAG_D.into())).unwrap().as_ref();
            let v1 = a_entry.as_graph_vertex().unwrap();
            let v2 = b_entry.as_graph_vertex().unwrap();
            results.push(v1.id());
            results.push(v2.id());
        }
        let expected_results = vec![1, 2];
        assert_eq!(results, expected_results);
    }
}
