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

use std::convert::TryFrom;

use dyn_type::Object;
use graph_proxy::utils::expr::eval::{Evaluate, Evaluator};
use ir_common::error::ParsePbError;
use ir_common::generated::common as common_pb;
use ir_common::generated::physical as pb;
use ir_common::KeyId;
use pegasus::api::function::{FilterMapFunction, FnResult};

use crate::error::{FnExecResult, FnGenResult};
use crate::process::entry::CollectionEntry;
use crate::process::entry::DynEntry;
use crate::process::entry::PairEntry;
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::operator::TagKey;
use crate::process::record::Record;

/// Project entries with specified tags or further their properties.
/// Notice that when projecting a single column, if the result is a None-Entry,
/// Caused by either the given `tag` or the required properties do not exist, the record will be filtered.
#[derive(Debug)]
struct ProjectOperator {
    is_append: bool,
    projected_columns: Vec<(Projector, Option<KeyId>)>,
}

#[derive(Debug)]
pub enum Projector {
    ExprProjector(Evaluator),
    GraphElementProjector(TagKey),
    /// MultiGraphElementProject will output a collection entry.
    /// If the key is given, it is a collection of PairEntry with user-given key, and value of projected graph elements (computed via TagKey);
    /// If the key is None, it is a collection of projected graph elements (computed via TagKey).
    MultiGraphElementProjector(Vec<(Option<Object>, TagKey)>),
}

// TODO:
// 1. Currently, the behavior of filtering none-entry is identical to e.g., `g.V().values('name')`,
//    but differs to `g.V().valueMap('name')`, which will output the none-entry.
//    To support both cases, we may further need a flag to identify whether to filter or not.
//    BTW, if it is necessary to output none-entry,
//    we may need to further distinguish the cases of none-exist tags (filtering case) and none-exist properties (output none-entry).
// 2. When projecting multiple columns, even all projected columns are none-entry, the record won't be filtered for now.
//    This seems ambiguous. But multi-column project always appears in the end of the query. Can modify this logic if necessary.
fn exec_projector(input: &Record, projector: &Projector) -> FnExecResult<DynEntry> {
    let entry = match projector {
        Projector::ExprProjector(evaluator) => {
            let projected_result = evaluator.eval::<DynEntry, Record>(Some(&input))?;
            DynEntry::new(projected_result)
        }
        Projector::GraphElementProjector(tag_key) => tag_key.get_arc_entry(input)?,
        Projector::MultiGraphElementProjector(key_vals) => {
            let mut collection = Vec::with_capacity(key_vals.len());
            for (key, tag_key) in key_vals.iter() {
                let entry = tag_key.get_arc_entry(input)?;
                if let Some(key) = key {
                    collection.push(PairEntry::new(key.clone().into(), entry).into());
                } else {
                    collection.push(entry);
                }
            }
            DynEntry::new(CollectionEntry { inner: collection })
        }
    };
    Ok(entry)
}

impl FilterMapFunction<Record, Record> for ProjectOperator {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        if self.is_append {
            if self.projected_columns.len() == 1 {
                let (projector, alias) = self.projected_columns.get(0).unwrap();
                let entry = exec_projector(&input, &projector)?;
                if entry.is_none() {
                    Ok(None)
                } else {
                    input.append_arc_entry(entry, alias.clone());
                    Ok(Some(input))
                }
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
                input.set_curr_entry(None);
                Ok(Some(input))
            }
        } else {
            let mut new_record = Record::default();
            if self.projected_columns.len() == 1 {
                let (projector, alias) = self.projected_columns.get(0).unwrap();
                let entry = exec_projector(&input, &projector)?;
                if entry.is_none() {
                    Ok(None)
                } else {
                    new_record.append_arc_entry(entry, alias.clone());
                    Ok(Some(new_record))
                }
            } else {
                for (projector, alias) in self.projected_columns.iter() {
                    let entry = exec_projector(&input, &projector)?;
                    // Notice that if multiple columns, alias cannot be None
                    if let Some(alias) = alias {
                        let columns = new_record.get_columns_mut();
                        columns.insert(*alias as usize, entry);
                    }
                }
                Ok(Some(new_record))
            }
        }
    }
}

impl FilterMapFuncGen for pb::Project {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        let mut projected_columns = Vec::with_capacity(self.mappings.len());
        for expr_alias in self.mappings.into_iter() {
            let expr = expr_alias
                .expr
                .ok_or_else(|| ParsePbError::from("expr eval is missing in project"))?;
            let projector = if expr.operators.len() == 1 {
                match expr.operators.get(0).unwrap() {
                    common_pb::ExprOpr { item: Some(common_pb::expr_opr::Item::Var(var)), .. } => {
                        let tag_key = TagKey::try_from(var.clone())?;
                        Projector::GraphElementProjector(tag_key)
                    }
                    common_pb::ExprOpr { item: Some(common_pb::expr_opr::Item::Vars(vars)), .. } => {
                        let tag_keys = vars
                            .keys
                            .iter()
                            .map(|var| match TagKey::try_from(var.clone()) {
                                Ok(tag_key) => Ok((None, tag_key)),
                                Err(err) => Err(err),
                            })
                            .collect::<Result<Vec<(Option<Object>, TagKey)>, _>>()?;
                        Projector::MultiGraphElementProjector(tag_keys)
                    }
                    common_pb::ExprOpr { item: Some(common_pb::expr_opr::Item::Map(key_vals)), .. } => {
                        let mut key_value_vec = Vec::with_capacity(key_vals.key_vals.len());
                        for key_val in key_vals.key_vals.iter() {
                            let key = key_val.key.as_ref().ok_or_else(|| {
                                ParsePbError::EmptyFieldError(format!("key in Map Expr {:?}", key_val))
                            })?;
                            let key_obj = Object::try_from(key.clone())?;
                            let val = key_val.value.as_ref().ok_or_else(|| {
                                ParsePbError::EmptyFieldError(format!("value in Map Expr {:?}", key_val))
                            })?;
                            let tag_key = TagKey::try_from(val.clone())?;
                            key_value_vec.push((Some(key_obj), tag_key));
                        }
                        Projector::MultiGraphElementProjector(key_value_vec)
                    }
                    _ => {
                        let evaluator = Evaluator::try_from(expr)?;
                        Projector::ExprProjector(evaluator)
                    }
                }
            } else {
                let evaluator = Evaluator::try_from(expr)?;
                Projector::ExprProjector(evaluator)
            };
            projected_columns.push((projector, expr_alias.alias));
        }
        let project_operator = ProjectOperator { is_append: self.is_append, projected_columns };
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime project operator {:?}", project_operator);
        }
        Ok(Box::new(project_operator))
    }
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use dyn_type::Object;
    use graph_proxy::apis::{DynDetails, GraphElement, Vertex};
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::{common as common_pb, physical as pb};
    use ir_common::NameOrId;
    use pegasus::api::{Map, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;
    use pegasus_common::downcast::AsAny;

    use crate::process::entry::{CollectionEntry, Entry, PairEntry};
    use crate::process::operator::map::FilterMapFuncGen;
    use crate::process::operator::tests::{
        init_source, init_source_with_multi_tags, init_source_with_tag, init_vertex1, init_vertex2,
        to_expr_map_pb, to_expr_var_pb, to_expr_vars_pb, to_var_pb, PERSON_LABEL, TAG_A, TAG_B, TAG_C,
        TAG_D, TAG_E, TAG_F, TAG_G,
    };
    use crate::process::record::Record;

    fn project_test(source: Vec<Record>, project_opr_pb: pb::Project) -> ResultStream<Record> {
        let conf = JobConf::new("project_test");
        let result = pegasus::run(conf, || {
            let source = source.clone();
            let project_opr_pb = project_opr_pb.clone();
            |input, output| {
                let mut stream = input.input_from(source.into_iter())?;
                let project_func = project_opr_pb.gen_filter_map().unwrap();
                stream = stream.filter_map(move |i| project_func.exec(i))?;
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
            object_result.push(
                res.get(None)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone(),
            );
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
            let a_entry = res.get(Some(TAG_A));
            assert_eq!(a_entry, None);
            object_result.push(
                res.get(Some(TAG_B))
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone(),
            );
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
            object_result.push(
                res.get(None)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone(),
            );
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
            let age_val = res
                .get(Some(TAG_B))
                .unwrap()
                .as_object()
                .unwrap();
            let name_val = res
                .get(Some(TAG_C))
                .unwrap()
                .as_object()
                .unwrap();
            object_result.push((age_val.clone(), name_val.clone()));
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
            let age = res
                .get(Some(TAG_B))
                .unwrap()
                .as_object()
                .unwrap();
            let name = res
                .get(Some(TAG_C))
                .unwrap()
                .as_object()
                .unwrap();
            object_result.push((age.clone(), name.clone()));
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
        let v3 = Vertex::new(3, Some(PERSON_LABEL), DynDetails::new(map3));
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
            let a_age = res
                .get(Some(TAG_C))
                .unwrap()
                .as_object()
                .unwrap();
            let a_name = res
                .get(Some(TAG_D))
                .unwrap()
                .as_object()
                .unwrap();
            let b_name = res
                .get(Some(TAG_E))
                .unwrap()
                .as_object()
                .unwrap();
            object_result.push((a_age.clone(), a_name.clone(), b_name.clone()));
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
                .get(Some(TAG_A))
                .unwrap()
                .as_vertex()
                .unwrap();
            a_results.push(v.id());
            let b_val = res
                .get(Some(TAG_B))
                .unwrap()
                .as_object()
                .unwrap();
            b_results.push(b_val.clone());
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
            let age = res
                .get(Some(TAG_B))
                .unwrap()
                .as_object()
                .unwrap();
            let name = res
                .get(Some(TAG_C))
                .unwrap()
                .as_object()
                .unwrap();
            object_result.push((age.clone(), name.clone()));
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
        let project_func = project_opr_pb.gen_filter_map();
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
        let project_func = project_opr_pb.gen_filter_map();
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
        let mut collection_result: Vec<Vec<Object>> = vec![];
        while let Some(Ok(res)) = result.next() {
            let collection_entry = res
                .get(None)
                .unwrap()
                .as_any_ref()
                .downcast_ref::<CollectionEntry>()
                .unwrap()
                .inner
                .clone()
                .into_iter()
                .map(|entry| entry.as_object().unwrap().clone())
                .collect();
            collection_result.push(collection_entry);
        }
        let expected_result =
            vec![vec![object!(29), object!("marko")], vec![object!(27), object!("vadas")]];
        assert_eq!(collection_result, expected_result);
    }

    // g.V().valueMap("age", "name") // by varmap
    #[test]
    fn project_varmap_mapping_test() {
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
            object_result.push(
                res.get(None)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone(),
            );
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

    // g.V().valueMap('age', 'name').isNull, with alias of 'age' as 'newAge' and 'name' as 'newName' (by map)
    // this is projected by ExprProjector
    #[test]
    fn project_map_mapping_test() {
        let mut expr = to_expr_map_pb(vec![
            ("newAge".to_string(), (None, Some("age".into()))),
            ("newName".to_string(), (None, Some("name".into()))),
        ]);
        expr.operators.push(common_pb::ExprOpr {
            item: Some(common_pb::expr_opr::Item::Logical(13)), // isNull
            node_type: None,
        });
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias { expr: Some(expr), alias: None }],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let value = res.get(None).unwrap().as_object().unwrap();
            object_result.push(value.clone());
        }
        let expected_result = vec![object!(false), object!(false)];
        assert_eq!(object_result, expected_result);
    }

    // g.V().valueMap('age', 'name') with alias of 'age' as 'newAge' and 'name' as 'newName', by map
    // this is projected by MultiGraphElementProjector
    #[test]
    fn simple_project_map_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_map_pb(vec![
                    ("newAge".to_string(), (None, Some("age".into()))),
                    ("newName".to_string(), (None, Some("name".into()))),
                ])),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let collection = res
                .get(None)
                .unwrap()
                .as_any_ref()
                .downcast_ref::<CollectionEntry>()
                .unwrap();
            let mut result = vec![];
            for entry in collection.inner.iter() {
                let pair_entry = entry
                    .as_any_ref()
                    .downcast_ref::<PairEntry>()
                    .unwrap();
                let key = pair_entry
                    .get_left()
                    .as_any_ref()
                    .downcast_ref::<Object>()
                    .unwrap();
                let value = pair_entry
                    .get_right()
                    .as_any_ref()
                    .downcast_ref::<Object>()
                    .unwrap();
                result.push((key.clone(), value.clone()));
            }
            object_result.push(result);
        }
        let expected_result = vec![
            vec![(object!("newAge"), object!(29)), (object!("newName"), object!("marko"))],
            vec![(object!("newAge"), object!(27)), (object!("newName"), object!("vadas"))],
        ];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as("a").select("a").by(valueMap("age", "name")) // by map
    #[test]
    fn project_tag_varmap_mapping_test() {
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
            object_result.push(
                res.get(None)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone(),
            );
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

    // g.V().as("a").select("a").by(valueMap("age", "name")),with alias of 'a.age' as 'newAge' and 'a.name' as 'newName', by map
    // this is projected by ExprProjector
    #[test]
    fn project_tag_map_mapping_test() {
        let mut expr = to_expr_map_pb(vec![
            ("newAge".to_string(), (Some(TAG_A.into()), Some("age".into()))),
            ("newName".to_string(), (Some(TAG_A.into()), Some("name".into()))),
        ]);
        expr.operators.push(common_pb::ExprOpr {
            item: Some(common_pb::expr_opr::Item::Logical(13)), // isNull
            node_type: None,
        });
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias { expr: Some(expr), alias: None }],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);
        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            object_result.push(
                res.get(None)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone(),
            );
        }

        let expected_result = vec![object!(false), object!(false)];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as("a").select("a").by(valueMap("age", "name")),with alias of 'a.age' as 'newAge' and 'a.tname' as 'newName', by map
    // this is projected by MultiGraphElementProjector
    #[test]
    fn simple_project_tag_map_mapping_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_map_pb(vec![
                    ("newName".to_string(), (Some(TAG_A.into()), Some("name".into()))),
                    ("newAge".to_string(), (Some(TAG_A.into()), Some("age".into()))),
                ])),
                alias: None,
            }],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);

        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            let collection = res
                .get(None)
                .unwrap()
                .as_any_ref()
                .downcast_ref::<CollectionEntry>()
                .unwrap();
            let mut result = vec![];
            for entry in collection.inner.iter() {
                let pair_entry = entry
                    .as_any_ref()
                    .downcast_ref::<PairEntry>()
                    .unwrap();
                let key = pair_entry
                    .get_left()
                    .as_any_ref()
                    .downcast_ref::<Object>()
                    .unwrap();
                let value = pair_entry
                    .get_right()
                    .as_any_ref()
                    .downcast_ref::<Object>()
                    .unwrap();
                result.push((key.clone(), value.clone()));
            }
            object_result.push(result);
        }
        let expected_result = vec![
            vec![(object!("newName"), object!("marko")), (object!("newAge"), object!(29))],
            vec![(object!("newName"), object!("vadas")), (object!("newAge"), object!(27))],
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
            let v1 = res
                .get(Some(TAG_C))
                .unwrap()
                .as_vertex()
                .unwrap();
            let v2 = res
                .get(Some(TAG_D))
                .unwrap()
                .as_vertex()
                .unwrap();
            results.push(v1.id());
            results.push(v2.id());
        }
        let expected_results = vec![1, 2];
        assert_eq!(results, expected_results);
    }

    // g.V().select('a')
    #[test]
    fn project_none_exist_tag_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_var_pb(Some(TAG_A.into()), None)),
                alias: None,
            }],
            is_append: true,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut result_cnt = 0;
        while let Some(Ok(_res)) = result.next() {
            result_cnt += 1;
        }
        assert_eq!(result_cnt, 0);
    }

    // g.V().as('a').select('a').by('test')
    #[test]
    fn project_none_exist_prop_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("test".into()))),
                alias: None,
            }],
            is_append: true,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);
        let mut result_cnt = 0;
        while let Some(Ok(_res)) = result.next() {
            result_cnt += 1;
        }
        assert_eq!(result_cnt, 0);
    }

    // g.V().select("a","b")
    #[test]
    fn project_multi_none_exist_tag_test() {
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
            is_append: false,
        };
        let mut result = project_test(init_source(), project_opr_pb);
        let mut result_cnt = 0;
        while let Some(Ok(res)) = result.next() {
            result_cnt += 1;
            // head should be None
            assert_eq!(res.get(None), None);
            let c_val = res
                .get(Some(TAG_C))
                .unwrap()
                .as_object()
                .unwrap();
            let d_val = res
                .get(Some(TAG_D))
                .unwrap()
                .as_object()
                .unwrap();
            match (c_val, d_val) {
                (Object::None, Object::None) => {
                    assert!(true)
                }
                _ => {
                    assert!(false)
                }
            }
        }
        assert_eq!(result_cnt, 2);
    }

    // g.V().as("a").select("a").by(valueMap("test1", "test2"))
    #[test]
    fn project_multi_none_exist_props_test() {
        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("test1".into()))),
                    alias: Some(TAG_C.into()),
                },
                pb::project::ExprAlias {
                    expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("test2".into()))),
                    alias: Some(TAG_D.into()),
                },
            ],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);
        let mut result_cnt = 0;
        while let Some(Ok(res)) = result.next() {
            result_cnt += 1;
            // head should be None
            assert_eq!(res.get(None), None);
            let c_val = res
                .get(Some(TAG_C))
                .unwrap()
                .as_object()
                .unwrap();
            let d_val = res
                .get(Some(TAG_D))
                .unwrap()
                .as_object()
                .unwrap();
            match (c_val, d_val) {
                (Object::None, Object::None) => {
                    assert!(true)
                }
                _ => {
                    assert!(false)
                }
            }
        }
        assert_eq!(result_cnt, 2);
    }

    // g.V().values('id').as('a').select('a').by('id'), throw an unexpected data type error
    #[test]
    fn project_error_test() {
        let common_obj1 = object!(1);
        let common_obj2 = object!(2);
        let r1 = Record::new(common_obj1, Some(TAG_A.into()));
        let r2 = Record::new(common_obj2, Some(TAG_A.into()));
        let source = vec![r1, r2];
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_var_pb(Some(TAG_A.into()), Some("id".into()))),
                alias: None,
            }],
            is_append: true,
        };
        let mut result = project_test(source, project_opr_pb);
        if let Some(Err(_res)) = result.next() {
            assert!(true)
        }
    }

    #[test]
    fn project_extract_from_date_test() {
        // 2010-01-02
        let date_obj = Object::DateFormat(dyn_type::DateTimeFormats::from_date32(20100102).unwrap());
        // 12:34:56.100
        let time_obj = Object::DateFormat(dyn_type::DateTimeFormats::from_time32(123456100).unwrap());
        // 2020-10-10 10:10:10
        let datetime_obj =
            Object::DateFormat(dyn_type::DateTimeFormats::from_timestamp_millis(1602324610100).unwrap());
        let mut r1 = Record::new(date_obj.clone(), Some(TAG_A.into()));
        r1.append(time_obj, Some(TAG_B.into()));
        r1.append(datetime_obj, Some(TAG_C.into()));

        let extract_date_year_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Extract(common_pb::Extract {
                interval: common_pb::extract::Interval::Year as i32,
            })),
        };

        let extract_time_hour_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Extract(common_pb::Extract {
                interval: common_pb::extract::Interval::Hour as i32,
            })),
        };

        let extract_datetime_month_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Extract(common_pb::Extract {
                interval: common_pb::extract::Interval::Month as i32,
            })),
        };

        let extract_datetime_minute_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Extract(common_pb::Extract {
                interval: common_pb::extract::Interval::Minute as i32,
            })),
        };

        let tag_a_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Var(to_var_pb(Some(TAG_A.into()), None))),
        };
        let tag_b_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Var(to_var_pb(Some(TAG_B.into()), None))),
        };
        let tag_c_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Var(to_var_pb(Some(TAG_C.into()), None))),
        };

        let expr1 = common_pb::Expression { operators: vec![extract_date_year_opr, tag_a_opr] };

        let expr2 = common_pb::Expression { operators: vec![extract_time_hour_opr, tag_b_opr] };

        let expr3 =
            common_pb::Expression { operators: vec![extract_datetime_month_opr, tag_c_opr.clone()] };

        let expr4 =
            common_pb::Expression { operators: vec![extract_datetime_minute_opr, tag_c_opr.clone()] };

        let source = vec![r1];
        // To project: year of 2010-01-02, hour of 12:34:56.100, month of 2020-10-10 10:10:10, and minute of 2020-10-10 10:10:10
        let project_opr_pb = pb::Project {
            mappings: vec![
                pb::project::ExprAlias { expr: Some(expr1), alias: Some(TAG_D.into()) },
                pb::project::ExprAlias { expr: Some(expr2), alias: Some(TAG_E.into()) },
                pb::project::ExprAlias { expr: Some(expr3), alias: Some(TAG_F.into()) },
                pb::project::ExprAlias { expr: Some(expr4), alias: Some(TAG_G.into()) },
            ],
            is_append: true,
        };

        let mut result = project_test(source, project_opr_pb);
        let expected_results = vec![object!(2010), object!(12), object!(10), object!(10)];
        let mut results = vec![];
        while let Some(Ok(res)) = result.next() {
            let year = res
                .get(Some(TAG_D))
                .unwrap()
                .as_object()
                .unwrap();
            let hour = res
                .get(Some(TAG_E))
                .unwrap()
                .as_object()
                .unwrap();
            let month = res
                .get(Some(TAG_F))
                .unwrap()
                .as_object()
                .unwrap();
            let minute = res
                .get(Some(TAG_G))
                .unwrap()
                .as_object()
                .unwrap();
            results.push(year.clone());
            results.push(hour.clone());
            results.push(month.clone());
            results.push(minute.clone());
        }
        assert_eq!(results, expected_results);
    }
}
