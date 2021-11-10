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

use crate::error::{str_to_dyn_error, FnGenResult};
use crate::expr::eval::Evaluator;
use crate::process::operator::map::MapFuncGen;
use crate::process::record::{ObjectElement, Record};
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::NameOrId;
use pegasus::api::function::{FnResult, MapFunction};
use std::convert::{TryFrom, TryInto};

struct ProjectOperator {
    is_append: bool,
    projected_columns: Vec<(Evaluator, Option<NameOrId>)>,
}

impl MapFunction<Record, Record> for ProjectOperator {
    fn exec(&self, mut input: Record) -> FnResult<Record> {
        let mut new_record = Record::default();
        for (evaluator, alias) in self.projected_columns.iter() {
            let projected_result = evaluator
                .eval(Some(&input))
                .map_err(|e| str_to_dyn_error(&format!("{}", e)))?;
            if self.is_append {
                input.append(ObjectElement::Prop(projected_result), alias.clone());
            } else {
                new_record.append(ObjectElement::Prop(projected_result), alias.clone());
            }
        }
        if self.is_append {
            Ok(input)
        } else {
            Ok(new_record)
        }
    }
}

impl MapFuncGen for algebra_pb::Project {
    fn gen_map(self) -> FnGenResult<Box<dyn MapFunction<Record, Record>>> {
        let mut projected_columns = Vec::with_capacity(self.mappings.len());
        for expr_alias in self.mappings.into_iter() {
            // TODO: the tag_option of is_query_given may not necessary
            let (alias_pb, _is_given_tag) = {
                let expr_alias = expr_alias
                    .alias
                    .ok_or(ParsePbError::from("expr alias is missing in project"))?;
                (expr_alias.alias, expr_alias.is_query_given)
            };
            let alias = alias_pb.map(|alias| alias.try_into()).transpose()?;
            let expr = expr_alias
                .expr
                .ok_or(ParsePbError::from("expr eval is missing in project"))?;
            let evaluator = Evaluator::try_from(expr)?;
            projected_columns.push((evaluator, alias));
        }
        Ok(Box::new(ProjectOperator {
            is_append: self.is_append,
            projected_columns,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::expr::str_to_expr_pb;
    use crate::process::operator::map::MapFuncGen;
    use crate::process::operator::tests::source_gen;
    use crate::process::record::{Entry, ObjectElement, RecordElement};
    use ir_common::generated::algebra as pb;
    use ir_common::NameOrId;
    use pegasus::api::{Map, Sink};
    use pegasus::JobConf;

    // g.V().project("id")
    #[test]
    fn project_test_01() {
        let conf = JobConf::new("project_test_01");
        let mut result = pegasus::run(conf, || {
            |input, output| {
                let mut stream = input.input_from(source_gen())?;
                let project_opr_pb = pb::Project {
                    mappings: vec![pb::project::ExprAlias {
                        expr: Some(str_to_expr_pb("@.id".to_string()).unwrap()),
                        alias: Some(pb::project::Alias {
                            alias: None,
                            is_query_given: false,
                        }),
                    }],
                    is_append: false,
                };
                let project_func = project_opr_pb.gen_map().unwrap();
                stream = stream.map(move |i| project_func.exec(i))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            match res.get(None).unwrap() {
                Entry::Element(RecordElement::OutGraph(ObjectElement::Prop(val))) => {
                    object_result.push(val.clone());
                }
                _ => {}
            }
        }
        let expected_result = vec![1.into(), 2.into()];
        assert_eq!(object_result, expected_result);
    }

    // g.V().project("name").as("a")
    #[test]
    fn project_test_02() {
        let conf = JobConf::new("project_test_02");
        let mut result = pegasus::run(conf, || {
            |input, output| {
                let mut stream = input.input_from(source_gen())?;
                let project_opr_pb = pb::Project {
                    mappings: vec![pb::project::ExprAlias {
                        expr: Some(str_to_expr_pb("@.name".to_string()).unwrap()),
                        alias: Some(pb::project::Alias {
                            alias: Some(NameOrId::Str("a".to_string()).into()),
                            is_query_given: false,
                        }),
                    }],
                    is_append: false,
                };
                let project_func = project_opr_pb.gen_map().unwrap();
                stream = stream.map(move |i| project_func.exec(i))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            match res.get(Some(&NameOrId::Str("a".to_string()))).unwrap() {
                Entry::Element(RecordElement::OutGraph(ObjectElement::Prop(val))) => {
                    object_result.push(val.clone());
                }
                _ => {}
            }
        }
        let expected_result = vec!["marko".into(), "vadas".into()];
        assert_eq!(object_result, expected_result);
    }
}
