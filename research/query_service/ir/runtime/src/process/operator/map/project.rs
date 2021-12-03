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

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::NameOrId;
use pegasus::api::function::{FnResult, MapFunction};

use crate::error::{FnExecError, FnGenResult};
use crate::expr::eval::Evaluator;
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
                let projected_result = evaluator
                    .eval(Some(&input))
                    .map_err(|e| FnExecError::from(e))?;
                input.append(ObjectElement::Prop(projected_result), alias.clone());
            }
            Ok(input)
        } else {
            let mut new_record = Record::default();
            for (evaluator, alias) in self.projected_columns.iter() {
                let projected_result = evaluator
                    .eval(Some(&input))
                    .map_err(|e| FnExecError::from(e))?;
                new_record.append(ObjectElement::Prop(projected_result), alias.clone());
            }
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
            let alias = alias_pb
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
    use ir_common::generated::algebra as pb;
    use ir_common::NameOrId;
    use pegasus::api::{Map, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;

    use crate::expr::str_to_expr_pb;
    use crate::process::operator::map::MapFuncGen;
    use crate::process::operator::tests::{init_source, init_source_with_tag};
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

    // g.V().project("id")
    #[test]
    fn project_test_01() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_expr_pb("@.id".to_string()).unwrap()),
                alias: Some(pb::project::Alias { alias: None, is_query_given: false }),
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
        let expected_result = vec![1.into(), 2.into()];
        assert_eq!(object_result, expected_result);
    }

    // g.V().as("a").select("a").by("name").as("b")
    #[test]
    fn project_test_02() {
        let project_opr_pb = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_expr_pb("@a.name".to_string()).unwrap()),
                alias: Some(pb::project::Alias {
                    alias: Some(NameOrId::Str("b".to_string()).into()),
                    is_query_given: false,
                }),
            }],
            is_append: false,
        };
        let mut result = project_test(init_source_with_tag(), project_opr_pb);

        let mut object_result = vec![];
        while let Some(Ok(res)) = result.next() {
            match res
                .get(Some(&NameOrId::Str("b".to_string())))
                .unwrap()
                .as_ref()
            {
                Entry::Element(RecordElement::OffGraph(ObjectElement::Prop(val))) => {
                    object_result.push(val.clone());
                }
                _ => {}
            }
        }
        let expected_result = vec!["marko".into(), "vadas".into()];
        assert_eq!(object_result, expected_result);
    }
}
