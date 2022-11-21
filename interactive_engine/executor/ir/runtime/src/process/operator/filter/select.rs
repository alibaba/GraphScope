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

use std::convert::TryInto;

use graph_proxy::utils::expr::eval_pred::{EvalPred, PEvaluator};
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use pegasus::api::function::{FilterFunction, FnResult};

use crate::error::{FnExecError, FnGenResult};
use crate::process::operator::filter::FilterFuncGen;
use crate::process::record::Record;

#[derive(Debug)]
struct SelectOperator {
    pub filter: PEvaluator,
}

impl FilterFunction<Record> for SelectOperator {
    fn test(&self, input: &Record) -> FnResult<bool> {
        let res = self
            .filter
            .eval_bool(Some(input))
            .map_err(|e| FnExecError::from(e))?;
        Ok(res)
    }
}

impl FilterFuncGen for algebra_pb::Select {
    fn gen_filter(self) -> FnGenResult<Box<dyn FilterFunction<Record>>> {
        if let Some(predicate) = self.predicate {
            let select_operator = SelectOperator { filter: predicate.try_into()? };
            if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
                debug!("Runtime select operator: {:?}", select_operator);
            }
            Ok(Box::new(select_operator))
        } else {
            Err(ParsePbError::EmptyFieldError("empty select pb".to_string()).into())
        }
    }
}

#[cfg(test)]
mod tests {
    use graph_proxy::apis::{Details, GraphElement};
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use pegasus::api::{Filter, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;

    use crate::process::operator::filter::FilterFuncGen;
    use crate::process::operator::tests::{init_source, PERSON_LABEL};
    use crate::process::record::Record;

    fn select_test(source: Vec<Record>, select_opr_pb: pb::Select) -> ResultStream<Record> {
        let conf = JobConf::new("select_test");
        let result = pegasus::run(conf, || {
            let source = source.clone();
            let select_opr_pb = select_opr_pb.clone();
            |input, output| {
                let mut stream = input.input_from(source)?;
                let select_func = select_opr_pb.gen_filter().unwrap();
                stream = stream.filter(move |i| select_func.test(i))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        result
    }

    // g.V().has("~label")
    #[test]
    fn select_label_exist_test() {
        let select_opr_pb = pb::Select { predicate: Some(str_to_expr_pb("@.~label".to_string()).unwrap()) };
        let mut result = select_test(init_source(), select_opr_pb);
        let mut count = 0;
        while let Some(Ok(_)) = result.next() {
            count += 1;
        }

        assert_eq!(count, 2);
    }

    // g.V().has("~id")
    #[test]
    fn select_id_exist_test() {
        let select_opr_pb = pb::Select { predicate: Some(str_to_expr_pb("@.~id".to_string()).unwrap()) };
        let mut result = select_test(init_source(), select_opr_pb);
        let mut count = 0;
        while let Some(Ok(_)) = result.next() {
            count += 1;
        }

        assert_eq!(count, 2);
    }

    // g.V().has("code")
    #[test]
    fn select_property_exist_test() {
        let select_opr_pb = pb::Select { predicate: Some(str_to_expr_pb("@.code".to_string()).unwrap()) };
        let mut result = select_test(init_source(), select_opr_pb);
        let mut count = 0;
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                assert!(element.id() < 2)
            }
            count += 1;
        }

        assert_eq!(count, 1);
    }

    // g.V().has("code11")
    #[test]
    fn select_property_none_exist_test() {
        let select_opr_pb = pb::Select { predicate: Some(str_to_expr_pb("@.code11".to_string()).unwrap()) };
        let mut result = select_test(init_source(), select_opr_pb);
        let mut count = 0;
        while let Some(res) = result.next() {
            match res {
                Ok(_) => {
                    count += 1;
                }
                Err(_) => {
                    assert!(false)
                }
            }
        }

        assert_eq!(count, 0);
    }

    // g.V().has("code11").or(has("code"))
    #[test]
    fn select_property_none_exist_or_exist_test() {
        let select_opr_pb =
            pb::Select { predicate: Some(str_to_expr_pb("@.code11||@.code".to_string()).unwrap()) };
        let mut result = select_test(init_source(), select_opr_pb);
        let mut count = 0;
        while let Some(res) = result.next() {
            match res {
                Ok(_) => {
                    count += 1;
                }
                Err(_) => {
                    assert!(false)
                }
            }
        }

        assert_eq!(count, 1);
    }

    // g.V().has("code11").and(has("age",27))
    #[test]
    fn select_property_none_exist_and_predicate_test() {
        let select_opr_pb =
            pb::Select { predicate: Some(str_to_expr_pb("@.code11&&@.age==27".to_string()).unwrap()) };
        let mut result = select_test(init_source(), select_opr_pb);
        let mut count = 0;
        while let Some(res) = result.next() {
            match res {
                Ok(_) => {
                    count += 1;
                }
                Err(_) => {
                    assert!(false)
                }
            }
        }

        assert_eq!(count, 0);
    }

    // g.V().has("code11").or(has("age",27))
    #[test]
    fn select_property_none_exist_or_predicate_test() {
        let select_opr_pb =
            pb::Select { predicate: Some(str_to_expr_pb("@.code11||@.age==27".to_string()).unwrap()) };
        let mut result = select_test(init_source(), select_opr_pb);
        let mut count = 0;
        while let Some(res) = result.next() {
            match res {
                Ok(_) => {
                    count += 1;
                }
                Err(_) => {
                    assert!(false)
                }
            }
        }

        assert_eq!(count, 1);
    }

    // g.V().has("id",gt(1))
    #[test]
    fn select_gt_test() {
        let select_opr_pb = pb::Select { predicate: Some(str_to_expr_pb("@.id > 1".to_string()).unwrap()) };
        let mut result = select_test(init_source(), select_opr_pb);
        let mut count = 0;
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                assert!(element.id() > 1)
            }
            count += 1;
        }

        assert_eq!(count, 1);
    }

    // g.V().has("id",lt(2))
    #[test]
    fn select_lt_test() {
        let select_opr_pb = pb::Select { predicate: Some(str_to_expr_pb("@.id < 2".to_string()).unwrap()) };
        let mut result = select_test(init_source(), select_opr_pb);
        let mut count = 0;
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                assert!(element.id() < 2)
            }
            count += 1;
        }

        assert_eq!(count, 1);
    }

    // g.V().has("name","marko")
    #[test]
    fn select_eq_test() {
        let select_opr_pb =
            pb::Select { predicate: Some(str_to_expr_pb("@.name == \"marko\"".to_string()).unwrap()) };
        let mut result = select_test(init_source(), select_opr_pb);
        let mut count = 0;
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                {
                    assert_eq!(
                        element
                            .details()
                            .unwrap()
                            .get_property(&"name".into())
                            .unwrap()
                            .try_to_owned()
                            .unwrap(),
                        object!("marko")
                    );
                }
                count += 1;
            }
        }
        assert_eq!(count, 1);
    }

    // g.V().hasId(1)
    #[test]
    fn select_id_test() {
        let select_opr_pb =
            pb::Select { predicate: Some(str_to_expr_pb("@.~id == 1".to_string()).unwrap()) };
        let mut result = select_test(init_source(), select_opr_pb);
        let mut count = 0;
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                {
                    assert_eq!(element.id(), 1);
                }
                count += 1;
            }
        }
        assert_eq!(count, 1);
    }

    // g.V().hasLabel("person")
    #[test]
    fn select_label_test() {
        let select_opr_pb =
            pb::Select { predicate: Some(str_to_expr_pb("@.~label == 0".to_string()).unwrap()) };
        let mut result = select_test(init_source(), select_opr_pb);
        let mut count = 0;
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                {
                    assert_eq!(element.label().unwrap().clone(), PERSON_LABEL);
                }
                count += 1;
            }
        }
        assert_eq!(count, 2);
    }
}
