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

use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::order_by::ordering_pair::Order;

use crate::error::FnGenResult;
use crate::process::functions::CompareFunction;
use crate::process::operator::sort::CompareFunctionGen;
use crate::process::operator::TagKey;
use crate::process::record::Record;

#[derive(Debug)]
struct RecordCompare {
    tag_key_order: Vec<(TagKey, Order)>,
}

impl CompareFunction<Record> for RecordCompare {
    fn compare(&self, left: &Record, right: &Record) -> Ordering {
        let mut result = Ordering::Equal;
        for (tag_key, order) in self.tag_key_order.iter() {
            let left_obj = tag_key.get_entry(left).ok();
            let right_obj = tag_key.get_entry(right).ok();
            let ordering = left_obj.partial_cmp(&right_obj);
            if let Some(ordering) = ordering {
                if Ordering::Equal != ordering {
                    result = {
                        match order {
                            Order::Desc => ordering.reverse(),
                            _ => ordering,
                        }
                    };
                    break;
                }
            }
        }
        result
    }
}

impl CompareFunctionGen for algebra_pb::OrderBy {
    fn gen_cmp(self) -> FnGenResult<Box<dyn CompareFunction<Record>>> {
        let record_compare = RecordCompare::try_from(self)?;
        debug!("Runtime order operator cmp: {:?}", record_compare);
        Ok(Box::new(record_compare))
    }
}

impl TryFrom<algebra_pb::OrderBy> for RecordCompare {
    type Error = ParsePbError;

    fn try_from(order_pb: algebra_pb::OrderBy) -> Result<Self, Self::Error> {
        let mut tag_key_order = Vec::with_capacity(order_pb.pairs.len());
        for order_pair in order_pb.pairs {
            let key = order_pair
                .key
                .ok_or(ParsePbError::EmptyFieldError("key is empty in order".to_string()))?
                .try_into()?;
            let order: Order = unsafe { ::std::mem::transmute(order_pair.order) };
            tag_key_order.push((key, order));
        }
        Ok(RecordCompare { tag_key_order })
    }
}

#[cfg(test)]
mod tests {
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_common::NameOrId;
    use pegasus::api::{Sink, SortBy};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;

    use crate::graph::element::{Element, GraphElement};
    use crate::graph::property::Details;
    use crate::process::operator::sort::CompareFunctionGen;
    use crate::process::operator::tests::{init_source, init_source_with_tag};
    use crate::process::record::Record;

    fn sort_test(source: Vec<Record>, sort_opr: pb::OrderBy) -> ResultStream<Record> {
        let conf = JobConf::new("sort_test");
        let result = pegasus::run(conf, || {
            let source = source.clone();
            let sort_opr = sort_opr.clone();
            |input, output| {
                let mut stream = input.input_from(source.into_iter())?;
                let sort_func = sort_opr.gen_cmp().unwrap();
                stream = stream.sort_by(move |a, b| sort_func.compare(a, b))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");
        result
    }

    // g.V().order()
    #[test]
    fn sort_test_01() {
        let sort_opr = pb::OrderBy {
            pairs: vec![pb::order_by::OrderingPair {
                key: Some(common_pb::Variable { tag: None, property: None }),
                order: 1, // ascending
            }],
            limit: None,
        };
        let mut result = sort_test(init_source(), sort_opr);
        let mut result_ids = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_element() {
                result_ids.push(element.id());
            }
        }
        let expected_ids = vec![1, 2];
        assert_eq!(result_ids, expected_ids);
    }

    // g.V().order().by(desc)
    #[test]
    fn sort_test_02() {
        let sort_opr = pb::OrderBy {
            pairs: vec![pb::order_by::OrderingPair {
                key: Some(common_pb::Variable { tag: None, property: None }),
                order: 2, // descending
            }],
            limit: None,
        };
        let mut result = sort_test(init_source(), sort_opr);
        let mut result_ids = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_element() {
                result_ids.push(element.id());
            }
        }
        let expected_ids = vec![2, 1];
        assert_eq!(result_ids, expected_ids);
    }

    // g.V().order().by('name',desc)
    #[test]
    fn sort_test_03() {
        let sort_opr = pb::OrderBy {
            pairs: vec![pb::order_by::OrderingPair {
                key: Some(common_pb::Variable::from("@.name".to_string())),
                order: 2, // descending
            }],
            limit: None,
        };
        let mut result = sort_test(init_source(), sort_opr);
        let mut result_name = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_element() {
                result_name.push(
                    element
                        .details()
                        .unwrap()
                        .get_property(&NameOrId::Str("name".to_string()))
                        .unwrap()
                        .try_to_owned()
                        .unwrap(),
                );
            }
        }
        let expected_names = vec!["vadas".to_string().into(), "marko".to_string().into()];
        assert_eq!(result_name, expected_names);
    }

    // g.V().as("a").order().by(select('a'))
    #[test]
    fn sort_test_04() {
        let sort_opr = pb::OrderBy {
            pairs: vec![pb::order_by::OrderingPair {
                key: Some(common_pb::Variable::from("@a".to_string())),
                order: 2, // descending
            }],
            limit: None,
        };
        let mut result = sort_test(init_source_with_tag(), sort_opr);
        let mut result_ids = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record
                .get(Some(&NameOrId::Str("a".to_string())))
                .unwrap()
                .as_graph_element()
            {
                result_ids.push(element.id());
            }
        }
        let expected_ids = vec![2, 1];
        assert_eq!(result_ids, expected_ids);
    }
}
