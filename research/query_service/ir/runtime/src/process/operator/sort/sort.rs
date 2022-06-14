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
            let left_obj = tag_key.get_arc_entry(left).ok();
            let right_obj = tag_key.get_arc_entry(right).ok();
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
    use std::collections::HashMap;

    use dyn_type::Object;
    use graph_proxy::apis::{DefaultDetails, Details, DynDetails, Element, GraphElement, Vertex};
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_common::NameOrId;
    use pegasus::api::{Sink, SortBy};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;

    use crate::process::operator::sort::CompareFunctionGen;
    use crate::process::operator::tests::{init_source, init_source_with_tag, to_var_pb, TAG_A};
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
    fn sort_simple_ascending_test() {
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
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                result_ids.push(element.id());
            }
        }
        let expected_ids = vec![1, 2];
        assert_eq!(result_ids, expected_ids);
    }

    // g.V().order().by(desc)
    #[test]
    fn sort_simple_descending_test() {
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
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                result_ids.push(element.id());
            }
        }
        let expected_ids = vec![2, 1];
        assert_eq!(result_ids, expected_ids);
    }

    // g.V().order().by('name',desc)
    #[test]
    fn sort_by_property_test() {
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
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                result_name.push(
                    element
                        .details()
                        .unwrap()
                        .get_property(&"name".into())
                        .unwrap()
                        .try_to_owned()
                        .unwrap(),
                );
            }
        }
        let expected_names = vec![object!("vadas"), object!("marko")];
        assert_eq!(result_name, expected_names);
    }

    // g.V().order().by('name',asc).by('age', desc)
    #[test]
    fn sort_by_multi_property_test() {
        let map3: HashMap<NameOrId, Object> =
            vec![("id".into(), object!(3)), ("age".into(), object!(20)), ("name".into(), object!("marko"))]
                .into_iter()
                .collect();
        let v3 = Vertex::new(1, Some("person".into()), DynDetails::new(DefaultDetails::new(map3)));
        let mut source = init_source();
        source.push(Record::new(v3, None));

        let sort_opr = pb::OrderBy {
            pairs: vec![
                pb::order_by::OrderingPair {
                    key: Some(common_pb::Variable::from("@.name".to_string())),
                    order: 1, // ascending
                },
                pb::order_by::OrderingPair {
                    key: Some(common_pb::Variable::from("@.age".to_string())),
                    order: 2, // descending
                },
            ],
            limit: None,
        };
        let mut result = sort_test(source, sort_opr);
        let mut result_name_ages = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                let details = element.details().unwrap();
                result_name_ages.push((
                    details
                        .get_property(&"name".into())
                        .unwrap()
                        .try_to_owned()
                        .unwrap(),
                    details
                        .get_property(&"age".into())
                        .unwrap()
                        .try_to_owned()
                        .unwrap(),
                ));
            }
        }
        let expected_name_ages = vec![
            (object!("marko"), object!(29)),
            (object!("marko"), object!(20)),
            (object!("vadas"), object!(27)),
        ];
        assert_eq!(result_name_ages, expected_name_ages);
    }

    // g.V().as("a").order().by(select('a'))
    #[test]
    fn sort_by_tag_test() {
        let sort_opr = pb::OrderBy {
            pairs: vec![pb::order_by::OrderingPair {
                key: Some(to_var_pb(Some(TAG_A.into()), None)),
                order: 2, // descending
            }],
            limit: None,
        };
        let mut result = sort_test(init_source_with_tag(), sort_opr);
        let mut result_ids = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record
                .get(Some(&TAG_A.into()))
                .unwrap()
                .as_graph_vertex()
            {
                result_ids.push(element.id());
            }
        }
        let expected_ids = vec![2, 1];
        assert_eq!(result_ids, expected_ids);
    }

    // g.V().as("a").order().by(select('a').by('age'))
    #[test]
    fn sort_by_tag_property_test() {
        let sort_opr = pb::OrderBy {
            pairs: vec![pb::order_by::OrderingPair {
                key: Some(to_var_pb(Some(TAG_A.into()), Some("age".into()))),
                order: 2, // descending
            }],
            limit: None,
        };
        let mut result = sort_test(init_source_with_tag(), sort_opr);
        let mut result_ids = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record
                .get(Some(&TAG_A.into()))
                .unwrap()
                .as_graph_vertex()
            {
                result_ids.push(element.id());
            }
        }
        let expected_ids = vec![1, 2];
        assert_eq!(result_ids, expected_ids);
    }
}
