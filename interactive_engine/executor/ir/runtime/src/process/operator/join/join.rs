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

use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::join::JoinKind;

use crate::error::FnGenResult;
use crate::process::functions::{JoinKeyGen, KeyFunction};
use crate::process::operator::keyed::KeySelector;
use crate::process::record::{Record, RecordKey};

impl JoinKeyGen<Record, RecordKey, Record> for algebra_pb::Join {
    fn gen_left_kv_fn(&self) -> FnGenResult<Box<dyn KeyFunction<Record, RecordKey, Record>>> {
        let left_kv_fn = KeySelector::with(self.left_keys.clone())?;
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime join operator left_kv_fn {:?}", left_kv_fn);
        }
        Ok(Box::new(left_kv_fn))
    }

    fn gen_right_kv_fn(&self) -> FnGenResult<Box<dyn KeyFunction<Record, RecordKey, Record>>> {
        let right_kv_fn = KeySelector::with(self.right_keys.clone())?;
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime join operator right_kv_fn {:?}", right_kv_fn);
        }
        Ok(Box::new(right_kv_fn))
    }

    fn get_join_kind(&self) -> JoinKind {
        let join_kind = unsafe { ::std::mem::transmute(self.kind) };
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime join operator join_kind {:?}", join_kind);
        }
        join_kind
    }
}

#[cfg(test)]
mod tests {
    use graph_proxy::apis::{DynDetails, GraphElement, Vertex, ID};
    use ir_common::generated::algebra as pb;
    use ir_common::generated::algebra::join::JoinKind;
    use ir_common::generated::common as common_pb;
    use pegasus::api::{Join, KeyBy, Map, PartitionByKey, Sink};
    use pegasus::JobConf;

    use crate::process::functions::JoinKeyGen;
    use crate::process::record::Record;

    fn source_s1_gen() -> Box<dyn Iterator<Item = Record> + Send> {
        let v1 = Vertex::new(1, None, DynDetails::default());
        let v2 = Vertex::new(2, None, DynDetails::default());
        let r1 = Record::new(v1, None);
        let r2 = Record::new(v2, None);
        Box::new(vec![r1, r2].into_iter())
    }

    fn source_s2_gen() -> Box<dyn Iterator<Item = Record> + Send> {
        let v3 = Vertex::new(1, None, DynDetails::default());
        let v4 = Vertex::new(4, None, DynDetails::default());
        let r3 = Record::new(v3, None);
        let r4 = Record::new(v4, None);
        Box::new(vec![r3, r4].into_iter())
    }

    fn join_test(join_kind: i32, expected_ids: Vec<ID>) {
        let conf = JobConf::new("join_test");
        let mut result = pegasus::run(conf, || {
            move |input, output| {
                let s1 = input.input_from(source_s1_gen())?;
                let s2 = input.input_from(source_s2_gen())?;
                let join_opr_pb = pb::Join {
                    left_keys: vec![common_pb::Variable::from("@.~id".to_string())],
                    right_keys: vec![common_pb::Variable::from("@.~id".to_string())],
                    kind: join_kind,
                };
                let left_key_selector = join_opr_pb.gen_left_kv_fn()?;
                let right_key_selector = join_opr_pb.gen_right_kv_fn()?;
                let join_kind = join_opr_pb.get_join_kind();
                let left_stream = s1
                    .key_by(move |record| left_key_selector.get_kv(record))?
                    // TODO(bingqing): remove this when new keyed-join in gaia-x is ready;
                    .partition_by_key();
                let right_stream = s2
                    .key_by(move |record| right_key_selector.get_kv(record))?
                    // TODO(bingqing): remove this when new keyed-join in gaia-x is ready;
                    .partition_by_key();

                let stream = match join_kind {
                    JoinKind::Inner => left_stream
                        .inner_join(right_stream)?
                        .map(|(left, right)| Ok(left.value.join(right.value, Some(true))))?,
                    JoinKind::LeftOuter => {
                        left_stream
                            .left_outer_join(right_stream)?
                            .map(|(left, right)| {
                                let left = left.unwrap();
                                if let Some(right) = right {
                                    Ok(left.value.join(right.value, Some(true)))
                                } else {
                                    Ok(left.value)
                                }
                            })?
                    }
                    JoinKind::RightOuter => left_stream
                        .right_outer_join(right_stream)?
                        .map(|(left, right)| {
                            let right = right.unwrap();
                            if let Some(left) = left {
                                Ok(left.value.join(right.value, Some(true)))
                            } else {
                                Ok(right.value)
                            }
                        })?,
                    JoinKind::FullOuter => {
                        left_stream
                            .full_outer_join(right_stream)?
                            .map(|(left, right)| match (left, right) {
                                (Some(left), Some(right)) => Ok(left.value.join(right.value, Some(true))),
                                (Some(left), None) => Ok(left.value),
                                (None, Some(right)) => Ok(right.value),
                                (None, None) => {
                                    unreachable!()
                                }
                            })?
                    }
                    JoinKind::Semi => left_stream
                        .semi_join(right_stream)?
                        .map(|left| Ok(left.value))?,
                    JoinKind::Anti => left_stream
                        .anti_join(right_stream)?
                        .map(|left| Ok(left.value))?,
                    JoinKind::Times => {
                        todo!()
                    }
                };
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        let mut result_ids = vec![];
        while let Some(Ok(record)) = result.next() {
            if let Some(element) = record.get(None).unwrap().as_graph_vertex() {
                result_ids.push(element.id());
            }
        }
        result_ids.sort();
        assert_eq!(result_ids, expected_ids);
    }

    #[test]
    fn inner_join_test() {
        let expected_ids = vec![1];
        join_test(0, expected_ids);
    }

    #[test]
    fn left_join_test() {
        let expected_ids = vec![1, 2];
        join_test(1, expected_ids);
    }

    #[test]
    fn right_join_test() {
        let expected_ids = vec![1, 4];
        join_test(2, expected_ids);
    }

    #[test]
    fn full_outer_join_test() {
        let expected_ids = vec![1, 2, 4];
        join_test(3, expected_ids);
    }

    #[test]
    fn semi_join_test() {
        let expected_ids = vec![1];
        join_test(4, expected_ids);
    }

    #[test]
    fn anti_join_test() {
        let expected_ids = vec![2];
        join_test(5, expected_ids);
    }
}
