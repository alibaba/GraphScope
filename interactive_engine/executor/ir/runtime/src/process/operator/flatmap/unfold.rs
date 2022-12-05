//
//! Copyright 2022 Alibaba Group Holding Limited.
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
use std::sync::Arc;

use ir_common::generated::algebra as algebra_pb;
use ir_common::KeyId;
use pegasus::api::function::{DynIter, FlatMapFunction, FnResult};

use crate::error::{FnExecError, FnGenResult};
use crate::process::operator::flatmap::FlatMapFuncGen;
use crate::process::record::{Entry, Record};

#[derive(Debug)]
/// Unfold the Collection entry referred by a given `tag`.
/// Notice that unfold will remove the Collection entry from the Record,
/// and append items in collection as new entries.
pub struct UnfoldOperator {
    tag: Option<KeyId>,
    alias: Option<KeyId>,
}

impl FlatMapFunction<Record, Record> for UnfoldOperator {
    type Target = DynIter<Record>;

    fn exec(&self, mut input: Record) -> FnResult<Self::Target> {
        // Consider 'take' the column since the collection won't be used anymore in most cases.
        // e.g., in EdgeExpandIntersection case, we only set alias of the collection to give the hint of intersection.
        let mut entry = input
            .take(self.tag.as_ref())
            .ok_or(FnExecError::get_tag_error(&format!(
                "get tag {:?} from record in `Unfold` operator, the record is {:?}",
                self.tag, input
            )))?;
        // take head in case that head entry is an arc clone of `self.tag`;
        // besides, head will be replaced by the items in collections anyway.
        input.take(None);
        if let Some(entry) = Arc::get_mut(&mut entry) {
            match entry {
                Entry::Collection(collection) => {
                    let mut res = Vec::with_capacity(collection.len());
                    for item in collection.drain(..) {
                        let mut new_entry = input.clone();
                        new_entry.append(item, self.alias);
                        res.push(new_entry);
                    }
                    Ok(Box::new(res.into_iter()))
                }
                Entry::Intersection(intersection) => {
                    let mut res = Vec::with_capacity(intersection.len());
                    for item in intersection.iter() {
                        let mut new_entry = input.clone();
                        new_entry.append(item.clone(), self.alias);
                        res.push(new_entry);
                    }
                    Ok(Box::new(res.into_iter()))
                }
                Entry::P(_) => {
                    // TODO: to support path_unwinding
                    Err(FnExecError::unsupported_error(&format!(
                        "unfold path entry {:?} in UnfoldOperator",
                        entry
                    )))?
                }
                _ => Err(FnExecError::unexpected_data_error(&format!(
                    "unfold entry {:?} in UnfoldOperator",
                    entry
                )))?,
            }
        } else {
            Err(FnExecError::unexpected_data_error(&format!(
                "get mutable entry {:?} of tag {:?} failed in UnfoldOperator",
                entry,
                self.tag.as_ref()
            )))?
        }
    }
}

impl FlatMapFuncGen for algebra_pb::Unfold {
    fn gen_flat_map(
        self,
    ) -> FnGenResult<Box<dyn FlatMapFunction<Record, Record, Target = DynIter<Record>>>> {
        let tag = self.tag.map(|tag| tag.try_into()).transpose()?;
        let alias = self
            .alias
            .map(|alias| alias.try_into())
            .transpose()?;
        let unfold_operator = UnfoldOperator { tag, alias };
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime unfold operator {:?}", unfold_operator);
        }
        Ok(Box::new(unfold_operator))
    }
}

#[cfg(test)]
mod tests {
    use graph_proxy::apis::graph::element::GraphElement;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use pegasus::api::{Fold, Map, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;

    use crate::process::functions::FoldGen;
    use crate::process::operator::accum::accumulator::Accumulator;
    use crate::process::operator::flatmap::FlatMapFuncGen;
    use crate::process::operator::tests::{init_source, TAG_A};
    use crate::process::record::Record;

    fn fold_unfold_test(fold_opr_pb: pb::GroupBy, unfold_opr_pb: pb::Unfold) -> ResultStream<Record> {
        let conf = JobConf::new("fold_unfold_test");
        pegasus::run(conf, || {
            let source = init_source().clone();
            let fold_opr = fold_opr_pb.clone();
            let unfold_opr = unfold_opr_pb.clone();
            move |input, output| {
                let mut stream = input.input_from(source.into_iter())?;
                let fold_accum = fold_opr.gen_fold_accum()?;
                let unfold = unfold_opr.gen_flat_map()?;
                stream = stream
                    .fold(fold_accum, || {
                        |mut accumulator, next| {
                            accumulator.accum(next)?;
                            Ok(accumulator)
                        }
                    })?
                    .map(move |mut accum| Ok(accum.finalize()?))?
                    .into_stream()?
                    .flat_map(move |e| unfold.exec(e))?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure")
    }

    #[test]
    // g.V().fold().as('a').unfold('a')
    fn fold_as_a_unfold_a_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 5, // ToList
            alias: Some(TAG_A.into()),
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function] };
        let unfold_opr_pb = pb::Unfold { tag: Some(TAG_A.into()), alias: None };
        let mut result = fold_unfold_test(fold_opr_pb, unfold_opr_pb);

        let expected_result = vec![1, 2];
        let mut result_ids = vec![];
        while let Some(Ok(res)) = result.next() {
            if let Some(v) = res.get(None).unwrap().as_graph_vertex() {
                result_ids.push(v.id());
            }
        }
        result_ids.sort();
        assert_eq!(result_ids, expected_result);
    }

    #[test]
    // g.V().fold().unfold()
    fn fold_as_head_unfold_head_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 5, // ToList
            alias: None,
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function] };
        let unfold_opr_pb = pb::Unfold { tag: None, alias: None };
        let mut result = fold_unfold_test(fold_opr_pb, unfold_opr_pb);

        let expected_result = vec![1, 2];
        let mut result_ids = vec![];
        while let Some(Ok(res)) = result.next() {
            if let Some(v) = res.get(None).unwrap().as_graph_vertex() {
                result_ids.push(v.id());
            }
        }
        result_ids.sort();
        assert_eq!(result_ids, expected_result);
    }

    #[test]
    // g.V().fold().as('a').unfold(head)
    // This is not expected, since we can only unfold 'head', while collection tagged 'a' is still in the record.
    fn fold_as_a_unfold_head_fail_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 5, // ToList
            alias: Some(TAG_A.into()),
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function] };
        let unfold_opr_pb = pb::Unfold { tag: None, alias: None };

        let mut result = fold_unfold_test(fold_opr_pb, unfold_opr_pb);
        if let Some(result) = result.next() {
            match result {
                Ok(_) => {
                    assert!(false)
                }
                Err(_) => {
                    assert!(true)
                }
            }
        }
    }
}
