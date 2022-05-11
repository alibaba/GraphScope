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
use crate::graph::element::GraphObject;
use crate::process::operator::flatmap::FlatMapFuncGen;
use crate::process::record::{Entry, Record, RecordElement};

#[derive(Debug)]
pub struct UnfoldOperator {
    tag: Option<KeyId>,
    alias: Option<KeyId>,
}

impl FlatMapFunction<Record, Record> for UnfoldOperator {
    type Target = DynIter<Record>;

    fn exec(&self, mut input: Record) -> FnResult<Self::Target> {
        // Consider 'take' the column since the collection won't be used anymore in most cases.
        // e.g., in EdgeExpandIntersection case, we only set alias of the collection to give the hint of intersection.
        // TODO: This may be an opt for other operators as well, as long as the tag is not in need anymore.
        // A hint of "erasing the tag" maybe a better way (rather than assuming tag is not needed here).
        let mut entry = input
            .take(self.tag.as_ref())
            .ok_or(FnExecError::get_tag_error(&format!("tag {:?} in UnfoldOperator", self.tag)))?;
        if let Some(entry) = Arc::get_mut(&mut entry) {
            match entry {
                Entry::Element(e) => match e {
                    RecordElement::OnGraph(GraphObject::P(_graph_path)) => {
                        // let path = graph_path.get_path_mut().ok_or(FnExecError::unexpected_data_error(
                        //     "get path failed in UnfoldOperator",
                        // ))?;
                        // let mut res = Vec::with_capacity(path.len());
                        // for item in path.drain(..) {
                        //     let mut new_entry = input.clone();
                        //     new_entry.append(item, self.alias);
                        //     res.push(new_entry);
                        // }
                        // Ok(Box::new(res.into_iter()))
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
                },
                Entry::Collection(collection) => {
                    let mut res = Vec::with_capacity(collection.len());
                    for item in collection.drain(..) {
                        let mut new_entry = input.clone();
                        new_entry.append(item, self.alias);
                        res.push(new_entry);
                    }
                    Ok(Box::new(res.into_iter()))
                }
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
        debug!("Runtime unfold operator {:?}", unfold_operator);
        Ok(Box::new(unfold_operator))
    }
}

#[cfg(test)]
mod tests {
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use pegasus::api::{Fold, Map, Sink};
    use pegasus::JobConf;

    use crate::graph::element::GraphElement;
    use crate::process::functions::FoldGen;
    use crate::process::operator::accum::accumulator::Accumulator;
    use crate::process::operator::flatmap::FlatMapFuncGen;
    use crate::process::operator::tests::{init_source, TAG_A};

    #[test]
    fn unfold_fold_test() {
        let conf = JobConf::new("unfold_test");
        // g.V().fold().as('a')
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 5, // ToList
            alias: Some(TAG_A.into()),
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function] };
        let unfold_opr_pb = pb::Unfold { tag: Some(TAG_A.into()), alias: None };
        let mut result = pegasus::run(conf, || {
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
        .expect("build job failure");

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
}
