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

use dyn_type::Object;
use graph_proxy::apis::{DynDetails, Element, Vertex};
use ir_common::generated::physical as pb;
use ir_common::KeyId;
use pegasus::api::function::{DynIter, FlatMapFunction, FnResult};
use pegasus_common::downcast::AsAny;

use crate::error::{FnExecError, FnGenResult};
use crate::process::entry::{CollectionEntry, Entry, EntryType};
use crate::process::operator::flatmap::FlatMapFuncGen;
use crate::process::operator::map::{GeneralIntersectionEntry, IntersectionEntry};
use crate::process::record::Record;

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
        let entry_type = input
            .get(self.tag)
            .ok_or_else(|| {
                FnExecError::get_tag_error(&format!(
                    "get tag {:?} from record in `Unfold` operator, the record is {:?}",
                    self.tag, input
                ))
            })?
            .get_type();
        match entry_type {
            EntryType::Intersection => {
                // Take the entry when it is EdgeExpandIntersection.
                // The reason is that the alias of the collection is system-given, which is used as a hint of intersection,
                // hence there's no need to preserve the collection anymore.
                let entry = input.take(self.tag.as_ref()).unwrap();
                if let Some(intersection) = entry
                    .as_any_ref()
                    .downcast_ref::<IntersectionEntry>()
                {
                    let len = intersection.len();
                    if len == 0 {
                        input.append(Object::None, self.alias);
                        Ok(Box::new(vec![input].into_iter()))
                    } else {
                        let mut res = Vec::with_capacity(len);
                        for item in intersection.iter().cloned() {
                            let mut new_entry = input.clone();
                            new_entry.append(Vertex::new(item, None, DynDetails::default()), self.alias);
                            res.push(new_entry);
                        }
                        Ok(Box::new(res.into_iter()))
                    }
                } else if let Some(general_intersection) = entry
                    .as_any_ref()
                    .downcast_ref::<GeneralIntersectionEntry>()
                {
                    let len = general_intersection.len();
                    if len == 0 {
                        input.append(Object::None, self.alias);
                        Ok(Box::new(vec![input].into_iter()))
                    } else {
                        let mut res = Vec::with_capacity(len);
                        for (vid, matchings) in general_intersection.matchings_iter() {
                            for matching in matchings {
                                let mut new_entry = input.clone();
                                for (column, tag) in matching {
                                    new_entry.append(column.clone(), Some(tag));
                                }
                                new_entry.append(Vertex::new(vid, None, DynDetails::default()), self.alias);
                                res.push(new_entry);
                            }
                        }
                        Ok(Box::new(res.into_iter()))
                    }
                } else {
                    Err(FnExecError::unexpected_data_error(
                        "downcast intersection entry in UnfoldOperator",
                    ))?
                }
            }
            EntryType::Collection => {
                let entry = input.get(self.tag).unwrap();
                let collection = entry
                    .as_any_ref()
                    .downcast_ref::<CollectionEntry>()
                    .ok_or_else(|| {
                        FnExecError::unexpected_data_error("downcast collection entry in UnfoldOperator")
                    })?;
                let mut res = Vec::with_capacity(collection.len());
                for item in collection.inner.iter().cloned() {
                    let mut new_entry = input.clone();
                    new_entry.append(item, self.alias);
                    res.push(new_entry);
                }
                Ok(Box::new(res.into_iter()))
            }
            EntryType::Path => {
                let entry = input.get(self.tag).unwrap();
                let path = entry.as_graph_path().ok_or_else(|| {
                    FnExecError::unexpected_data_error("downcast path entry in UnfoldOperatro")
                })?;
                let path_vec = if let Some(path) = path.get_path() {
                    path.clone()
                } else {
                    vec![path.get_path_end().clone()]
                };
                let mut res = Vec::with_capacity(path_vec.len());
                for item in path_vec {
                    let mut new_entry = input.clone();
                    new_entry.append(item, self.alias);
                    res.push(new_entry)
                }
                Ok(Box::new(res.into_iter()))
            }
            _ => Err(FnExecError::unexpected_data_error(&format!(
                "unfold entry {:?} in UnfoldOperator",
                input.get(self.tag)
            )))?,
        }
    }
}

impl FlatMapFuncGen for pb::Unfold {
    fn gen_flat_map(
        self,
    ) -> FnGenResult<Box<dyn FlatMapFunction<Record, Record, Target = DynIter<Record>>>> {
        let unfold_operator = UnfoldOperator { tag: self.tag, alias: self.alias };
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime unfold operator {:?}", unfold_operator);
        }
        Ok(Box::new(unfold_operator))
    }
}

#[cfg(test)]
mod tests {
    use graph_proxy::apis::graph::element::GraphElement;
    use ir_common::generated::common as common_pb;
    use ir_common::generated::physical as pb;
    use pegasus::api::{Fold, Map, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;
    use pegasus_common::downcast::AsAny;

    use crate::process::entry::CollectionEntry;
    use crate::process::entry::Entry;
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
            if let Some(v) = res.get(None).unwrap().as_vertex() {
                result_ids.push(v.id());
            }
            let collection = res
                .get(Some(TAG_A))
                .unwrap()
                .as_any_ref()
                .downcast_ref::<CollectionEntry>()
                .unwrap();
            assert!(collection.inner.len() == 2);
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
            if let Some(v) = res.get(None).unwrap().as_vertex() {
                result_ids.push(v.id());
            }
        }
        result_ids.sort();
        assert_eq!(result_ids, expected_result);
    }

    #[test]
    // g.V().fold().as('a').unfold(head)
    // in this case, after unfold by head, each result record still contains a collection tagged 'a'.
    fn fold_as_a_unfold_head_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 5, // ToList
            alias: Some(TAG_A.into()),
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function] };
        let unfold_opr_pb = pb::Unfold { tag: None, alias: None };

        let mut result = fold_unfold_test(fold_opr_pb, unfold_opr_pb);
        let expected_result = vec![1, 2];
        let mut result_ids = vec![];
        while let Some(Ok(res)) = result.next() {
            if let Some(v) = res.get(None).unwrap().as_vertex() {
                result_ids.push(v.id());
            }
            let entry = res.get(Some(TAG_A)).unwrap();
            let collection = entry
                .as_any_ref()
                .downcast_ref::<CollectionEntry>()
                .unwrap();
            assert!(collection.inner.len() == 2);
        }
        result_ids.sort();
        assert_eq!(result_ids, expected_result);
    }
}
