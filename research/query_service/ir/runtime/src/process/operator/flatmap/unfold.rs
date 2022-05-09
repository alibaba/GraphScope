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
use crate::process::record::{Entry, Record, RecordElement, RecordExpandIter};

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
        let entry = input
            .take(self.tag.as_ref())
            .ok_or(FnExecError::get_tag_error(&format!("tag {:?} in UnfoldOperator", self.tag)))?;
        unsafe {
            let entry_ptr = Arc::into_raw(entry) as *mut Entry;
            match &mut *entry_ptr {
                Entry::Element(e) => match e {
                    RecordElement::OnGraph(GraphObject::P(p)) => {
                        let path = p
                            .get_path_mut()
                            .ok_or(FnExecError::unexpected_data_error(
                                "get path failed in UnfoldOperator",
                            ))?;
                        Ok(Box::new(RecordExpandIter::new(
                            input,
                            self.alias.as_ref(),
                            Box::new(path.drain(..)),
                        )))
                    }
                    _ => Err(FnExecError::unexpected_data_error(&format!(
                        "unfold entry {:?} in UnfoldOperator",
                        e
                    )))?,
                },
                Entry::Collection(collection) => Ok(Box::new(RecordExpandIter::new(
                    input,
                    self.alias.as_ref(),
                    Box::new(collection.drain(..)),
                ))),
            }
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
