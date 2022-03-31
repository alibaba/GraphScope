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

use std::convert::TryFrom;
use std::sync::Arc;

use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::join::JoinKind;
use ir_common::NameOrId;
use pegasus::api::function::{BinaryFunction, FnResult};

use crate::error::{FnExecError, FnGenError, FnGenResult};
use crate::process::functions::ApplyGen;
use crate::process::record::{CommonObject, Entry, Record};

#[derive(Debug)]
struct ApplyOperator {
    join_kind: JoinKind,
    alias: Option<NameOrId>,
}

impl BinaryFunction<Record, Vec<Record>, Option<Record>> for ApplyOperator {
    fn exec(&self, mut parent: Record, sub: Vec<Record>) -> FnResult<Option<Record>> {
        match self.join_kind {
            JoinKind::Inner => {
                if sub.is_empty() {
                    Ok(None)
                } else {
                    let sub_result = sub.get(0).unwrap();
                    // We assume the result of sub_entry is always saved on head of Record for now.
                    let sub_entry = sub_result
                        .get(None)
                        .ok_or(FnExecError::get_tag_error("get entry of subtask result failed"))?;
                    if let Some(alias) = self.alias.as_ref() {
                        // append sub_entry without moving head
                        let columns = parent.get_columns_mut();
                        columns.insert(alias.clone(), sub_entry.clone());
                    } else {
                        parent.append_arc_entry(sub_entry.clone(), None);
                    }
                    Ok(Some(parent))
                }
            }
            JoinKind::LeftOuter => {
                if sub.is_empty() {
                    let entry: Arc<Entry> = Arc::new((CommonObject::None).into());
                    if let Some(alias) = self.alias.as_ref() {
                        let columns = parent.get_columns_mut();
                        columns.insert(alias.clone(), entry.clone());
                    } else {
                        parent.append_arc_entry(entry.clone(), None);
                    }
                    Ok(Some(parent))
                } else {
                    let sub_result = sub.get(0).unwrap();
                    // We assume the result of sub_entry is always saved on head of Record for now.
                    let sub_entry = sub_result
                        .get(None)
                        .ok_or(FnExecError::get_tag_error("get entry of subtask result failed"))?;
                    if let Some(alias) = self.alias.as_ref() {
                        let columns = parent.get_columns_mut();
                        columns.insert(alias.clone(), sub_entry.clone());
                    } else {
                        parent.append_arc_entry(sub_entry.clone(), None);
                    }
                    Ok(Some(parent))
                }
            }
            _ => Err(FnExecError::UnSupported(format!(
                "Do not support the join type {:?} in Apply",
                self.join_kind
            )))?,
        }
    }
}

impl ApplyGen<Record, Vec<Record>, Option<Record>> for algebra_pb::Apply {
    fn get_join_kind(&self) -> JoinKind {
        unsafe { ::std::mem::transmute(self.join_kind) }
    }

    fn gen_left_join_func(
        &self,
    ) -> FnGenResult<Box<dyn BinaryFunction<Record, Vec<Record>, Option<Record>>>> {
        let join_kind: JoinKind = unsafe { ::std::mem::transmute(self.join_kind) };
        let alias = self
            .alias
            .as_ref()
            .map(|tag_pb| NameOrId::try_from(tag_pb.clone()))
            .transpose()?;
        match join_kind {
            JoinKind::Inner | JoinKind::LeftOuter | JoinKind::Semi | JoinKind::Anti => {}
            JoinKind::RightOuter | JoinKind::FullOuter | JoinKind::Times => {
                Err(FnGenError::unsupported_error(&format!(
                    "Do not support join_kind {:?} in Apply",
                    join_kind
                )))?
            }
        }
        let apply_operator = ApplyOperator { join_kind, alias };
        debug!("Runtime apply operator {:?}", apply_operator);
        Ok(Box::new(apply_operator))
    }
}
