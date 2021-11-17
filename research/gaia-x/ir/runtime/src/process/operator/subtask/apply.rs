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

use crate::error::{str_to_dyn_error, FnGenResult};
use crate::process::operator::subtask::RecordLeftJoinGen;
use crate::process::record::Record;
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::join::JoinKind;
use ir_common::NameOrId;
use pegasus::api::function::{BinaryFunction, FnResult};
use std::convert::TryFrom;

struct ApplyOperator {
    join_kind: JoinKind,
    alias: Option<NameOrId>,
}

impl BinaryFunction<Record, Vec<Record>, Option<Record>> for ApplyOperator {
    fn exec(&self, mut parent: Record, mut sub: Vec<Record>) -> FnResult<Option<Record>> {
        if sub.len() > 1 {
            return Err(str_to_dyn_error(
                "the length of result of subtask should be no larger than 1",
            ));
        }
        match self.join_kind {
            JoinKind::Inner => {
                if sub.len() == 0 {
                    return Ok(None);
                } else {
                    let sub_result = sub.get_mut(0).unwrap();
                    // TODO: 1. consider the situation of g.V().where(out().out()), that we do not need to append sub result;
                    // TODO: 2. is sub_entry always saved on head of Record? Or give the tag of sub_entry.
                    let sub_entry = sub_result
                        .take(None)
                        .ok_or(str_to_dyn_error("result of subtask is None"))?;
                    parent.append(sub_entry, self.alias.clone());
                    Ok(Some(parent))
                }
            }
            _ => {
                return Err(str_to_dyn_error(&format!(
                    "Do not support the join type {:?} in Apply",
                    self.join_kind
                )))
            }
        }
    }
}

impl RecordLeftJoinGen for algebra_pb::Apply {
    fn gen_subtask(
        self,
    ) -> FnGenResult<Box<dyn BinaryFunction<Record, Vec<Record>, Option<Record>>>> {
        let join_kind: JoinKind = unsafe { ::std::mem::transmute(self.join_kind) };
        let (alias_pb, _is_query_given_tag) = {
            let expr_alias = self
                .subtask
                .as_ref()
                .ok_or(ParsePbError::from("subtask is missing"))?
                .alias
                .as_ref()
                .ok_or(ParsePbError::from("expr_alias is missing"))?;
            (expr_alias.alias.clone(), expr_alias.is_query_given)
        };
        let alias = alias_pb
            .map(|tag_pb| NameOrId::try_from(tag_pb))
            .transpose()?;
        Ok(Box::new(ApplyOperator { join_kind, alias }))
    }
}
