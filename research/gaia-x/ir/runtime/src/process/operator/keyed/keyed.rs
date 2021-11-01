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

use crate::process::functions::KeyFunction;
use crate::process::operator::keyed::KeyFunctionGen;
use crate::process::operator::TagKey;
use crate::process::record::{Entry, Record, RecordKey};
use ir_common::error::{str_to_dyn_error, DynResult, ParsePbError};
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::common as common_pb;
use pegasus::api::function::FnResult;
use std::convert::TryFrom;

pub struct KeySelector {
    keys: Vec<TagKey>,
}

impl KeySelector {
    pub fn with(keys_pb: Vec<common_pb::Variable>) -> Result<Self, ParsePbError> {
        let keys = keys_pb
            .into_iter()
            .map(|tag_key_pb| TagKey::try_from(tag_key_pb))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(KeySelector { keys })
    }
}

impl KeyFunction<Record, RecordKey, Record> for KeySelector {
    fn select_key(&self, mut input: Record) -> FnResult<(RecordKey, Record)> {
        let mut keys = Vec::with_capacity(self.keys.len());
        for key in self.keys.iter() {
            let key_entry = key
                .get_entry(&mut input)
                .map_err(|e| str_to_dyn_error(&format!("{}", e)))?;
            match key_entry {
                Entry::Element(key_element) => keys.push(key_element),
                Entry::Collection(_) => {
                    // TODO:
                    return Err(str_to_dyn_error(
                        "Do not support a Collection type of record key for now",
                    ));
                }
            }
        }
        Ok((RecordKey::new(keys), input))
    }
}

impl KeyFunctionGen for algebra_pb::GroupBy {
    fn gen_key(self) -> DynResult<Box<dyn KeyFunction<Record, RecordKey, Record>>> {
        Ok(Box::new(KeySelector::with(self.keys)?))
    }
}

impl KeyFunctionGen for algebra_pb::Dedup {
    fn gen_key(self) -> DynResult<Box<dyn KeyFunction<Record, RecordKey, Record>>> {
        Ok(Box::new(KeySelector::with(self.keys)?))
    }
}
