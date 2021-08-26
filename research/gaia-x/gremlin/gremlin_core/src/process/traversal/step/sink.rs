//
//! Copyright 2020 Alibaba Group Holding Limited.
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

use crate::functions::EncodeFunction;
use crate::process::traversal::traverser::Traverser;
use crate::result_process::result_to_pb;
use pegasus::api::function::FnResult;
use prost::Message;

pub struct TraverserSinkEncoder;

impl EncodeFunction<Traverser> for TraverserSinkEncoder {
    fn encode(&self, data: Traverser) -> FnResult<Vec<u8>> {
        let result_pb = result_to_pb(data);
        let mut bytes = vec![];
        result_pb.encode_raw(&mut bytes);
        Ok(bytes)
    }
}
