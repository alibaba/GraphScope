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

use crate::generated::protobuf as result_pb;
use crate::process::traversal::step::accum::Accumulator;
use crate::process::traversal::step::functions::EncodeFunction;
use crate::process::traversal::step::result_process::{pair_element_to_pb, result_to_pb};
use crate::process::traversal::step::TraverserAccumulator;
use crate::process::traversal::traverser::Traverser;
use pegasus::api::function::FnResult;
use pegasus::codec::{ReadExt, WriteExt};
use pegasus_common::codec::{Decode, Encode};
use prost::Message;
use std::collections::HashMap;

pub struct TraverserSinkEncoder;

impl Encode for result_pb::Result {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        let mut bytes = vec![];
        self.encode_raw(&mut bytes);
        writer.write_u32(bytes.len() as u32)?;
        writer.write_all(bytes.as_slice())?;
        Ok(())
    }
}

impl Decode for result_pb::Result {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let len = reader.read_u32()? as usize;
        let mut buffer = Vec::with_capacity(len);
        reader.read_exact(&mut buffer)?;
        result_pb::Result::decode(buffer.as_slice()).map_err(|_e| {
            std::io::Error::new(std::io::ErrorKind::Other, "decoding result_pb failed!")
        })
    }
}

impl EncodeFunction<Traverser, result_pb::Result> for TraverserSinkEncoder {
    fn encode(&self, data: Traverser) -> FnResult<result_pb::Result> {
        let result_pb = result_to_pb(data);
        Ok(result_pb)
    }
}

impl EncodeFunction<HashMap<Traverser, TraverserAccumulator>, result_pb::Result>
    for TraverserSinkEncoder
{
    fn encode(
        &self, data: HashMap<Traverser, TraverserAccumulator>,
    ) -> FnResult<result_pb::Result> {
        let mut pairs_encode = vec![];
        for (k, mut accum) in data {
            let key_pb = pair_element_to_pb(&k);
            let value_pb = pair_element_to_pb(&accum.finalize());
            let map_pair_pb = result_pb::MapPair { first: Some(key_pb), second: Some(value_pb) };
            pairs_encode.push(map_pair_pb);
        }
        let map = result_pb::MapArray { item: pairs_encode };
        let result_pb = result_pb::Result { inner: Some(result_pb::result::Inner::MapResult(map)) };
        Ok(result_pb)
    }
}
