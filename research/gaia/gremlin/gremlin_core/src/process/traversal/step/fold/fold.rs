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

use crate::generated::common as common_pb;
use crate::generated::protobuf as pb_result;
use crate::process::traversal::traverser::Traverser;
use crate::str_to_dyn_error;
use pegasus::api::accum::{AccumFactory, Accumulator};
use pegasus::api::function::{DynIter, EncodeFunction, FlatMapFunction, FnResult};
use pegasus_common::downcast::AsAny;
use pegasus_server::factory::{CompileResult, FoldFunction};
use prost::Message;

pub struct FoldFunc {}
struct FoldUnfold {}
struct FoldSink {}

type DynFoldUnfold = Box<
    dyn FlatMapFunction<Box<dyn Accumulator<Traverser>>, Traverser, Target = DynIter<Traverser>>,
>;

impl FoldFunction<Traverser> for FoldFunc {
    // TODO(yyy)
    fn accumulate(
        &self,
    ) -> CompileResult<Box<dyn AccumFactory<Traverser, Target = Box<dyn Accumulator<Traverser>>>>>
    {
        unimplemented!()
    }

    fn fold_unfold(&self) -> CompileResult<DynFoldUnfold> {
        let fold_unfold = FoldUnfold {};
        Ok(Box::new(fold_unfold) as DynFoldUnfold)
    }

    fn fold_sink(&self) -> CompileResult<Box<dyn EncodeFunction<Box<dyn Accumulator<Traverser>>>>> {
        let count_sink = FoldSink {};
        Ok(Box::new(count_sink) as Box<dyn EncodeFunction<Box<dyn Accumulator<Traverser>>>>)
    }
}

impl FlatMapFunction<Box<dyn Accumulator<Traverser>>, Traverser> for FoldUnfold {
    type Target = DynIter<Traverser>;

    fn exec(&self, input: Box<dyn Accumulator<Traverser>>) -> FnResult<Self::Target> {
        if let Some(count) = input.as_any_ref().downcast_ref::<u64>() {
            let result = vec![Ok(Traverser::Object((*count).into()))];
            Ok(Box::new(result.into_iter()) as DynIter<Traverser>)
        } else {
            // TODO: for other fold-unfold cases
            Err(str_to_dyn_error("Unimplemented fold-unfold cases"))
        }
    }
}

impl EncodeFunction<Box<dyn Accumulator<Traverser>>> for FoldSink {
    fn encode(&self, data: Vec<Box<dyn Accumulator<Traverser>>>) -> Vec<u8> {
        for datum in data {
            if let Some(count) = datum.as_any_ref().downcast_ref::<u64>() {
                println!("count result {:?}", count);
                let val_item = common_pb::value::Item::I64(*count as i64);
                let result_pb = pb_result::Result {
                    inner: Some(pb_result::result::Inner::Value {
                        0: common_pb::Value { item: Some(val_item) },
                    }),
                };
                let mut bytes = vec![];
                result_pb.encode_raw(&mut bytes);
                return bytes;
            } else {
                // TODO: for other fold-sink cases
                unimplemented!()
            }
        }
        vec![]
    }
}
