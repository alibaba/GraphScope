//
//! Copyright 2023 Alibaba Group Holding Limited.
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

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use pegasus::api::function::FilterFunction;
use pegasus::api::function::FnResult;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};

use crate::error::FnGenError;
use crate::error::FnGenResult;
use crate::process::operator::filter::FilterFuncGen;
use crate::process::record::Record;

#[derive(Debug)]
struct CoinOperator {
    seed: Option<i32>,
    ratio: f64,
}

impl FilterFunction<Record> for CoinOperator {
    fn test(&self, _input: &Record) -> FnResult<bool> {
        let mut rng = if let Some(seed) = self.seed {
            StdRng::seed_from_u64(seed as u64)
        } else {
            StdRng::from_entropy()
        };
        if rng.gen_bool(self.ratio) {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl FilterFuncGen for algebra_pb::Sample {
    fn gen_filter(self) -> FnGenResult<Box<dyn FilterFunction<Record>>> {
        if let Some(sample_type) = self.sample_type {
            let sample_type = sample_type.inner.ok_or_else(|| {
                FnGenError::ParseError(ParsePbError::EmptyFieldError("sample_type.inner".to_owned()))
            })?;
            match sample_type {
                algebra_pb::sample::sample_type::Inner::SampleByRatio(ratio) => {
                    if ratio.ratio < 0.0 || ratio.ratio > 1.0 {
                        return Err(FnGenError::ParseError(
                            "SampleByRatio ratio should be in [0, 1]".into(),
                        ));
                    }
                    let coin = CoinOperator { seed: self.seed, ratio: ratio.ratio };
                    if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
                        debug!("Runtime coin operator: {:?}", coin);
                    }
                    Ok(Box::new(coin))
                }
                algebra_pb::sample::sample_type::Inner::SampleByNum(_num) => {
                    Err(FnGenError::ParseError("SampleByNum should be a fold function".into()))
                }
            }
        } else {
            Err(FnGenError::ParseError(ParsePbError::EmptyFieldError("sample_type".to_owned())))
        }
    }
}
