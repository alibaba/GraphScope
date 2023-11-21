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
use pegasus::api::function::DynIter;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};

use crate::error::{FnExecResult, FnGenError, FnGenResult};
use crate::process::operator::accum::accumulator::Accumulator;
use crate::process::operator::accum::SampleAccumFactoryGen;
use crate::process::record::Record;

/// Sample accumulator, which will keep a sampled vector of records, with the specified sample number.
/// Implemented via Reservoir Sampling.
#[derive(Clone, Debug)]
pub struct SampleAccum {
    accumulator: Vec<Record>,
    count: usize,
    sample_num: usize,
    rng: StdRng,
    seed: Option<u64>,
}

impl Accumulator<Record, DynIter<Record>> for SampleAccum {
    fn accum(&mut self, next: Record) -> FnExecResult<()> {
        if self.count < self.sample_num {
            self.accumulator.push(next);
        } else {
            let index = self.rng.gen_range(0..=self.count);
            if index < self.sample_num {
                self.accumulator[index] = next;
            }
        }
        self.count = self.count + 1;
        Ok(())
    }

    fn finalize(&mut self) -> FnExecResult<DynIter<Record>> {
        let collection = std::mem::replace(&mut self.accumulator, vec![]);
        self.count = 0;
        Ok(Box::new(collection.into_iter()))
    }
}

impl SampleAccumFactoryGen for algebra_pb::Sample {
    fn gen_accum(self) -> FnGenResult<SampleAccum> {
        if let Some(sample_type) = self.sample_type {
            let sample_type = sample_type.inner.ok_or_else(|| {
                FnGenError::ParseError(ParsePbError::EmptyFieldError("sample_type.inner".to_owned()))
            })?;
            match sample_type {
                algebra_pb::sample::sample_type::Inner::SampleByNum(num) => {
                    let sample = SampleAccum {
                        sample_num: num.num as usize,
                        accumulator: Vec::with_capacity(num.num as usize),
                        count: 0,
                        rng: if let Some(seed) = self.seed {
                            StdRng::seed_from_u64(seed as u64)
                        } else {
                            StdRng::from_entropy()
                        },
                        seed: self.seed.map(|s| s as u64),
                    };
                    if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
                        debug!("Runtime sample operator: {:?}", sample);
                    }
                    Ok(sample)
                }
                algebra_pb::sample::sample_type::Inner::SampleByRatio(_) => {
                    Err(FnGenError::ParseError("SampleByRatio should be a filter function".into()))
                }
            }
        } else {
            Err(FnGenError::ParseError(ParsePbError::EmptyFieldError("sample_type".to_owned())))
        }
    }
}

impl Encode for SampleAccum {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.accumulator.write_to(writer)?;
        writer.write_u64(self.count as u64)?;
        writer.write_u64(self.sample_num as u64)?;
        self.seed.write_to(writer)?;
        Ok(())
    }
}

impl Decode for SampleAccum {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let accumulator = <Vec<Record>>::read_from(reader)?;
        let count = reader.read_u64()? as usize;
        let sample_num = reader.read_u64()? as usize;
        let seed = Option::<u64>::read_from(reader)?;
        let rng = if let Some(seed) = seed { StdRng::seed_from_u64(seed) } else { StdRng::from_entropy() };
        Ok(SampleAccum { accumulator, count, sample_num, rng, seed })
    }
}
