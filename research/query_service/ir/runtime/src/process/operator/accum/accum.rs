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

use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::group_by::agg_func::Aggregate;
use ir_common::NameOrId;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::error::{FnExecError, FnExecResult, FnGenError, FnGenResult};
use crate::process::operator::accum::accumulator::{Accumulator, Count, ToList};
use crate::process::operator::accum::AccumFactoryGen;
use crate::process::operator::TagKey;
use crate::process::record::{Entry, ObjectElement, Record};

#[derive(Debug, Clone)]
pub enum EntryAccumulator {
    // TODO(bingqing): more accum kind
    ToCount(Count<Arc<Entry>>),
    ToList(ToList<Arc<Entry>>),
}

#[derive(Debug, Clone)]
pub struct RecordAccumulator {
    accum_ops: Vec<(EntryAccumulator, TagKey, NameOrId)>,
}

impl Accumulator<Record, Record> for RecordAccumulator {
    fn accum(&mut self, mut next: Record) -> FnExecResult<()> {
        for (accumulator, tag_key, _) in self.accum_ops.iter_mut() {
            let entry = tag_key.get_entry(&mut next)?;
            accumulator.accum(entry)?;
        }
        Ok(())
    }

    fn finalize(&mut self) -> FnExecResult<Record> {
        let mut record = Record::default();
        for (accumulator, _, alias) in self.accum_ops.iter_mut() {
            let entry = accumulator.finalize()?;
            record.append_arc_entry(entry, Some(alias.clone()));
        }
        Ok(record)
    }
}

impl Accumulator<Arc<Entry>, Arc<Entry>> for EntryAccumulator {
    fn accum(&mut self, next: Arc<Entry>) -> FnExecResult<()> {
        match self {
            EntryAccumulator::ToCount(count) => count.accum(next),
            EntryAccumulator::ToList(list) => list.accum(next),
        }
    }

    fn finalize(&mut self) -> FnExecResult<Arc<Entry>> {
        match self {
            EntryAccumulator::ToCount(count) => {
                let cnt = count.finalize()?;
                Ok(Arc::new(ObjectElement::Count(cnt).into()))
            }
            EntryAccumulator::ToList(list) => {
                let list_entry = list
                    .finalize()?
                    .into_iter()
                    .map(|entry| match entry.as_ref() {
                        Entry::Element(e) => Ok(e.clone()),
                        Entry::Collection(_) => {
                            Err(FnExecError::unsupported_error("fold collections is not supported yet"))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(Entry::Collection(list_entry)))
            }
        }
    }
}

impl AccumFactoryGen for algebra_pb::GroupBy {
    fn gen_accum(self) -> FnGenResult<RecordAccumulator> {
        let mut accum_ops = Vec::with_capacity(self.functions.len());
        for agg_func in self.functions {
            let agg_kind: algebra_pb::group_by::agg_func::Aggregate =
                unsafe { ::std::mem::transmute(agg_func.aggregate) };
            if agg_func.vars.len() > 1 {
                // e.g., count_distinct((a,b));
                // TODO: to support this, we may need to define MultiTagKey (could define TagKey Trait, and impl for SingleTagKey and MultiTagKey)
                Err(FnGenError::unsupported_error("Do not support to aggregate multiple fields yet"))?
            }
            let tag_key = agg_func
                .vars
                .get(0)
                .map(|v| TagKey::try_from(v.clone()))
                .transpose()?
                .unwrap_or(TagKey::default());
            let alias: Option<NameOrId> = agg_func
                .alias
                .ok_or(ParsePbError::from("accum value alias is missing in group"))?
                .try_into()?;
            let alias = alias.ok_or(ParsePbError::from("accum value alias cannot be None in group"))?;
            let entry_accumulator = match agg_kind {
                Aggregate::Count => EntryAccumulator::ToCount(Count { value: 0, _ph: Default::default() }),
                Aggregate::ToList => EntryAccumulator::ToList(ToList { inner: vec![] }),
                _ => Err(FnGenError::unsupported_error(&format!(
                    "Unsupported aggregate kind {:?}",
                    agg_kind
                )))?,
            };
            accum_ops.push((entry_accumulator, tag_key, alias));
        }
        debug!("Runtime accumulator operator: {:?}", accum_ops);
        Ok(RecordAccumulator { accum_ops })
    }
}

impl Encode for RecordAccumulator {
    fn write_to<W: WriteExt>(&self, _writer: &mut W) -> std::io::Result<()> {
        todo!()
    }
}

impl Decode for RecordAccumulator {
    fn read_from<R: ReadExt>(_reader: &mut R) -> std::io::Result<Self> {
        todo!()
    }
}
