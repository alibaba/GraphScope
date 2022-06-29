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

use std::collections::HashSet;
use std::convert::{TryFrom, TryInto};

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::group_by::agg_func::Aggregate;
use ir_common::KeyId;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::error::{FnExecError, FnExecResult, FnGenError, FnGenResult};
use crate::process::operator::accum::accumulator::{
    Accumulator, Count, DistinctCount, Maximum, Minimum, ToList, ToSet,
};
use crate::process::operator::accum::AccumFactoryGen;
use crate::process::operator::TagKey;
use crate::process::record::{CommonObject, Entry, Record};

#[derive(Debug, Clone)]
pub enum EntryAccumulator {
    // TODO(bingqing): more accum kind
    ToCount(Count<()>),
    ToList(ToList<Entry>),
    ToMin(Minimum<Entry>),
    ToMax(Maximum<Entry>),
    ToSet(ToSet<Entry>),
    ToDistinctCount(DistinctCount<Entry>),
}

#[derive(Debug, Clone)]
pub struct RecordAccumulator {
    accum_ops: Vec<(EntryAccumulator, TagKey, KeyId)>,
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
            record.append(entry, Some(alias.clone()));
        }
        Ok(record)
    }
}

impl Accumulator<Entry, Entry> for EntryAccumulator {
    fn accum(&mut self, next: Entry) -> FnExecResult<()> {
        match self {
            EntryAccumulator::ToCount(count) => count.accum(()),
            EntryAccumulator::ToList(list) => list.accum(next),
            EntryAccumulator::ToMin(min) => min.accum(next),
            EntryAccumulator::ToMax(max) => max.accum(next),
            EntryAccumulator::ToSet(set) => set.accum(next),
            EntryAccumulator::ToDistinctCount(distinct_count) => distinct_count.accum(next),
        }
    }

    fn finalize(&mut self) -> FnExecResult<Entry> {
        match self {
            EntryAccumulator::ToCount(count) => {
                let cnt = count.finalize()?;
                Ok(CommonObject::Count(cnt).into())
            }
            EntryAccumulator::ToList(list) => {
                let list_entry = list
                    .finalize()?
                    .into_iter()
                    .map(|entry| match entry {
                        Entry::Element(e) => Ok(e.clone()),
                        Entry::Collection(_) => {
                            Err(FnExecError::unsupported_error("fold collections is not supported yet"))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Entry::Collection(list_entry))
            }
            EntryAccumulator::ToMin(min) => min
                .finalize()?
                .ok_or(FnExecError::accum_error("min_entry is none")),
            EntryAccumulator::ToMax(max) => max
                .finalize()?
                .ok_or(FnExecError::accum_error("max_entry is none")),
            EntryAccumulator::ToSet(set) => {
                let set_entry = set
                    .finalize()?
                    .into_iter()
                    .map(|entry| match entry {
                        Entry::Element(e) => Ok(e.clone()),
                        Entry::Collection(_) => {
                            Err(FnExecError::unsupported_error("set of collections is not supported yet"))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Entry::Collection(set_entry))
            }
            EntryAccumulator::ToDistinctCount(distinct_count) => {
                let cnt = distinct_count.finalize()?;
                Ok(CommonObject::Count(cnt).into())
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
            let alias = Some(
                agg_func
                    .alias
                    .ok_or(ParsePbError::from("accum value alias is missing"))?
                    .try_into()?,
            );
            // TODO: accum value alias in fold can be None;
            let alias = alias.ok_or(ParsePbError::from("accum value alias cannot be None"))?;
            let entry_accumulator = match agg_kind {
                Aggregate::Count => EntryAccumulator::ToCount(Count { value: 0, _ph: Default::default() }),
                Aggregate::ToList => EntryAccumulator::ToList(ToList { inner: vec![] }),
                Aggregate::Min => EntryAccumulator::ToMin(Minimum { min: None }),
                Aggregate::Max => EntryAccumulator::ToMax(Maximum { max: None }),
                Aggregate::ToSet => EntryAccumulator::ToSet(ToSet { inner: HashSet::new() }),
                Aggregate::CountDistinct => {
                    EntryAccumulator::ToDistinctCount(DistinctCount { inner: HashSet::new() })
                }
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

impl Encode for EntryAccumulator {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            EntryAccumulator::ToCount(count) => {
                writer.write_u8(0)?;
                count.write_to(writer)?;
            }
            EntryAccumulator::ToList(list) => {
                writer.write_u8(1)?;
                list.write_to(writer)?;
            }
            EntryAccumulator::ToMin(min) => {
                writer.write_u8(2)?;
                min.write_to(writer)?;
            }
            EntryAccumulator::ToMax(max) => {
                writer.write_u8(3)?;
                max.write_to(writer)?;
            }
            EntryAccumulator::ToSet(set) => {
                writer.write_u8(4)?;
                set.write_to(writer)?;
            }
            EntryAccumulator::ToDistinctCount(distinct_count) => {
                writer.write_u8(5)?;
                distinct_count.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for EntryAccumulator {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let e = reader.read_u8()?;
        match e {
            0 => {
                let cnt = <Count<()>>::read_from(reader)?;
                Ok(EntryAccumulator::ToCount(cnt))
            }
            1 => {
                let list = <ToList<Entry>>::read_from(reader)?;
                Ok(EntryAccumulator::ToList(list))
            }
            2 => {
                let min = <Minimum<Entry>>::read_from(reader)?;
                Ok(EntryAccumulator::ToMin(min))
            }
            3 => {
                let max = <Maximum<Entry>>::read_from(reader)?;
                Ok(EntryAccumulator::ToMax(max))
            }
            4 => {
                let set = <ToSet<Entry>>::read_from(reader)?;
                Ok(EntryAccumulator::ToSet(set))
            }
            5 => {
                let distinct_count = <DistinctCount<Entry>>::read_from(reader)?;
                Ok(EntryAccumulator::ToDistinctCount(distinct_count))
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
        }
    }
}

impl Encode for RecordAccumulator {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u32(self.accum_ops.len() as u32)?;
        for (accumulator, tag_key, alias) in self.accum_ops.iter() {
            accumulator.write_to(writer)?;
            tag_key.write_to(writer)?;
            alias.write_to(writer)?
        }
        Ok(())
    }
}

impl Decode for RecordAccumulator {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let len = reader.read_u32()?;
        let mut accum_ops = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let accumulator = <EntryAccumulator>::read_from(reader)?;
            let tag_key = <TagKey>::read_from(reader)?;
            let alias = <KeyId>::read_from(reader)?;
            accum_ops.push((accumulator, tag_key, alias));
        }
        Ok(RecordAccumulator { accum_ops })
    }
}

#[cfg(test)]
mod tests {

    use std::borrow::BorrowMut;
    use std::cmp::Ordering;

    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use pegasus::api::{Fold, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;

    use crate::process::operator::accum::accumulator::Accumulator;
    use crate::process::operator::accum::AccumFactoryGen;
    use crate::process::operator::tests::{init_source, init_vertex1, init_vertex2, TAG_A, TAG_B};
    use crate::process::record::{CommonObject, Entry, Record, RecordElement};

    fn fold_test(source: Vec<Record>, fold_opr_pb: pb::GroupBy) -> ResultStream<Record> {
        let conf = JobConf::new("fold_test");
        let result = pegasus::run(conf, || {
            let fold_opr_pb = fold_opr_pb.clone();
            let source = source.clone();
            move |input, output| {
                let stream = input.input_from(source.into_iter())?;
                let accum = fold_opr_pb.clone().gen_accum().unwrap();
                let res_stream = stream
                    .fold(accum, || {
                        move |mut accumulator, next| {
                            accumulator.accum(next)?;
                            Ok(accumulator)
                        }
                    })?
                    .map(|mut accum| Ok(accum.finalize()?))?
                    .into_stream()?;
                res_stream.sink_into(output)
            }
        })
        .expect("build job failure");

        result
    }

    // g.V().fold().as("a")
    #[test]
    fn fold_to_list_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 5, // to_list
            alias: Some(TAG_A.into()),
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function] };
        let mut result = fold_test(init_source(), fold_opr_pb);
        let mut fold_result = Entry::Collection(vec![]);
        let expected_result = Entry::Collection(vec![
            RecordElement::OnGraph(init_vertex1().into()),
            RecordElement::OnGraph(init_vertex2().into()),
        ]);
        if let Some(Ok(record)) = result.next() {
            if let Some(entry) = record.get(Some(TAG_A)) {
                fold_result = entry.as_ref().clone();
            }
        }
        assert_eq!(fold_result, expected_result);
    }

    // g.V().count().as("a") // unoptimized version, use accumulator directly
    #[test]
    fn count_unopt_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 3, // count
            alias: Some(TAG_A.into()),
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function] };
        let mut result = fold_test(init_source(), fold_opr_pb);
        let mut cnt = 0;
        if let Some(Ok(record)) = result.next() {
            if let Some(entry) = record.get(Some(TAG_A)) {
                cnt = match entry.as_ref() {
                    Entry::Element(RecordElement::OffGraph(CommonObject::Count(cnt))) => *cnt,
                    _ => {
                        unreachable!()
                    }
                };
            }
        }
        assert_eq!(cnt, 2);
    }

    // g.V().fold(to_list().as("a"), count().as("b"))
    #[test]
    fn fold_multi_accum_test() {
        let function_1 = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 5, // to_list
            alias: Some(TAG_A.into()),
        };
        let function_2 = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 3, // Count
            alias: Some(TAG_B.into()),
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function_1, function_2] };
        let mut result = fold_test(init_source(), fold_opr_pb);
        let mut fold_result: (Entry, Entry) = (Entry::Collection(vec![]), CommonObject::Count(0).into());
        let expected_result: (Entry, Entry) = (
            Entry::Collection(vec![
                RecordElement::OnGraph(init_vertex1().into()),
                RecordElement::OnGraph(init_vertex2().into()),
            ]),
            CommonObject::Count(2).into(),
        );
        if let Some(Ok(record)) = result.next() {
            let collection_entry = record
                .get(Some(TAG_A))
                .unwrap()
                .as_ref()
                .clone();
            let count_entry = record
                .get(Some(TAG_B))
                .unwrap()
                .as_ref()
                .clone();
            fold_result = (collection_entry, count_entry);
        }
        assert_eq!(fold_result, expected_result);
    }

    // g.V().values('age').min().as("a")
    #[test]
    fn min_test() {
        let r1 = Record::new(CommonObject::Prop(29.into()), None);
        let r2 = Record::new(CommonObject::Prop(27.into()), None);
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 1, // min
            alias: Some(TAG_A.into()),
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function] };
        let mut result = fold_test(vec![r1, r2], fold_opr_pb);
        let mut res = 0.into();
        if let Some(Ok(record)) = result.next() {
            if let Some(entry) = record.get(Some(TAG_A)) {
                res = match entry.as_ref() {
                    // this is Prop, since get_entry returns entry type of prop
                    Entry::Element(RecordElement::OffGraph(CommonObject::Prop(obj))) => obj.clone(),
                    _ => {
                        unreachable!()
                    }
                };
            }
        }
        assert_eq!(res, object!(27));
    }

    // g.V().values('name').max().as("a")
    #[test]
    fn max_test() {
        let r1 = Record::new(CommonObject::Prop("marko".into()), None);
        let r2 = Record::new(CommonObject::Prop("vadas".into()), None);
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 2, // max
            alias: Some(TAG_A.into()),
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function] };
        let mut result = fold_test(vec![r1, r2], fold_opr_pb);
        let mut res = "".into();
        if let Some(Ok(record)) = result.next() {
            if let Some(entry) = record.get(Some(TAG_A)) {
                res = match entry.as_ref() {
                    // this is Prop, since get_entry returns entry type of prop
                    Entry::Element(RecordElement::OffGraph(CommonObject::Prop(obj))) => obj.clone(),
                    _ => {
                        unreachable!()
                    }
                };
            }
        }
        assert_eq!(res, object!("vadas"));
    }

    // g.V().distinct_count().as("a")
    #[test]
    fn distinct_count_test() {
        let v1 = init_vertex1();
        let v2 = init_vertex2();
        let r1 = Record::new(v1.clone(), None);
        let r2 = Record::new(v2.clone(), None);
        let r3 = Record::new(v1, None);
        let r4 = Record::new(v2, None);

        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 4, // distinct_count
            alias: Some(TAG_A.into()),
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function] };
        let mut result = fold_test(vec![r1, r2, r3, r4], fold_opr_pb);
        let mut cnt = 0;
        if let Some(Ok(record)) = result.next() {
            if let Some(entry) = record.get(Some(TAG_A)) {
                cnt = match entry.as_ref() {
                    Entry::Element(RecordElement::OffGraph(CommonObject::Count(cnt))) => *cnt,
                    _ => {
                        unreachable!()
                    }
                };
            }
        }
        assert_eq!(cnt, 2);
    }

    // g.V().fold().as("a") // fold by set
    #[test]
    fn fold_to_set_test() {
        let mut source = init_source();
        let v3 = init_vertex1();
        let r3 = Record::new(v3, None);
        source.push(r3);
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 6, // to_set
            alias: Some(TAG_A.into()),
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function] };
        let mut result = fold_test(source, fold_opr_pb);
        let mut fold_result = Entry::Collection(vec![]);
        let expected_result = Entry::Collection(vec![
            RecordElement::OnGraph(init_vertex1().into()),
            RecordElement::OnGraph(init_vertex2().into()),
        ]);
        if let Some(Ok(record)) = result.next() {
            if let Some(entry) = record.get(Some(TAG_A)) {
                fold_result = entry.as_ref().clone();
            }
        }
        if let Entry::Collection(collection) = fold_result.borrow_mut() {
            collection.sort_by(|v1, v2| v1.partial_cmp(&v2).unwrap_or(Ordering::Equal));
        }
        assert_eq!(fold_result, expected_result);
    }
}
