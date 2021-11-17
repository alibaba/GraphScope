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

use crate::error::{FnGenError, FnGenResult};
use crate::process::operator::accum::accumulator::{AccumError, Accumulator, Count, ToList};
use crate::process::operator::accum::AccumFactoryGen;
use crate::process::operator::TagKey;
use crate::process::record::{Entry, ObjectElement, Record};
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::group_by::agg_func::Aggregate;
use ir_common::NameOrId;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use std::convert::{TryFrom, TryInto};

#[derive(Debug, Clone)]
pub enum EntryAccumulator {
    // ToSum(DataSum<Entry>),
    // ToMin(Minimum<Entry>),
    // ToMax(Maximum<Entry>),
    ToCount(Count<Entry>),
    ToList(ToList<Entry>),
    // ToSet(ToSet<Entry>),
    // TODO: more...
}

#[derive(Debug, Clone)]
pub struct RecordAccumulator {
    accum_ops: Vec<(EntryAccumulator, TagKey, Option<NameOrId>)>,
}

impl Accumulator<Record, Record> for RecordAccumulator {
    fn accum(&mut self, next: Record) -> Result<(), AccumError> {
        for (accum_op, tag_key, _) in self.accum_ops.iter_mut() {
            let entry = tag_key.get_entry(&next).map_err(|e| format!("{}", e))?;
            accum_op.accum(entry)?;
        }
        Ok(())
    }

    fn finalize(&mut self) -> Record {
        let mut record = Record::default();
        for (accum_op, _, alias) in self.accum_ops.iter_mut() {
            let result = accum_op.finalize();
            record.append(result, alias.clone());
        }
        record
    }
}

#[derive(Debug)]
struct RecordSingleAccumulator {
    accumulator: EntryAccumulator,
    tag_key: TagKey,
    alias: Option<NameOrId>,
}

impl Accumulator<Record, Record> for RecordSingleAccumulator {
    fn accum(&mut self, next: Record) -> Result<(), AccumError> {
        let entry = self
            .tag_key
            .get_entry(&next)
            .map_err(|e| format!("{}", e))?;
        self.accumulator.accum(entry)
    }

    fn finalize(&mut self) -> Record {
        let result = self.accumulator.finalize();
        Record::new(result, self.alias.clone())
    }
}

impl Accumulator<Entry, Entry> for EntryAccumulator {
    fn accum(&mut self, next: Entry) -> Result<(), AccumError> {
        match self {
            EntryAccumulator::ToCount(count) => count.accum(next),
            EntryAccumulator::ToList(list) => list.accum(next),
            // TODO: more accum kind
        }
    }

    fn finalize(&mut self) -> Entry {
        match self {
            EntryAccumulator::ToCount(count) => ObjectElement::Count(count.finalize()).into(),
            EntryAccumulator::ToList(list) => {
                let list_entry = list
                    .finalize()
                    .into_iter()
                    .map(|entry| match entry {
                        Entry::Element(e) => e,
                        Entry::Collection(_) => {
                            // TODO: fold collections, e.g., fold paths
                            panic!("Haven't support fold collections for now");
                        }
                    })
                    .collect();
                Entry::Collection(list_entry)
            } // TODO: more accum kind
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
                return Err(FnGenError::Unsupported(
                    "Do not support to aggregate multiple fields yet".to_string(),
                ));
            }
            let tag_key = agg_func
                .vars
                .get(0)
                .map(|v| TagKey::try_from(v.clone()))
                .transpose()?
                .unwrap_or(TagKey::default());
            let alias = agg_func.alias.map(|tag| tag.try_into()).transpose()?;
            let entry_accumulator = match agg_kind {
                Aggregate::Count => EntryAccumulator::ToCount(Count {
                    value: 0,
                    _ph: Default::default(),
                }),
                Aggregate::ToList => EntryAccumulator::ToList(ToList { inner: vec![] }),
                _ => {
                    unimplemented!()
                }
            };
            accum_ops.push((entry_accumulator, tag_key, alias));
        }

        Ok(RecordAccumulator { accum_ops })
    }
}

// impl Encode for EntryAccumulator {
//     fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
//         match self {
//             EntryAccumulator::ToCount(count) => {
//                 writer.write_u8(0)?;
//                 count.write_to(writer)?;
//             }
//             EntryAccumulator::ToList(list) => {
//                 writer.write_u8(1)?;
//                 list.write_to(writer)?;
//             }
//         }
//         Ok(())
//     }
// }
//
// impl Decode for EntryAccumulator {
//     fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
//         let e = reader.read_u8()?;
//         match e {
//             0 => {
//                 let cnt = Count::read_from(reader)?;
//                 Ok(EntryAccumulator::ToCount(cnt))
//             }
//             1 => {
//                 let list = ToList::read_from(reader)?;
//                 Ok(EntryAccumulator::ToList(list))
//             }
//             _ => Err(std::io::Error::new(
//                 std::io::ErrorKind::Other,
//                 "unreachable",
//             )),
//         }
//     }
// }

impl Encode for RecordAccumulator {
    fn write_to<W: WriteExt>(&self, _writer: &mut W) -> std::io::Result<()> {
        unimplemented!()
    }
}

impl Decode for RecordAccumulator {
    fn read_from<R: ReadExt>(_reader: &mut R) -> std::io::Result<Self> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use crate::graph::element::Element;
    use crate::process::operator::accum::accumulator::Accumulator;
    use crate::process::operator::accum::AccumFactoryGen;
    use crate::process::operator::keyed::KeyFunctionGen;
    use crate::process::operator::tests::{init_source, init_vertex1, init_vertex2};
    use crate::process::record::{Entry, ObjectElement, Record, RecordElement, RecordKey};
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use pegasus::api::{FoldByKey, KeyBy, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;
    use std::collections::HashSet;

    fn group_test(group_opr_pb: pb::GroupBy) -> ResultStream<(RecordKey, Record)> {
        let conf = JobConf::new("group_test");
        let result = pegasus::run(conf, || {
            let group_opr_pb = group_opr_pb.clone();
            move |input, output| {
                let stream = input.input_from(init_source().into_iter())?;
                let key_selector = group_opr_pb.clone().gen_key().unwrap();
                let accum = group_opr_pb.clone().gen_accum().unwrap();
                let res_stream = stream
                    .key_by(move |record| key_selector.select_key(record))?
                    .fold_by_key(accum, || {
                        |mut accumulator, next| {
                            accumulator.accum(next).unwrap();
                            Ok(accumulator)
                        }
                    })?
                    .unfold(|map| {
                        Ok(map
                            .into_iter()
                            .map(|(key, mut accumulator)| (key, accumulator.finalize())))
                    })?;
                res_stream.sink_into(output)
            }
        })
        .expect("build job failure");

        result
    }

    // g.V().group()
    #[test]
    fn group_test_01() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 5,
            alias: None,
        };
        let group_opr_pb = pb::GroupBy {
            keys: vec![common_pb::Variable::from("@".to_string())],
            functions: vec![function],
        };
        let mut result = group_test(group_opr_pb);
        let mut group_result = HashSet::new();
        let expected_result: HashSet<(RecordKey, u128)> = [
            (RecordKey::new(vec![init_vertex1().into()]), 1),
            (RecordKey::new(vec![init_vertex2().into()]), 2),
        ]
        .iter()
        .cloned()
        .collect();

        while let Some(Ok((k, v))) = result.next() {
            let v = match v.get(None).unwrap() {
                Entry::Collection(collection) => {
                    assert!(collection.len() == 1);
                    match collection.get(0).unwrap() {
                        RecordElement::OnGraph(v) => v.id().unwrap(),
                        RecordElement::OutGraph(_) => {
                            unreachable!()
                        }
                    }
                }
                _ => unreachable!(),
            };
            group_result.insert((k, v));
        }
        assert_eq!(group_result, expected_result);
    }

    // g.V().groupCount()
    #[test]
    fn group_test_02() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 3,
            alias: None,
        };
        let group_opr_pb = pb::GroupBy {
            keys: vec![common_pb::Variable::from("@".to_string())],
            functions: vec![function],
        };
        let mut result = group_test(group_opr_pb);
        let mut group_result = HashSet::new();
        let expected_result: HashSet<(RecordKey, u64)> = [
            (RecordKey::new(vec![init_vertex1().into()]), 1),
            (RecordKey::new(vec![init_vertex2().into()]), 1),
        ]
        .iter()
        .cloned()
        .collect();
        while let Some(Ok((k, v))) = result.next() {
            let v = match v.get(None).unwrap() {
                Entry::Element(RecordElement::OutGraph(ObjectElement::Count(cnt))) => *cnt,
                _ => {
                    unreachable!()
                }
            };
            group_result.insert((k, v));
        }
        assert_eq!(group_result, expected_result);
    }
}
