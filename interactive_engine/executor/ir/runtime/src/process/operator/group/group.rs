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

use std::convert::TryInto;

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::KeyId;
use pegasus::api::function::{FnResult, MapFunction};

use crate::error::{FnExecError, FnGenResult};
use crate::process::functions::{GroupGen, KeyFunction};
use crate::process::operator::accum::{AccumFactoryGen, RecordAccumulator};
use crate::process::operator::keyed::KeyFunctionGen;
use crate::process::record::{Record, RecordKey};

impl GroupGen<Record, RecordKey, Record> for algebra_pb::GroupBy {
    fn gen_group_key(&self) -> FnGenResult<Box<dyn KeyFunction<Record, RecordKey, Record>>> {
        self.clone().gen_key()
    }

    fn gen_group_accum(&self) -> FnGenResult<RecordAccumulator> {
        self.clone().gen_accum()
    }

    fn gen_group_map(&self) -> FnGenResult<Box<dyn MapFunction<(RecordKey, Record), Record>>> {
        let mut key_aliases = Vec::with_capacity(self.mappings.len());
        for key_alias in self.mappings.iter() {
            let alias: Option<KeyId> = Some(
                key_alias
                    .alias
                    .clone()
                    .ok_or(ParsePbError::from("key alias is missing in group"))?
                    .try_into()?,
            );
            let alias = alias.ok_or(ParsePbError::from("key alias cannot be None in group"))?;
            key_aliases.push(alias);
        }
        let group_map = GroupMap { key_aliases };
        debug!("Runtime group operator group_map: {:?}", group_map);
        Ok(Box::new(group_map))
    }
}

#[derive(Debug)]
struct GroupMap {
    /// aliases for group keys, if some key is not not required to be preserved, give None alias
    key_aliases: Vec<KeyId>,
}

impl MapFunction<(RecordKey, Record), Record> for GroupMap {
    fn exec(&self, (group_key, mut group_value): (RecordKey, Record)) -> FnResult<Record> {
        let group_key_entries = group_key.take();
        if group_key_entries.len() != self.key_aliases.len() {
            Err(FnExecError::unexpected_data_error(
                "the number of group_keys and group_key_aliases should be equal",
            ))?
        }
        for (entry, alias) in group_key_entries
            .iter()
            .zip(self.key_aliases.iter())
        {
            group_value.append_arc_entry(entry.clone(), Some(alias.clone()));
        }
        Ok(group_value)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use ahash::HashMap;
    use dyn_type::Object;
    use graph_proxy::apis::{DynDetails, Vertex};
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_common::NameOrId;
    use pegasus::api::{FoldByKey, KeyBy, Map, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;

    use crate::process::functions::GroupGen;
    use crate::process::operator::accum::accumulator::Accumulator;
    use crate::process::operator::tests::{
        init_source, init_vertex1, init_vertex2, PERSON_LABEL, TAG_A, TAG_B, TAG_C,
    };
    use crate::process::record::{Entry, Record};

    // v1: marko, 29;
    // v2: vadas, 27;
    // v3: marko, 27;
    fn init_group_source() -> Vec<Record> {
        let mut source = init_source();
        source.push(Record::new(init_vertex3(), None));
        source
    }

    fn init_vertex3() -> Vertex {
        let map3: HashMap<NameOrId, Object> =
            vec![("id".into(), object!(3)), ("age".into(), object!(27)), ("name".into(), object!("marko"))]
                .into_iter()
                .collect();
        Vertex::new(3, Some(PERSON_LABEL), DynDetails::new(map3))
    }

    fn group_test(group_opr_pb: pb::GroupBy) -> ResultStream<Record> {
        let conf = JobConf::new("group_test");
        let result = pegasus::run(conf, || {
            let group_opr_pb = group_opr_pb.clone();
            move |input, output| {
                let stream = input.input_from(init_group_source().into_iter())?;
                let group_key = group_opr_pb.gen_group_key()?;
                let group_accum = group_opr_pb.gen_group_accum()?;
                let group_map = group_opr_pb.gen_group_map()?;
                let res_stream = stream
                    .key_by(move |record| group_key.get_kv(record))?
                    .fold_by_key(group_accum, || {
                        |mut accumulator, next| {
                            accumulator.accum(next).unwrap();
                            Ok(accumulator)
                        }
                    })?
                    .unfold(|map| {
                        Ok(map
                            .into_iter()
                            .map(|(key, mut accumulator)| (key, accumulator.finalize().unwrap())))
                    })?
                    .map(move |key_value| group_map.exec(key_value))?;
                res_stream.sink_into(output)
            }
        })
        .expect("build job failure");

        result
    }

    // g.V().group() with key as 'a', value as 'b'
    #[test]
    fn group_to_list_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 5, // ToList
            alias: Some(TAG_B.into()),
        };
        let key_alias = pb::group_by::KeyAlias {
            key: Some(common_pb::Variable::from("@".to_string())),
            alias: Some(TAG_A.into()),
        };
        let group_opr_pb = pb::GroupBy { mappings: vec![key_alias], functions: vec![function] };
        let mut result = group_test(group_opr_pb);
        let mut group_result = HashSet::new();
        let expected_result: HashSet<(Entry, Entry)> = [
            (init_vertex1().into(), Entry::Collection(vec![(init_vertex1().into())])),
            (init_vertex2().into(), Entry::Collection(vec![(init_vertex2().into())])),
            (init_vertex3().into(), Entry::Collection(vec![(init_vertex3().into())])),
        ]
        .iter()
        .cloned()
        .collect();

        while let Some(Ok(result)) = result.next() {
            let key = result.get(Some(TAG_A)).unwrap().as_ref();
            let val = result.get(Some(TAG_B)).unwrap().as_ref();
            group_result.insert((key.clone(), val.clone()));
        }
        assert_eq!(group_result, expected_result);
    }

    // g.V().group().by("name") with key as 'a', value as 'b'
    #[test]
    fn group_by_key_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 5, // ToList
            alias: Some(TAG_B.into()),
        };
        let key_alias = pb::group_by::KeyAlias {
            key: Some(common_pb::Variable::from("@.name".to_string())),
            alias: Some(TAG_A.into()),
        };
        let group_opr_pb = pb::GroupBy { mappings: vec![key_alias], functions: vec![function] };
        let mut result = group_test(group_opr_pb);
        let mut group_result = HashSet::new();
        let expected_result: HashSet<(Entry, Entry)> = [
            (
                object!("marko").into(),
                Entry::Collection(vec![(init_vertex1().into()), (init_vertex3().into())]),
            ),
            (object!("vadas").into(), Entry::Collection(vec![(init_vertex2().into())])),
        ]
        .iter()
        .cloned()
        .collect();

        while let Some(Ok(result)) = result.next() {
            let key = result.get(Some(TAG_A)).unwrap().as_ref();
            let val = result.get(Some(TAG_B)).unwrap().as_ref();
            group_result.insert((key.clone(), val.clone()));
        }
        assert_eq!(group_result, expected_result);
    }

    // g.V().group().by("id","name") with key of 'id' as 'a', 'name' as 'b', and value as 'c'
    #[test]
    fn group_by_tuple_key_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 5, // ToList
            alias: Some(TAG_C.into()),
        };
        let key_alias_1 = pb::group_by::KeyAlias {
            key: Some(common_pb::Variable::from("@.id".to_string())),
            alias: Some(TAG_A.into()),
        };
        let key_alias_2 = pb::group_by::KeyAlias {
            key: Some(common_pb::Variable::from("@.name".to_string())),
            alias: Some(TAG_B.into()),
        };
        let group_opr_pb =
            pb::GroupBy { mappings: vec![key_alias_1, key_alias_2], functions: vec![function] };
        let mut result = group_test(group_opr_pb);
        let mut group_result = HashSet::new();
        let expected_result: HashSet<((Entry, Entry), Entry)> = [
            (
                (object!(1).into(), object!("marko").into()),
                Entry::Collection(vec![(init_vertex1().into())]),
            ),
            (
                (object!(2).into(), object!("vadas").into()),
                Entry::Collection(vec![(init_vertex2().into())]),
            ),
            (
                (object!(3).into(), object!("marko").into()),
                Entry::Collection(vec![(init_vertex3().into())]),
            ),
        ]
        .iter()
        .cloned()
        .collect();

        while let Some(Ok(result)) = result.next() {
            let key_1 = result.get(Some(TAG_A)).unwrap().as_ref();
            let key_2 = result.get(Some(TAG_B)).unwrap().as_ref();
            let val = result.get(Some(TAG_C)).unwrap().as_ref();
            group_result.insert(((key_1.clone(), key_2.clone()), val.clone()));
        }
        assert_eq!(group_result, expected_result);
    }

    // g.V().group().by().by(to_list().as("a"), count().as("b")) with key as "c", value of list as 'a' and count as 'b';
    #[test]
    fn group_multi_accum_test() {
        let function_1 = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 5, // ToList
            alias: Some(TAG_A.into()),
        };
        let function_2 = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 3, // Count
            alias: Some(TAG_B.into()),
        };
        let key_alias = pb::group_by::KeyAlias {
            key: Some(common_pb::Variable::from("@".to_string())),
            alias: Some(TAG_C.into()),
        };
        let group_opr_pb =
            pb::GroupBy { mappings: vec![key_alias], functions: vec![function_1, function_2] };
        let mut result = group_test(group_opr_pb);
        let mut group_result = HashSet::new();
        let expected_result: HashSet<(Entry, (Entry, Entry))> = [
            (
                init_vertex1().into(),
                (Entry::Collection(vec![(init_vertex1().into())]), object!(1u64).into()),
            ),
            (
                init_vertex2().into(),
                (Entry::Collection(vec![(init_vertex2().into())]), object!(1u64).into()),
            ),
            (
                init_vertex3().into(),
                (Entry::Collection(vec![(init_vertex3().into())]), object!(1u64).into()),
            ),
        ]
        .iter()
        .cloned()
        .collect();

        while let Some(Ok(result)) = result.next() {
            let key = result.get(Some(TAG_C)).unwrap().as_ref();
            let val_1 = result.get(Some(TAG_A)).unwrap().as_ref();
            let val_2 = result.get(Some(TAG_B)).unwrap().as_ref();
            group_result.insert((key.clone(), (val_1.clone(), val_2.clone())));
        }
        assert_eq!(group_result, expected_result);
    }

    // g.V().groupCount() with key as 'a', value as "b"
    #[test]
    fn group_count_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 3, // Count
            alias: Some(TAG_B.into()),
        };
        let key_alias = pb::group_by::KeyAlias {
            key: Some(common_pb::Variable::from("@".to_string())),
            alias: Some(TAG_A.into()),
        };
        let group_opr_pb = pb::GroupBy { mappings: vec![key_alias], functions: vec![function] };
        let mut result = group_test(group_opr_pb);
        let mut group_result = HashSet::new();
        let expected_result: HashSet<(Entry, Entry)> = [
            (init_vertex1().into(), object!(1u64).into()),
            (init_vertex2().into(), object!(1u64).into()),
            (init_vertex3().into(), object!(1u64).into()),
        ]
        .iter()
        .cloned()
        .collect();
        while let Some(Ok(result)) = result.next() {
            let key = result.get(Some(TAG_A)).unwrap().as_ref();
            let val = result.get(Some(TAG_B)).unwrap().as_ref();
            group_result.insert((key.clone(), val.clone()));
        }
        assert_eq!(group_result, expected_result);
    }

    // g.V().groupCount().by('name') with key as 'a', value as "b"
    #[test]
    fn group_count_by_prop_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 3, // Count
            alias: Some(TAG_B.into()),
        };
        let key_alias = pb::group_by::KeyAlias {
            key: Some(common_pb::Variable::from("@.name".to_string())),
            alias: Some(TAG_A.into()),
        };
        let group_opr_pb = pb::GroupBy { mappings: vec![key_alias], functions: vec![function] };
        let mut result = group_test(group_opr_pb);
        let mut group_result = HashSet::new();
        let expected_result: HashSet<(Entry, Entry)> = [
            (object!("marko").into(), object!(2u64).into()),
            (object!("vadas").into(), object!(1u64).into()),
        ]
        .iter()
        .cloned()
        .collect();
        while let Some(Ok(result)) = result.next() {
            let key = result.get(Some(TAG_A)).unwrap().as_ref();
            let val = result.get(Some(TAG_B)).unwrap().as_ref();
            group_result.insert((key.clone(), val.clone()));
        }
        assert_eq!(group_result, expected_result);
    }

    // g.V().group().by("name").by(values('age').min()) with key as 'a', value as 'b'
    #[test]
    fn group_min_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@.age".to_string())],
            aggregate: 1, // min
            alias: Some(TAG_B.into()),
        };
        let key_alias = pb::group_by::KeyAlias {
            key: Some(common_pb::Variable::from("@.name".to_string())),
            alias: Some(TAG_A.into()),
        };
        let group_opr_pb = pb::GroupBy { mappings: vec![key_alias], functions: vec![function] };
        let mut result = group_test(group_opr_pb);
        let mut group_result = HashSet::new();
        let expected_result: HashSet<(Entry, Entry)> =
            [(object!("vadas").into(), object!(27).into()), (object!("marko").into(), object!(27).into())]
                .iter()
                .cloned()
                .collect();

        while let Some(Ok(result)) = result.next() {
            let key = result.get(Some(TAG_A)).unwrap().as_ref();
            let val = result.get(Some(TAG_B)).unwrap().as_ref();
            group_result.insert((key.clone(), val.clone()));
        }
        assert_eq!(group_result, expected_result);
    }

    // g.V().group().by("name").by(values('age').max()) with key as 'a', value as 'b'
    #[test]
    fn group_max_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@.age".to_string())],
            aggregate: 2, // max
            alias: Some(TAG_B.into()),
        };
        let key_alias = pb::group_by::KeyAlias {
            key: Some(common_pb::Variable::from("@.name".to_string())),
            alias: Some(TAG_A.into()),
        };
        let group_opr_pb = pb::GroupBy { mappings: vec![key_alias], functions: vec![function] };
        let mut result = group_test(group_opr_pb);
        let mut group_result = HashSet::new();
        let expected_result: HashSet<(Entry, Entry)> =
            [(object!("marko").into(), object!(29).into()), (object!("vadas").into(), object!(27).into())]
                .iter()
                .cloned()
                .collect();

        while let Some(Ok(result)) = result.next() {
            let key = result.get(Some(TAG_A)).unwrap().as_ref();
            let val = result.get(Some(TAG_B)).unwrap().as_ref();
            group_result.insert((key.clone(), val.clone()));
        }
        assert_eq!(group_result, expected_result);
    }
}
