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

use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::job_service as server_pb;
use ir_common::KeyId;
use pegasus::api::function::{FnResult, MapFunction};

use crate::error::{FnGenError, FnGenResult};
use crate::process::functions::FoldGen;
use crate::process::operator::accum::{AccumFactoryGen, RecordAccumulator};
use crate::process::record::{CommonObject, Entry, Record};

impl FoldGen<u64, Record> for algebra_pb::GroupBy {
    fn get_accum_kind(&self) -> server_pb::AccumKind {
        let accum_functions = &self.functions;
        if accum_functions.len() == 1 {
            let agg_kind: algebra_pb::group_by::agg_func::Aggregate =
                unsafe { std::mem::transmute(accum_functions[0].aggregate) };
            match agg_kind {
                algebra_pb::group_by::agg_func::Aggregate::Count => server_pb::AccumKind::Cnt,
                _ => server_pb::AccumKind::Custom,
            }
        } else {
            server_pb::AccumKind::Custom
        }
    }

    fn gen_fold_map(&self) -> FnGenResult<Box<dyn MapFunction<u64, Record>>> {
        if self.get_accum_kind() == server_pb::AccumKind::Cnt {
            let count_alias = self.functions[0]
                .alias
                .clone()
                .map(|alias| alias.try_into())
                .transpose()?;
            Ok(Box::new(CountAlias { alias: count_alias }))
        } else {
            Err(FnGenError::unsupported_error("Do not support fold_map except simple count"))
        }
    }

    fn gen_fold_accum(&self) -> FnGenResult<RecordAccumulator> {
        self.clone().gen_accum()
    }
}

#[derive(Debug)]
struct CountAlias {
    alias: Option<KeyId>,
}

impl MapFunction<u64, Record> for CountAlias {
    fn exec(&self, cnt: u64) -> FnResult<Record> {
        let cnt_entry: Entry = CommonObject::Count(cnt).into();
        Ok(Record::new(cnt_entry, self.alias.clone()))
    }
}

#[cfg(test)]
mod tests {
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use pegasus::api::{Count, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;
    use pegasus_server::pb as server_pb;

    use crate::process::functions::FoldGen;
    use crate::process::operator::tests::{init_source, TAG_A};
    use crate::process::record::{CommonObject, Entry, Record, RecordElement};

    fn count_test(source: Vec<Record>, fold_opr_pb: pb::GroupBy) -> ResultStream<Record> {
        let conf = JobConf::new("fold_test");
        let result = pegasus::run(conf, || {
            let fold_opr_pb = fold_opr_pb.clone();
            let source = source.clone();
            move |input, output| {
                let mut stream = input.input_from(source.into_iter())?;
                if let server_pb::AccumKind::Cnt = fold_opr_pb.get_accum_kind() {
                    let fold_map = fold_opr_pb.gen_fold_map()?;
                    stream = stream
                        .count()?
                        .map(move |cnt| fold_map.exec(cnt))?
                        .into_stream()?;
                }
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        result
    }

    // g.V().count()
    #[test]
    fn count_opt_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 3, // count
            alias: None,
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function] };
        let mut result = count_test(init_source(), fold_opr_pb);
        let mut cnt = 0;
        if let Some(Ok(record)) = result.next() {
            if let Some(entry) = record.get(None) {
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

    // g.V().count().as("a")
    #[test]
    fn count_opt_with_alias_test() {
        let function = pb::group_by::AggFunc {
            vars: vec![common_pb::Variable::from("@".to_string())],
            aggregate: 3, // count
            alias: Some(TAG_A.into()),
        };
        let fold_opr_pb = pb::GroupBy { mappings: vec![], functions: vec![function] };
        let mut result = count_test(init_source(), fold_opr_pb);
        let mut cnt = 0;
        if let Some(Ok(record)) = result.next() {
            if let Some(entry) = record.get(Some(&TAG_A.into())) {
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
}
