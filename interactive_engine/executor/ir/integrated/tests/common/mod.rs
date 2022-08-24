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
//!
//!

#[cfg(test)]
#[allow(dead_code)]
#[allow(unused_imports)]
pub mod test {
    use std::collections::HashMap;
    use std::convert::{TryFrom, TryInto};
    use std::sync::{Arc, Once};

    use graph_proxy::apis::{DynDetails, Edge, Vertex, VertexOrEdge, ID};
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_common::generated::results as result_pb;
    use ir_common::{KeyId, NameOrId};
    use lazy_static::lazy_static;
    use pegasus::result::{ResultSink, ResultStream};
    use pegasus::{run_opt, Configuration, JobConf, StartupError};
    use pegasus_server::job::{JobAssembly, JobDesc};
    use pegasus_server::rpc::RpcSink;
    use pegasus_server::JobRequest;
    use prost::Message;
    use runtime::process::record::{Entry, Record, RecordElement};
    use runtime::IRJobAssembly;
    use runtime_integration::{InitializeJobAssembly, QueryExpGraph};

    pub const TAG_A: KeyId = 0;
    pub const TAG_B: KeyId = 1;
    pub const TAG_C: KeyId = 2;
    pub const TAG_D: KeyId = 3;

    static INIT: Once = Once::new();

    lazy_static! {
        static ref FACTORY: IRJobAssembly = initialize_job_assembly();
    }

    pub fn initialize() {
        INIT.call_once(|| {
            start_pegasus();
        });
    }

    fn start_pegasus() {
        match pegasus::startup(Configuration::singleton()) {
            Ok(_) => {
                lazy_static::initialize(&FACTORY);
            }
            Err(err) => match err {
                StartupError::AlreadyStarted(_) => {}
                _ => panic!("start pegasus failed"),
            },
        }
    }

    fn initialize_job_assembly() -> IRJobAssembly {
        let query_exp_graph = QueryExpGraph::new(1);
        query_exp_graph.initialize_job_assembly()
    }

    pub fn submit_query(job_req: JobRequest, num_workers: u32) -> ResultStream<Vec<u8>> {
        let mut conf = JobConf::default();
        conf.workers = num_workers;
        let (tx, rx) = crossbeam_channel::unbounded();
        let sink = ResultSink::new(tx);
        let cancel_hook = sink.get_cancel_hook().clone();
        let results = ResultStream::new(conf.job_id, cancel_hook, rx);
        let service = &FACTORY;
        let job = JobDesc { input: job_req.source, plan: job_req.plan, resource: job_req.resource };
        run_opt(conf, sink, move |worker| service.assemble(&job, worker)).expect("submit job failure;");
        results
    }

    pub fn parse_result(result: Vec<u8>) -> Option<Record> {
        let result: result_pb::Results = result_pb::Results::decode(result.as_slice()).unwrap();
        if let Some(result_pb::results::Inner::Record(record_pb)) = result.inner {
            let mut record = Record::default();
            for column in record_pb.columns {
                let tag: Option<KeyId> = if let Some(tag) = column.name_or_id {
                    match tag.item.unwrap() {
                        common_pb::name_or_id::Item::Name(name) => Some(
                            name.parse::<KeyId>()
                                .unwrap_or(KeyId::max_value()),
                        ),
                        common_pb::name_or_id::Item::Id(id) => Some(id),
                    }
                } else {
                    None
                };
                let entry = column.entry.unwrap();
                // append entry without moving head
                if let Some(tag) = tag {
                    let columns = record.get_columns_mut();
                    columns.insert(tag as usize, Arc::new(Entry::try_from(entry).unwrap()));
                } else {
                    record.append(Entry::try_from(entry).unwrap(), None);
                }
            }
            Some(record)
        } else {
            None
        }
    }

    pub fn query_params(
        tables: Vec<common_pb::NameOrId>, columns: Vec<common_pb::NameOrId>,
        predicate: Option<common_pb::Expression>,
    ) -> pb::QueryParams {
        pb::QueryParams {
            tables,
            columns,
            is_all_columns: false,
            limit: None,
            predicate,
            sample_ratio: 1.0,
            extra: HashMap::new(),
        }
    }

    pub fn query_params_all_columns(
        tables: Vec<common_pb::NameOrId>, columns: Vec<common_pb::NameOrId>,
        predicate: Option<common_pb::Expression>,
    ) -> pb::QueryParams {
        pb::QueryParams {
            tables,
            columns,
            is_all_columns: true,
            limit: None,
            predicate,
            sample_ratio: 1.0,
            extra: HashMap::new(),
        }
    }

    pub fn to_var_pb(tag: Option<NameOrId>, key: Option<NameOrId>) -> common_pb::Variable {
        common_pb::Variable {
            tag: tag.map(|t| t.into()),
            property: key
                .map(|k| common_pb::Property { item: Some(common_pb::property::Item::Key(k.into())) }),
        }
    }

    pub fn to_expr_var_pb(tag: Option<NameOrId>, key: Option<NameOrId>) -> common_pb::Expression {
        common_pb::Expression {
            operators: vec![common_pb::ExprOpr {
                item: Some(common_pb::expr_opr::Item::Var(to_var_pb(tag, key))),
            }],
        }
    }

    pub fn to_expr_var_all_prop_pb(tag: Option<NameOrId>) -> common_pb::Expression {
        common_pb::Expression {
            operators: vec![common_pb::ExprOpr {
                item: Some(common_pb::expr_opr::Item::Var(common_pb::Variable {
                    tag: tag.map(|t| t.into()),
                    property: Some(common_pb::Property {
                        item: Some(common_pb::property::Item::All(common_pb::AllKey {})),
                    }),
                })),
            }],
        }
    }

    pub fn to_expr_vars_pb(
        tag_keys: Vec<(Option<NameOrId>, Option<NameOrId>)>, is_map: bool,
    ) -> common_pb::Expression {
        let vars = tag_keys
            .into_iter()
            .map(|(tag, key)| to_var_pb(tag, key))
            .collect();
        common_pb::Expression {
            operators: vec![common_pb::ExprOpr {
                item: if is_map {
                    Some(common_pb::expr_opr::Item::VarMap(common_pb::VariableKeys { keys: vars }))
                } else {
                    Some(common_pb::expr_opr::Item::Vars(common_pb::VariableKeys { keys: vars }))
                },
            }],
        }
    }

    pub fn default_sink_pb() -> pb::Sink {
        pb::Sink {
            tags: vec![common_pb::NameOrIdKey { key: None }],
            sink_target: Some(pb::sink::SinkTarget {
                inner: Some(pb::sink::sink_target::Inner::SinkDefault(pb::SinkDefault {
                    id_name_mappings: vec![],
                })),
            }),
        }
    }

    pub fn default_sink_target() -> Option<pb::sink::SinkTarget> {
        Some(pb::sink::SinkTarget {
            inner: Some(pb::sink::sink_target::Inner::SinkDefault(pb::SinkDefault {
                id_name_mappings: vec![],
            })),
        })
    }
}
