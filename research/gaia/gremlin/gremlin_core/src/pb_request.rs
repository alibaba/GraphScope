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

use crate::generated::gremlin as pb;

use crate::process::traversal::step::{
    BySubJoin, FilterStep, FlatMapStep, GraphVertexStep, GroupStep, HasAnyJoin, JoinFuncGen,
    MapStep, OrderStep, Step,
};
use crate::process::traversal::traverser::Requirement;
use crate::structure::filter::codec::from_pb;
use crate::structure::Label;
use crate::ID;
use graph_store::prelude::LabelId;
use pegasus::BuildJobError;
use pegasus_common::downcast::*;
use pegasus_server::desc::{GroupByDesc, JobDesc, Resource, SharedResource, SortByDesc, UnionDesc};
use pegasus_server::desc::{OpKind, OperatorDesc, RepeatDesc, SubtaskDesc};
use pegasus_server::service::JobPreprocess;
use prost::Message;
use std::sync::Arc;

impl_as_any!(JoinFuncGen);

pub struct ProtoReflect;

impl JobPreprocess for ProtoReflect {
    fn preprocess(&self, job: &mut JobDesc) -> Result<(), BuildJobError> {
        println!("yes we have received an plan");
        job.conf.plan_print = true;
        reflect_source(job)?;
        let plan = job.get_plan_mut().expect("get mut plan failure;");
        reflect_plan(plan)
    }
}

fn reflect_source(job: &mut JobDesc) -> Result<(), BuildJobError> {
    let pb = job.source.get().as_any_ref().downcast_ref::<Vec<u8>>().unwrap();
    let mut gremlin_step = pb::GremlinStep::decode(&pb[0..]).unwrap();
    let step = graph_step_from(&mut gremlin_step)?;
    job.source = SharedResource::new(step);
    Ok(())
}

fn reflect_plan(plan: &mut [OperatorDesc]) -> Result<(), BuildJobError> {
    for op in plan {
        println!("op in plan {:?}", op.op_kind);
        match op.op_kind {
            OpKind::Map => {
                let bytes = op
                    .resource
                    .as_any_ref()
                    .downcast_ref::<Vec<u8>>()
                    .ok_or("resource is not bytes;")?;
                let step: MapStep = pb::GremlinStep::decode(bytes.as_slice())
                    .map_err(|e| format!("decode GremlinStep error {}", e))?
                    .into();
                op.resource = SharedResource::new(step);
            }
            OpKind::Repeat => {
                let res = op.resource.get_mut().ok_or("can't modify repeat resource")?;

                let repeat = res
                    .as_any_mut()
                    .downcast_mut::<RepeatDesc>()
                    .ok_or("repeat resource type error;")?;

                if let Some(_until) = repeat.until.as_mut() {
                    // TODO: reflect until
                }

                reflect_plan(&mut repeat.body)?;
            }
            OpKind::Subtask => {
                let res = op.resource.get_mut().ok_or("can't modify subtask resource;")?;
                let subtask = res
                    .as_any_mut()
                    .downcast_mut::<SubtaskDesc>()
                    .ok_or("subtask resource type error;")?;
                if let Some(joiner_res) = subtask.joiner.as_mut() {
                    let bytes = joiner_res
                        .as_any_ref()
                        .downcast_ref::<Vec<u8>>()
                        .ok_or("joiner resource is not bytes")?;
                    let joiner: pb::SubTaskJoiner = pb::SubTaskJoiner::decode(bytes.as_slice())
                        .map_err(|e| format!("decode SubTaskJoiner error {}", e))?;
                    let join_func = match joiner.inner.unwrap() {
                        pb::sub_task_joiner::Inner::WhereJoiner(_) => {
                            let join = Arc::new(HasAnyJoin);
                            Box::new(JoinFuncGen::new(join)) as Box<dyn Resource>
                        }
                        pb::sub_task_joiner::Inner::ByJoiner(_) => {
                            let join = Arc::new(BySubJoin);
                            Box::new(JoinFuncGen::new(join)) as Box<dyn Resource>
                        }
                    };
                    subtask.joiner = Some(SharedResource::from(join_func));
                }
                reflect_plan(&mut subtask.subtask)?;
            }
            OpKind::Flatmap => {
                let bytes = op
                    .resource
                    .as_any_ref()
                    .downcast_ref::<Vec<u8>>()
                    .ok_or("resource is not bytes;")?;
                let step: FlatMapStep = pb::GremlinStep::decode(bytes.as_slice())
                    .map_err(|e| format!("decode GremlinStep error {}", e))?
                    .into();
                op.resource = SharedResource::new(step);
            }
            OpKind::Filter => {
                let bytes = op
                    .resource
                    .as_any_ref()
                    .downcast_ref::<Vec<u8>>()
                    .ok_or("resource is not bytes;")?;
                let step: FilterStep = pb::GremlinStep::decode(bytes.as_slice())
                    .map_err(|e| format!("decode GremlinStep error {}", e))?
                    .into();
                op.resource = SharedResource::new(step);
            }
            OpKind::Sort => {
                let res = op.resource.get_mut().ok_or("can't modify sort resource")?;
                let sort = res
                    .as_any_mut()
                    .downcast_mut::<SortByDesc>()
                    .ok_or("sort resource type error;")?;

                let bytes = sort
                    .cmp
                    .as_any_ref()
                    .downcast_ref::<Vec<u8>>()
                    .ok_or("resource is not bytes;")?;
                let step: OrderStep = pb::GremlinStep::decode(bytes.as_slice())
                    .map_err(|e| format!("decode GremlinStep error {}", e))?
                    .into();
                sort.cmp.replace(step);
            }
            OpKind::Group => {
                let res = op.resource.get_mut().ok_or("can't modify group resource")?;
                let group = res
                    .as_any_mut()
                    .downcast_mut::<GroupByDesc>()
                    .ok_or("group resource type error")?;
                let bytes = group
                    .key_func
                    .as_any_ref()
                    .downcast_ref::<Vec<u8>>()
                    .ok_or("resource is not bytes")?;
                let step: GroupStep = pb::GremlinStep::decode(bytes.as_slice())
                    .map_err(|e| format!("decode GremlinStep error {}", e))?
                    .into();
                group.key_func.replace(step);
                // TODO: reflect customized AccumKind
            }
            OpKind::Union => {
                let res = op.resource.get_mut().ok_or("can't modify union resource")?;
                let union = res
                    .as_any_mut()
                    .downcast_mut::<UnionDesc>()
                    .ok_or("union resource type error;")?;
                let task = &mut union.tasks;
                for op_vec in task {
                    reflect_plan(op_vec.as_mut_slice())?;
                }
            }
            _ => (),
        }
    }
    Ok(())
}

fn graph_step_from(gremlin_step: &mut pb::GremlinStep) -> Result<GraphVertexStep, BuildJobError> {
    let mut step = GraphVertexStep::new(Requirement::PATH);
    for tag in gremlin_step.tags.drain(..) {
        step.add_tag(tag);
    }

    if let Some(option) = gremlin_step.step.take() {
        match option {
            pb::gremlin_step::Step::GraphStep(mut opt) => {
                let mut ids = vec![];
                for id in opt.ids {
                    ids.push(id as ID);
                }

                if !ids.is_empty() {
                    step.set_src(ids, 1);
                }
                let labels = std::mem::replace(&mut opt.labels, vec![]);
                step.params.labels =
                    labels.into_iter().map(|id| Label::Id(id as LabelId)).collect();
                if let Some(ref test) = opt.predicates {
                    if let Some(filter) = from_pb(test)? {
                        step.params.set_filter(filter);
                    }
                }
            }
            _ => (),
        }
    }
    Ok(step)
}
