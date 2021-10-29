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
//!
//! The physical module will help package a ir logical plan into a pegasus physical plan.
//! Basically, it will wrap each ir operator into a corresponding pegasus operator (map, flatmap, etc)
//! and then represent the udf as a BinarySource (byte array) that is directly encoded from the
//! protobuf structure.
//!

use crate::plan::logical::{LogicalPlan, NodeType};
use ir_common::generated::algebra as pb;
use ir_common::generated::algebra::join::JoinKind;
use ir_common::generated::common as common_pb;
use pegasus_client::builder::*;
use pegasus_server::pb as server_pb;
use prost::{EncodeError, Message};
use std::fmt;

/// Record any error while transforming ir to a pegasus physical plan
#[derive(Debug, Clone)]
pub enum PhysicalError {
    PbEncodeError(EncodeError),
    MissingDataError,
    Unsupported,
}

pub type PhysicalResult<T> = Result<T, PhysicalError>;

impl fmt::Display for PhysicalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PhysicalError::PbEncodeError(err) => write!(f, "encoding protobuf error: {:?}", err),
            PhysicalError::MissingDataError => write!(f, "missing necessary data."),
            PhysicalError::Unsupported => write!(f, "the function has not been supported"),
        }
    }
}

impl std::error::Error for PhysicalError {}

impl From<EncodeError> for PhysicalError {
    fn from(err: EncodeError) -> Self {
        Self::PbEncodeError(err)
    }
}

pub trait AsPhysical {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()>;
}

#[derive(PartialEq)]
enum PegasusOpr {
    Source,
    Map,
    Flatmap,
    Filter,
}

fn simple_add_job_builder<M: Message>(
    builder: &mut JobBuilder,
    ir_opr: &M,
    opr: PegasusOpr,
) -> PhysicalResult<()> {
    let mut bytes = vec![];
    ir_opr.encode(&mut bytes)?;
    match opr {
        PegasusOpr::Source => builder.add_source(bytes),
        PegasusOpr::Map => builder.map(bytes),
        PegasusOpr::Flatmap => builder.flat_map(bytes),
        PegasusOpr::Filter => builder.filter(bytes),
    };
    Ok(())
}

impl AsPhysical for pb::Project {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        simple_add_job_builder(builder, self, PegasusOpr::Map)
    }
}

impl AsPhysical for pb::Select {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        simple_add_job_builder(builder, self, PegasusOpr::Filter)
    }
}

impl AsPhysical for pb::Scan {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        simple_add_job_builder(builder, self, PegasusOpr::Source)
    }
}

impl AsPhysical for pb::IndexedScan {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        simple_add_job_builder(builder, self, PegasusOpr::Source)
    }
}

impl AsPhysical for pb::EdgeExpand {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        simple_add_job_builder(builder, self, PegasusOpr::Flatmap)
    }
}

impl AsPhysical for pb::GetV {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        simple_add_job_builder(builder, self, PegasusOpr::Map)
    }
}

impl AsPhysical for pb::Limit {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        if let Some(range) = &self.range {
            builder.limit(range.upper as u32);
            Ok(())
        } else {
            Err(PhysicalError::MissingDataError)
        }
    }
}

impl AsPhysical for pb::logical_plan::Operator {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        use pb::logical_plan::operator::Opr::*;
        if let Some(opr) = &self.opr {
            match opr {
                Project(project) => project.add_job_builder(builder),
                Select(select) => select.add_job_builder(builder),
                Vertex(getv) => getv.add_job_builder(builder),
                Edge(edgexpd) => edgexpd.add_job_builder(builder),
                Scan(scan) => scan.add_job_builder(builder),
                IndexedScan(idxscan) => idxscan.add_job_builder(builder),
                Limit(limit) => limit.add_job_builder(builder),
                _ => Err(PhysicalError::Unsupported),
            }
        } else {
            Err(PhysicalError::MissingDataError)
        }
    }
}

impl AsPhysical for LogicalPlan {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        use pb::logical_plan::operator::Opr::*;
        let peers = builder.conf.servers().len() * (builder.conf.workers as usize);
        let mut prev_node_opt: Option<NodeType> = None;
        let mut curr_node_opt = self.root();

        while curr_node_opt.is_some() {
            let curr_node = curr_node_opt.as_ref().unwrap();
            if curr_node.borrow().children.len() <= 2 {
                if peers > 1 {
                    if let Some(prev) = &prev_node_opt {
                        let prev_ref = prev.borrow();
                        let node_ref = curr_node.borrow();
                        match (&prev_ref.opr.opr, &node_ref.opr.opr) {
                            (Some(Edge(_)), Some(Edge(edgexpd))) => {
                                let key_pb = common_pb::NameOrIdKey {
                                    key: edgexpd.base.as_ref().unwrap().v_tag.clone(),
                                };
                                let mut bytes = vec![];
                                key_pb.encode(&mut bytes)?;
                                builder.exchange(bytes);
                            }
                            (Some(Edge(_)), Some(Vertex(getv))) => {
                                let key_pb = common_pb::NameOrIdKey {
                                    key: getv.tag.clone(),
                                };
                                let mut bytes = vec![];
                                key_pb.encode(&mut bytes)?;
                                builder.exchange(bytes);
                            }
                            _ => {}
                        }
                    }
                }
                curr_node.borrow().opr.add_job_builder(builder)?;
                prev_node_opt = curr_node_opt.clone();

                if curr_node.borrow().children.len() == 1 {
                    let next_node_id = curr_node.borrow().get_first_child().unwrap();
                    curr_node_opt = self.get_node(next_node_id);
                } else if curr_node.borrow().children.len() == 2 {
                    let (merge_node_opt, subplans) = self.get_branch_plans(curr_node.clone());
                    assert_eq!(subplans.len(), 2);

                    let mut builder0 = JobBuilder::new(builder.conf.clone());
                    subplans.get(0).unwrap().add_job_builder(&mut builder0)?;

                    let mut builder1 = JobBuilder::new(builder.conf.clone());
                    subplans.get(1).unwrap().add_job_builder(&mut builder1)?;

                    if let Some(merge_node) = merge_node_opt.clone() {
                        match &merge_node.borrow().opr.opr {
                            Some(Union(_)) => {
                                builder.merge(
                                    |src0| {
                                        src0.extend(builder0.get_plan());
                                    },
                                    |src1| {
                                        src1.extend(builder1.get_plan());
                                    },
                                );
                            }
                            Some(Join(join_opr)) => {
                                let join_kind = unsafe {
                                    std::mem::transmute::<i32, pb::join::JoinKind>(join_opr.kind)
                                };
                                let pegasus_join_kind = match join_kind {
                                    JoinKind::Inner => server_pb::join::JoinKind::Inner,
                                    JoinKind::LeftOuter => server_pb::join::JoinKind::LeftOuter,
                                    JoinKind::RightOuter => server_pb::join::JoinKind::RightOuter,
                                    JoinKind::FullOuter => server_pb::join::JoinKind::FullOuter,
                                    JoinKind::Semi => server_pb::join::JoinKind::Semi,
                                    JoinKind::Anti => server_pb::join::JoinKind::Anti,
                                    JoinKind::Times => server_pb::join::JoinKind::Times,
                                };
                                let mut left_key_bytes = vec![];
                                let mut right_key_bytes = vec![];
                                common_pb::VariableKeys {
                                    keys: join_opr.left_keys.clone(),
                                }
                                .encode(&mut left_key_bytes)?;

                                common_pb::VariableKeys {
                                    keys: join_opr.right_keys.clone(),
                                }
                                .encode(&mut right_key_bytes)?;

                                builder.join(
                                    pegasus_join_kind,
                                    |src0| {
                                        src0.extend(builder0.get_plan())
                                            .key_by(left_key_bytes.clone());
                                    },
                                    |src1| {
                                        src1.extend(builder1.get_plan())
                                            .key_by(right_key_bytes.clone());
                                    },
                                );
                            }
                            _ => return Err(PhysicalError::Unsupported),
                        }
                    }

                    curr_node_opt = merge_node_opt;
                }
            } else {
                return Err(PhysicalError::Unsupported);
            }
        }
        // TODO(longbin) Shall consider the option of sinking the results.
        builder.sink(vec![]);

        Ok(())
    }
}
