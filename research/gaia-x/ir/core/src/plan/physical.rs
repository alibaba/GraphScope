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
use ir_common::generated::common as common_pb;
use pegasus_client::builder::*;
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
        let mut prev_node: Option<NodeType> = None;
        for (_, node) in &self.nodes {
            let node_ref = node.borrow();
            // TODO(longbin) At the first stage, we first assume that the plan has a chain shape.
            if node_ref.children.len() > 1 {
                return Err(PhysicalError::Unsupported);
            }
            if peers > 1 {
                if let Some(prev) = &prev_node {
                    let prev_ref = prev.borrow();
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
            node_ref.opr.add_job_builder(builder)?;
            prev_node = Some(node.clone());
        }
        // TODO(longbin) Shall consider the option of sinking the results.
        builder.sink(vec![]);

        Ok(())
    }
}
