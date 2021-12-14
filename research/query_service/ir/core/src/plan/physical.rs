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

use std::fmt;

use ir_common::generated::algebra as pb;
use ir_common::generated::algebra::join::JoinKind;
use ir_common::generated::common as common_pb;
use pegasus_client::builder::*;
use pegasus_server::pb as server_pb;
use prost::{EncodeError, Message};

use crate::plan::logical::{LogicalPlan, NodeType};

/// Record any error while transforming ir to a pegasus physical plan
#[derive(Debug, Clone)]
pub enum PhysicalError {
    PbEncodeError(EncodeError),
    MissingDataError,
    InvalidRangeError(i32, i32),
    Unsupported,
}

pub type PhysicalResult<T> = Result<T, PhysicalError>;

impl fmt::Display for PhysicalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PhysicalError::PbEncodeError(err) => write!(f, "encoding protobuf error: {:?}", err),
            PhysicalError::MissingDataError => write!(f, "missing necessary data."),
            PhysicalError::InvalidRangeError(lo, up) => {
                write!(f, "invalid range ({:?}, {:?})", lo, up)
            }
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
    SortBy,
}

fn simple_add_job_builder<M: Message>(
    builder: &mut JobBuilder, ir_opr: &M, opr: PegasusOpr,
) -> PhysicalResult<()> {
    let bytes = ir_opr.encode_to_vec();
    match opr {
        PegasusOpr::Source => builder.add_source(bytes),
        PegasusOpr::Map => builder.map(bytes),
        PegasusOpr::Flatmap => builder.flat_map(bytes),
        PegasusOpr::Filter => builder.filter(bytes),
        PegasusOpr::SortBy => builder.sort_by(bytes),
    };
    Ok(())
}

impl AsPhysical for pb::Project {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(self.clone()), PegasusOpr::Map)
    }
}

impl AsPhysical for pb::Select {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(self.clone()), PegasusOpr::Filter)
    }
}

impl AsPhysical for pb::Scan {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(self.clone()), PegasusOpr::Source)
    }
}

impl AsPhysical for pb::IndexedScan {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(self.clone()), PegasusOpr::Source)
    }
}

impl AsPhysical for pb::EdgeExpand {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        simple_add_job_builder(
            builder,
            &pb::logical_plan::Operator::from(self.clone()),
            PegasusOpr::Flatmap,
        )
    }
}

impl AsPhysical for pb::GetV {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(self.clone()), PegasusOpr::Map)
    }
}

impl AsPhysical for pb::Auxilia {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(self.clone()), PegasusOpr::Map)
    }
}

impl AsPhysical for pb::Limit {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        if let Some(range) = &self.range {
            if range.upper <= range.lower || range.lower < 0 || range.upper <= 0 {
                Err(PhysicalError::InvalidRangeError(range.lower, range.upper))
            } else {
                builder.limit((range.upper - 1) as u32);
                Ok(())
            }
        } else {
            Err(PhysicalError::MissingDataError)
        }
    }
}

impl AsPhysical for pb::OrderBy {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        let opr = pb::logical_plan::Operator::from(self.clone());
        if self.limit.is_none() {
            simple_add_job_builder(builder, &opr, PegasusOpr::SortBy)
        } else {
            let range = self.limit.clone().unwrap();
            if range.upper <= range.lower || range.lower < 0 || range.upper <= 0 {
                Err(PhysicalError::InvalidRangeError(range.lower, range.upper))
            } else {
                let bytes = opr.encode_to_vec();
                builder.sort_limit_by((range.upper - 1) as i64, bytes);
                Ok(())
            }
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
                OrderBy(orderby) => orderby.add_job_builder(builder),
                Auxilia(auxilia) => auxilia.add_job_builder(builder),
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
        let is_partition = builder.conf.workers as usize > 1 || builder.conf.servers().len() > 1;
        let mut prev_node_opt: Option<NodeType> = None;
        let mut curr_node_opt = self.root();

        while curr_node_opt.is_some() {
            let curr_node = curr_node_opt.as_ref().unwrap();
            if is_partition {
                if let Some(prev) = &prev_node_opt {
                    let prev_ref = prev.borrow();
                    let node_ref = curr_node.borrow();
                    match (&prev_ref.opr.opr, &node_ref.opr.opr) {
                        (Some(Edge(_)), Some(Edge(edgexpd))) => {
                            let key_pb = common_pb::NameOrIdKey {
                                key: edgexpd.base.as_ref().unwrap().v_tag.clone(),
                            };
                            builder.repartition(key_pb.encode_to_vec());
                        }
                        (Some(Edge(_)), Some(Vertex(getv))) => {
                            let key_pb = common_pb::NameOrIdKey { key: getv.tag.clone() };
                            builder.repartition(key_pb.encode_to_vec());
                        }
                        _ => {}
                    }
                }
            }
            curr_node
                .borrow()
                .opr
                .add_job_builder(builder)?;
            prev_node_opt = curr_node_opt.clone();

            if curr_node.borrow().children.is_empty() {
                break;
            } else if curr_node.borrow().children.len() == 1 {
                let next_node_id = curr_node.borrow().get_first_child().unwrap();
                curr_node_opt = self.get_node(next_node_id);
            } else if curr_node.borrow().children.len() >= 2 {
                let (merge_node_opt, subplans) = self.get_branch_plans(curr_node.clone());
                let mut plans: Vec<Plan> = vec![];
                for subplan in subplans {
                    let mut bldr = JobBuilder::new(builder.conf.clone());
                    subplan.add_job_builder(&mut bldr)?;
                    plans.push(bldr.take_plan());
                }

                if let Some(merge_node) = merge_node_opt.clone() {
                    match &merge_node.borrow().opr.opr {
                        Some(Union(_)) => {
                            builder.merge(plans);
                        }
                        Some(Join(join_opr)) => {
                            if curr_node.borrow().children.len() > 2 {
                                // For now we only support joining two branches
                                return Err(PhysicalError::Unsupported);
                            }
                            assert_eq!(plans.len(), 2);
                            let left_plan = plans.get(0).unwrap().clone();
                            let right_plan = plans.get(1).unwrap().clone();

                            let join_kind =
                                unsafe { std::mem::transmute::<i32, pb::join::JoinKind>(join_opr.kind) };
                            let pegasus_join_kind = match join_kind {
                                JoinKind::Inner => server_pb::join::JoinKind::Inner,
                                JoinKind::LeftOuter => server_pb::join::JoinKind::LeftOuter,
                                JoinKind::RightOuter => server_pb::join::JoinKind::RightOuter,
                                JoinKind::FullOuter => server_pb::join::JoinKind::FullOuter,
                                JoinKind::Semi => server_pb::join::JoinKind::Semi,
                                JoinKind::Anti => server_pb::join::JoinKind::Anti,
                                JoinKind::Times => server_pb::join::JoinKind::Times,
                            };
                            let mut join_bytes = vec![];
                            pb::logical_plan::Operator::from(join_opr.clone()).encode(&mut join_bytes)?;

                            builder.join(pegasus_join_kind, left_plan, right_plan, join_bytes);
                        }
                        _ => return Err(PhysicalError::Unsupported),
                    }
                }
                curr_node_opt = merge_node_opt;

                if let Some(curr_node_clone) = curr_node_opt.clone() {
                    if curr_node_clone.borrow().children.len() <= 1 {
                        let next_id_opt = curr_node_clone.borrow().get_first_child();
                        prev_node_opt = curr_node_opt.clone();
                        // the current node has been processed in this round, should skip to the next node
                        curr_node_opt = next_id_opt.and_then(|id| self.get_node(id));
                    }
                }
            }
        }
        // TODO(longbin) Shall consider the option of sinking the results.
        builder.sink(vec![]);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use runtime::expr::str_to_expr_pb;

    use super::*;
    use crate::plan::logical::Node;

    #[test]
    fn test_poc_plan() {
        // g.V().hasLabel("person").has("id", 10).out("knows").limit(10)
        let source_opr = pb::logical_plan::Operator::from(pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(pb::QueryParams {
                table_names: vec![common_pb::NameOrId::from("person".to_string())],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
        });
        let select_opr = pb::logical_plan::Operator::from(pb::Select {
            predicate: Some(str_to_expr_pb("@.id == 10".to_string()).unwrap()),
        });
        let expand_opr = pb::logical_plan::Operator::from(pb::EdgeExpand {
            base: Some(pb::ExpandBase {
                v_tag: None,
                direction: 0,
                params: Some(pb::QueryParams {
                    table_names: vec![common_pb::NameOrId::from("knows".to_string())],
                    columns: vec![],
                    limit: None,
                    predicate: None,
                    requirements: vec![],
                }),
            }),
            is_edge: false,
            alias: None,
        });
        let limit_opr =
            pb::logical_plan::Operator::from(pb::Limit { range: Some(pb::Range { lower: 10, upper: 11 }) });
        let source_opr_bytes = source_opr.encode_to_vec();
        let select_opr_bytes = select_opr.encode_to_vec();
        let expand_opr_bytes = expand_opr.encode_to_vec();

        let mut logical_plan = LogicalPlan::with_root(Node::new(0, source_opr));
        logical_plan.append_operator_as_node(select_opr.clone(), vec![0]); // node 1
        logical_plan.append_operator_as_node(expand_opr.clone(), vec![1]); // node 2
        logical_plan.append_operator_as_node(limit_opr.clone(), vec![2]); // node 3
        let mut builder = JobBuilder::default();
        let _ = logical_plan.add_job_builder(&mut builder);

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(source_opr_bytes.clone());
        expected_builder.filter(select_opr_bytes);
        expected_builder.flat_map(expand_opr_bytes.clone());
        expected_builder.limit(10);
        expected_builder.sink(vec![]);

        assert_eq!(builder, expected_builder);
    }

    #[test]
    fn test_orderby() {
        let source_opr = pb::logical_plan::Operator::from(pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
        });

        let orderby_opr = pb::logical_plan::Operator::from(pb::OrderBy { pairs: vec![], limit: None });

        let topby_opr = pb::logical_plan::Operator::from(pb::OrderBy {
            pairs: vec![],
            limit: Some(pb::Range { lower: 10, upper: 11 }),
        });

        let source_opr_bytes = source_opr.encode_to_vec();
        let orderby_opr_bytes = orderby_opr.encode_to_vec();
        let topby_opr_bytes = topby_opr.encode_to_vec();

        let mut logical_plan = LogicalPlan::with_root(Node::new(0, source_opr));
        logical_plan.append_operator_as_node(orderby_opr.clone(), vec![0]); // node 1
        logical_plan.append_operator_as_node(topby_opr.clone(), vec![1]); // node 2
        let mut builder = JobBuilder::default();
        let _ = logical_plan.add_job_builder(&mut builder);

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(source_opr_bytes);
        expected_builder.sort_by(orderby_opr_bytes);
        expected_builder.sort_limit_by(10, topby_opr_bytes);

        assert_eq!(builder, expected_builder);
    }

    #[test]
    fn test_join_plan() {
        let source_opr = pb::logical_plan::Operator::from(pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
        });
        let expand_opr = pb::logical_plan::Operator::from(pb::EdgeExpand {
            base: Some(pb::ExpandBase { v_tag: None, direction: 0, params: None }),
            is_edge: false,
            alias: None,
        });
        let join_opr =
            pb::logical_plan::Operator::from(pb::Join { left_keys: vec![], right_keys: vec![], kind: 0 });
        let limit_opr =
            pb::logical_plan::Operator::from(pb::Limit { range: Some(pb::Range { lower: 10, upper: 11 }) });

        let source_opr_bytes = source_opr.encode_to_vec();
        let expand_opr_bytes = expand_opr.encode_to_vec();
        let join_opr_bytes = join_opr.encode_to_vec();

        let mut logical_plan = LogicalPlan::with_root(Node::new(0, source_opr));
        logical_plan.append_operator_as_node(expand_opr.clone(), vec![0]); // node 1
        logical_plan.append_operator_as_node(expand_opr.clone(), vec![0]); // node 2
        logical_plan.append_operator_as_node(expand_opr.clone(), vec![2]); // node 3
        logical_plan.append_operator_as_node(join_opr.clone(), vec![1, 3]); // node 4
        logical_plan.append_operator_as_node(pb::logical_plan::Operator::from(limit_opr.clone()), vec![4]); // node 5
        let mut builder = JobBuilder::default();
        let _ = logical_plan.add_job_builder(&mut builder);

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(source_opr_bytes);
        let mut left_plan = Plan::default();
        let mut right_plan = Plan::default();
        left_plan.flat_map(expand_opr_bytes.clone());
        right_plan.flat_map(expand_opr_bytes.clone());
        right_plan.flat_map(expand_opr_bytes);
        expected_builder.join(server_pb::join::JoinKind::Inner, left_plan, right_plan, join_opr_bytes);
        expected_builder.limit(10);

        assert_eq!(builder, expected_builder);
    }
}
