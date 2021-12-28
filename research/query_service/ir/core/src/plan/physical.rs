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

use ir_common::expr_parse::error::{ExprError, ExprResult};
use ir_common::expr_parse::to_suffix_expr;
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
    ExprParseError(ExprError),
    MissingDataError,
    InvalidRangeError(i32, i32),
    Unsupported,
}

pub type PhysicalResult<T> = Result<T, PhysicalError>;

impl fmt::Display for PhysicalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PhysicalError::PbEncodeError(err) => write!(f, "encoding protobuf error: {:?}", err),
            PhysicalError::ExprParseError(err) => write!(f, "parse expression error: {:?}", err),
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

impl From<ExprError> for PhysicalError {
    fn from(err: ExprError) -> Self {
        Self::ExprParseError(err)
    }
}

/// A trait for building physical plan (pegasus) from the logical plan
pub trait AsPhysical {
    /// To add pegasus's `JobBuilder`
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()>;

    /// To conduct necessary post processing before transforming into a physical plan.
    fn post_process(&mut self) -> PhysicalResult<()> {
        Ok(())
    }
}

#[derive(PartialEq)]
enum SimpleOpr {
    Source,
    Map,
    FilterMap,
    Flatmap,
    Filter,
    SortBy,
    Dedup,
    GroupBy,
    Fold,
}

fn simple_add_job_builder<M: Message>(
    builder: &mut JobBuilder, ir_opr: &M, opr: SimpleOpr,
) -> PhysicalResult<()> {
    let bytes = ir_opr.encode_to_vec();
    match opr {
        SimpleOpr::Source => builder.add_source(bytes),
        SimpleOpr::Map => builder.map(bytes),
        SimpleOpr::FilterMap => builder.filter_map(bytes),
        SimpleOpr::Flatmap => builder.flat_map(bytes),
        SimpleOpr::Filter => builder.filter(bytes),
        SimpleOpr::SortBy => builder.sort_by(bytes),
        SimpleOpr::Dedup => builder.dedup(bytes),
        SimpleOpr::GroupBy => builder.group_by(pegasus_server::pb::AccumKind::Custom, bytes),
        SimpleOpr::Fold => builder.fold_custom(pegasus_server::pb::AccumKind::Custom, bytes),
    };
    Ok(())
}

fn expr_to_suffix_expr(expr: common_pb::Expression) -> ExprResult<common_pb::Expression> {
    let operators = to_suffix_expr(expr.operators)?;
    Ok(common_pb::Expression { operators })
}

impl AsPhysical for pb::Project {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        let mut project = self.clone();
        project.post_process()?;
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(project), SimpleOpr::Map)
    }

    fn post_process(&mut self) -> PhysicalResult<()> {
        for mapping in self.mappings.iter_mut() {
            if let Some(expr) = mapping.expr.as_mut() {
                *expr = expr_to_suffix_expr(expr.clone())?;
            }
        }

        Ok(())
    }
}

impl AsPhysical for pb::Select {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        let mut select = self.clone();
        select.post_process()?;
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(select), SimpleOpr::Filter)
    }

    fn post_process(&mut self) -> PhysicalResult<()> {
        if let Some(pred) = &mut self.predicate {
            *pred = expr_to_suffix_expr(pred.clone())?;
        }

        Ok(())
    }
}

impl AsPhysical for pb::Scan {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        let mut scan = self.clone();
        scan.post_process()?;
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(scan), SimpleOpr::Source)
    }

    fn post_process(&mut self) -> PhysicalResult<()> {
        if let Some(params) = &mut self.params {
            if let Some(pred) = &mut params.predicate {
                *pred = expr_to_suffix_expr(pred.clone())?;
            }
        }

        Ok(())
    }
}

impl AsPhysical for pb::EdgeExpand {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        let mut xpd = self.clone();
        xpd.post_process()?;
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(self.clone()), SimpleOpr::Flatmap)
    }

    fn post_process(&mut self) -> PhysicalResult<()> {
        if let Some(base) = &mut self.base {
            if let Some(params) = &mut base.params {
                if let Some(pred) = &mut params.predicate {
                    *pred = expr_to_suffix_expr(pred.clone())?;
                }
            }
        }

        Ok(())
    }
}

impl AsPhysical for pb::GetV {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(self.clone()), SimpleOpr::Map)
    }
}

impl AsPhysical for pb::Auxilia {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        let mut auxilia = self.clone();
        auxilia.post_process()?;
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(auxilia), SimpleOpr::FilterMap)
    }

    fn post_process(&mut self) -> PhysicalResult<()> {
        if let Some(params) = self.params.as_mut() {
            if let Some(pred) = params.predicate.as_mut() {
                *pred = expr_to_suffix_expr(pred.clone())?
            }
        }

        Ok(())
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
            simple_add_job_builder(builder, &opr, SimpleOpr::SortBy)
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

impl AsPhysical for pb::Dedup {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(self.clone()), SimpleOpr::Dedup)
    }
}
impl AsPhysical for pb::GroupBy {
    fn add_job_builder(&self, builder: &mut JobBuilder) -> PhysicalResult<()> {
        let opr = pb::logical_plan::Operator::from(self.clone());
        if self.mappings.is_empty() {
            simple_add_job_builder(builder, &opr, SimpleOpr::Fold)
        } else {
            simple_add_job_builder(builder, &opr, SimpleOpr::GroupBy)
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
                Limit(limit) => limit.add_job_builder(builder),
                OrderBy(orderby) => orderby.add_job_builder(builder),
                Auxilia(auxilia) => auxilia.add_job_builder(builder),
                Dedup(dedup) => dedup.add_job_builder(builder),
                GroupBy(groupby) => groupby.add_job_builder(builder),
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
                        (_, Some(Edge(edgexpd))) => {
                            let key_pb = common_pb::NameOrIdKey {
                                key: edgexpd.base.as_ref().unwrap().v_tag.clone(),
                            };
                            builder.repartition(key_pb.encode_to_vec());
                        }
                        (Some(Edge(edge_expand)), Some(Auxilia(_))) => {
                            if !edge_expand.is_edge {
                                let key_pb = common_pb::NameOrIdKey { key: None };
                                builder.repartition(key_pb.encode_to_vec());
                            }
                        }
                        (Some(Vertex(_)), Some(Auxilia(_))) => {
                            let key_pb = common_pb::NameOrIdKey { key: None };
                            builder.repartition(key_pb.encode_to_vec());
                        }
                        // TODO: add more shuffle situations, e.g., auxilia after group/fold/limit etc.
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
    use ir_common::expr_parse::{str_to_expr_pb, str_to_suffix_expr_pb};
    use ir_common::generated::algebra as pb;
    use ir_common::generated::algebra::project::ExprAlias;
    use ir_common::generated::common as common_pb;

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
            idx_predicate: None,
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
        let expand_opr_bytes = expand_opr.encode_to_vec();

        let mut logical_plan = LogicalPlan::with_root(Node::new(0, source_opr));
        logical_plan
            .append_operator_as_node(select_opr.clone(), vec![0])
            .unwrap(); // node 1
        logical_plan
            .append_operator_as_node(expand_opr.clone(), vec![1])
            .unwrap(); // node 2
        logical_plan
            .append_operator_as_node(limit_opr.clone(), vec![2])
            .unwrap(); // node 3
        let mut builder = JobBuilder::default();
        let _ = logical_plan.add_job_builder(&mut builder);

        let mut expected_builder = JobBuilder::default();
        let select_opr = pb::logical_plan::Operator::from(pb::Select {
            predicate: Some(str_to_suffix_expr_pb("@.id == 10".to_string()).unwrap()),
        });
        expected_builder.add_source(source_opr_bytes);
        expected_builder.filter(select_opr.encode_to_vec());
        expected_builder.flat_map(expand_opr_bytes);
        expected_builder.limit(10);
        expected_builder.sink(vec![]);

        assert_eq!(builder, expected_builder);
    }

    #[test]
    fn test_project() {
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
            idx_predicate: None,
        });

        let project_opr = pb::logical_plan::Operator::from(pb::Project {
            mappings: vec![
                ExprAlias {
                    expr: Some(str_to_expr_pb("10 * (@.class - 10)".to_string()).unwrap()),
                    alias: None,
                },
                ExprAlias { expr: Some(str_to_expr_pb("@.age - 1".to_string()).unwrap()), alias: None },
            ],
            is_append: false,
        });

        let mut logical_plan = LogicalPlan::with_root(Node::new(0, source_opr.clone()));
        logical_plan
            .append_operator_as_node(project_opr.clone(), vec![0])
            .unwrap(); // node 1
        let mut builder = JobBuilder::default();
        logical_plan
            .add_job_builder(&mut builder)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        let project_opr = pb::logical_plan::Operator::from(pb::Project {
            mappings: vec![
                ExprAlias {
                    expr: Some(str_to_suffix_expr_pb("10 * (@.class - 10)".to_string()).unwrap()),
                    alias: None,
                },
                ExprAlias {
                    expr: Some(str_to_suffix_expr_pb("@.age - 1".to_string()).unwrap()),
                    alias: None,
                },
            ],
            is_append: false,
        });

        expected_builder.add_source(source_opr.encode_to_vec());
        expected_builder.map(project_opr.encode_to_vec());
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
            idx_predicate: None,
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
        logical_plan
            .append_operator_as_node(orderby_opr.clone(), vec![0])
            .unwrap(); // node 1
        logical_plan
            .append_operator_as_node(topby_opr.clone(), vec![1])
            .unwrap(); // node 2
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
            idx_predicate: None,
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
        logical_plan
            .append_operator_as_node(expand_opr.clone(), vec![0])
            .unwrap(); // node 1
        logical_plan
            .append_operator_as_node(expand_opr.clone(), vec![0])
            .unwrap(); // node 2
        logical_plan
            .append_operator_as_node(expand_opr.clone(), vec![2])
            .unwrap(); // node 3
        logical_plan
            .append_operator_as_node(join_opr.clone(), vec![1, 3])
            .unwrap(); // node 4
        logical_plan
            .append_operator_as_node(pb::logical_plan::Operator::from(limit_opr.clone()), vec![4])
            .unwrap(); // node 5
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
