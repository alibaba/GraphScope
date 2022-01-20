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

use ir_common::generated::algebra as pb;
use ir_common::generated::algebra::join::JoinKind;
use ir_common::generated::common as common_pb;
use pegasus_client::builder::*;
use pegasus_server::pb as server_pb;
use prost::Message;

use crate::error::{IrError, IrResult};
use crate::plan::logical::{LogicalPlan, NodeType};
use crate::plan::meta::PlanMeta;

/// A trait for building physical plan (pegasus) from the logical plan
pub trait AsPhysical {
    /// To add pegasus's `JobBuilder`
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()>;

    /// To conduct necessary post processing before transforming into a physical plan.
    fn post_process(&mut self, _builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
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
    Sink,
}

fn simple_add_job_builder<M: Message>(
    builder: &mut JobBuilder, ir_opr: &M, opr: SimpleOpr,
) -> IrResult<()> {
    let bytes = ir_opr.encode_to_vec();
    match opr {
        SimpleOpr::Source => {
            builder.add_source(bytes);
        }
        SimpleOpr::Map => {
            builder.map(bytes);
        }
        SimpleOpr::FilterMap => {
            builder.filter_map(bytes);
        }
        SimpleOpr::Flatmap => {
            builder.flat_map(bytes);
        }
        SimpleOpr::Filter => {
            builder.filter(bytes);
        }
        SimpleOpr::SortBy => {
            builder.sort_by(bytes);
        }
        SimpleOpr::Dedup => {
            builder.dedup(bytes);
        }
        SimpleOpr::GroupBy => {
            builder.group_by(pegasus_server::pb::AccumKind::Custom, bytes);
        }
        SimpleOpr::Fold => {
            builder.fold_custom(pegasus_server::pb::AccumKind::Custom, bytes);
        }
        SimpleOpr::Sink => {
            builder.sink(bytes);
        }
    }
    Ok(())
}

impl AsPhysical for pb::Project {
    fn add_job_builder(&self, builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(self.clone()), SimpleOpr::Map)
    }
}

impl AsPhysical for pb::Select {
    fn add_job_builder(&self, builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(self.clone()), SimpleOpr::Filter)
    }
}

impl AsPhysical for pb::Scan {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        let mut scan = self.clone();
        scan.post_process(builder, plan_meta)?;
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(scan), SimpleOpr::Source)
    }

    fn post_process(&mut self, _builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        if let Some(params) = &mut self.params {
            if let Some(node_meta) = plan_meta.curr_node_meta() {
                let columns = node_meta.get_columns();
                if !columns.is_empty() {
                    params.columns.clear();
                    params.columns.extend(
                        columns
                            .iter()
                            .map(|tag| common_pb::NameOrId::from(tag.clone())),
                    )
                }
            }
        } else {
            if let Some(node_meta) = plan_meta.curr_node_meta() {
                let columns = node_meta.get_columns();
                if !columns.is_empty() {
                    self.params = Some(pb::QueryParams {
                        table_names: vec![],
                        columns: columns
                            .iter()
                            .map(|tag| common_pb::NameOrId::from(tag.clone()))
                            .collect(),
                        limit: None,
                        predicate: None,
                        requirements: vec![],
                    })
                }
            }
        }

        Ok(())
    }
}

impl AsPhysical for pb::EdgeExpand {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        let mut xpd = self.clone();
        xpd.post_process(builder, plan_meta)
        // simple_add_job_builder(builder, &pb::logical_plan::Operator::from(self.clone()), SimpleOpr::Flatmap)
    }

    fn post_process(&mut self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        let mut is_adding_auxilia = false;
        let mut auxilia = pb::Auxilia { params: None, alias: None };
        if let Some(node_meta) = plan_meta.curr_node_meta() {
            let columns = node_meta.get_columns();
            if !columns.is_empty() {
                if !self.is_edge {
                    // Vertex expansion
                    // Move everything to Auxilia
                    auxilia.params = Some(pb::QueryParams {
                        table_names: vec![],
                        columns: columns
                            .iter()
                            .map(|tag| common_pb::NameOrId::from(tag.clone()))
                            .collect(),
                        limit: None,
                        predicate: None,
                        requirements: vec![],
                    });
                    if self.alias.is_some() {
                        auxilia.alias = self.alias.clone();
                        self.alias = None;
                    }
                    is_adding_auxilia = true;
                } else {
                    if let Some(base) = self.base.as_mut() {
                        if let Some(params) = base.params.as_mut() {
                            params.columns.clear();
                            params.columns.extend(
                                columns
                                    .iter()
                                    .map(|tag| common_pb::NameOrId::from(tag.clone())),
                            )
                        }
                    }
                }
            }
        }
        simple_add_job_builder(
            builder,
            &pb::logical_plan::Operator::from(self.clone()),
            SimpleOpr::Flatmap,
        )?;
        if is_adding_auxilia {
            if plan_meta.is_partition() {
                let key_pb = common_pb::NameOrIdKey { key: None };
                builder.repartition(key_pb.encode_to_vec());
            }
            pb::logical_plan::Operator::from(auxilia).add_job_builder(builder, plan_meta)?;
        }

        Ok(())
    }
}

impl AsPhysical for pb::GetV {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        let mut getv = self.clone();
        getv.post_process(builder, plan_meta)
        // simple_add_job_builder(builder, &pb::logical_plan::Operator::from(getv), SimpleOpr::Map)
    }

    fn post_process(&mut self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        let mut is_adding_auxilia = false;
        let mut auxilia = pb::Auxilia { params: None, alias: None };
        if let Some(node_meta) = plan_meta.curr_node_meta() {
            let columns = node_meta.get_columns();
            if !columns.is_empty() {
                auxilia.alias = self.alias.clone();
                auxilia.params = Some(pb::QueryParams {
                    table_names: vec![],
                    columns: columns
                        .iter()
                        .map(|tag| common_pb::NameOrId::from(tag.clone()))
                        .collect(),
                    limit: None,
                    predicate: None,
                    requirements: vec![],
                });
                self.alias = None;
                is_adding_auxilia = true;
            }
        }
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(self.clone()), SimpleOpr::Map)?;
        if is_adding_auxilia {
            if plan_meta.is_partition() {
                let key_pb = common_pb::NameOrIdKey { key: None };
                builder.repartition(key_pb.encode_to_vec());
            }
            pb::logical_plan::Operator::from(auxilia).add_job_builder(builder, plan_meta)?;
        }

        Ok(())
    }
}

impl AsPhysical for pb::As {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        // Transform to `Auxilia` internally.
        let auxilia = pb::Auxilia { params: None, alias: self.alias.clone() };
        auxilia.add_job_builder(builder, plan_meta)
    }
}

impl AsPhysical for pb::Auxilia {
    fn add_job_builder(&self, builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        simple_add_job_builder(
            builder,
            &pb::logical_plan::Operator::from(self.clone()),
            SimpleOpr::FilterMap,
        )
    }
}

impl AsPhysical for pb::Limit {
    fn add_job_builder(&self, builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        if let Some(range) = &self.range {
            if range.upper <= range.lower || range.lower < 0 || range.upper <= 0 {
                Err(IrError::InvalidRangeError(range.lower, range.upper))
            } else {
                builder.limit((range.upper - 1) as u32);
                Ok(())
            }
        } else {
            Err(IrError::MissingDataError)
        }
    }
}

impl AsPhysical for pb::OrderBy {
    fn add_job_builder(&self, builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        let opr = pb::logical_plan::Operator::from(self.clone());
        if self.limit.is_none() {
            simple_add_job_builder(builder, &opr, SimpleOpr::SortBy)
        } else {
            let range = self.limit.clone().unwrap();
            if range.upper <= range.lower || range.lower < 0 || range.upper <= 0 {
                Err(IrError::InvalidRangeError(range.lower, range.upper))
            } else {
                let bytes = opr.encode_to_vec();
                builder.sort_limit_by((range.upper - 1) as i64, bytes);
                Ok(())
            }
        }
    }
}

impl AsPhysical for pb::Dedup {
    fn add_job_builder(&self, builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(self.clone()), SimpleOpr::Dedup)
    }
}
impl AsPhysical for pb::GroupBy {
    fn add_job_builder(&self, builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        let opr = pb::logical_plan::Operator::from(self.clone());
        if self.mappings.is_empty() {
            simple_add_job_builder(builder, &opr, SimpleOpr::Fold)
        } else {
            simple_add_job_builder(builder, &opr, SimpleOpr::GroupBy)
        }
    }
}

impl AsPhysical for pb::Sink {
    fn add_job_builder(&self, builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(self.clone()), SimpleOpr::Sink)
    }
}

impl AsPhysical for pb::logical_plan::Operator {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        use pb::logical_plan::operator::Opr::*;
        if let Some(opr) = &self.opr {
            match opr {
                Project(project) => project.add_job_builder(builder, plan_meta),
                Select(select) => select.add_job_builder(builder, plan_meta),
                Vertex(getv) => getv.add_job_builder(builder, plan_meta),
                Edge(edgexpd) => edgexpd.add_job_builder(builder, plan_meta),
                Scan(scan) => scan.add_job_builder(builder, plan_meta),
                Limit(limit) => limit.add_job_builder(builder, plan_meta),
                OrderBy(orderby) => orderby.add_job_builder(builder, plan_meta),
                Auxilia(auxilia) => auxilia.add_job_builder(builder, plan_meta),
                As(as_opr) => as_opr.add_job_builder(builder, plan_meta),
                Dedup(dedup) => dedup.add_job_builder(builder, plan_meta),
                GroupBy(groupby) => groupby.add_job_builder(builder, plan_meta),
                Sink(sink) => sink.add_job_builder(builder, plan_meta),
                _ => Err(IrError::Unsupported(format!("the operator {:?}", self))),
            }
        } else {
            Err(IrError::MissingDataError)
        }
    }
}

impl AsPhysical for NodeType {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        plan_meta.set_curr_node(self.borrow().id);
        self.borrow()
            .opr
            .add_job_builder(builder, plan_meta)
    }
}

impl AsPhysical for LogicalPlan {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        use pb::logical_plan::operator::Opr::*;
        let mut prev_node_opt: Option<NodeType> = None;
        let mut curr_node_opt = self.root();

        while curr_node_opt.is_some() {
            if plan_meta.is_partition() {
                if let Some(prev) = &prev_node_opt {
                    let prev_ref = prev.borrow();
                    let node_ref = curr_node_opt.as_ref().unwrap().borrow();
                    match (&prev_ref.opr.opr, &node_ref.opr.opr) {
                        (_, Some(Edge(edgexpd))) => {
                            let key_pb = common_pb::NameOrIdKey { key: edgexpd.v_tag.clone() };
                            builder.repartition(key_pb.encode_to_vec());
                        }
                        _ => {}
                    }
                }
            }

            let curr_node = curr_node_opt.as_ref().unwrap();
            curr_node.add_job_builder(builder, plan_meta)?;
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
                    subplan.add_job_builder(&mut bldr, plan_meta)?;
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
                                return Err(IrError::Unsupported(
                                    "joining more than two branches".to_string(),
                                ));
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
                        _ => {
                            return Err(IrError::Unsupported(
                                "operators other than `Union` and `Join`".to_string(),
                            ))
                        }
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

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::algebra::project::ExprAlias;
    use ir_common::generated::common as common_pb;

    use super::*;
    use crate::plan::logical::Node;

    #[allow(dead_code)]
    fn build_scan(columns: Vec<common_pb::NameOrId>) -> pb::Scan {
        pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns,
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
            idx_predicate: None,
        }
    }

    #[allow(dead_code)]
    fn build_edgexpd(
        is_edge: bool, columns: Vec<common_pb::NameOrId>, alias: Option<common_pb::NameOrId>,
    ) -> pb::EdgeExpand {
        pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(pb::QueryParams {
                table_names: vec![],
                columns,
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
            is_edge,
            alias,
        }
    }

    #[allow(dead_code)]
    fn build_getv() -> pb::GetV {
        pb::GetV { tag: None, opt: 1, alias: None }
    }

    #[allow(dead_code)]
    fn build_select(expr: &str) -> pb::Select {
        pb::Select { predicate: str_to_expr_pb(expr.to_string()).ok() }
    }

    #[allow(dead_code)]
    fn build_project(expr: &str) -> pb::Project {
        pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb(expr.to_string()).ok(),
                alias: None,
            }],
            is_append: false,
        }
    }

    #[test]
    fn test_post_process_edgexpd() {
        // g.V().outE()
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(build_scan(vec![]).into(), vec![])
            .unwrap();
        plan.append_operator_as_node(build_edgexpd(true, vec![], None).into(), vec![0])
            .unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(true, vec![], None)).encode_to_vec());
        expected_builder.sink(vec![]);

        assert_eq!(job_builder, expected_builder);

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta.set_partition(true);
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(true, vec![], None)).encode_to_vec());
        expected_builder.sink(vec![]);

        assert_eq!(job_builder, expected_builder);
    }

    #[test]
    fn test_post_process_edgexpd_columns_no_auxilia() {
        // g.V().outE().has("creationDate", 20220101)
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(build_scan(vec![]).into(), vec![])
            .unwrap();
        plan.append_operator_as_node(build_edgexpd(true, vec![], None).into(), vec![0])
            .unwrap();
        plan.append_operator_as_node(build_select("@.creationDate == 20220101").into(), vec![1])
            .unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta.set_partition(true);
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder.flat_map(
            pb::logical_plan::Operator::from(build_edgexpd(true, vec!["creationDate".into()], None))
                .encode_to_vec(),
        );
        expected_builder.filter(
            pb::logical_plan::Operator::from(build_select("@.creationDate == 20220101")).encode_to_vec(),
        );
        expected_builder.sink(vec![]);

        assert_eq!(job_builder, expected_builder);
    }

    #[test]
    fn test_post_process_edgexpd_columns_auxilia_shuffle() {
        // g.V().out().has("birthday", 20220101)
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(build_scan(vec![]).into(), vec![])
            .unwrap();
        plan.append_operator_as_node(build_edgexpd(false, vec![], None).into(), vec![0])
            .unwrap();
        plan.append_operator_as_node(build_select("@.birthday == 20220101").into(), vec![1])
            .unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(false, vec![], None)).encode_to_vec());
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(pb::Auxilia {
                params: Some(pb::QueryParams {
                    table_names: vec![],
                    columns: vec!["birthday".into()],
                    limit: None,
                    predicate: None,
                    requirements: vec![],
                }),
                alias: None,
            })
            .encode_to_vec(),
        );
        expected_builder.filter(
            pb::logical_plan::Operator::from(build_select("@.birthday == 20220101")).encode_to_vec(),
        );
        expected_builder.sink(vec![]);

        assert_eq!(job_builder, expected_builder);

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta.set_partition(true);
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(false, vec![], None)).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(pb::Auxilia {
                params: Some(pb::QueryParams {
                    table_names: vec![],
                    columns: vec!["birthday".into()],
                    limit: None,
                    predicate: None,
                    requirements: vec![],
                }),
                alias: None,
            })
            .encode_to_vec(),
        );
        expected_builder.filter(
            pb::logical_plan::Operator::from(build_select("@.birthday == 20220101")).encode_to_vec(),
        );
        expected_builder.sink(vec![]);

        assert_eq!(job_builder, expected_builder);
    }

    #[test]
    fn test_post_process_edgexpd_tag_auxilia_shuffle() {
        // g.V().out().as('a').select('a').by(valueMap("name", "id", "age")
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(build_scan(vec![]).into(), vec![])
            .unwrap();
        plan.append_operator_as_node(build_edgexpd(false, vec![], Some("a".into())).into(), vec![0])
            .unwrap();
        plan.append_operator_as_node(build_project("{@a.name, @a.id, @a.age}").into(), vec![1])
            .unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(false, vec![], None)).encode_to_vec());
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(pb::Auxilia {
                params: Some(pb::QueryParams {
                    table_names: vec![],
                    columns: vec!["age".into(), "id".into(), "name".into()],
                    limit: None,
                    predicate: None,
                    requirements: vec![],
                }),
                alias: Some("a".into()),
            })
            .encode_to_vec(),
        );
        expected_builder.map(
            pb::logical_plan::Operator::from(build_project("{@a.name, @a.id, @a.age}")).encode_to_vec(),
        );
        expected_builder.sink(vec![]);

        assert_eq!(job_builder, expected_builder);

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta.set_partition(true);
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(false, vec![], None)).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(pb::Auxilia {
                params: Some(pb::QueryParams {
                    table_names: vec![],
                    columns: vec!["age".into(), "id".into(), "name".into()],
                    limit: None,
                    predicate: None,
                    requirements: vec![],
                }),
                alias: Some("a".into()),
            })
            .encode_to_vec(),
        );
        expected_builder.map(
            pb::logical_plan::Operator::from(build_project("{@a.name, @a.id, @a.age}")).encode_to_vec(),
        );
        expected_builder.sink(vec![]);

        assert_eq!(job_builder, expected_builder);
    }

    #[test]
    fn test_post_process_edgexpd_tag_no_auxilia() {
        // g.V().out().as('a').select('a')
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(build_scan(vec![]).into(), vec![])
            .unwrap();
        plan.append_operator_as_node(build_edgexpd(false, vec![], Some("a".into())).into(), vec![0])
            .unwrap();
        plan.append_operator_as_node(build_project("@a").into(), vec![1])
            .unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta.set_partition(true);
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder.flat_map(
            pb::logical_plan::Operator::from(build_edgexpd(false, vec![], Some("a".into())))
                .encode_to_vec(),
        );
        expected_builder.map(pb::logical_plan::Operator::from(build_project("@a")).encode_to_vec());
        expected_builder.sink(vec![]);

        assert_eq!(job_builder, expected_builder);
    }

    #[test]
    fn test_post_process_scan() {
        let mut plan = LogicalPlan::default();
        // g.V().hasLabel("person").has("age", 27).valueMap("age", "name", "id")
        plan.append_operator_as_node(build_scan(vec![]).into(), vec![])
            .unwrap();
        // .hasLabel("person")
        plan.append_operator_as_node(build_select("@.~label == \"person\"").into(), vec![0])
            .unwrap();
        // .has("age", 27)
        plan.append_operator_as_node(build_select("@.age == 27").into(), vec![1])
            .unwrap();

        // .valueMap("age", "name", "id")
        plan.append_operator_as_node(build_project("{@.name, @.age, @.id}").into(), vec![2])
            .unwrap();

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(
            pb::logical_plan::Operator::from(build_scan(vec!["age".into(), "id".into(), "name".into()]))
                .encode_to_vec(),
        );

        expected_builder.filter(
            pb::logical_plan::Operator::from(build_select("@.~label == \"person\"")).encode_to_vec(),
        );
        expected_builder
            .filter(pb::logical_plan::Operator::from(build_select("@.age == 27")).encode_to_vec());
        expected_builder
            .map(pb::logical_plan::Operator::from(build_project("{@.name, @.age, @.id}")).encode_to_vec());
        expected_builder.sink(vec![]);

        assert_eq!(job_builder, expected_builder);
    }

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
            v_tag: None,
            direction: 0,
            params: Some(pb::QueryParams {
                table_names: vec![common_pb::NameOrId::from("knows".to_string())],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
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
        let mut plan_meta = PlanMeta::default();
        let _ = logical_plan.add_job_builder(&mut builder, &mut plan_meta);

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(source_opr_bytes);
        expected_builder.filter(select_opr_bytes);
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
        let mut plan_meta = PlanMeta::default();
        logical_plan
            .add_job_builder(&mut builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
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
        let mut plan_meta = PlanMeta::default();
        let _ = logical_plan.add_job_builder(&mut builder, &mut plan_meta);

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
            v_tag: None,
            direction: 0,
            params: None,
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
        let mut plan_meta = PlanMeta::default();
        let _ = logical_plan.add_job_builder(&mut builder, &mut plan_meta);

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
