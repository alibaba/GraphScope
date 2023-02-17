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

use ir_common::expr_parse::str_to_expr_pb;
use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use ir_common::generated::common::expr_opr::Item;
use ir_common::KeyId;
use ir_physical_client::physical_builder::{JobBuilder, Plan};

use crate::error::{IrError, IrResult};
use crate::plan::logical::{LogicalPlan, NodeType};
use crate::plan::meta::{ColumnsOpt, PlanMeta, TagId};

/// A trait for building physical plan (pegasus) from the logical plan
pub trait AsPhysical {
    /// To add pegasus's `JobBuilder`
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()>;

    /// To conduct necessary post processing before transforming into a physical plan.
    fn post_process(&mut self, _builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        Ok(())
    }
}

fn process_tag_columns(builder: &mut JobBuilder, tag: Option<TagId>, columns_opt: ColumnsOpt) {
    if columns_opt.len() > 0 {
        let tag_pb = tag.map(|tag_id| (tag_id as i32).into());
        builder.shuffle(tag_pb.clone());
        let params = pb::QueryParams {
            tables: vec![],
            columns: columns_opt
                .get()
                .into_iter()
                .map(|column| column.into())
                .collect(),
            is_all_columns: columns_opt.is_all(),
            limit: None,
            predicate: None,
            sample_ratio: 1.0,
            extra: Default::default(),
        };
        let auxilia = pb::GetV { tag: tag_pb.clone(), opt: 4, params: Some(params), alias: tag_pb.clone() };
        builder.get_v(auxilia);
    }
}

// Fetch properties before used in select, order, dedup, group, join, and apply.
fn post_process_vars(builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
    let node_meta = plan_meta.get_curr_node_meta().unwrap();
    let tag_columns = node_meta.get_tag_columns();
    let len = tag_columns.len();
    // the case when head not changed
    if len == 1 {
        let tag = tag_columns.iter().next().unwrap().0;
        if tag.is_none() {
            let (tag, columns_opt) = tag_columns.into_iter().next().unwrap();
            process_tag_columns(builder, tag, columns_opt);
            return Ok(());
        }
    }

    // the case when head changed
    if len > 0 {
        let tmp_head_alias = plan_meta.get_or_set_tag_id("~tmp_head_tag").1;
        // preserve head first; i.e., .as("~tmp_head_tag")
        builder.get_v(pb::GetV {
            tag: None,
            opt: 4,
            params: None,
            alias: Some((tmp_head_alias as KeyId).into()),
        });
        for (tag, columns_opt) in tag_columns.into_iter() {
            let tag = if tag.is_none() { Some(tmp_head_alias) } else { tag };
            process_tag_columns(builder, tag, columns_opt);
        }
        // project back to head; i.e., .select("~tmp_head_tag")
        builder.project(pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_expr_pb(format!("@{}", tmp_head_alias))?),
                alias: None,
            }],
            is_append: true,
        });
    }
    Ok(())
}

impl AsPhysical for pb::Project {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        let mut project = self.clone();
        project.post_process(builder, plan_meta)?;
        builder.project(project);
        Ok(())
    }

    fn post_process(&mut self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        if plan_meta.is_partition() {
            let tag_columns = plan_meta
                .get_curr_node_meta()
                .unwrap()
                .get_tag_columns();
            if tag_columns.len() > 0 {
                for (tag, columns_opt) in tag_columns.into_iter() {
                    process_tag_columns(builder, tag, columns_opt);
                }
            }
        }
        Ok(())
    }
}

impl AsPhysical for pb::Select {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        let mut select = self.clone();
        select.post_process(builder, plan_meta)?;
        builder.select(select);
        Ok(())
    }

    fn post_process(&mut self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        // This is the case when g.V().out().has(xxx), which was like Source + EdgeExpand(ExpandV) + Filter in logical plan.
        // This would be refined as:
        // In Logical Plan: `Source + EdgeExpand(ExpandE) + GetV`
        // In Physical Plan:
        //       1. on distributed graph store, `Source + EdgeExpand(ExpandV) + Shuffle + Auxilia`
        //    or 2. on single graph store, `Source + EdgeExpand(ExpandE) + GetV`
        // where Auxilia means get property or filter on a vertex.
        if plan_meta.is_partition() {
            post_process_vars(builder, plan_meta)?;
        }
        Ok(())
    }
}

impl AsPhysical for pb::Scan {
    fn add_job_builder(&self, builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        let scan = self.clone();
        builder.add_scan_source(scan);
        Ok(())
    }
}

impl AsPhysical for pb::EdgeExpand {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        let mut xpd = self.clone();
        xpd.post_process(builder, plan_meta)?;
        builder.edge_expand(xpd);
        Ok(())
    }

    fn post_process(&mut self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        if plan_meta.is_partition() {
            builder.shuffle(self.v_tag.clone());
            // Notice that if expand edges, we need to carry its demanded properties,
            // since query edge by eid is not supported in storage for now.
            if self.expand_opt == pb::edge_expand::ExpandOpt::Edge as i32 {
                if let Some(params) = self.params.as_mut() {
                    let node_meta = plan_meta.get_curr_node_meta().unwrap();
                    let columns = node_meta.get_columns();
                    let is_all_columns = node_meta.is_all_columns();
                    if !columns.is_empty() || is_all_columns {
                        params.columns = columns
                            .into_iter()
                            .map(|tag| tag.into())
                            .collect();
                        params.is_all_columns = is_all_columns;
                    }
                }
            }
        }
        Ok(())
    }
}

impl AsPhysical for pb::PathExpand {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        // [range.lower, range.upper)
        let range = self
            .hop_range
            .as_ref()
            .ok_or(IrError::MissingData("PathExpand::hop_range".to_string()))?;
        if range.upper <= range.lower || range.lower < 0 || range.upper <= 0 {
            Err(IrError::InvalidRange(range.lower, range.upper))?
        }
        // Currently, we assume ExpandBase in PathExpand is `EdgeExpand` with ExpandOpt = Vertex.
        // TODO: Support ExpandBase of EdgeExpand + GetV
        // and furthermore, fuse EdgeExpand + GetV if possible.
        let mut path = self.clone();
        path.post_process(builder, plan_meta)?;
        builder.path_expand(path);
        Ok(())
    }

    fn post_process(&mut self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        if plan_meta.is_partition() {
            builder.shuffle(self.start_tag.clone());
        }
        Ok(())
    }
}

impl AsPhysical for pb::GetV {
    fn add_job_builder(&self, builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        // TODO: ExpandEdge + GetV fusion rule:
        // For example,currently, the case of `g.V().out().has(xxx)` would be translated into `Source + EdgeExpand(ExpandV) + Filter` in logical plan.
        // This would be refined as:
        // In Logical Plan: `Source + EdgeExpand(ExpandE) + GetV`
        // In Physical Plan:
        //       1. on distributed graph store, `Source + EdgeExpand(ExpandV) + Shuffle + GetV(Self)`
        //    or 2. on single graph store, `Source + EdgeExpand(ExpandE) + GetV(Self)`
        // where GetV(Self) means to get property or filter on the adj vertex itself.
        // Besides, if no properties fetching on the adj vertices, we can directly fuse `EdgeExpand(ExpandE) + GetV` into `EdgeExpand(ExpandV)`.
        let getv = self.clone();
        // no need to shuffle since GetV (in logical plan) only fetches adjacent vertex ids from an edge.
        builder.get_v(getv);
        Ok(())
    }
}

impl AsPhysical for pb::As {
    fn add_job_builder(&self, builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        let auxilia = pb::GetV { tag: None, opt: 4, params: None, alias: self.alias.clone() };
        builder.get_v(auxilia);
        Ok(())
    }
}

impl AsPhysical for pb::Limit {
    fn add_job_builder(&self, builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        let range = self
            .range
            .as_ref()
            .ok_or(IrError::MissingData("Limit::range".to_string()))?;
        if range.upper <= range.lower || range.lower < 0 || range.upper <= 0 {
            Err(IrError::InvalidRange(range.lower, range.upper))?
        }
        builder.limit(self.clone());
        Ok(())
    }
}

impl AsPhysical for pb::OrderBy {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        if let Some(range) = &self.limit {
            if range.upper <= range.lower || range.lower < 0 || range.upper <= 0 {
                Err(IrError::InvalidRange(range.lower, range.upper))?
            }
        }
        let mut order = self.clone();
        order.post_process(builder, plan_meta)?;
        builder.order(order);
        Ok(())
    }

    fn post_process(&mut self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        if plan_meta.is_partition() {
            post_process_vars(builder, plan_meta)?;
        }
        Ok(())
    }
}

impl AsPhysical for pb::Dedup {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        let mut dedup = self.clone();
        dedup.post_process(builder, plan_meta)?;
        builder.dedup(dedup);
        Ok(())
    }

    fn post_process(&mut self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        if plan_meta.is_partition() {
            post_process_vars(builder, plan_meta)?;
        }
        Ok(())
    }
}

impl AsPhysical for pb::GroupBy {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        let mut group = self.clone();
        group.post_process(builder, plan_meta)?;
        builder.group(group);
        Ok(())
    }
    fn post_process(&mut self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        if plan_meta.is_partition() {
            post_process_vars(builder, plan_meta)?;
        }
        Ok(())
    }
}

impl AsPhysical for pb::Unfold {
    fn add_job_builder(&self, builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        builder.unfold(self.clone());
        Ok(())
    }
}

impl AsPhysical for pb::Sink {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        let mut sink_opr = self.clone();
        let target = self
            .sink_target
            .as_ref()
            .ok_or(IrError::MissingData("Sink::sink_target".to_string()))?;
        match target
            .inner
            .as_ref()
            .ok_or(IrError::MissingData("Sink::sink_target::Inner".to_string()))?
        {
            pb::sink::sink_target::Inner::SinkDefault(_) => {
                let tag_id_mapping = plan_meta
                    .get_tag_id_mappings()
                    .iter()
                    .map(|(tag, id)| pb::sink_default::IdNameMapping {
                        id: *id as KeyId,
                        name: tag.clone(),
                        meta_type: 3,
                    })
                    .collect();
                let sink_target = pb::sink::SinkTarget {
                    inner: Some(pb::sink::sink_target::Inner::SinkDefault(pb::SinkDefault {
                        id_name_mappings: tag_id_mapping,
                    })),
                };
                sink_opr.sink_target = Some(sink_target);
            }
            pb::sink::sink_target::Inner::SinkVineyard(sink_vineyard) => {
                use crate::plan::meta::STORE_META;
                let graph_name = sink_vineyard.graph_name.clone();
                loop {
                    if let Ok(meta) = STORE_META.try_read() {
                        let sink_target = pb::sink::SinkTarget {
                            inner: Some(pb::sink::sink_target::Inner::SinkVineyard(pb::SinkVineyard {
                                graph_name,
                                graph_schema: meta.schema.clone().map(|schema| schema.into()),
                            })),
                        };
                        sink_opr.sink_target = Some(sink_target);
                        break;
                    }
                }
            }
        };

        builder.sink(sink_opr.clone());
        Ok(())
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
                Path(pathxpd) => pathxpd.add_job_builder(builder, plan_meta),
                Scan(scan) => scan.add_job_builder(builder, plan_meta),
                Limit(limit) => limit.add_job_builder(builder, plan_meta),
                OrderBy(orderby) => orderby.add_job_builder(builder, plan_meta),
                As(as_opr) => as_opr.add_job_builder(builder, plan_meta),
                Dedup(dedup) => dedup.add_job_builder(builder, plan_meta),
                GroupBy(groupby) => groupby.add_job_builder(builder, plan_meta),
                Sink(sink) => sink.add_job_builder(builder, plan_meta),
                Union(_) => Ok(()),
                Intersect(_) => Ok(()),
                Unfold(unfold) => unfold.add_job_builder(builder, plan_meta),
                _ => Err(IrError::Unsupported(format!("the operator {:?}", self))),
            }
        } else {
            Err(IrError::MissingData("logical_plan::Operator::opr".to_string()))
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

fn extract_expand_degree(node: NodeType) -> Option<pb::EdgeExpand> {
    if let Some(pb::logical_plan::operator::Opr::Edge(edgexpd)) = &node.borrow().opr.opr {
        if edgexpd.expand_opt == 2 {
            // expand to degree
            return Some(edgexpd.clone());
        }
    }

    None
}

fn extract_project_single_tag(node: NodeType) -> Option<common_pb::NameOrId> {
    if let Some(pb::logical_plan::operator::Opr::Project(project)) = &node.borrow().opr.opr {
        if project.mappings.len() == 1 {
            if let Some(expr) = &project.mappings.first().unwrap().expr {
                if expr.operators.len() == 1 {
                    if let Some(expr_opr) = expr.operators.first() {
                        match expr_opr.item.as_ref().unwrap() {
                            Item::Var(var) => {
                                if var.property.is_none() {
                                    return var.tag.clone(); // project tag only
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    None
}

impl AsPhysical for LogicalPlan {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        use pb::logical_plan::operator::Opr::*;
        let mut _prev_node_opt: Option<NodeType> = None;
        let mut curr_node_opt = self.get_first_node();
        debug!("plan: {:#?}", self);
        debug!("is_partition: {:?}", self.meta.is_partition());
        while curr_node_opt.is_some() {
            let curr_node = curr_node_opt.as_ref().unwrap();
            let curr_node_id = curr_node.borrow().id;
            if let Some(Apply(apply_opr)) = curr_node.borrow().opr.opr.as_ref() {
                let mut sub_bldr = JobBuilder::default();
                if let Some(subplan) = self.extract_subplan(curr_node.clone()) {
                    let mut expand_degree_opt = None;
                    if subplan.len() <= 2 {
                        if subplan.len() == 1 {
                            expand_degree_opt = subplan
                                .get_first_node()
                                .and_then(|node| extract_expand_degree(node));
                        } else {
                            let first_node_opt = subplan.get_first_node();
                            let second_node_opt = subplan.get_last_node();
                            let tag_opt: Option<common_pb::NameOrId> =
                                first_node_opt.and_then(|node| extract_project_single_tag(node));
                            if let Some(tag) = tag_opt {
                                if let Some(mut expand_degree) =
                                    second_node_opt.and_then(|node| extract_expand_degree(node))
                                {
                                    expand_degree.v_tag = Some(tag);
                                    expand_degree_opt = Some(expand_degree);
                                }
                            }
                        }
                    }
                    if let Some(mut expand_degree) = expand_degree_opt {
                        // The alias of `Apply` becomes the `alias` of `EdgeExpand`
                        expand_degree.alias = apply_opr.alias.clone();
                        if plan_meta.is_partition() {
                            let key_pb = expand_degree.v_tag.clone();
                            builder.shuffle(key_pb);
                        }
                        // If the subtask of apply takes only one operator and the operator is
                        // `EdgeExpand` with `expand_opt` set as `Degree`, then the following
                        //  steps are conducted instead of executing subtask:
                        //   `As('~expand_degree_<id>')` +
                        //   `EdgeExpand()` +
                        //   `Select('~expand_degree_<id>')`
                        let new_tag = plan_meta
                            .get_or_set_tag_id(&format!("~expand_degree_{:?}", curr_node_id))
                            .1 as i32;
                        builder.get_v(pb::GetV {
                            tag: None,
                            opt: 4,
                            params: None,
                            alias: Some(common_pb::NameOrId {
                                item: Some(common_pb::name_or_id::Item::Id(new_tag)),
                            }),
                        });
                        builder.edge_expand(expand_degree);
                        builder.project(pb::Project {
                            mappings: vec![pb::project::ExprAlias {
                                expr: str_to_expr_pb(format!("@{:?}", new_tag)).ok(),
                                alias: None,
                            }],
                            is_append: true,
                        });
                    } else {
                        subplan.add_job_builder(&mut sub_bldr, plan_meta)?;
                        let plan = sub_bldr.take_plan();
                        builder.apply(
                            unsafe { std::mem::transmute(apply_opr.join_kind) },
                            plan,
                            apply_opr.alias.clone(),
                        );
                    }
                } else {
                    return Err(IrError::MissingData("Apply::subplan".to_string()));
                }
            } else {
                curr_node.add_job_builder(builder, plan_meta)?;
            }

            _prev_node_opt = curr_node_opt.clone();

            if curr_node.borrow().children.is_empty() {
                break;
            } else if curr_node.borrow().children.len() == 1 {
                let next_node_id = curr_node.borrow().get_first_child().unwrap();
                curr_node_opt = self.get_node(next_node_id);
            } else if curr_node.borrow().children.len() >= 2 {
                let (merge_node_opt, subplans) = self.get_branch_plans(curr_node.clone());
                let mut plans: Vec<Plan> = vec![];
                for subplan in &subplans {
                    let mut sub_bldr = JobBuilder::new(builder.conf.clone());
                    subplan.add_job_builder(&mut sub_bldr, plan_meta)?;
                    plans.push(sub_bldr.take_plan());
                }

                if let Some(merge_node) = merge_node_opt.clone() {
                    match &merge_node.borrow().opr.opr {
                        Some(Union(_)) => {
                            builder.union(plans);
                        }
                        Some(Intersect(intersect)) => {
                            add_intersect_job_builder(builder, plan_meta, intersect, &subplans)?;
                        }
                        Some(Join(join_opr)) => {
                            if curr_node.borrow().children.len() != 2 {
                                // For now we only support joining two branches
                                return Err(IrError::Unsupported(
                                    "joining more than two branches".to_string(),
                                ));
                            }
                            let left_plan = plans.get(0).unwrap().clone();
                            let right_plan = plans.get(1).unwrap().clone();

                            let node_meta = plan_meta.get_curr_node_meta().unwrap();
                            let tag_columns = node_meta.get_tag_columns();
                            println!("notice the tag_columns in join!! {:?}", tag_columns);
                            post_process_vars(builder, plan_meta)?;
                            builder.join(
                                unsafe { std::mem::transmute(join_opr.kind) },
                                left_plan,
                                right_plan,
                                join_opr.left_keys.clone(),
                                join_opr.right_keys.clone(),
                            );
                        }
                        None => {
                            return Err(IrError::MissingData(
                                "Union/Intersect/Join::merge_node".to_string(),
                            ))
                        }
                        _ => {
                            return Err(IrError::Unsupported(format!(
                            "Operators other than `Union` , `Intersect`, or `Join`. The operator is {:?}",
                            merge_node
                        )))
                        }
                    }
                }
                curr_node_opt = merge_node_opt;

                if let Some(curr_node_clone) = curr_node_opt.clone() {
                    if curr_node_clone.borrow().children.len() <= 1 {
                        let next_id_opt = curr_node_clone.borrow().get_first_child();
                        _prev_node_opt = curr_node_opt.clone();
                        // the current node has been processed in this round, should skip to the next node
                        curr_node_opt = next_id_opt.and_then(|id| self.get_node(id));
                    }
                }
            }
        }

        Ok(())
    }
}

// Given a->b, we support intersecting their neighbors, e.g., Intersect{{a->c, b->c}, key=c}
// more cases as follows:
// 1. To intersect a->d->c and b->c with key=c,
// if so, translate into two operators, i.e., EdgeExpand{a->d} and Intersect{{d->c, b->c}, key=c}
// 2. To intersect a->c->d and b->c with key=c,
// if so, translate into two operators, i.e., Intersect{{a->c, b->c}, key=c} and Expand{c->d}
// 3. To intersect a->c, b->c with key=c, with filters
// we support expanding vertices with filters on edges (i.e., filters on a->c, b->c), e.g., Intersect{{a-filter->c, b-filter->c}, key=c};
// if expanding vertices with filters on vertices (i.e., filters on c), translate into Intersect{{a->c, b->c}, key=c} + Select {filter on c}
// For now, this logic is in the translation rule in Pattern in logical plan
// TODO: move this logic into physical layer, as we may able to directly filter vertices during intersection if we have the global view of storage.

// Thus, after build intersect, the physical plan looks like:
// 1. the last ops in intersect's sub_plans are the ones to intersect;
// 2. the intersect op can be:
//     1) EdgeExpand with Opt = ExpandV, which is to expand and intersect on id-only vertices; (supported currently)
//     2) EdgeExpand with Opt = ExpandE, which is to expand and intersect on edges (although, not considered in Pattern yet);
// and 3) GetV with Opt = Self, which is to expand and intersect on vertices, while there may be some filters on the intersected vertices. (TODO e2e)

fn add_intersect_job_builder(
    builder: &mut JobBuilder, plan_meta: &mut PlanMeta, intersect_opr: &pb::Intersect,
    subplans: &Vec<LogicalPlan>,
) -> IrResult<()> {
    use pb::logical_plan::operator::Opr::*;

    let intersect_tag = intersect_opr
        .key
        .as_ref()
        .ok_or(IrError::ParsePbError("Empty tag in `Intersect` opr".into()))?;
    let mut intersect_plans: Vec<Plan> = vec![];
    for subplan in subplans {
        // subplan would be like: 1. vec![EdgeExpand] for edge expand to intersect; 2. vec![PathExpand, GetV, EdgeExpand] for path expand to intersect
        let len = subplan.len();
        let mut sub_bldr = JobBuilder::new(builder.conf.clone());
        for (idx, (_, opr)) in subplan.nodes.iter().enumerate() {
            if idx != len - 1 {
                opr.add_job_builder(builder, plan_meta)?;
            } else {
                // the last node in subplan should be the one to intersect.
                if let Some(Edge(edgexpd)) = opr.borrow().opr.opr.as_ref() {
                    let edgexpd = edgexpd.clone();
                    if edgexpd.alias.is_none()
                        || !edgexpd
                            .alias
                            .as_ref()
                            .unwrap()
                            .eq(intersect_tag)
                    {
                        Err(IrError::InvalidPattern("Cannot intersect on different tags".to_string()))?
                    }
                    if plan_meta.is_partition() {
                        sub_bldr.shuffle(edgexpd.v_tag.clone());
                    }
                    sub_bldr.edge_expand(edgexpd);
                } else {
                    Err(IrError::Unsupported(format!(
                        "Should be `EdgeExpand` opr for intersection, but the opr is {:?}",
                        opr
                    )))?
                }
            }
        }
        let sub_plan = sub_bldr.take_plan();
        intersect_plans.push(sub_plan);
    }
    builder.intersect(intersect_plans, intersect_tag.clone());
    let unfold = pb::Unfold { tag: Some(intersect_tag.clone()), alias: Some(intersect_tag.clone()) };
    unfold.add_job_builder(builder, plan_meta)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::algebra::project::ExprAlias;
    use ir_common::generated::common as common_pb;

    use super::*;
    use crate::plan::logical::Node;

    #[allow(dead_code)]
    fn query_params(
        tables: Vec<common_pb::NameOrId>, columns: Vec<common_pb::NameOrId>,
    ) -> pb::QueryParams {
        pb::QueryParams {
            tables,
            columns,
            is_all_columns: false,
            limit: None,
            predicate: None,
            sample_ratio: 1.0,
            extra: HashMap::new(),
        }
    }

    #[allow(dead_code)]
    fn build_scan(columns: Vec<common_pb::NameOrId>) -> pb::Scan {
        pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![], columns)),
            idx_predicate: None,
        }
    }

    /// `expand_opt`: 0 -> Vertex, 1 -> Edge, 2 -> Degree
    #[allow(dead_code)]
    fn build_edgexpd(
        expand_opt: i32, columns: Vec<common_pb::NameOrId>, alias: Option<common_pb::NameOrId>,
    ) -> pb::EdgeExpand {
        pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![], columns)),
            alias,
            expand_opt,
        }
    }

    #[allow(dead_code)]
    fn build_getv(alias: Option<common_pb::NameOrId>) -> pb::GetV {
        pb::GetV { tag: None, opt: 1, params: Some(query_params(vec![], vec![])), alias }
    }

    #[allow(dead_code)]
    fn build_select(expr: &str) -> pb::Select {
        pb::Select { predicate: str_to_expr_pb(expr.to_string()).ok() }
    }

    #[allow(dead_code)]
    fn build_auxilia_with_predicates(expr: &str) -> pb::GetV {
        let mut params = query_params(vec![], vec![]);
        params.predicate = str_to_expr_pb(expr.to_string()).ok();
        pb::GetV { tag: None, opt: 4, params: Some(params), alias: None }
    }

    #[allow(dead_code)]
    fn build_auxilia_with_params(
        params: Option<pb::QueryParams>, alias: Option<common_pb::NameOrId>,
    ) -> pb::GetV {
        pb::GetV { tag: None, opt: 4, params, alias }
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

    #[allow(dead_code)]
    fn build_sink() -> pb::Sink {
        pb::Sink {
            tags: vec![],
            sink_target: Some(pb::sink::SinkTarget {
                inner: Some(pb::sink::sink_target::Inner::SinkDefault(pb::SinkDefault {
                    id_name_mappings: vec![],
                })),
            }),
        }
    }

    #[test]
    fn post_process_edgexpd() {
        // g.V().outE()
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(build_scan(vec![]).into(), vec![])
            .unwrap();
        plan.append_operator_as_node(build_edgexpd(1, vec![], None).into(), vec![0])
            .unwrap();
        plan.append_operator_as_node(build_sink().into(), vec![1])
            .unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(build_scan(vec![]));
        expected_builder.edge_expand(build_edgexpd(1, vec![], None));
        expected_builder.sink(build_sink());

        assert_eq!(job_builder, expected_builder);

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta = plan_meta.with_partition();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(build_scan(vec![]));
        expected_builder.shuffle(None);
        expected_builder.edge_expand(build_edgexpd(1, vec![], None));
        expected_builder.sink(build_sink());

        assert_eq!(job_builder, expected_builder);
    }

    #[test]
    fn post_process_edgexpd_property_filter_as_auxilia() {
        // g.V().out().has("birthday", 20220101)
        // In this case, the Select will be translated into an Auxilia, to fetch and filter the
        // results in one single `FilterMap` pegasus operator.
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(build_scan(vec![]).into(), vec![])
            .unwrap();
        plan.append_operator_as_node(build_edgexpd(0, vec![], None).into(), vec![0])
            .unwrap();
        plan.append_operator_as_node(build_select("@.birthday == 20220101").into(), vec![1])
            .unwrap();
        plan.append_operator_as_node(build_sink().into(), vec![2])
            .unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(build_scan(vec![]));
        expected_builder.edge_expand(build_edgexpd(0, vec![], None));
        expected_builder.get_v(build_auxilia_with_predicates("@.birthday == 20220101"));
        expected_builder.sink(build_sink());

        assert_eq!(job_builder, expected_builder);

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta = plan_meta.with_partition();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(build_scan(vec![]));
        expected_builder.shuffle(None);
        expected_builder.edge_expand(build_edgexpd(0, vec![], None));
        expected_builder.shuffle(None);
        expected_builder.get_v(build_auxilia_with_predicates("@.birthday == 20220101"));
        expected_builder.sink(build_sink());

        assert_eq!(job_builder, expected_builder);
    }

    #[test]
    fn post_process_edgexpd_label_filter() {
        // g.V().out().filter(@.~label == "person")
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(build_scan(vec![]).into(), vec![])
            .unwrap();
        plan.append_operator_as_node(build_edgexpd(0, vec![], None).into(), vec![0])
            .unwrap();
        plan.append_operator_as_node(build_select("@.~label == \"person\"").into(), vec![1])
            .unwrap();
        plan.append_operator_as_node(build_sink().into(), vec![2])
            .unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(build_scan(vec![]));
        expected_builder.edge_expand(build_edgexpd(0, vec![], None));
        expected_builder.select(build_select("@.~label == \"person\""));
        expected_builder.sink(build_sink());

        assert_eq!(job_builder, expected_builder);
    }

    #[test]
    fn post_process_edgexpd_multi_tag_property_filter() {
        // g.V().out().as(0).out().as(1).filter(0.age > 1.age)
        // In this case, the Select cannot be translated into an Auxilia, as it contains
        // fetching the properties from two different nodes. In this case, we need to fetch
        // the properties using `Auxilia` right after the first and second `out()`, and can
        // finally execute the selection of "0.age > 1.age".
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(build_scan(vec![]).into(), vec![])
            .unwrap();
        plan.append_operator_as_node(build_edgexpd(0, vec![], Some(0.into())).into(), vec![0])
            .unwrap();
        plan.append_operator_as_node(build_edgexpd(0, vec![], Some(1.into())).into(), vec![1])
            .unwrap();
        plan.append_operator_as_node(build_select("@0.age > @1.age").into(), vec![2])
            .unwrap();
        plan.append_operator_as_node(build_sink().into(), vec![3])
            .unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(build_scan(vec![]));
        expected_builder.edge_expand(build_edgexpd(0, vec![], None));
        expected_builder.get_v(build_auxilia_with_params(
            Some(query_params(vec![], vec!["age".into()])),
            Some(0.into()),
        ));
        expected_builder.edge_expand(build_edgexpd(0, vec![], None));
        expected_builder.get_v(build_auxilia_with_params(
            Some(query_params(vec![], vec!["age".into()])),
            Some(1.into()),
        ));
        expected_builder.select(build_select("@0.age > @1.age"));
        expected_builder.sink(build_sink());

        assert_eq!(job_builder, expected_builder);

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta = plan_meta.with_partition();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(build_scan(vec![]));
        expected_builder.shuffle(None);
        expected_builder.edge_expand(build_edgexpd(0, vec![], None));
        expected_builder.shuffle(None);
        expected_builder.get_v(build_auxilia_with_params(
            Some(query_params(vec![], vec!["age".into()])),
            Some(0.into()),
        ));
        expected_builder.shuffle(None);
        expected_builder.edge_expand(build_edgexpd(0, vec![], None));
        expected_builder.shuffle(None);
        expected_builder.get_v(build_auxilia_with_params(
            Some(query_params(vec![], vec!["age".into()])),
            Some(1.into()),
        ));
        expected_builder.select(build_select("@0.age > @1.age"));
        expected_builder.sink(build_sink());

        assert_eq!(job_builder, expected_builder);
    }

    #[test]
    fn post_process_edgexpd_project_auxilia() {
        // g.V().out().as(0).select(0).by(valueMap("name", "id", "age")
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(build_scan(vec![]).into(), vec![])
            .unwrap();
        plan.append_operator_as_node(build_edgexpd(0, vec![], Some(0.into())).into(), vec![0])
            .unwrap();
        plan.append_operator_as_node(build_project("{@0.name, @0.id, @0.age}").into(), vec![1])
            .unwrap();
        plan.append_operator_as_node(build_sink().into(), vec![2])
            .unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(build_scan(vec![]));
        expected_builder.edge_expand(build_edgexpd(0, vec![], None));
        expected_builder.get_v(build_auxilia_with_params(
            Some(query_params(vec![], vec!["age".into(), "id".into(), "name".into()])),
            Some(0.into()),
        ));
        expected_builder.project(build_project("{@0.name, @0.id, @0.age}"));
        expected_builder.sink(build_sink());

        assert_eq!(job_builder, expected_builder);

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta = plan_meta.with_partition();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(build_scan(vec![]));
        expected_builder.shuffle(None);
        expected_builder.edge_expand(build_edgexpd(0, vec![], None));
        expected_builder.shuffle(None);
        expected_builder.get_v(build_auxilia_with_params(
            Some(query_params(vec![], vec!["age".into(), "id".into(), "name".into()])),
            Some(0.into()),
        ));
        expected_builder.project(build_project("{@0.name, @0.id, @0.age}"));
        expected_builder.sink(build_sink());

        assert_eq!(job_builder, expected_builder);
    }

    #[test]
    fn post_process_edgexpd_tag_no_auxilia() {
        // g.V().out().as('a').select('a')
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(build_scan(vec![]).into(), vec![])
            .unwrap();
        plan.append_operator_as_node(build_edgexpd(0, vec![], Some(0.into())).into(), vec![0])
            .unwrap();
        plan.append_operator_as_node(build_project("@0").into(), vec![1])
            .unwrap();
        plan.append_operator_as_node(build_sink().into(), vec![2])
            .unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta = plan_meta.with_partition();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(build_scan(vec![]));
        expected_builder.shuffle(None);
        expected_builder.edge_expand(build_edgexpd(0, vec![], Some(0.into())));
        expected_builder.project(build_project("@0"));
        expected_builder.sink(build_sink());

        assert_eq!(job_builder, expected_builder);
    }

    #[test]
    fn post_process_scan() {
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
        plan.append_operator_as_node(build_project("{@.name, @.id}").into(), vec![2])
            .unwrap();

        plan.append_operator_as_node(build_sink().into(), vec![3])
            .unwrap();

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(build_scan(vec!["id".into(), "name".into()]));

        expected_builder.select(build_select("@.~label == \"person\""));
        expected_builder.get_v(build_auxilia_with_predicates("@.age == 27"));
        expected_builder.project(build_project("{@.name, @.id}"));
        expected_builder.sink(build_sink());

        assert_eq!(job_builder, expected_builder);
    }

    #[test]
    fn post_process_getv_auxilia_projection() {
        // g.V().outE().inV().as('a').select('a').by(valueMap("name", "id", "age")
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(build_scan(vec![]).into(), vec![])
            .unwrap();
        plan.append_operator_as_node(build_edgexpd(1, vec![], None).into(), vec![0])
            .unwrap();
        plan.append_operator_as_node(build_getv(Some(0.into())).into(), vec![1])
            .unwrap();
        plan.append_operator_as_node(build_project("{@0.name, @0.id, @0.age}").into(), vec![2])
            .unwrap();
        plan.append_operator_as_node(build_sink().into(), vec![3])
            .unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(build_scan(vec![]));
        expected_builder.edge_expand(build_edgexpd(1, vec![], None));
        expected_builder.get_v(build_getv(None));
        expected_builder.get_v(build_auxilia_with_params(
            Some(query_params(vec![], vec!["age".into(), "id".into(), "name".into()])),
            Some(0.into()),
        ));
        expected_builder.project(build_project("{@0.name, @0.id, @0.age}"));
        expected_builder.sink(build_sink());

        assert_eq!(job_builder, expected_builder);

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta = plan_meta.with_partition();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(build_scan(vec![]));
        expected_builder.shuffle(None);
        expected_builder.edge_expand(build_edgexpd(1, vec![], None));
        expected_builder.get_v(build_getv(None));
        expected_builder.shuffle(None);
        expected_builder.get_v(build_auxilia_with_params(
            Some(query_params(vec![], vec!["age".into(), "id".into(), "name".into()])),
            Some(0.into()),
        ));
        expected_builder.project(build_project("{@0.name, @0.id, @0.age}"));
        expected_builder.sink(build_sink());

        assert_eq!(job_builder, expected_builder);
    }

    #[test]
    fn post_process_getv_auxilia_filter() {
        // g.V().outE().inV().filter('age > 10')
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(build_scan(vec![]).into(), vec![])
            .unwrap();
        plan.append_operator_as_node(build_edgexpd(1, vec![], None).into(), vec![0])
            .unwrap();
        plan.append_operator_as_node(
            pb::GetV {
                tag: None,
                opt: 1,
                params: Some(pb::QueryParams {
                    tables: vec![],
                    columns: vec![],
                    is_all_columns: false,
                    limit: None,
                    predicate: str_to_expr_pb("@.age > 10".to_string()).ok(),
                    sample_ratio: 1.0,
                    extra: Default::default(),
                }),
                alias: None,
            }
            .into(),
            vec![1],
        )
        .unwrap();
        plan.append_operator_as_node(build_sink().into(), vec![2])
            .unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(build_scan(vec![]));
        expected_builder.edge_expand(build_edgexpd(1, vec![], None));
        expected_builder.get_v(build_getv(None));
        expected_builder.get_v(build_auxilia_with_predicates("@.age > 10"));
        expected_builder.sink(build_sink());
        assert_eq!(job_builder, expected_builder);
    }

    #[test]
    fn poc_plan_as_physical() {
        // g.V().hasLabel("person").has("id", 10).out("knows").limit(10)
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec!["person".into()], vec![])),
            idx_predicate: None,
        };
        let select_opr = pb::Select { predicate: str_to_expr_pb("@.id == 10".to_string()).ok() };
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec!["knows".into()], vec![])),
            expand_opt: 0,
            alias: None,
        };
        let limit_opr = pb::Limit { range: Some(pb::Range { lower: 10, upper: 11 }) };

        let mut logical_plan = LogicalPlan::default();

        logical_plan
            .append_operator_as_node(source_opr.clone().into(), vec![])
            .unwrap(); // node 0
        logical_plan
            .append_operator_as_node(select_opr.clone().into(), vec![0])
            .unwrap(); // node 1
        logical_plan
            .append_operator_as_node(expand_opr.clone().into(), vec![1])
            .unwrap(); // node 2
        logical_plan
            .append_operator_as_node(limit_opr.clone().into(), vec![2])
            .unwrap(); // node 3
        logical_plan
            .append_operator_as_node(build_sink().into(), vec![3])
            .unwrap();
        let mut builder = JobBuilder::default();
        let mut plan_meta = logical_plan.meta.clone();
        let _ = logical_plan.add_job_builder(&mut builder, &mut plan_meta);

        let auxilia = build_auxilia_with_predicates("@.id == 10");

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(source_opr);
        expected_builder.get_v(auxilia);
        expected_builder.edge_expand(expand_opr);
        expected_builder.limit(limit_opr);
        expected_builder.sink(build_sink());

        assert_eq!(builder, expected_builder);
    }

    #[test]
    fn project_as_physical() {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec!["person".into()], vec![])),
            idx_predicate: None,
        };

        let project_opr = pb::Project {
            mappings: vec![
                ExprAlias {
                    expr: Some(str_to_expr_pb("10 * (@.class - 10)".to_string()).unwrap()),
                    alias: None,
                },
                ExprAlias { expr: Some(str_to_expr_pb("@.age - 1".to_string()).unwrap()), alias: None },
            ],
            is_append: false,
        };

        let mut logical_plan = LogicalPlan::with_root(Node::new(0, source_opr.clone().into()));
        logical_plan
            .append_operator_as_node(project_opr.clone().into(), vec![0])
            .unwrap(); // node 1
        let mut builder = JobBuilder::default();
        let mut plan_meta = PlanMeta::default();
        logical_plan
            .add_job_builder(&mut builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(source_opr);
        expected_builder.project(project_opr);
        assert_eq!(builder, expected_builder);
    }

    #[test]
    fn path_expand_as_physical() {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec!["person".into()], vec![])),
            idx_predicate: None,
        };

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec!["knows".into()], vec![])),
            expand_opt: 0,
            alias: None,
        };

        let path_opr = pb::PathExpand {
            base: Some(edge_expand.clone()),
            start_tag: None,
            alias: None,
            hop_range: Some(pb::Range { lower: 1, upper: 4 }),
            path_opt: 0,
            result_opt: 0,
        };

        let mut logical_plan = LogicalPlan::with_root(Node::new(0, source_opr.clone().into()));
        logical_plan
            .append_operator_as_node(path_opr.clone().into(), vec![0])
            .unwrap(); // node 1

        // Case without partition
        let mut builder = JobBuilder::default();
        let mut plan_meta = PlanMeta::default();
        logical_plan
            .add_job_builder(&mut builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(source_opr.clone());
        expected_builder.path_expand(path_opr.clone());

        assert_eq!(builder, expected_builder);

        // Case with partition
        let mut builder = JobBuilder::default();
        let mut plan_meta = PlanMeta::default();
        plan_meta = plan_meta.with_partition();
        logical_plan
            .add_job_builder(&mut builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(source_opr);
        expected_builder.shuffle(None);
        expected_builder.path_expand(path_opr);

        assert_eq!(builder, expected_builder);
    }

    #[test]
    fn orderby_as_physical() {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![], vec![])),
            idx_predicate: None,
        };

        let topby_opr = pb::OrderBy { pairs: vec![], limit: Some(pb::Range { lower: 10, upper: 11 }) };

        let mut logical_plan = LogicalPlan::with_root(Node::new(0, source_opr.clone().into()));
        logical_plan
            .append_operator_as_node(topby_opr.clone().into(), vec![0])
            .unwrap(); // node 1
        let mut builder = JobBuilder::default();
        let mut plan_meta = PlanMeta::default();
        let _ = logical_plan.add_job_builder(&mut builder, &mut plan_meta);

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(source_opr);
        expected_builder.order(topby_opr);

        assert_eq!(builder, expected_builder);
    }

    #[test]
    fn apply_as_physical_case1() {
        let mut plan = LogicalPlan::default();
        // g.V().as("0").where(out().as("1").has("lang", "java")).select("0").values("name")
        plan.meta = plan.meta.with_partition();

        // g.V("person")
        let scan = pb::Scan {
            scan_opt: 0,
            alias: Some(0.into()),
            params: Some(query_params(vec![], vec!["name".into()])),
            idx_predicate: None,
        };

        let opr_id = plan
            .append_operator_as_node(scan.clone().into(), vec![])
            .unwrap();

        // .out().as("1")
        let expand = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![], vec![])),
            expand_opt: 0,
            alias: Some(1.into()),
        };

        let root_id = plan
            .append_operator_as_node(expand.clone().into(), vec![])
            .unwrap();

        // .has("lang", "Java")
        let select = pb::Select { predicate: str_to_expr_pb("@.lang == \"Java\"".to_string()).ok() };
        plan.append_operator_as_node(select.clone().into(), vec![root_id])
            .unwrap();

        let apply = pb::Apply { join_kind: 4, tags: vec![], subtask: root_id as i32, alias: None };
        let opr_id = plan
            .append_operator_as_node(apply.clone().into(), vec![opr_id])
            .unwrap();

        let project = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("@0.name".to_string()).ok(),
                alias: None,
            }],
            is_append: true,
        };
        plan.append_operator_as_node(project.clone().into(), vec![opr_id])
            .unwrap();

        let mut builder = JobBuilder::default();
        let mut meta = plan.meta.clone();
        plan.add_job_builder(&mut builder, &mut meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(scan);
        let auxilia = build_auxilia_with_predicates("@.lang == \"Java\"");

        let mut sub_builder = JobBuilder::default();
        sub_builder
            .shuffle(None)
            .edge_expand(expand.clone())
            .shuffle(None)
            .get_v(auxilia.clone());
        let apply_subplan = sub_builder.take_plan();
        expected_builder.apply(
            unsafe { std::mem::transmute(4) }, // SEMI
            apply_subplan,
            None,
        );
        expected_builder.project(project);

        assert_eq!(expected_builder, builder);
    }

    #[test]
    fn apply_as_physical_with_expand_degree_fuse() {
        let mut plan = LogicalPlan::default();
        // g.V().as("0").select("0").by(out().count().as("degree"))
        // out().degree() fused
        plan.meta = plan.meta.with_partition();

        // g.V()
        let scan = pb::Scan {
            scan_opt: 0,
            alias: Some(0.into()),
            params: Some(query_params(vec![], vec![])),
            idx_predicate: None,
        };

        let opr_id = plan
            .append_operator_as_node(scan.clone().into(), vec![])
            .unwrap();

        // .out().count()
        let mut expand = build_edgexpd(2, vec![], None);
        let subplan_id = plan
            .append_operator_as_node(expand.clone().into(), vec![])
            .unwrap();

        // Select("0").by()
        let apply =
            pb::Apply { join_kind: 4, tags: vec![], subtask: subplan_id as i32, alias: Some(1.into()) };
        plan.append_operator_as_node(apply.clone().into(), vec![opr_id])
            .unwrap();

        let mut builder = JobBuilder::default();
        let mut meta = plan.meta.clone();
        plan.add_job_builder(&mut builder, &mut meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(scan);
        expected_builder.shuffle(None);
        expected_builder.get_v(build_auxilia_with_params(
            None,
            Some(common_pb::NameOrId { item: Some(common_pb::name_or_id::Item::Id(2)) }),
        ));

        expand.alias = Some(1.into()); // must carry `Apply`'s alias
        expected_builder.edge_expand(expand);
        expected_builder.project(pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("@2".to_string()).ok(),
                alias: None,
            }],
            is_append: true,
        });

        assert_eq!(expected_builder, builder);
    }

    #[test]
    fn apply_as_physical_with_select_expand_degree_fuse() {
        let mut plan = LogicalPlan::default();
        // g.V().as("0").select().by(select("0").out().count().as("degree"))
        // select("0").out().degree() fused
        plan.meta = plan.meta.with_partition();

        // g.V()
        let scan = pb::Scan {
            scan_opt: 0,
            alias: Some(0.into()),
            params: Some(query_params(vec![], vec![])),
            idx_predicate: None,
        };

        let opr_id = plan
            .append_operator_as_node(scan.clone().into(), vec![])
            .unwrap();

        // .select("0").out().count()
        let project = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("@0".to_string()).ok(),
                alias: None,
            }],
            is_append: true,
        };
        let mut expand = build_edgexpd(2, vec![], None);
        let subplan_id = plan
            .append_operator_as_node(project.clone().into(), vec![])
            .unwrap();
        plan.append_operator_as_node(expand.clone().into(), vec![subplan_id])
            .unwrap();

        // Select().by()
        let apply: pb::logical_plan::Operator =
            pb::Apply { join_kind: 4, tags: vec![], subtask: subplan_id as i32, alias: Some(1.into()) }
                .into();
        plan.append_operator_as_node(apply.clone(), vec![opr_id])
            .unwrap();

        let mut builder = JobBuilder::default();
        let mut meta = plan.meta.clone();
        plan.add_job_builder(&mut builder, &mut meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(scan);
        expected_builder.shuffle(Some(0.into()));
        expected_builder.get_v(build_auxilia_with_params(
            None,
            Some(common_pb::NameOrId { item: Some(common_pb::name_or_id::Item::Id(2)) }),
        ));
        expand.v_tag = Some(0.into());
        expand.alias = Some(1.into()); // must carry `Apply`'s alias
        expected_builder.edge_expand(expand);
        expected_builder.project(pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("@2".to_string()).ok(),
                alias: None,
            }],
            is_append: true,
        });

        assert_eq!(expected_builder, builder);
    }

    #[test]
    fn join_plan_as_physical() {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![], vec![])),
            idx_predicate: None,
        };
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![], vec![])),
            expand_opt: 0,
            alias: None,
        };
        let join_opr = pb::Join { left_keys: vec![], right_keys: vec![], kind: 0 };
        let limit_opr = pb::Limit { range: Some(pb::Range { lower: 10, upper: 11 }) };

        let mut logical_plan = LogicalPlan::with_root(Node::new(0, source_opr.clone().into()));
        logical_plan
            .append_operator_as_node(expand_opr.clone().into(), vec![0])
            .unwrap(); // node 1
        logical_plan
            .append_operator_as_node(expand_opr.clone().into(), vec![0])
            .unwrap(); // node 2
        logical_plan
            .append_operator_as_node(expand_opr.clone().into(), vec![2])
            .unwrap(); // node 3
        logical_plan
            .append_operator_as_node(join_opr.clone().into(), vec![1, 3])
            .unwrap(); // node 4
        logical_plan
            .append_operator_as_node(limit_opr.clone().into(), vec![4])
            .unwrap(); // node 5
        let mut builder = JobBuilder::default();
        let mut plan_meta = PlanMeta::default();
        let _ = logical_plan.add_job_builder(&mut builder, &mut plan_meta);

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(source_opr);
        let mut left_builder = JobBuilder::default();
        let mut right_builder = JobBuilder::default();
        left_builder.edge_expand(expand_opr.clone());
        right_builder.edge_expand(expand_opr.clone());
        right_builder.edge_expand(expand_opr);
        expected_builder.join(
            unsafe { std::mem::transmute(0) }, // INNER
            left_builder.take_plan(),
            right_builder.take_plan(),
            vec![],
            vec![],
        );
        expected_builder.limit(limit_opr);

        assert_eq!(builder, expected_builder);
    }

    #[test]
    fn intersection_as_physical() {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: Some(0.into()),
            params: Some(query_params(vec![], vec![])),
            idx_predicate: None,
        };

        // extend 0->1
        let expand_ab_opr = pb::EdgeExpand {
            v_tag: Some(0.into()),
            direction: 0,
            params: Some(query_params(vec![], vec![])),
            expand_opt: 0,
            alias: Some(1.into()),
        };

        // extend 0->2, 1->2, and intersect on 2
        let expand_ac_opr = pb::EdgeExpand {
            v_tag: Some(0.into()),
            direction: 0,
            params: Some(query_params(vec![], vec![])),
            expand_opt: 0,
            alias: Some(2.into()),
        };

        let expand_bc_opr = pb::EdgeExpand {
            v_tag: Some(1.into()),
            direction: 0,
            params: Some(query_params(vec!["knows".into()], vec![])),
            expand_opt: 0,
            alias: Some(2.into()),
        };

        // parents are expand_ac_opr and expand_bc_opr
        let intersect_opr = pb::Intersect { parents: vec![2, 3], key: Some(2.into()) };

        let mut logical_plan = LogicalPlan::with_root(Node::new(0, source_opr.clone().into()));
        logical_plan
            .append_operator_as_node(expand_ab_opr.clone().into(), vec![0])
            .unwrap(); // node 1
        logical_plan
            .append_operator_as_node(expand_ac_opr.clone().into(), vec![1])
            .unwrap(); // node 2
        logical_plan
            .append_operator_as_node(expand_bc_opr.clone().into(), vec![1])
            .unwrap(); // node 3
        logical_plan
            .append_operator_as_node(intersect_opr.clone().into(), vec![2, 3])
            .unwrap(); // node 4
        let mut builder = JobBuilder::default();
        let mut plan_meta = PlanMeta::default();
        logical_plan
            .add_job_builder(&mut builder, &mut plan_meta)
            .unwrap();

        let unfold_opr = pb::Unfold { tag: Some(2.into()), alias: Some(2.into()) };

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(source_opr);
        expected_builder.edge_expand(expand_ab_opr);

        let mut sub_builder_1 = JobBuilder::default();
        let mut sub_builder_2 = JobBuilder::default();
        sub_builder_1.edge_expand(expand_ac_opr.clone());
        sub_builder_2.edge_expand(expand_bc_opr.clone());
        expected_builder.intersect(vec![sub_builder_1.take_plan(), sub_builder_2.take_plan()], 2.into());
        expected_builder.unfold(unfold_opr);
        assert_eq!(builder, expected_builder);
    }

    #[test]
    fn intersection_with_auxilia_as_physical() {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: Some(0.into()),
            params: Some(query_params(vec![], vec![])),
            idx_predicate: None,
        };

        // extend 0->1
        let expand_ab_opr = pb::EdgeExpand {
            v_tag: Some(0.into()),
            direction: 0,
            params: Some(query_params(vec![], vec![])),
            expand_opt: 0,
            alias: Some(1.into()),
        };

        // extend 0->2, 1->2, and intersect on 2 (where 2 has required columns)
        let mut expand_ac_opr = pb::EdgeExpand {
            v_tag: Some(0.into()),
            direction: 0,
            params: Some(query_params(vec![], vec!["id".into()])),
            expand_opt: 0,
            alias: Some(2.into()),
        };

        let mut expand_bc_opr = pb::EdgeExpand {
            v_tag: Some(1.into()),
            direction: 0,
            params: Some(query_params(vec!["knows".into()], vec!["name".into()])),
            expand_opt: 0,
            alias: Some(2.into()),
        };

        // parents are expand_ac_opr and expand_bc_opr
        let intersect_opr = pb::Intersect { parents: vec![2, 3], key: Some(2.into()) };

        let mut logical_plan = LogicalPlan::with_root(Node::new(0, source_opr.clone().into()));
        logical_plan
            .append_operator_as_node(expand_ab_opr.clone().into(), vec![0])
            .unwrap(); // node 1
        logical_plan
            .append_operator_as_node(expand_ac_opr.clone().into(), vec![1])
            .unwrap(); // node 2
        logical_plan
            .append_operator_as_node(expand_bc_opr.clone().into(), vec![1])
            .unwrap(); // node 3
        logical_plan
            .append_operator_as_node(intersect_opr.clone().into(), vec![2, 3])
            .unwrap(); // node 4
        let mut builder = JobBuilder::default();
        let mut plan_meta = PlanMeta::default();
        logical_plan
            .add_job_builder(&mut builder, &mut plan_meta)
            .unwrap();

        let unfold_opr = pb::Unfold { tag: Some(2.into()), alias: None };
        let mut auxilia_param = pb::QueryParams::default();
        auxilia_param.sample_ratio = 1.0;
        merge_query_params(&mut auxilia_param, expand_ac_opr.params.as_mut().unwrap());
        merge_query_params(&mut auxilia_param, expand_bc_opr.params.as_mut().unwrap());
        let auxilia_opr = build_auxilia_with_params(Some(auxilia_param), Some(2.into()));

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_scan_source(source_opr);
        expected_builder.edge_expand(expand_ab_opr);
        let mut sub_builder_1 = JobBuilder::default();
        let mut sub_builder_2 = JobBuilder::default();
        sub_builder_1.edge_expand(expand_ac_opr.clone());
        sub_builder_2.edge_expand(expand_bc_opr.clone());
        expected_builder.intersect(vec![sub_builder_1.take_plan(), sub_builder_2.take_plan()], 2.into());
        expected_builder.unfold(unfold_opr);
        expected_builder.get_v(auxilia_opr);
        assert_eq!(builder, expected_builder);
    }
}
