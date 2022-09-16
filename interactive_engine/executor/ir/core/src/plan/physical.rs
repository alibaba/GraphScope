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
use pegasus_client::builder::{JobBuilder, Plan};
use pegasus_server::job_pb as server_pb;
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
    let _ = match opr {
        SimpleOpr::Source => builder.add_source(bytes),
        SimpleOpr::Map => builder.map(bytes),
        SimpleOpr::FilterMap => builder.filter_map(bytes),
        SimpleOpr::Flatmap => builder.flat_map(bytes),
        SimpleOpr::Filter => builder.filter(bytes),
        SimpleOpr::SortBy => builder.sort_by(bytes),
        SimpleOpr::Dedup => builder.dedup(bytes),
        SimpleOpr::GroupBy => builder.group_by(server_pb::AccumKind::Custom, bytes),
        SimpleOpr::Fold => builder.fold_custom(server_pb::AccumKind::Custom, bytes),
        SimpleOpr::Sink => {
            builder.sink(bytes);
            builder
        }
    };
    Ok(())
}

impl AsPhysical for pb::Project {
    fn add_job_builder(&self, builder: &mut JobBuilder, _plan_meta: &mut PlanMeta) -> IrResult<()> {
        simple_add_job_builder(
            builder,
            &pb::logical_plan::Operator::from(self.clone()),
            SimpleOpr::FilterMap,
        )
    }
}

impl AsPhysical for pb::Select {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        let node_meta = plan_meta.get_curr_node_meta().unwrap();
        let tag_columns = node_meta.get_tag_columns();
        if tag_columns.len() == 1 {
            let (tag, columns_opt) = tag_columns.into_iter().next().unwrap();
            if columns_opt.len() > 0 {
                let tag_pb = tag.map(|tag_id| (tag_id as i32).into());
                if plan_meta.is_partition() {
                    let key_pb = common_pb::NameOrIdKey { key: tag_pb.clone() };
                    builder.repartition(key_pb.encode_to_vec());
                }
                let mut params = pb::QueryParams {
                    tables: vec![],
                    columns: vec![],
                    is_all_columns: columns_opt.is_all(),
                    limit: None,
                    predicate: None,
                    sample_ratio: 1.0,
                    extra: Default::default(),
                };
                params.predicate = self.predicate.clone();
                let auxilia = pb::Auxilia {
                    tag: tag_pb.clone(),
                    params: Some(params),
                    alias: tag_pb,
                    remove_tags: vec![],
                };
                pb::logical_plan::Operator::from(auxilia).add_job_builder(builder, plan_meta)
            } else {
                Err(IrError::MissingData(format!(
                    "The tag: {:?} refer to empty columns: {:?}",
                    tag, columns_opt
                )))
            }
        } else {
            simple_add_job_builder(
                builder,
                &pb::logical_plan::Operator::from(self.clone()),
                SimpleOpr::Filter,
            )
        }
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
            if let Some(node_meta) = plan_meta.get_curr_node_meta() {
                let columns = node_meta.get_columns();
                let is_all_columns = node_meta.is_all_columns();
                if is_all_columns {
                    // is_all_columns yield the settings of columns
                    params.columns = vec![];
                    params.is_all_columns = true;
                } else if !columns.is_empty() {
                    params.columns = columns
                        .into_iter()
                        .map(|tag| tag.into())
                        .collect();
                }
            }
            Ok(())
        } else {
            Err(IrError::MissingData("Scan::params".to_string()))
        }
    }
}

impl AsPhysical for pb::EdgeExpand {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        let mut xpd = self.clone();
        xpd.post_process(builder, plan_meta)
    }

    fn post_process(&mut self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        let mut is_adding_auxilia = false;
        let mut auxilia = pb::Auxilia { tag: None, params: None, alias: None, remove_tags: vec![] };
        if let Some(params) = self.params.as_mut() {
            if let Some(node_meta) = plan_meta.get_curr_node_meta() {
                let columns = node_meta.get_columns();
                let is_all_columns = node_meta.is_all_columns();
                if !columns.is_empty() || is_all_columns {
                    let expand_opt: pb::edge_expand::ExpandOpt =
                        unsafe { ::std::mem::transmute(self.expand_opt) };
                    match expand_opt {
                        pb::edge_expand::ExpandOpt::Vertex => {
                            // Expand to adjacent vertices
                            let new_params = pb::QueryParams {
                                tables: vec![],
                                columns: columns
                                    .into_iter()
                                    .map(|tag| tag.into())
                                    .collect(),
                                is_all_columns,
                                limit: None,
                                predicate: None,
                                sample_ratio: 1.0,
                                extra: Default::default(),
                            };
                            auxilia.params = Some(new_params);
                            auxilia.alias = self.alias.clone();
                            self.alias = None;
                            is_adding_auxilia = true;
                        }
                        pb::edge_expand::ExpandOpt::Edge => {
                            params.columns = columns
                                .into_iter()
                                .map(|tag| tag.into())
                                .collect();
                            params.is_all_columns = is_all_columns;
                        }
                        _ => {}
                    }
                }
            }
        } else {
            return Err(IrError::MissingData("EdgeExpand::params".to_string()));
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

impl AsPhysical for pb::PathExpand {
    fn add_job_builder(&self, builder: &mut JobBuilder, plan_meta: &mut PlanMeta) -> IrResult<()> {
        // [range.lower, range.upper)
        if let Some(range) = &self.hop_range {
            if range.upper <= range.lower || range.lower < 0 || range.upper <= 0 {
                Err(IrError::InvalidRange(range.lower, range.upper))
            } else {
                if let Some(base) = &self.base {
                    let path_start = pb::PathStart {
                        start_tag: self.start_tag.clone(),
                        path_opt: self.path_opt,
                        result_opt: self.result_opt,
                    };
                    simple_add_job_builder(
                        builder,
                        &pb::logical_plan::Operator::from(path_start),
                        SimpleOpr::FilterMap,
                    )?;
                    let is_partition = plan_meta.is_partition();
                    let emit_kind = server_pb::iteration_emit::EmitKind::EmitBefore;
                    for _ in 0..range.lower {
                        if is_partition {
                            let key_pb = common_pb::NameOrIdKey { key: None };
                            builder.repartition(key_pb.encode_to_vec());
                        }
                        pb::logical_plan::Operator::from(base.clone())
                            .add_job_builder(builder, plan_meta)?;
                    }
                    let times = range.upper - range.lower - 1;
                    if times > 0 {
                        builder.iterate_emit(emit_kind, times as u32, move |plan| {
                            if is_partition {
                                let key_pb = common_pb::NameOrIdKey { key: None };
                                plan.repartition(key_pb.encode_to_vec());
                            }
                            plan.flat_map(pb::logical_plan::Operator::from(base.clone()).encode_to_vec());
                        });
                    }
                    let path_end = pb::PathEnd { alias: self.alias.clone() };
                    simple_add_job_builder(
                        builder,
                        &pb::logical_plan::Operator::from(path_end),
                        SimpleOpr::Map,
                    )
                } else {
                    Err(IrError::MissingData("PathExpand::base".to_string()))
                }
            }
        } else {
            Err(IrError::MissingData("PathExpand::hop_range".to_string()))
        }
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
        let mut auxilia = pb::Auxilia { tag: None, params: None, alias: None, remove_tags: vec![] };
        if let Some(params) = self.params.as_mut() {
            if let Some(node_meta) = plan_meta.get_curr_node_meta() {
                let columns = node_meta.get_columns();
                let is_all_columns = node_meta.is_all_columns();
                if !columns.is_empty() || is_all_columns || params.predicate.is_some() {
                    let mut new_params = params.clone();
                    if is_all_columns {
                        new_params.columns = vec![];
                        new_params.is_all_columns = is_all_columns;
                    } else {
                        new_params.columns = columns
                            .into_iter()
                            .map(|params| params.into())
                            .collect();
                        new_params.is_all_columns = is_all_columns;
                    }

                    auxilia.params = Some(new_params);

                    *params = pb::QueryParams {
                        tables: params.tables.clone(),
                        columns: vec![],
                        is_all_columns: false,
                        limit: None,
                        predicate: None,
                        sample_ratio: 1.0,
                        extra: Default::default(),
                    };

                    auxilia.alias = self.alias.clone();
                    self.alias = None;
                    is_adding_auxilia = true;
                }
            }
        } else {
            return Err(IrError::MissingData("GetV::params".to_string()));
        }
        let opt: pb::get_v::VOpt = unsafe { ::std::mem::transmute(self.opt) };
        match opt {
            pb::get_v::VOpt::Both => {
                simple_add_job_builder(
                    builder,
                    &pb::logical_plan::Operator::from(self.clone()),
                    SimpleOpr::Flatmap,
                )?;
            }
            _ => simple_add_job_builder(
                builder,
                &pb::logical_plan::Operator::from(self.clone()),
                SimpleOpr::FilterMap,
            )?,
        }
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
        let auxilia =
            pb::Auxilia { tag: None, params: None, alias: self.alias.clone(), remove_tags: vec![] };
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
                Err(IrError::InvalidRange(range.lower, range.upper))
            } else {
                builder.limit((range.upper - 1) as u32);
                Ok(())
            }
        } else {
            Err(IrError::MissingData("Limit::range".to_string()))
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
                Err(IrError::InvalidRange(range.lower, range.upper))
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

        simple_add_job_builder(builder, &pb::logical_plan::Operator::from(sink_opr), SimpleOpr::Sink)
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
                Auxilia(auxilia) => auxilia.add_job_builder(builder, plan_meta),
                As(as_opr) => as_opr.add_job_builder(builder, plan_meta),
                Dedup(dedup) => dedup.add_job_builder(builder, plan_meta),
                GroupBy(groupby) => groupby.add_job_builder(builder, plan_meta),
                Sink(sink) => sink.add_job_builder(builder, plan_meta),
                Union(_) => Ok(()),
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
        use pb::join::JoinKind;
        use pb::logical_plan::operator::Opr::*;
        let mut _prev_node_opt: Option<NodeType> = None;
        let mut curr_node_opt = self.root();
        debug!("plan: {:#?}", self);
        debug!("is_partition: {:?}", self.meta.is_partition());
        while curr_node_opt.is_some() {
            let curr_node = curr_node_opt.as_ref().unwrap();
            let curr_node_id = curr_node.borrow().id;
            if let Some(Apply(apply_opr)) = curr_node.borrow().opr.opr.as_ref() {
                let mut sub_bldr = JobBuilder::default();
                if let Some(subplan) = self.extract_subplan(curr_node.clone()) {
                    let mut expand_degree_opt = None;
                    if subplan.num_nodes() <= 2 {
                        if subplan.num_nodes() == 1 {
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
                        let key_pb = common_pb::NameOrIdKey { key: expand_degree.v_tag.clone() };
                        if plan_meta.is_partition() {
                            builder.repartition(key_pb.encode_to_vec());
                        }
                        // If the subtask of apply takes only one operator and the operator is
                        // `EdgeExpand` with `expand_opt` set as `Degree`, then the following
                        // fused steps are conducted instead of executing subtask:
                        //   `As('~expand_degree_<id>')` +
                        //   `EdgeExpand()` +
                        //   `Select('~expand_degree_<id>')`
                        let mut fused = pb::FusedOperator { oprs: vec![] };
                        let new_tag = plan_meta
                            .get_or_set_tag_id(&format!("~expand_degree_{:?}", curr_node_id))
                            .1 as i32;
                        fused.oprs.push(
                            pb::Auxilia {
                                tag: None,
                                params: None,
                                alias: Some(common_pb::NameOrId {
                                    item: Some(common_pb::name_or_id::Item::Id(new_tag)),
                                }),
                                remove_tags: vec![],
                            }
                            .into(),
                        );
                        fused.oprs.push(expand_degree.into());
                        fused.oprs.push(
                            pb::Project {
                                mappings: vec![pb::project::ExprAlias {
                                    expr: str_to_expr_pb(format!("@{:?}", new_tag)).ok(),
                                    alias: None,
                                }],
                                is_append: true,
                            }
                            .into(),
                        );
                        builder.flat_map(pb::logical_plan::Operator::from(fused).encode_to_vec());
                    } else {
                        subplan.add_job_builder(&mut sub_bldr, plan_meta)?;
                        let plan = sub_bldr.take_plan();
                        builder.apply_join(
                            move |p| *p = plan.clone(),
                            pb::logical_plan::Operator::from(apply_opr.clone()).encode_to_vec(),
                        );
                    }
                } else {
                    return Err(IrError::MissingData("Apply::subplan".to_string()));
                }
            } else {
                if let Some(Edge(edgexpd)) = curr_node.borrow().opr.opr.as_ref() {
                    let key_pb = common_pb::NameOrIdKey { key: edgexpd.v_tag.clone() };
                    if plan_meta.is_partition() {
                        builder.repartition(key_pb.encode_to_vec());
                    }
                }
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
                for subplan in subplans {
                    let mut sub_bldr = JobBuilder::new(builder.conf.clone());
                    subplan.add_job_builder(&mut sub_bldr, plan_meta)?;
                    plans.push(sub_bldr.take_plan());
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
                        None => return Err(IrError::MissingData("Union/Join::merge_node".to_string())),
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
    fn build_auxilia(expr: &str) -> pb::Auxilia {
        pb::Auxilia {
            tag: None,
            params: Some(pb::QueryParams {
                tables: vec![],
                columns: vec![],
                is_all_columns: false,
                limit: None,
                predicate: str_to_expr_pb(expr.to_string()).ok(),
                sample_ratio: 1.0,
                extra: Default::default(),
            }),
            alias: None,
        }
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
    fn post_process_edgexpd() {
        // g.V().outE()
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(build_scan(vec![]).into(), vec![])
            .unwrap();
        plan.append_operator_as_node(build_edgexpd(1, vec![], None).into(), vec![0])
            .unwrap();
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(1, vec![], None)).encode_to_vec());
        expected_builder.sink(vec![]);

        assert_eq!(job_builder, expected_builder);

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta = plan_meta.with_partition();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(1, vec![], None)).encode_to_vec());
        expected_builder.sink(vec![]);

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
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(0, vec![], None)).encode_to_vec());
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(build_auxilia("@.birthday == 20220101")).encode_to_vec(),
        );
        expected_builder.sink(vec![]);

        assert_eq!(job_builder, expected_builder);

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta = plan_meta.with_partition();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(0, vec![], None)).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(build_auxilia("@.birthday == 20220101")).encode_to_vec(),
        );
        expected_builder.sink(vec![]);

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
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(0, vec![], None)).encode_to_vec());
        expected_builder.filter(
            pb::logical_plan::Operator::from(build_select("@.~label == \"person\"")).encode_to_vec(),
        );
        expected_builder.sink(vec![]);

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
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(0, vec![], None)).encode_to_vec());
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(pb::Auxilia {
                tag: None,
                params: Some(query_params(vec![], vec!["age".into()])),
                alias: Some(0.into()),
            })
            .encode_to_vec(),
        );
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(0, vec![], None)).encode_to_vec());
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(pb::Auxilia {
                tag: None,
                params: Some(query_params(vec![], vec!["age".into()])),
                alias: Some(1.into()),
            })
            .encode_to_vec(),
        );
        expected_builder
            .filter(pb::logical_plan::Operator::from(build_select("@0.age > @1.age")).encode_to_vec());
        expected_builder.sink(vec![]);

        assert_eq!(job_builder, expected_builder);

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta = plan_meta.with_partition();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(0, vec![], None)).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(pb::Auxilia {
                tag: None,
                params: Some(query_params(vec![], vec!["age".into()])),
                alias: Some(0.into()),
            })
            .encode_to_vec(),
        );
        expected_builder.repartition(vec![]);
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(0, vec![], None)).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(pb::Auxilia {
                tag: None,
                params: Some(query_params(vec![], vec!["age".into()])),
                alias: Some(1.into()),
            })
            .encode_to_vec(),
        );
        expected_builder
            .filter(pb::logical_plan::Operator::from(build_select("@0.age > @1.age")).encode_to_vec());
        expected_builder.sink(vec![]);

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
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(0, vec![], None)).encode_to_vec());
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(pb::Auxilia {
                tag: None,
                params: Some(query_params(vec![], vec!["age".into(), "id".into(), "name".into()])),
                alias: Some(0.into()),
            })
            .encode_to_vec(),
        );
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(build_project("{@0.name, @0.id, @0.age}")).encode_to_vec(),
        );
        expected_builder.sink(vec![]);

        assert_eq!(job_builder, expected_builder);

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta = plan_meta.with_partition();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(0, vec![], None)).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(pb::Auxilia {
                tag: None,
                params: Some(query_params(vec![], vec!["age".into(), "id".into(), "name".into()])),
                alias: Some(0.into()),
            })
            .encode_to_vec(),
        );
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(build_project("{@0.name, @0.id, @0.age}")).encode_to_vec(),
        );
        expected_builder.sink(vec![]);

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
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta = plan_meta.with_partition();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder.flat_map(
            pb::logical_plan::Operator::from(build_edgexpd(0, vec![], Some(0.into()))).encode_to_vec(),
        );
        expected_builder.filter_map(pb::logical_plan::Operator::from(build_project("@0")).encode_to_vec());
        expected_builder.sink(vec![]);

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

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(
            pb::logical_plan::Operator::from(build_scan(vec!["id".into(), "name".into()])).encode_to_vec(),
        );

        expected_builder.filter(
            pb::logical_plan::Operator::from(build_select("@.~label == \"person\"")).encode_to_vec(),
        );
        expected_builder
            .filter_map(pb::logical_plan::Operator::from(build_auxilia("@.age == 27")).encode_to_vec());
        expected_builder
            .filter_map(pb::logical_plan::Operator::from(build_project("{@.name, @.id}")).encode_to_vec());
        expected_builder.sink(vec![]);

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
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(1, vec![], None)).encode_to_vec());
        expected_builder.filter_map(pb::logical_plan::Operator::from(build_getv(None)).encode_to_vec());
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(pb::Auxilia {
                tag: None,
                params: Some(query_params(vec![], vec!["age".into(), "id".into(), "name".into()])),
                alias: Some(0.into()),
            })
            .encode_to_vec(),
        );
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(build_project("{@0.name, @0.id, @0.age}")).encode_to_vec(),
        );
        expected_builder.sink(vec![]);

        assert_eq!(job_builder, expected_builder);

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan_meta = plan_meta.with_partition();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(1, vec![], None)).encode_to_vec());
        expected_builder.filter_map(pb::logical_plan::Operator::from(build_getv(None)).encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(pb::Auxilia {
                tag: None,
                params: Some(query_params(vec![], vec!["age".into(), "id".into(), "name".into()])),
                alias: Some(0.into()),
            })
            .encode_to_vec(),
        );
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(build_project("{@0.name, @0.id, @0.age}")).encode_to_vec(),
        );
        expected_builder.sink(vec![]);

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

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(pb::logical_plan::Operator::from(build_scan(vec![])).encode_to_vec());
        expected_builder
            .flat_map(pb::logical_plan::Operator::from(build_edgexpd(1, vec![], None)).encode_to_vec());
        expected_builder.filter_map(pb::logical_plan::Operator::from(build_getv(None)).encode_to_vec());
        expected_builder.filter_map(
            pb::logical_plan::Operator::from(pb::Auxilia {
                tag: None,
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
            })
            .encode_to_vec(),
        );
        expected_builder.sink(vec![]);
        assert_eq!(job_builder, expected_builder);
    }

    #[test]
    fn poc_plan_as_physical() {
        // g.V().hasLabel("person").has("id", 10).out("knows").limit(10)
        let source_opr = pb::logical_plan::Operator::from(pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec!["person".into()], vec![])),
            idx_predicate: None,
        });
        let select_opr = pb::logical_plan::Operator::from(pb::Select {
            predicate: str_to_expr_pb("@.id == 10".to_string()).ok(),
        });
        let expand_opr = pb::logical_plan::Operator::from(pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec!["knows".into()], vec![])),
            expand_opt: 0,
            alias: None,
        });
        let limit_opr =
            pb::logical_plan::Operator::from(pb::Limit { range: Some(pb::Range { lower: 10, upper: 11 }) });
        let source_opr_bytes = source_opr.encode_to_vec();
        let expand_opr_bytes = expand_opr.encode_to_vec();

        let mut logical_plan = LogicalPlan::default();

        logical_plan
            .append_operator_as_node(source_opr.clone(), vec![])
            .unwrap(); // node 0
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
        let mut plan_meta = logical_plan.meta.clone();
        let _ = logical_plan.add_job_builder(&mut builder, &mut plan_meta);

        let auxilia: pb::logical_plan::Operator = build_auxilia("@.id == 10").into();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(source_opr_bytes);
        expected_builder.filter_map(auxilia.encode_to_vec());
        expected_builder.flat_map(expand_opr_bytes);
        expected_builder.limit(10);
        expected_builder.sink(vec![]);

        assert_eq!(builder, expected_builder);
    }

    #[test]
    fn project_as_physical() {
        let source_opr = pb::logical_plan::Operator::from(pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec!["person".into()], vec![])),
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
        expected_builder.filter_map(project_opr.encode_to_vec());
        assert_eq!(builder, expected_builder);
    }

    #[test]
    fn path_expand_as_physical() {
        let source_opr = pb::logical_plan::Operator::from(pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec!["person".into()], vec![])),
            idx_predicate: None,
        });

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec!["knows".into()], vec![])),
            expand_opt: 0,
            alias: None,
        };

        let expand_opr = pb::logical_plan::Operator::from(edge_expand.clone());
        let path_start_opr =
            pb::logical_plan::Operator::from(pb::PathStart { start_tag: None, path_opt: 0, result_opt: 0 });
        let path_opr = pb::logical_plan::Operator::from(pb::PathExpand {
            base: Some(edge_expand.clone()),
            start_tag: None,
            alias: None,
            hop_range: Some(pb::Range { lower: 1, upper: 4 }),
            path_opt: 0,
            result_opt: 0,
        });
        let path_end_opr = pb::logical_plan::Operator::from(pb::PathEnd { alias: None });

        let mut logical_plan = LogicalPlan::with_root(Node::new(0, source_opr.clone()));
        logical_plan
            .append_operator_as_node(path_opr.clone(), vec![0])
            .unwrap(); // node 1

        // Case without partition
        let mut builder = JobBuilder::default();
        let mut plan_meta = PlanMeta::default();
        logical_plan
            .add_job_builder(&mut builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(source_opr.encode_to_vec());
        expected_builder.filter_map(path_start_opr.encode_to_vec());
        expected_builder.flat_map(expand_opr.clone().encode_to_vec());
        expected_builder.iterate_emit(server_pb::iteration_emit::EmitKind::EmitBefore, 2, |plan| {
            plan.flat_map(expand_opr.clone().encode_to_vec());
        });
        expected_builder.map(path_end_opr.encode_to_vec());

        assert_eq!(builder, expected_builder);

        // Case with partition
        let mut builder = JobBuilder::default();
        let mut plan_meta = PlanMeta::default();
        plan_meta = plan_meta.with_partition();
        logical_plan
            .add_job_builder(&mut builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(source_opr.encode_to_vec());
        expected_builder.filter_map(path_start_opr.encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder.flat_map(expand_opr.clone().encode_to_vec());
        expected_builder.iterate_emit(server_pb::iteration_emit::EmitKind::EmitBefore, 2, |plan| {
            plan.repartition(vec![])
                .flat_map(expand_opr.clone().encode_to_vec());
        });
        expected_builder.map(path_end_opr.encode_to_vec());

        assert_eq!(builder, expected_builder);
    }

    #[test]
    fn path_expand_exactly_as_physical() {
        let source_opr = pb::logical_plan::Operator::from(pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec!["person".into()], vec![])),
            idx_predicate: None,
        });

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec!["knows".into()], vec![])),
            expand_opt: 0,
            alias: None,
        };

        let expand_opr = pb::logical_plan::Operator::from(edge_expand.clone());
        let path_start_opr =
            pb::logical_plan::Operator::from(pb::PathStart { start_tag: None, path_opt: 0, result_opt: 0 });
        let path_opr = pb::logical_plan::Operator::from(pb::PathExpand {
            base: Some(edge_expand.clone()),
            start_tag: None,
            alias: None,
            hop_range: Some(pb::Range { lower: 3, upper: 4 }),
            path_opt: 0,
            result_opt: 0,
        });
        let path_end_opr = pb::logical_plan::Operator::from(pb::PathEnd { alias: None });

        let mut logical_plan = LogicalPlan::with_root(Node::new(0, source_opr.clone()));
        logical_plan
            .append_operator_as_node(path_opr.clone(), vec![0])
            .unwrap(); // node 1
                       // Case with partition
        let mut builder = JobBuilder::default();
        let mut plan_meta = PlanMeta::default().with_partition();
        logical_plan
            .add_job_builder(&mut builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(source_opr.encode_to_vec());
        expected_builder.filter_map(path_start_opr.encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder.flat_map(expand_opr.clone().encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder.flat_map(expand_opr.clone().encode_to_vec());
        expected_builder.repartition(vec![]);
        expected_builder.flat_map(expand_opr.clone().encode_to_vec());
        expected_builder.map(path_end_opr.encode_to_vec());

        assert_eq!(builder, expected_builder);
    }

    #[test]
    fn path_expand_range_from_zero_as_physical() {
        let source_opr = pb::logical_plan::Operator::from(pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec!["person".into()], vec![])),
            idx_predicate: None,
        });

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec!["knows".into()], vec![])),
            expand_opt: 0,
            alias: None,
        };

        let expand_opr = pb::logical_plan::Operator::from(edge_expand.clone());
        let path_start_opr =
            pb::logical_plan::Operator::from(pb::PathStart { start_tag: None, path_opt: 0, result_opt: 0 });
        let path_opr = pb::logical_plan::Operator::from(pb::PathExpand {
            base: Some(edge_expand.clone()),
            start_tag: None,
            alias: None,
            hop_range: Some(pb::Range { lower: 0, upper: 4 }),
            path_opt: 0,
            result_opt: 0,
        });
        let path_end_opr = pb::logical_plan::Operator::from(pb::PathEnd { alias: None });

        let mut logical_plan = LogicalPlan::with_root(Node::new(0, source_opr.clone()));
        logical_plan
            .append_operator_as_node(path_opr.clone(), vec![0])
            .unwrap(); // node 1

        // Case without partition
        let mut builder = JobBuilder::default();
        let mut plan_meta = PlanMeta::default();
        logical_plan
            .add_job_builder(&mut builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(source_opr.encode_to_vec());
        expected_builder.filter_map(path_start_opr.encode_to_vec());
        expected_builder.iterate_emit(server_pb::iteration_emit::EmitKind::EmitBefore, 3, |plan| {
            plan.flat_map(expand_opr.clone().encode_to_vec());
        });
        expected_builder.map(path_end_opr.encode_to_vec());

        assert_eq!(builder, expected_builder);

        // Case with partition
        let mut builder = JobBuilder::default();
        let mut plan_meta = PlanMeta::default();
        plan_meta = plan_meta.with_partition();
        logical_plan
            .add_job_builder(&mut builder, &mut plan_meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(source_opr.encode_to_vec());
        expected_builder.filter_map(path_start_opr.encode_to_vec());
        expected_builder.iterate_emit(server_pb::iteration_emit::EmitKind::EmitBefore, 3, |plan| {
            plan.repartition(vec![])
                .flat_map(expand_opr.clone().encode_to_vec());
        });
        expected_builder.map(path_end_opr.encode_to_vec());

        assert_eq!(builder, expected_builder);
    }

    #[test]
    fn orderby_as_physical() {
        let source_opr = pb::logical_plan::Operator::from(pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![], vec![])),
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
    fn apply_as_physical_case1() {
        let mut plan = LogicalPlan::default();
        // g.V().as("0").where(out().as("1").has("lang", "java")).select("0").values("name")
        plan.meta = plan.meta.with_partition();

        // g.V("person")
        let scan: pb::logical_plan::Operator = pb::Scan {
            scan_opt: 0,
            alias: Some(0.into()),
            params: Some(query_params(vec![], vec!["name".into()])),
            idx_predicate: None,
        }
        .into();

        let opr_id = plan
            .append_operator_as_node(scan.clone(), vec![])
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
        let select: pb::logical_plan::Operator =
            pb::Select { predicate: str_to_expr_pb("@.lang == \"Java\"".to_string()).ok() }.into();
        plan.append_operator_as_node(select.clone(), vec![root_id])
            .unwrap();

        let apply: pb::logical_plan::Operator =
            pb::Apply { join_kind: 4, tags: vec![], subtask: root_id as i32, alias: None }.into();
        let opr_id = plan
            .append_operator_as_node(apply.clone(), vec![opr_id])
            .unwrap();

        let project: pb::logical_plan::Operator = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: str_to_expr_pb("@0.name".to_string()).ok(),
                alias: None,
            }],
            is_append: true,
        }
        .into();
        plan.append_operator_as_node(project.clone(), vec![opr_id])
            .unwrap();

        let mut builder = JobBuilder::default();
        let mut meta = plan.meta.clone();
        plan.add_job_builder(&mut builder, &mut meta)
            .unwrap();

        let mut expected_builder = JobBuilder::default();
        expected_builder.add_source(scan.encode_to_vec());
        let auxilia = pb::Auxilia {
            tag: None,
            params: Some(pb::QueryParams {
                tables: vec![],
                columns: vec![],
                is_all_columns: false,
                limit: None,
                predicate: str_to_expr_pb("@.lang == \"Java\"".to_string()).ok(),
                sample_ratio: 1.0,
                extra: Default::default(),
            }),
            alias: None,
        };

        expected_builder.apply_join(
            |plan| {
                plan.repartition(vec![])
                    .flat_map(pb::logical_plan::Operator::from(expand.clone()).encode_to_vec())
                    .repartition(vec![])
                    .filter_map(pb::logical_plan::Operator::from(auxilia.clone()).encode_to_vec());
            },
            apply.encode_to_vec(),
        );
        expected_builder.filter_map(project.encode_to_vec());
        expected_builder.sink(vec![]);

        assert_eq!(expected_builder, builder);
    }

    #[test]
    fn apply_as_physical_with_expand_degree_fuse() {
        let mut plan = LogicalPlan::default();
        // g.V().as("0").select("0").by(out().count().as("degree"))
        // out().degree() fused
        plan.meta = plan.meta.with_partition();

        // g.V()
        let scan: pb::logical_plan::Operator = pb::Scan {
            scan_opt: 0,
            alias: Some(0.into()),
            params: Some(query_params(vec![], vec![])),
            idx_predicate: None,
        }
        .into();

        let opr_id = plan
            .append_operator_as_node(scan.clone(), vec![])
            .unwrap();

        // .out().count()
        let mut expand = build_edgexpd(2, vec![], None);
        let subplan_id = plan
            .append_operator_as_node(expand.clone().into(), vec![])
            .unwrap();

        // Select("0").by()
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
        expected_builder.add_source(scan.encode_to_vec());
        expected_builder.repartition(vec![]);
        let mut fused = pb::FusedOperator { oprs: vec![] };
        fused.oprs.push(
            pb::Auxilia {
                tag: None,
                params: None,
                alias: Some(common_pb::NameOrId { item: Some(common_pb::name_or_id::Item::Id(2)) }),
            }
            .into(),
        );
        expand.alias = Some(1.into()); // must carry `Apply`'s alias
        fused.oprs.push(expand.into());
        fused.oprs.push(
            pb::Project {
                mappings: vec![pb::project::ExprAlias {
                    expr: str_to_expr_pb("@2".to_string()).ok(),
                    alias: None,
                }],
                is_append: true,
            }
            .into(),
        );
        expected_builder.flat_map(pb::logical_plan::Operator::from(fused).encode_to_vec());

        assert_eq!(expected_builder, builder);
    }

    #[test]
    fn apply_as_physical_with_select_expand_degree_fuse() {
        let mut plan = LogicalPlan::default();
        // g.V().as("0").select().by(select("0").out().count().as("degree"))
        // select("0").out().degree() fused
        plan.meta = plan.meta.with_partition();

        // g.V()
        let scan: pb::logical_plan::Operator = pb::Scan {
            scan_opt: 0,
            alias: Some(0.into()),
            params: Some(query_params(vec![], vec![])),
            idx_predicate: None,
        }
        .into();

        let opr_id = plan
            .append_operator_as_node(scan.clone(), vec![])
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
            .append_operator_as_node(project.into(), vec![])
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
        expected_builder.add_source(scan.encode_to_vec());
        expected_builder.repartition(
            common_pb::NameOrIdKey { key: Some(common_pb::NameOrId::from(0)) }.encode_to_vec(),
        );
        let mut fused = pb::FusedOperator { oprs: vec![] };
        fused.oprs.push(
            pb::Auxilia {
                tag: None,
                params: None,
                alias: Some(common_pb::NameOrId { item: Some(common_pb::name_or_id::Item::Id(2)) }),
            }
            .into(),
        );
        expand.v_tag = Some(0.into());
        expand.alias = Some(1.into()); // must carry `Apply`'s alias
        fused.oprs.push(expand.into());
        fused.oprs.push(
            pb::Project {
                mappings: vec![pb::project::ExprAlias {
                    expr: str_to_expr_pb("@2".to_string()).ok(),
                    alias: None,
                }],
                is_append: true,
            }
            .into(),
        );
        expected_builder.flat_map(pb::logical_plan::Operator::from(fused).encode_to_vec());

        assert_eq!(expected_builder, builder);
    }

    #[test]
    fn join_plan_as_physical() {
        let source_opr = pb::logical_plan::Operator::from(pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![], vec![])),
            idx_predicate: None,
        });
        let expand_opr = pb::logical_plan::Operator::from(pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![], vec![])),
            expand_opt: 0,
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
