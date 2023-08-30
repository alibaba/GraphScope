//
//! Copyright 2023 Alibaba Group Holding Limited.
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

mod common;

#[cfg(test)]
mod tests {
    use std::vec;

    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_core::plan::logical::LogicalPlan;
    use ir_core::plan::physical::AsPhysical;
    use ir_physical_client::physical_builder::PlanBuilder;

    fn default_sink_pb() -> pb::Sink {
        pb::Sink {
            tags: vec![common_pb::NameOrIdKey { key: None }],
            sink_target: Some(pb::sink::SinkTarget {
                inner: Some(pb::sink::sink_target::Inner::SinkDefault(pb::SinkDefault {
                    id_name_mappings: vec![],
                })),
            }),
        }
    }

    // join(scan.match1, scan.match2)
    fn single_source_multi_match_join_logical_plan() -> LogicalPlan {
        let scan_opr =
            pb::Scan { scan_opt: 0, alias: None, params: None, idx_predicate: None, meta_data: None };

        let expand_opr1 = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: 0,
            alias: None,
            meta_data: None,
        };

        let expand_opr2 = expand_opr1.clone();

        // build pattern 1: as('a').out().as('b')
        let left_pattern = pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some("a".into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr1)),
                }],
                end: Some("b".into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        };

        // build pattern 2: as('c').out().as('b')
        let right_pattern = pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some("c".into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr2)),
                }],
                end: Some("b".into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        };

        let join_opr = pb::Join {
            kind: 0,
            left_keys: vec![common_pb::Variable { tag: Some("b".into()), property: None, node_type: None }],
            right_keys: vec![common_pb::Variable {
                tag: Some("b".into()),
                property: None,
                node_type: None,
            }],
        };

        let sink = default_sink_pb();

        let mut plan = LogicalPlan::with_root();
        let scan_id = plan
            .append_operator_as_node(scan_opr.into(), vec![0])
            .unwrap();
        let left_match_id = plan
            .append_operator_as_node(left_pattern.into(), vec![scan_id])
            .unwrap();
        let right_match_id = plan
            .append_operator_as_node(right_pattern.into(), vec![scan_id])
            .unwrap();
        let join_id = plan
            .append_operator_as_node(join_opr.into(), vec![left_match_id, right_match_id])
            .unwrap();
        plan.append_operator_as_node(sink.into(), vec![join_id])
            .unwrap();

        plan
    }

    // join(scan1.match1, scan2.match2)
    fn multi_source_multi_match_join_logical_plan() -> LogicalPlan {
        let scan_opr =
            pb::Scan { scan_opt: 0, alias: None, params: None, idx_predicate: None, meta_data: None };

        let expand_opr1 = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: 0,
            alias: None,
            meta_data: None,
        };

        let expand_opr2 = expand_opr1.clone();

        // build pattern 1: as('a').out().as('b')
        let left_pattern = pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some("a".into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr1)),
                }],
                end: Some("b".into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        };

        // build pattern 2: as('c').out().as('b')
        let right_pattern = pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some("c".into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr2)),
                }],
                end: Some("b".into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        };

        let join_opr = pb::Join {
            kind: 0,
            left_keys: vec![common_pb::Variable { tag: Some("b".into()), property: None, node_type: None }],
            right_keys: vec![common_pb::Variable {
                tag: Some("b".into()),
                property: None,
                node_type: None,
            }],
        };

        let sink = default_sink_pb();

        let mut plan = LogicalPlan::with_root();
        let left_scan_id = plan
            .append_operator_as_node(scan_opr.clone().into(), vec![0])
            .unwrap();
        let left_match_id = plan
            .append_operator_as_node(left_pattern.into(), vec![left_scan_id])
            .unwrap();
        let right_scan_id = plan
            .append_operator_as_node(scan_opr.into(), vec![0])
            .unwrap();
        let right_match_id = plan
            .append_operator_as_node(right_pattern.into(), vec![right_scan_id])
            .unwrap();
        let join_id = plan
            .append_operator_as_node(join_opr.into(), vec![left_match_id, right_match_id])
            .unwrap();

        plan.append_operator_as_node(sink.into(), vec![join_id])
            .unwrap();

        plan
    }

    // join(join(scan1.match1, scan2.match2), scan3.match3)
    fn multi_source_multi_match_multi_join_logical_plan() -> LogicalPlan {
        let scan_opr =
            pb::Scan { scan_opt: 0, alias: None, params: None, idx_predicate: None, meta_data: None };

        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: 0,
            alias: None,
            meta_data: None,
        };

        // build pattern 1: as('a').out().as('b')
        let left_pattern = pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some("a".into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some("b".into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        };

        // build pattern 2: as('c').out().as('b')
        let right_pattern = pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some("c".into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some("b".into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        };

        // build pattern 3: as('d').out().as('b')
        let third_pattern = pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some("d".into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some("b".into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        };

        let join_opr = pb::Join {
            kind: 0,
            left_keys: vec![common_pb::Variable { tag: Some("b".into()), property: None, node_type: None }],
            right_keys: vec![common_pb::Variable {
                tag: Some("b".into()),
                property: None,
                node_type: None,
            }],
        };

        let sink = default_sink_pb();

        let mut plan = LogicalPlan::with_root();
        let left_scan_id = plan
            .append_operator_as_node(scan_opr.clone().into(), vec![0])
            .unwrap();
        let left_match_id = plan
            .append_operator_as_node(left_pattern.into(), vec![left_scan_id])
            .unwrap();
        let right_scan_id = plan
            .append_operator_as_node(scan_opr.clone().into(), vec![0])
            .unwrap();
        let right_match_id = plan
            .append_operator_as_node(right_pattern.into(), vec![right_scan_id])
            .unwrap();
        let join_id = plan
            .append_operator_as_node(join_opr.clone().into(), vec![left_match_id, right_match_id])
            .unwrap();

        let third_scan_id = plan
            .append_operator_as_node(scan_opr.into(), vec![0])
            .unwrap();
        let third_match_id = plan
            .append_operator_as_node(third_pattern.into(), vec![third_scan_id])
            .unwrap();
        let second_join_id = plan
            .append_operator_as_node(join_opr.into(), vec![join_id, third_match_id])
            .unwrap();

        plan.append_operator_as_node(sink.into(), vec![second_join_id])
            .unwrap();

        plan
    }

    #[test]
    fn test_sinlge_source_multi_match_join_logical() {
        let plan = single_source_multi_match_join_logical_plan();
        // dummy, scan1, expand1, getv1, scan2, expand2, getv2, join1, sink
        assert_eq!(plan.len(), 9);
    }

    #[test]
    fn test_sinlge_source_multi_match_join_physical() {
        let plan = single_source_multi_match_join_logical_plan();

        let mut plan_meta = plan.get_plan_meta();
        let mut builder = PlanBuilder::default();
        let _ = plan.add_job_builder(&mut builder, &mut plan_meta);
        let physical_plan = builder.take();
        // dummy, join, sink
        assert_eq!(physical_plan.len(), 3);
    }

    #[test]
    fn test_multi_source_multi_match_join_logical() {
        let plan = multi_source_multi_match_join_logical_plan();
        // dummy, scan1, expand1, getv1, scan2, expand2, getv2, join1, sink
        assert_eq!(plan.len(), 9);
    }

    #[test]
    fn test_multi_source_multi_match_join_physical() {
        let plan = multi_source_multi_match_join_logical_plan();

        let mut plan_meta = plan.get_plan_meta();
        let mut builder = PlanBuilder::default();
        let _ = plan.add_job_builder(&mut builder, &mut plan_meta);
        let physical_plan = builder.take();
        // dummy, join, sink
        assert_eq!(physical_plan.len(), 3);
    }

    #[test]
    fn test_multi_source_multi_match_multi_join_logical() {
        let plan = multi_source_multi_match_multi_join_logical_plan();
        // dummy, scan1, expand1, getv1, scan2, expand2, getv2, join1, scan3, expand3, getv3, join2, sink
        assert_eq!(plan.len(), 13);
    }

    #[test]
    fn test_multi_source_multi_match_multi_join_physical() {
        let plan = multi_source_multi_match_multi_join_logical_plan();

        let mut plan_meta = plan.get_plan_meta();
        let mut builder = PlanBuilder::default();
        let _res = plan.add_job_builder(&mut builder, &mut plan_meta);
        let physical_plan = builder.take();
        // dummy, join2, sink
        assert_eq!(physical_plan.len(), 3);
    }

    // join(join(scan1.match1, scan2.match2), scan3.match3)
    fn multi_join_logical_plan() -> LogicalPlan {
        let scan_opr =
            pb::Scan { scan_opt: 0, alias: None, params: None, idx_predicate: None, meta_data: None };
        let join_opr = pb::Join {
            kind: 0,
            left_keys: vec![common_pb::Variable { tag: None, property: None, node_type: None }],
            right_keys: vec![common_pb::Variable { tag: None, property: None, node_type: None }],
        };

        let sink = default_sink_pb();

        let mut plan = LogicalPlan::with_root();
        let scan1_id = plan
            .append_operator_as_node(scan_opr.clone().into(), vec![0])
            .unwrap();
        let scan2_id = plan
            .append_operator_as_node(scan_opr.clone().into(), vec![0])
            .unwrap();
        let join1_id = plan
            .append_operator_as_node(join_opr.clone().into(), vec![scan1_id, scan2_id])
            .unwrap();
        let scan3_id = plan
            .append_operator_as_node(scan_opr.clone().into(), vec![0])
            .unwrap();
        let join2_id = plan
            .append_operator_as_node(join_opr.clone().into(), vec![join1_id, scan3_id])
            .unwrap();
        plan.append_operator_as_node(sink.into(), vec![join2_id])
            .unwrap();

        plan
    }

    #[test]
    fn test_multi_join_logical() {
        let plan = multi_join_logical_plan();
        // dummy, scan1, scan2,  join1, scan3, join2, sink
        assert_eq!(plan.len(), 7);
    }

    #[test]
    fn test_multi_join_physical() {
        let plan = multi_join_logical_plan();

        let mut plan_meta = plan.get_plan_meta();
        let mut builder = PlanBuilder::default();
        let res = plan.add_job_builder(&mut builder, &mut plan_meta);
        assert!(res.is_ok());
        let physical_plan = builder.take();
        // dummy, join2, sink
        assert_eq!(physical_plan.len(), 3);
    }

    // join(join(scan1.match1, scan2.match2), scan3.match3) with branch op
    fn multi_join_with_branch_logical_plan() -> LogicalPlan {
        let scan1_opr = pb::Scan {
            scan_opt: 0,
            alias: Some(0.into()),
            params: None,
            idx_predicate: None,
            meta_data: None,
        };
        let scan2_opr = pb::Scan {
            scan_opt: 0,
            alias: Some(1.into()),
            params: None,
            idx_predicate: None,
            meta_data: None,
        };
        let scan3_opr = pb::Scan {
            scan_opt: 0,
            alias: Some(2.into()),
            params: None,
            idx_predicate: None,
            meta_data: None,
        };
        let dummy_opr = pb::Root {};
        let join_opr = pb::Join {
            kind: 0,
            left_keys: vec![common_pb::Variable { tag: None, property: None, node_type: None }],
            right_keys: vec![common_pb::Variable { tag: None, property: None, node_type: None }],
        };

        let sink = default_sink_pb();

        let mut plan = LogicalPlan::with_root();
        let branch2_id = plan
            .append_operator_as_node(dummy_opr.clone().into(), vec![0])
            .unwrap();
        let branch1_id = plan
            .append_operator_as_node(dummy_opr.into(), vec![branch2_id])
            .unwrap();
        let scan1_id = plan
            .append_operator_as_node(scan1_opr.clone().into(), vec![branch1_id])
            .unwrap();
        let scan2_id = plan
            .append_operator_as_node(scan2_opr.clone().into(), vec![branch1_id])
            .unwrap();
        let join1_id = plan
            .append_operator_as_node(join_opr.clone().into(), vec![scan1_id, scan2_id])
            .unwrap();
        let scan3_id = plan
            .append_operator_as_node(scan3_opr.clone().into(), vec![branch2_id])
            .unwrap();
        let join2_id = plan
            .append_operator_as_node(join_opr.clone().into(), vec![join1_id, scan3_id])
            .unwrap();
        plan.append_operator_as_node(sink.into(), vec![join2_id])
            .unwrap();

        plan
    }

    #[test]
    fn test_multi_join_with_branch_logical() {
        let plan = multi_join_with_branch_logical_plan();
        //  branch2,  branch1, scan1, scan2,  join1, scan3, join2, sink
        assert_eq!(plan.len(), 8);
    }

    #[test]
    fn test_multi_join_with_branch_physical() {
        let plan = multi_join_with_branch_logical_plan();
        let mut plan_meta = plan.get_plan_meta();
        let mut builder = PlanBuilder::default();
        let res = plan.add_job_builder(&mut builder, &mut plan_meta);
        assert!(res.is_ok());
        let physical_plan = builder.take();
        // dummy, join2, sink
        assert_eq!(physical_plan.len(), 3);
    }
}
