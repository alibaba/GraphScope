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

    fn match_join_match_logical_plan() -> LogicalPlan {
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

        let mut plan = LogicalPlan::default();
        let scan_id = plan
            .append_operator_as_node(scan_opr.into(), vec![])
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

        println!("scan_id: {:?}", scan_id);
        println!("left_match_id: {:?}", left_match_id);
        println!("right_match_id: {:?}", right_match_id);
        println!("join_id: {:?}", join_id);

        plan
    }

    #[test]
    fn test_match_join_match_logical() {
        let plan = match_join_match_logical_plan();
        println!("hello i'm logical plan");
        println!("{:#?}", plan);
    }

    #[test]
    fn test_match_join_match_physical() {
        let plan = match_join_match_logical_plan();

        let mut plan_meta = plan.get_plan_meta();
        let mut builder = PlanBuilder::default();
        let _ = plan.add_job_builder(&mut builder, &mut plan_meta);
        let physical_plan = builder.build();
        println!("hello i'm physical plan");
        println!("{:#?}", physical_plan);
    }
}
