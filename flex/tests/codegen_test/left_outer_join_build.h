#ifndef LEFT_OUTER_JOIN_BUILD_H
#define LEFT_OUTER_JOIN_BUILD_H

#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

#include "flex/codegen/util/test_utils.h"

namespace gs {

void make_left_outer_join_query_pb(physical::PhysicalPlan& input,
                                   int32_t person_label_id,
                                   int32_t place_label_id, int32_t org_label_id,
                                   int32_t knows_label_id,
                                   int32_t is_located_in_label_id,
                                   int32_t work_at_label_id) {
  // scan
  {
    auto scan_node = input.add_plan()->mutable_opr()->mutable_scan();
    scan_node->mutable_alias()->set_value(0);
    // alias none
    auto query_params = scan_node->mutable_params();
    {
      query_params->add_tables()->set_id(person_label_id);
      auto predicate = query_params->mutable_predicate();
      {
        auto cur_op = predicate->add_operators();
        auto op1 = cur_op->mutable_var();
        op1->mutable_property()->mutable_key()->set_name("id");
        op1->mutable_node_type()->set_data_type(common::DataType::INT64);
      }
      { predicate->add_operators()->set_logical(common::Logical::EQ); }
      {
        auto op3 = predicate->add_operators();
        auto param = op3->mutable_param();
        // op3->set_str(countryx_name);
        op3->mutable_node_type()->set_data_type(common::DataType::INT64);
        param->set_name("personIdQ2");
        param->set_index(0);
        // param->set_data_type(common::DataType::INT64);
      }
    }
  }
  // edge expand to friend
  {
    // edge expand to comment and post
    auto edge_expand_opr = input.add_plan();
    auto edge_expand_node = edge_expand_opr->mutable_opr()->mutable_edge();
    edge_expand_node->set_direction(
        physical::EdgeExpand::Direction::EdgeExpand_Direction_BOTH);
    edge_expand_node->set_expand_opt(
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
    edge_expand_node->mutable_alias()->set_value(1);
    auto query_params = edge_expand_node->mutable_params();
    { query_params->add_tables()->set_id(knows_label_id); }
    auto meta_data = edge_expand_opr->add_meta_data();
    add_edge_graph_data(meta_data, person_label_id, person_label_id,
                        common::DataType::INT64, "creationDate");
  }
  {
    // join start from here
    auto join_opr = input.add_plan();
    auto join_op_node = join_opr->mutable_opr()->mutable_join();
    join_op_node->set_join_kind(physical::Join::LEFT_OUTER);
    {
      auto left = join_op_node->add_left_keys();
      left->mutable_tag()->set_id(0);
      left->mutable_property()->mutable_id();

      auto left2 = join_op_node->add_left_keys();
      left2->mutable_tag()->set_id(1);
      left2->mutable_property()->mutable_id();
    }
    {
      auto right = join_op_node->add_right_keys();
      right->mutable_tag()->set_id(0);
      right->mutable_property()->mutable_id();

      auto right2 = join_op_node->add_right_keys();
      right2->mutable_tag()->set_id(1);
      right2->mutable_property()->mutable_id();
    }
    {
      // add left plan
      auto left_plans = join_op_node->mutable_left_plan();

      // expand to locationCity
      auto edge_expand_opr = left_plans->add_plan();
      auto edge_expand_node = edge_expand_opr->mutable_opr()->mutable_edge();
      edge_expand_node->mutable_alias()->set_value(2);
      edge_expand_node->set_direction(
          physical::EdgeExpand::Direction::EdgeExpand_Direction_OUT);
      edge_expand_node->set_expand_opt(
          physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
      auto query_params = edge_expand_node->mutable_params();
      { query_params->add_tables()->set_id(is_located_in_label_id); }
      auto meta_data = edge_expand_opr->add_meta_data();
      add_edge_graph_data(meta_data, person_label_id, place_label_id,
                          common::DataType::INT64, "creationDate");
    }
    {
      auto right_plans = join_op_node->mutable_right_plan();
      // expand to  orgnization
      auto edge_expand_opr = right_plans->add_plan();
      auto edge_expand_node = edge_expand_opr->mutable_opr()->mutable_edge();
      edge_expand_node->mutable_alias()->set_value(3);
      edge_expand_node->set_direction(
          physical::EdgeExpand::Direction::EdgeExpand_Direction_OUT);
      edge_expand_node->set_expand_opt(
          physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
      auto query_params = edge_expand_node->mutable_params();
      { query_params->add_tables()->set_id(work_at_label_id); }
      auto meta_data = edge_expand_opr->add_meta_data();
      add_edge_graph_data(meta_data, person_label_id, org_label_id,
                          common::DataType::INT32, "creationDate");
    }
  }
  //
}
}  // namespace gs

#endif  // LEFT_OUTER_JOIN_BUILD_H