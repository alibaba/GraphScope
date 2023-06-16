#ifndef IC2_BUILD_H
#define IC2_BUILD_H

#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

#include "flex/codegen/util/test_utils.h"

namespace gs {
void make_ic2_query_pb(physical::PhysicalPlan& input, int32_t person_label_id,
                       int32_t post_label_id, int32_t comment_label_id,
                       int32_t knows_label_id, int32_t has_creator_label_id) {
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
        param->set_index(1);
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
  // expand to post and comment
  {
    // edge expand to comment and post
    auto edge_expand_opr = input.add_plan();
    auto edge_expand_node = edge_expand_opr->mutable_opr()->mutable_edge();
    edge_expand_node->set_direction(
        physical::EdgeExpand::Direction::EdgeExpand_Direction_IN);
    edge_expand_node->set_expand_opt(
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
    edge_expand_node->mutable_alias()->set_value(-1);
    auto query_params = edge_expand_node->mutable_params();
    { query_params->add_tables()->set_id(has_creator_label_id); }
    auto meta_data = edge_expand_opr->add_meta_data();
    add_edge_graph_data(meta_data,
                        std::make_pair(post_label_id, person_label_id),
                        std::make_pair(comment_label_id, person_label_id));
  }
  {
    // get v
    auto get_v_opr = input.add_plan();
    auto get_v_node = get_v_opr->mutable_opr()->mutable_vertex();
    get_v_node->mutable_alias()->set_value(2);
    get_v_node->mutable_params()->add_tables()->set_id(post_label_id);
    get_v_node->mutable_params()->add_tables()->set_id(comment_label_id);
    auto pred = get_v_node->mutable_params()->mutable_predicate();
    {
      auto opr = pred->add_operators();
      opr->mutable_var()->mutable_property()->mutable_key()->set_name(
          "creationDate");
      opr->mutable_var()->mutable_node_type()->set_data_type(
          common::DataType::INT64);
    }
    {
      auto opr = pred->add_operators();
      opr->set_logical(common::Logical::LT);
      opr->mutable_node_type()->set_data_type(common::DataType::BOOLEAN);
    }
    {
      auto opr = pred->add_operators();
      opr->mutable_param()->set_index(0);
      opr->mutable_param()->set_name("maxDate");
      // opr->mutable_param()->set_data_type(common::DataType::INT64);
      opr->mutable_node_type()->set_data_type(common::DataType::INT64);
    }
  }
  {
    // prject
    auto proj_opr = input.add_plan();
    auto proj_node = proj_opr->mutable_opr()->mutable_project();
    {
      auto first = proj_node->add_mappings();
      first->mutable_alias()->set_value(3);
      auto expr = first->mutable_expr();
      auto expr_opr = expr->add_operators();
      auto var = expr_opr->mutable_var();
      var->mutable_tag()->set_id(1);
      var->mutable_property()->mutable_key()->set_name("id");
      var->mutable_node_type()->set_data_type(common::DataType::INT64);
      expr_opr->mutable_node_type()->set_data_type(common::DataType::INT64);
    }
    {
      auto first = proj_node->add_mappings();
      first->mutable_alias()->set_value(4);
      auto expr = first->mutable_expr();
      auto expr_opr = expr->add_operators();
      auto var = expr_opr->mutable_var();
      var->mutable_tag()->set_id(1);
      var->mutable_property()->mutable_key()->set_name("firstName");
      var->mutable_node_type()->set_data_type(common::DataType::STRING);
      expr_opr->mutable_node_type()->set_data_type(common::DataType::STRING);
    }
    {
      auto first = proj_node->add_mappings();
      first->mutable_alias()->set_value(5);
      auto expr = first->mutable_expr();
      auto expr_opr = expr->add_operators();
      auto var = expr_opr->mutable_var();
      var->mutable_tag()->set_id(1);
      var->mutable_property()->mutable_key()->set_name("lastName");
      var->mutable_node_type()->set_data_type(common::DataType::STRING);
      expr_opr->mutable_node_type()->set_data_type(common::DataType::STRING);
    }
    {
      auto first = proj_node->add_mappings();
      first->mutable_alias()->set_value(6);
      auto expr = first->mutable_expr();
      auto expr_opr = expr->add_operators();
      auto var = expr_opr->mutable_var();
      var->mutable_tag()->set_id(2);
      var->mutable_property()->mutable_key()->set_name("id");
      var->mutable_node_type()->set_data_type(common::DataType::INT64);
      expr_opr->mutable_node_type()->set_data_type(common::DataType::INT64);
    }
    {
      auto first = proj_node->add_mappings();
      first->mutable_alias()->set_value(7);
      auto expr = first->mutable_expr();
      auto expr_opr = expr->add_operators();
      auto var = expr_opr->mutable_var();
      var->mutable_tag()->set_id(2);
      var->mutable_property()->mutable_key()->set_name("content");
      var->mutable_node_type()->set_data_type(common::DataType::STRING);
      expr_opr->mutable_node_type()->set_data_type(common::DataType::STRING);
    }
    {
      auto first = proj_node->add_mappings();
      first->mutable_alias()->set_value(8);
      auto expr = first->mutable_expr();
      auto expr_opr = expr->add_operators();
      auto var = expr_opr->mutable_var();
      var->mutable_tag()->set_id(2);
      var->mutable_property()->mutable_key()->set_name("imageFile");
      var->mutable_node_type()->set_data_type(common::DataType::STRING);
      expr_opr->mutable_node_type()->set_data_type(common::DataType::STRING);
    }
    {
      auto first = proj_node->add_mappings();
      first->mutable_alias()->set_value(9);
      auto expr = first->mutable_expr();
      auto expr_opr = expr->add_operators();
      auto var = expr_opr->mutable_var();
      var->mutable_tag()->set_id(2);
      var->mutable_property()->mutable_key()->set_name("creationDate");
      var->mutable_node_type()->set_data_type(common::DataType::INT64);
      expr_opr->mutable_node_type()->set_data_type(common::DataType::INT64);
    }
  }
  {
    auto order_by_opr = input.add_plan();
    auto order_by_node = order_by_opr->mutable_opr()->mutable_order_by();
    {
      auto pair = order_by_node->add_pairs();
      pair->set_order(algebra::OrderBy::OrderingPair::Order::
                          OrderBy_OrderingPair_Order_DESC);
      pair->mutable_key()->mutable_tag()->set_id(9);
      pair->mutable_key()->mutable_node_type()->set_data_type(
          common::DataType::INT64);
    }
    {
      auto pair = order_by_node->add_pairs();
      pair->set_order(algebra::OrderBy::OrderingPair::Order::
                          OrderBy_OrderingPair_Order_ASC);
      pair->mutable_key()->mutable_tag()->set_id(6);
      pair->mutable_key()->mutable_node_type()->set_data_type(
          common::DataType::INT64);
    }
    // order_by_node->mutable_limit()->mutable_upper()->set_value(20);
    order_by_node->mutable_limit()->set_upper(20);
  }
  {
    // sink
    auto sink_opr = input.add_plan();
    auto sink_node = sink_opr->mutable_opr()->mutable_sink();
  }
}

}  // namespace gs

#endif  // IC2_BUILD_H