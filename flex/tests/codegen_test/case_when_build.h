#ifndef CASE_WHEN_BUILD_H
#define CASE_WHEN_BUILD_H

#include <string>
#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

#include "flex/codegen/util/test_utils.h"

namespace gs {
// scan->edgeExpandV->EdgeExpandE->getv->project_with_case_when

void make_case_when_query_pb(physical::PhysicalPlan& input,
                             int32_t person_label_id, int32_t place_label_id,
                             int32_t org_label_id, int32_t knows_label_id,
                             int32_t is_located_in_label_id,
                             int32_t work_at_label_id) {
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
        op3->mutable_param()->set_index(0);
        op3->mutable_param()->set_name("personId");
        op3->mutable_node_type()->set_data_type(common::DataType::INT64);
      }
    }
  }
  {
    // edge expand to comment and post
    auto edge_expand_opr = input.add_plan();
    auto edge_expand_node = edge_expand_opr->mutable_opr()->mutable_edge();
    edge_expand_node->mutable_alias()->set_value(1);
    edge_expand_node->set_direction(
        physical::EdgeExpand::Direction::EdgeExpand_Direction_OUT);
    edge_expand_node->set_expand_opt(
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_EDGE);
    auto query_params = edge_expand_node->mutable_params();
    { query_params->add_tables()->set_id(is_located_in_label_id); }
    auto meta_data = edge_expand_opr->add_meta_data();
    add_edge_graph_data(meta_data, person_label_id, place_label_id);
  }

  {
    // project with case when.
    auto proj_opr = input.add_plan();
    auto proj_node = proj_opr->mutable_opr()->mutable_project();
    proj_node->set_is_append(false);
    {
      auto mapping = proj_node->add_mappings();
      mapping->mutable_alias()->set_value(0);
      auto expr = mapping->mutable_expr();
      {
        // add case when
        auto case_when_opr = expr->add_operators();
        auto case_when_node = case_when_opr->mutable_case_();
        case_when_opr->mutable_node_type()->set_data_type(
            common::DataType::BOOLEAN);

        {
          // input_expr
          auto input_expr = case_when_node->mutable_input_expression();

          {
            // tag:1
            auto input_expr_opr = input_expr->add_operators()->mutable_var();
            input_expr_opr->mutable_property()->mutable_id();
            input_expr_opr->mutable_tag()->set_id(1);
            auto graph_type =
                input_expr_opr->mutable_node_type()->mutable_graph_type();
            graph_type->set_element_opt(common::GraphDataType::GraphElementOpt::
                                            GraphDataType_GraphElementOpt_EDGE);
            {
              auto edge_tripet = graph_type->add_graph_data_type();
              edge_tripet->mutable_label()->mutable_src_label()->set_value(
                  person_label_id);
              edge_tripet->mutable_label()->mutable_dst_label()->set_value(
                  place_label_id);
              auto edata_type = edge_tripet->add_props();
            }
          }
          {
            // eq
            auto input_expr_opr = input_expr->add_operators();
            input_expr_opr->set_logical(common::Logical::EQ);
          }
          {
            // null
            auto input_expr_opr = input_expr->add_operators()->mutable_const_();
            input_expr_opr->mutable_none();
          }
        }
        {
          // when_when_expr
          auto when_then_expr = case_when_node->add_when_then_expressions();
          {
            auto when_expr = when_then_expr->mutable_when_expression();
            {
              auto var = when_expr->add_operators()->mutable_const_();
              var->set_boolean(true);
            }
            auto then_expr = when_then_expr->mutable_then_result_expression();
            {
              auto var = then_expr->add_operators()->mutable_const_();
              var->set_boolean(true);
            }
          }
        }
        {
          // else expr
          auto else_expr = case_when_node->mutable_else_result_expression();
          {
            auto var = else_expr->add_operators()->mutable_const_();
            var->set_boolean(false);
          }
        }
      }
    }
  }
  {
    // sink
    auto sink_opr = input.add_plan();
    auto sink_node = sink_opr->mutable_opr()->mutable_sink();
  }
}

}  // namespace gs

#endif  // CASE_WHEN_BUILD_H