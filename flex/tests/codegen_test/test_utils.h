#ifndef TEST_UTILS_H
#define TEST_UTILS_H

#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

namespace gs {

// building plan pb for query 7.

// add graph data element info for edge expand
void add_edge_graph_data(physical::PhysicalOpr::MetaData* meta_data,
                         int32_t src_label_id, int32_t dst_label_id,
                         common::DataType data_type, std::string prop_name) {
  // results is vertex.
  auto graph_data = meta_data->mutable_type()->mutable_graph_type();
  graph_data->set_element_opt(common::GraphDataType::GraphElementOpt::
                                  GraphDataType_GraphElementOpt_VERTEX);
  {
    auto edge_tripet = graph_data->add_graph_data_type();
    edge_tripet->mutable_label()->mutable_src_label()->set_value(src_label_id);
    edge_tripet->mutable_label()->mutable_dst_label()->set_value(dst_label_id);
    auto edata_type = edge_tripet->add_props();
    edata_type->set_type(data_type);
    edata_type->mutable_prop_id()->set_name(prop_name);
  }
}
void add_edge_graph_data(physical::PhysicalOpr::MetaData* meta_data,
                         int32_t src_label_id, int32_t dst_label_id) {
  // results is vertex.
  auto graph_data = meta_data->mutable_type()->mutable_graph_type();
  graph_data->set_element_opt(common::GraphDataType::GraphElementOpt::
                                  GraphDataType_GraphElementOpt_VERTEX);
  {
    auto edge_tripet = graph_data->add_graph_data_type();
    edge_tripet->mutable_label()->mutable_src_label()->set_value(src_label_id);
    edge_tripet->mutable_label()->mutable_dst_label()->set_value(dst_label_id);
  }
}

void add_edge_graph_data(physical::PhysicalOpr::MetaData* meta_data,
                         std::pair<int32_t, int32_t> src_dst_label_ids0,
                         std::pair<int32_t, int32_t> src_dst_label_ids1,
                         common::DataType data_type, std::string prop_name) {
  // results is vertex.
  auto graph_data = meta_data->mutable_type()->mutable_graph_type();
  graph_data->set_element_opt(common::GraphDataType::GraphElementOpt::
                                  GraphDataType_GraphElementOpt_VERTEX);
  {
    auto edge_tripet = graph_data->add_graph_data_type();
    edge_tripet->mutable_label()->mutable_src_label()->set_value(
        src_dst_label_ids0.first);
    edge_tripet->mutable_label()->mutable_dst_label()->set_value(
        src_dst_label_ids0.second);
    auto edata_type = edge_tripet->add_props();
    edata_type->set_type(data_type);
    edata_type->mutable_prop_id()->set_name(prop_name);
  }
  {
    auto edge_tripet = graph_data->add_graph_data_type();
    edge_tripet->mutable_label()->mutable_src_label()->set_value(
        src_dst_label_ids1.first);
    edge_tripet->mutable_label()->mutable_dst_label()->set_value(
        src_dst_label_ids1.second);
    auto edata_type = edge_tripet->add_props();
    edata_type->set_type(data_type);
    edata_type->mutable_prop_id()->set_name(prop_name);
  }
}

void add_edge_graph_data(physical::PhysicalOpr::MetaData* meta_data,
                         std::pair<int32_t, int32_t> src_dst_label_ids0,
                         std::pair<int32_t, int32_t> src_dst_label_ids1) {
  // results is vertex.
  auto graph_data = meta_data->mutable_type()->mutable_graph_type();
  graph_data->set_element_opt(common::GraphDataType::GraphElementOpt::
                                  GraphDataType_GraphElementOpt_VERTEX);
  {
    auto edge_tripet = graph_data->add_graph_data_type();
    edge_tripet->mutable_label()->mutable_src_label()->set_value(
        src_dst_label_ids0.first);
    edge_tripet->mutable_label()->mutable_dst_label()->set_value(
        src_dst_label_ids0.second);
  }
  {
    auto edge_tripet = graph_data->add_graph_data_type();
    edge_tripet->mutable_label()->mutable_src_label()->set_value(
        src_dst_label_ids1.first);
    edge_tripet->mutable_label()->mutable_dst_label()->set_value(
        src_dst_label_ids1.second);
  }
}

void make_ic7_query_pb(physical::PhysicalPlan& input, int64_t person_id,
                       int32_t person_label_id, int32_t comment_label_id,
                       int32_t post_label_id, int32_t knows_label_id,
                       int32_t has_creator_label_id, int32_t likes_label_id) {
  // scan
  {
    auto scan_node = input.add_plan()->mutable_opr()->mutable_scan();
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
        auto op3 = predicate->add_operators()->mutable_const_();
        op3->set_i64(person_id);
      }
    }
    LOG(INFO) << "Finish setting ic7 scan pb";
  }
  // edge expand
  {
    auto edge_expand_opr = input.add_plan();
    auto edge_expand_node = edge_expand_opr->mutable_opr()->mutable_edge();
    edge_expand_node->set_direction(
        physical::EdgeExpand::Direction::EdgeExpand_Direction_IN);
    edge_expand_node->mutable_alias()->set_value(0);
    edge_expand_node->set_expand_opt(
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
    auto query_params = edge_expand_node->mutable_params();
    { query_params->add_tables()->set_id(has_creator_label_id); }
    auto meta_data = edge_expand_opr->add_meta_data();
    {
      // results is edge.
      auto graph_data = meta_data->mutable_type()->mutable_graph_type();
      graph_data->set_element_opt(common::GraphDataType::GraphElementOpt::
                                      GraphDataType_GraphElementOpt_EDGE);
      {
        auto edge_tripet = graph_data->add_graph_data_type();
        edge_tripet->mutable_label()->mutable_src_label()->set_value(
            post_label_id);
        edge_tripet->mutable_label()->mutable_dst_label()->set_value(
            person_label_id);
      }
      {
        auto edge_tripet = graph_data->add_graph_data_type();
        edge_tripet->mutable_label()->mutable_src_label()->set_value(
            comment_label_id);
        edge_tripet->mutable_label()->mutable_dst_label()->set_value(
            person_label_id);
      }
    }

    LOG(INFO) << "Finish setting ic7 edge expand pb";
  }
  {
    // edge expand to person
    auto edge_expand_opr = input.add_plan();
    auto edge_expand_node = edge_expand_opr->mutable_opr()->mutable_edge();
    edge_expand_node->set_direction(
        physical::EdgeExpand::Direction::EdgeExpand_Direction_IN);
    edge_expand_node->mutable_alias()->set_value(1);
    edge_expand_node->set_expand_opt(
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_EDGE);
    auto query_params = edge_expand_node->mutable_params();
    { query_params->add_tables()->set_id(likes_label_id); }
    auto meta_data = edge_expand_opr->add_meta_data();
    {
      // results is edge.
      auto graph_data = meta_data->mutable_type()->mutable_graph_type();
      graph_data->set_element_opt(common::GraphDataType::GraphElementOpt::
                                      GraphDataType_GraphElementOpt_EDGE);
      {
        auto edge_tripet = graph_data->add_graph_data_type();
        edge_tripet->mutable_label()->mutable_src_label()->set_value(
            person_label_id);
        edge_tripet->mutable_label()->mutable_dst_label()->set_value(
            post_label_id);
        auto edata_type = edge_tripet->add_props();
        edata_type->set_type(common::DataType::INT64);
        edata_type->mutable_prop_id()->set_name("creationDate");
      }
      {
        auto edge_tripet = graph_data->add_graph_data_type();
        edge_tripet->mutable_label()->mutable_src_label()->set_value(
            person_label_id);
        edge_tripet->mutable_label()->mutable_dst_label()->set_value(
            comment_label_id);
        auto edata_type = edge_tripet->add_props();
        edata_type->set_type(common::DataType::INT64);
        edata_type->mutable_prop_id()->set_name("creationDate");
      }
    }
    LOG(INFO) << "Finish setting ic7 edge expand pb";
  }

  {
    // get person with out v
    auto get_vertex_opr = input.add_plan();
    auto get_vertex_node = get_vertex_opr->mutable_opr()->mutable_vertex();
    get_vertex_node->mutable_alias()->set_value(2);
    get_vertex_node->set_opt(physical::GetV::VOpt::GetV_VOpt_END);
    auto query_params = get_vertex_node->mutable_params();
    { query_params->add_tables()->set_id(person_label_id); }
    LOG(INFO) << "Finish setting ic7 get v pb";
  }

  {
    // order by three columns
    auto order_by_opr = input.add_plan();
    auto order_by_node = order_by_opr->mutable_opr()->mutable_order_by();
    // order_by_node->mutable_limit()->mutable_lower()->set_value(0);
    order_by_node->mutable_limit()->set_lower(0);
    // order_by_node->mutable_limit()->mutable_upper()->set_value(INT_MAX);
    order_by_node->mutable_limit()->set_upper(INT_MAX);
    {
      auto first_pair = order_by_node->add_pairs();
      first_pair->set_order(algebra::OrderBy::OrderingPair::ASC);
      first_pair->mutable_key()->mutable_tag()->set_id(2);
      first_pair->mutable_key()->mutable_property()->mutable_key()->set_name(
          "id");
    }
    {
      auto pair = order_by_node->add_pairs();
      pair->set_order(algebra::OrderBy::OrderingPair::DESC);
      pair->mutable_key()->mutable_tag()->set_id(1);
      pair->mutable_key()->mutable_property()->mutable_key()->set_name(
          "creationDate");
    }
    {
      auto pair = order_by_node->add_pairs();
      pair->set_order(algebra::OrderBy::OrderingPair::ASC);
      pair->mutable_key()->mutable_tag()->set_id(0);
      pair->mutable_key()->mutable_property()->mutable_key()->set_name("id");
    }
  }
  {
    // dedup
    auto dedup_opr = input.add_plan();
    auto dedup_node = dedup_opr->mutable_opr()->mutable_dedup();
    dedup_node->add_keys()->mutable_tag()->set_id(2);
  }
  // {
  //   // project
  //   auto project_opr = input.add_plan();
  //   auto project_node = project_opr->mutable_opr()->mutable_project();
  //   project_node->set_is_append(false);
  //   {
  //     auto mapping = project_node->add_mappings();
  //     mapping->mutable_alias()->set_value();
  //   }
  // }
}

// from person to other person.
void make_ic3_query_right_pb(physical::PhysicalPlan& input,
                             int32_t place_label_id, int32_t person_label_id,
                             int32_t comment_label_id, int32_t post_label_id,
                             int32_t knows_label_id,
                             int32_t has_creator_label_id,
                             int32_t is_located_in_label_id,
                             int32_t is_part_of_label_id) {
  // 0: personid
  // 1: otherPerson
  // 2: country
  {
    auto scan_node = input.add_plan()->mutable_opr()->mutable_scan();
    // alias none
    scan_node->mutable_alias()->set_value(0);
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
        // auto op3 = predicate->add_operators()->mutable_const_();
        // op3->set_i64(person_id);
        auto op3 = predicate->add_operators();

        op3->mutable_param()->set_index(0);
        op3->mutable_param()->set_name("personId");
        // op3->set_data_type(common::DataType::INT64);
        op3->mutable_node_type()->set_data_type(common::DataType::INT64);
      }
    }
  }
  // path expand to friend
  {
    auto path_expand_opr = input.add_plan();
    auto path_expand_node = path_expand_opr->mutable_opr()->mutable_path();
    {
      auto edge_expand_node =
          path_expand_node->mutable_base()->mutable_edge_expand();
      edge_expand_node->set_direction(
          physical::EdgeExpand::Direction::EdgeExpand_Direction_BOTH);
      edge_expand_node->set_expand_opt(
          physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
      auto query_params = edge_expand_node->mutable_params();
      { query_params->add_tables()->set_id(knows_label_id); }
    }
    {
      auto get_v_node = path_expand_node->mutable_base()->mutable_get_v();
      get_v_node->set_opt(physical::GetV::VOpt::GetV_VOpt_END);
      get_v_node->mutable_params()->add_tables()->set_id(person_label_id);
    }
    // path_expand_node->mutable_hop_range()->mutable_lower()->set_value(1);
    // path_expand_node->mutable_hop_range()->mutable_upper()->set_value(3);
    path_expand_node->mutable_hop_range()->set_lower(1);
    path_expand_node->mutable_hop_range()->set_upper(3);
    path_expand_node->mutable_alias()->set_value(1);

    LOG(INFO) << "Finish setting ic3 path expand pb";
  }
  {
    // add a get_v op
    auto get_v_opr = input.add_plan();
    auto get_v_node = get_v_opr->mutable_opr()->mutable_vertex();
    get_v_node->set_opt(physical::GetV::VOpt::GetV_VOpt_END);
    get_v_node->mutable_params()->add_tables()->set_id(person_label_id);
    get_v_node->mutable_alias()->set_value(1);
  }
  {
    // edge expand to place
    auto edge_expand_opr = input.add_plan();
    auto edge_expand_node = edge_expand_opr->mutable_opr()->mutable_edge();
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
    // edge expand to country
    auto edge_expand_opr = input.add_plan();
    auto edge_expand_node = edge_expand_opr->mutable_opr()->mutable_edge();
    edge_expand_node->set_direction(
        physical::EdgeExpand::Direction::EdgeExpand_Direction_OUT);
    edge_expand_node->set_expand_opt(
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
    auto query_params = edge_expand_node->mutable_params();
    { query_params->add_tables()->set_id(is_part_of_label_id); }
    auto meta_data = edge_expand_opr->add_meta_data();
    add_edge_graph_data(meta_data, place_label_id, place_label_id,
                        common::DataType::INT64, "creationDate");
  }

  {
    // get place with required condition
    auto get_vertex_opr = input.add_plan();
    auto get_vertex_node = get_vertex_opr->mutable_opr()->mutable_vertex();
    get_vertex_node->mutable_alias()->set_value(-1);
    get_vertex_node->set_opt(physical::GetV::VOpt::GetV_VOpt_Itself);
    auto query_params = get_vertex_node->mutable_params();
    {
      query_params->add_tables()->set_id(place_label_id);
      // add predicate
      auto predicate = get_vertex_node->mutable_params()->mutable_predicate();
      {
        predicate->add_operators()->set_brace(
            common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE);
      }
      {
        auto expr = predicate->add_operators();
        expr->mutable_var()->mutable_property()->mutable_key()->set_name(
            "name");
        expr->mutable_var()->mutable_node_type()->set_data_type(
            common::DataType::STRING);
        expr->mutable_node_type()->set_data_type(common::DataType::STRING);
      }
      {
        // eq
        auto expr = predicate->add_operators();
        expr->set_logical(common::Logical::NE);
        expr->mutable_node_type()->set_data_type(common::DataType::BOOLEAN);
      }
      {
        // dynamic params
        auto expr = predicate->add_operators();
        expr->mutable_param()->set_index(1);
        // expr->mutable_param()->set_data_type(common::DataType::STRING);
        expr->mutable_param()->set_name("countryX");
        expr->mutable_node_type()->set_data_type(common::DataType::STRING);
        // expr->mutable_const_()->set_str(countryx_name);
        // expr->mutable_node_type()->set_data_type(common::DataType::STRING);
      }
      {
        predicate->add_operators()->set_brace(
            common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE);
      }
      {
        auto expr = predicate->add_operators();
        expr->set_logical(common::Logical::AND);
      }
      {
        predicate->add_operators()->set_brace(
            common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE);
      }
      {
        auto expr = predicate->add_operators();
        expr->mutable_var()->mutable_property()->mutable_key()->set_name(
            "name");
        expr->mutable_var()->mutable_node_type()->set_data_type(
            common::DataType::STRING);
        expr->mutable_node_type()->set_data_type(common::DataType::STRING);
      }
      {
        // eq
        auto expr = predicate->add_operators();
        expr->set_logical(common::Logical::NE);
        expr->mutable_node_type()->set_data_type(common::DataType::BOOLEAN);
      }
      {
        auto expr = predicate->add_operators();
        // expr->mutable_const_()->set_str(countryy_name);
        // expr->mutable_node_type()->set_data_type(common::DataType::STRING);
        expr->mutable_param()->set_index(2);
        // expr->mutable_param()->set_data_type(common::DataType::STRING);
        expr->mutable_param()->set_name("countryY");
        expr->mutable_node_type()->set_data_type(common::DataType::STRING);
      }
      {
        predicate->add_operators()->set_brace(
            common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE);
      }
    }
    LOG(INFO) << "Finish setting ic3 get v pb";
  }
}
// start from countryx
void make_ic3_query_left_left_pb(
    physical::PhysicalPlan& input, int32_t place_label_id,
    int32_t person_label_id, int32_t comment_label_id, int32_t post_label_id,
    int32_t knows_label_id, int32_t has_creator_label_id,
    int32_t is_located_in_label_id, int32_t is_part_of_label_id) {
  {
    auto scan_node = input.add_plan()->mutable_opr()->mutable_scan();
    scan_node->mutable_alias()->set_value(2);
    // alias none
    auto query_params = scan_node->mutable_params();
    {
      query_params->add_tables()->set_id(place_label_id);
      auto predicate = query_params->mutable_predicate();
      {
        auto cur_op = predicate->add_operators();
        auto op1 = cur_op->mutable_var();
        op1->mutable_property()->mutable_key()->set_name("name");
        op1->mutable_node_type()->set_data_type(common::DataType::STRING);
      }
      { predicate->add_operators()->set_logical(common::Logical::EQ); }
      {
        auto op3 = predicate->add_operators();
        op3->mutable_param()->set_index(1);
        op3->mutable_param()->set_name("countryX");
        op3->mutable_node_type()->set_data_type(common::DataType::STRING);
        // op3->set_data_type(common::DataType::STRING);
        // op3->set_str(countryx_name);
      }
    }
  }
  {
    // edge expand to comment and post
    auto edge_expand_opr = input.add_plan();
    auto edge_expand_node = edge_expand_opr->mutable_opr()->mutable_edge();
    edge_expand_node->mutable_alias()->set_value(-1);
    edge_expand_node->set_direction(
        physical::EdgeExpand::Direction::EdgeExpand_Direction_IN);
    edge_expand_node->set_expand_opt(
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
    auto query_params = edge_expand_node->mutable_params();
    { query_params->add_tables()->set_id(is_located_in_label_id); }
    auto meta_data = edge_expand_opr->add_meta_data();
    add_edge_graph_data(meta_data,
                        std::make_pair(comment_label_id, place_label_id),
                        std::make_pair(post_label_id, place_label_id),
                        common::DataType::INT64, "creationDate");
  }
  {
    // get v
    auto get_v_opr = input.add_plan();
    auto get_v_node = get_v_opr->mutable_opr()->mutable_vertex();
    get_v_node->mutable_alias()->set_value(4);
    get_v_node->mutable_params()->add_tables()->set_id(post_label_id);
    get_v_node->mutable_params()->add_tables()->set_id(comment_label_id);
    auto pred = get_v_node->mutable_params()->mutable_predicate();
    {
      pred->add_operators()->set_brace(
          common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE);
    }
    {
      auto opr = pred->add_operators();
      opr->mutable_var()->mutable_property()->mutable_key()->set_name(
          "creationDate");
      opr->mutable_var()->mutable_node_type()->set_data_type(
          common::DataType::INT64);
    }
    {
      auto opr = pred->add_operators();
      opr->set_logical(common::Logical::GE);
      opr->mutable_node_type()->set_data_type(common::DataType::BOOLEAN);
    }
    {
      // auto opr = pred->add_operators();
      auto opr = pred->add_operators();
      opr->mutable_param()->set_index(3);
      opr->mutable_param()->set_name("startDate");
      opr->mutable_node_type()->set_data_type(common::DataType::INT64);

      // opr->set_data_type(common::DataType::INT64);
      // opr->mutable_const_()->set_i64(start_date);
    }
    {
      pred->add_operators()->set_brace(
          common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE);
    }
    { pred->add_operators()->set_logical(common::Logical::AND); }
    {
      pred->add_operators()->set_brace(
          common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE);
    }
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
      opr->mutable_param()->set_index(4);
      opr->mutable_param()->set_name("endDate");
      // opr->set_data_type(common::DataType::INT64);
      opr->mutable_node_type()->set_data_type(common::DataType::INT64);
    }
    {
      pred->add_operators()->set_brace(
          common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE);
    }
  }
  {
    // edge_expand
    auto edge_expand_opr = input.add_plan();
    auto edge_expand_node = edge_expand_opr->mutable_opr()->mutable_edge();
    edge_expand_node->mutable_alias()->set_value(1);
    edge_expand_node->set_direction(
        physical::EdgeExpand::Direction::EdgeExpand_Direction_OUT);
    edge_expand_node->set_expand_opt(
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
    auto query_params = edge_expand_node->mutable_params();
    { query_params->add_tables()->set_id(has_creator_label_id); }
    auto meta_data = edge_expand_opr->add_meta_data();
    add_edge_graph_data(meta_data,
                        std::make_pair(comment_label_id, person_label_id),
                        std::make_pair(post_label_id, person_label_id));
  }
}

void make_ic3_query_left_right_pb(
    physical::PhysicalPlan& input, int32_t place_label_id,
    int32_t person_label_id, int32_t comment_label_id, int32_t post_label_id,
    int32_t knows_label_id, int32_t has_creator_label_id,
    int32_t is_located_in_label_id, int32_t is_part_of_label_id) {
  {
    auto scan_node = input.add_plan()->mutable_opr()->mutable_scan();
    scan_node->mutable_alias()->set_value(3);
    // alias none
    auto query_params = scan_node->mutable_params();
    {
      query_params->add_tables()->set_id(place_label_id);
      auto predicate = query_params->mutable_predicate();
      {
        auto cur_op = predicate->add_operators();
        auto op1 = cur_op->mutable_var();
        op1->mutable_property()->mutable_key()->set_name("name");
        op1->mutable_node_type()->set_data_type(common::DataType::STRING);
      }
      { predicate->add_operators()->set_logical(common::Logical::EQ); }
      {
        auto op3 = predicate->add_operators();
        op3->mutable_param()->set_index(2);
        op3->mutable_param()->set_name("countryY");
        // op3->set_data_type(common::DataType::STRING);
        op3->mutable_node_type()->set_data_type(common::DataType::STRING);
        // op3->set_str(countryx_name);
      }
    }
  }
  {
    // edge expand to comment and post
    auto edge_expand_opr = input.add_plan();
    auto edge_expand_node = edge_expand_opr->mutable_opr()->mutable_edge();
    edge_expand_node->mutable_alias()->set_value(-1);
    edge_expand_node->set_direction(
        physical::EdgeExpand::Direction::EdgeExpand_Direction_IN);
    edge_expand_node->set_expand_opt(
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
    auto query_params = edge_expand_node->mutable_params();
    { query_params->add_tables()->set_id(is_located_in_label_id); }
    auto meta_data = edge_expand_opr->add_meta_data();
    add_edge_graph_data(meta_data,
                        std::make_pair(comment_label_id, place_label_id),
                        std::make_pair(post_label_id, place_label_id),
                        common::DataType::INT64, "creationDate");
  }
  {
    // get v
    auto get_v_opr = input.add_plan();
    auto get_v_node = get_v_opr->mutable_opr()->mutable_vertex();
    get_v_node->mutable_alias()->set_value(5);
    get_v_node->mutable_params()->add_tables()->set_id(post_label_id);
    get_v_node->mutable_params()->add_tables()->set_id(comment_label_id);
    auto pred = get_v_node->mutable_params()->mutable_predicate();
    {
      pred->add_operators()->set_brace(
          common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE);
    }
    {
      auto opr = pred->add_operators();
      opr->mutable_var()->mutable_property()->mutable_key()->set_name(
          "creationDate");
      opr->mutable_var()->mutable_node_type()->set_data_type(
          common::DataType::INT64);
    }
    {
      auto opr = pred->add_operators();
      opr->set_logical(common::Logical::GE);
      opr->mutable_node_type()->set_data_type(common::DataType::BOOLEAN);
    }
    {
      // auto opr = pred->add_operators();
      auto opr = pred->add_operators();
      opr->mutable_param()->set_index(3);
      opr->mutable_param()->set_name("startDate");
      // opr->set_data_type(common::DataType::INT64);
      opr->mutable_node_type()->set_data_type(common::DataType::INT64);
      // opr->mutable_const_()->set_i64(start_date);
    }
    {
      pred->add_operators()->set_brace(
          common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE);
    }
    { pred->add_operators()->set_logical(common::Logical::AND); }
    {
      pred->add_operators()->set_brace(
          common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE);
    }
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
      opr->mutable_param()->set_index(4);
      opr->mutable_param()->set_name("endDate");
      // opr->set_data_type(common::DataType::INT64);
      opr->mutable_node_type()->set_data_type(common::DataType::INT64);
    }
    {
      pred->add_operators()->set_brace(
          common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE);
    }
  }
  {
    // edge_expand
    auto edge_expand_opr = input.add_plan();
    auto edge_expand_node = edge_expand_opr->mutable_opr()->mutable_edge();
    edge_expand_node->mutable_alias()->set_value(1);
    edge_expand_node->set_direction(
        physical::EdgeExpand::Direction::EdgeExpand_Direction_OUT);
    edge_expand_node->set_expand_opt(
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
    auto query_params = edge_expand_node->mutable_params();
    { query_params->add_tables()->set_id(has_creator_label_id); }
    auto meta_data = edge_expand_opr->add_meta_data();
    add_edge_graph_data(meta_data,
                        std::make_pair(comment_label_id, person_label_id),
                        std::make_pair(post_label_id, person_label_id));
  }
}

void make_ic3_query_pb(physical::PhysicalPlan& input, int32_t place_label_id,
                       int32_t person_label_id, int32_t comment_label_id,
                       int32_t post_label_id, int32_t knows_label_id,
                       int32_t has_creator_label_id,
                       int32_t is_located_in_label_id,
                       int32_t is_part_of_label_id) {
  // ic3 needs three join.
  // branch 0: person->otherPerson->city->country->neq(x,y)
  // branch 1: countryx->message->person
  // branch 2: countryy->message->person
  // - branch 2
  //  - branch 0
  //  - branch 1
  // peronId : 0
  // otherPerson : 1
  // countryx : 2
  // countryy : 3
  // messagex : 4
  // messagey : 5
  auto join_node = input.add_plan()->mutable_opr()->mutable_join();
  join_node->set_join_kind(physical::Join::JoinKind::Join_JoinKind_INNER);
  join_node->add_left_keys()->mutable_tag()->set_id(1);
  join_node->add_right_keys()->mutable_tag()->set_id(1);
  auto left_plan = join_node->mutable_left_plan();
  auto right_plan = join_node->mutable_right_plan();
  {
    // left plan itself is a join
    auto left_node_join = left_plan->add_plan()->mutable_opr()->mutable_join();
    join_node->set_join_kind(physical::Join::JoinKind::Join_JoinKind_INNER);
    left_node_join->add_left_keys()->mutable_tag()->set_id(1);
    left_node_join->add_right_keys()->mutable_tag()->set_id(1);
    auto left_left_plan = left_node_join->mutable_left_plan();
    auto left_right_plan = left_node_join->mutable_right_plan();
    // start from countryx, till other person
    make_ic3_query_left_left_pb(
        *left_left_plan, place_label_id, person_label_id, comment_label_id,
        post_label_id, knows_label_id, has_creator_label_id,
        is_located_in_label_id, is_part_of_label_id);
    // start from countryy, till other person
    make_ic3_query_left_right_pb(
        *left_right_plan, place_label_id, person_label_id, comment_label_id,
        post_label_id, knows_label_id, has_creator_label_id,
        is_located_in_label_id, is_part_of_label_id);
  }
  // scan for branch 1.

  make_ic3_query_right_pb(*right_plan, place_label_id, person_label_id,
                          comment_label_id, post_label_id, knows_label_id,
                          has_creator_label_id, is_located_in_label_id,
                          is_part_of_label_id);
  {
    // add sink op
    auto sink_opr = input.add_plan()->mutable_opr()->mutable_sink();
  }
}

// person->friend->post1->tag, person->friend->post2->tag
void make_ic4_anti_join_left_plan(physical::PhysicalPlan& input,
                                  int32_t person_label_id,
                                  int32_t post_label_id,
                                  int32_t comment_label_id,
                                  int32_t tag_label_id, int32_t knows_label_id,
                                  int32_t has_creator_label_id,
                                  int32_t has_tag_label_id, int64_t person_id,
                                  int64_t start_date, int64_t end_date) {
  {
    // scan for person
    auto scan_node = input.add_plan()->mutable_opr()->mutable_scan();
    scan_node->mutable_alias()->set_value(0);
    auto params = scan_node->mutable_params();
    params->add_tables()->set_id(person_label_id);
    auto predicate = params->mutable_predicate();
    {
      auto left = predicate->add_operators();
      {
        auto var = left->mutable_var();
        var->mutable_node_type()->set_data_type(common::DataType::INT64);
        auto prop = var->mutable_property();
        auto prop_name = prop->mutable_key();
        prop_name->set_name("id");
      }
      auto mid = predicate->add_operators();
      mid->mutable_node_type()->set_data_type(common::DataType::BOOLEAN);
      { mid->set_logical(common::Logical::EQ); }

      auto right = predicate->add_operators();
      right->mutable_node_type()->set_data_type(common::DataType::INT64);
      {
        auto val = right->mutable_const_();
        val->set_i64(person_id);
      }
    }
    LOG(INFO) << "Building predicate size: " << predicate->operators_size();
  }
  {
    // edge expand to friend
    auto opr = input.add_plan();
    auto edge_node = opr->mutable_opr()->mutable_edge();
    edge_node->set_direction(
        physical::EdgeExpand::Direction::EdgeExpand_Direction_BOTH);
    auto params = edge_node->mutable_params();
    params->add_tables()->set_id(knows_label_id);

    auto meta_data = opr->add_meta_data();
    add_edge_graph_data(meta_data, person_label_id, person_label_id,
                        common::DataType::INT64, "creationDate");
    edge_node->mutable_alias()->set_value(-1);
  }
  {
    // get v
    auto opr = input.add_plan();
    auto get_node = opr->mutable_opr()->mutable_vertex();
    get_node->mutable_alias()->set_value(1);
    get_node->set_opt(physical::GetV::VOpt::GetV_VOpt_Itself);
    get_node->mutable_params()->add_tables()->set_id(person_label_id);
  }
  {
    // edge expand to post
    auto opr = input.add_plan();
    auto edge_node = opr->mutable_opr()->mutable_edge();
    edge_node->mutable_v_tag()->set_value(1);
    edge_node->mutable_alias()->set_value(-1);
    edge_node->set_direction(
        physical::EdgeExpand::Direction::EdgeExpand_Direction_IN);
    auto params = edge_node->mutable_params();
    params->add_tables()->set_id(has_creator_label_id);

    auto meta_data = opr->add_meta_data();
    add_edge_graph_data(meta_data,
                        std::make_pair(post_label_id, person_label_id),
                        std::make_pair(comment_label_id, person_label_id));
  }

  {
    // get post in [start, end)
    auto opr = input.add_plan();
    auto get_node = opr->mutable_opr()->mutable_vertex();
    get_node->mutable_alias()->set_value(2);
    get_node->set_opt(physical::GetV::VOpt::GetV_VOpt_START);
    auto params = get_node->mutable_params();
    params->add_tables()->set_id(post_label_id);
    auto predicate = params->mutable_predicate();
    {
      {
        auto left = predicate->add_operators();
        auto var = left->mutable_var();
        var->mutable_node_type()->set_data_type(common::DataType::INT64);
        auto prop = var->mutable_property();
        auto prop_name = prop->mutable_key();
        prop_name->set_name("creationDate");
      }

      {
        auto mid = predicate->add_operators();
        mid->mutable_node_type()->set_data_type(common::DataType::BOOLEAN);
        mid->set_logical(common::Logical::GE);
      }

      {
        auto right = predicate->add_operators();
        right->mutable_node_type()->set_data_type(common::DataType::INT64);
        auto val = right->mutable_const_();
        val->set_i64(start_date);
      }
      {
        auto opr = predicate->add_operators();
        opr->set_logical(common::Logical::AND);
        opr->mutable_node_type()->set_data_type(common::DataType::BOOLEAN);
      }
      {
        auto left = predicate->add_operators();
        auto var = left->mutable_var();
        var->mutable_node_type()->set_data_type(common::DataType::INT64);
        auto prop = var->mutable_property();
        auto prop_name = prop->mutable_key();
        prop_name->set_name("creationDate");
      }

      {
        auto mid = predicate->add_operators();
        mid->mutable_node_type()->set_data_type(common::DataType::BOOLEAN);
        mid->set_logical(common::Logical::LT);
      }

      {
        auto right = predicate->add_operators();
        right->mutable_node_type()->set_data_type(common::DataType::INT64);
        auto val = right->mutable_const_();
        val->set_i64(end_date);
      }
    }
  }
  // expand fromn the satisfied post to tags
  {
    auto opr = input.add_plan();
    auto edge_node = opr->mutable_opr()->mutable_edge();
    edge_node->mutable_v_tag()->set_value(2);
    edge_node->set_expand_opt(
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_EDGE);
    edge_node->mutable_params()->add_tables()->set_id(has_tag_label_id);
    add_edge_graph_data(opr->add_meta_data(), post_label_id, tag_label_id);
  }
  {
    // get tags
    auto opr = input.add_plan();
    auto get_node = opr->mutable_opr()->mutable_vertex();
    get_node->mutable_alias()->set_value(3);
    get_node->mutable_params()->add_tables()->set_id(tag_label_id);
    get_node->set_opt(physical::GetV::VOpt::GetV_VOpt_END);
  }
}

// person->friend->post2->tag
void make_ic4_anti_join_right_plan(physical::PhysicalPlan& input,
                                   int32_t person_label_id,
                                   int32_t post_label_id,
                                   int32_t comment_label_id,
                                   int32_t tag_label_id, int32_t knows_label_id,
                                   int32_t has_creator_label_id,
                                   int32_t has_tag_label_id, int64_t person_id,
                                   int64_t start_date, int64_t end_date) {
  {
    // scan for person
    auto scan_node = input.add_plan()->mutable_opr()->mutable_scan();
    scan_node->mutable_alias()->set_value(0);
    auto params = scan_node->mutable_params();
    params->add_tables()->set_id(person_label_id);
    auto predicate = params->mutable_predicate();
    {
      auto left = predicate->add_operators();
      {
        auto var = left->mutable_var();
        var->mutable_node_type()->set_data_type(common::DataType::INT64);
        auto prop = var->mutable_property();
        auto prop_name = prop->mutable_key();
        prop_name->set_name("id");
      }
      auto mid = predicate->add_operators();
      mid->mutable_node_type()->set_data_type(common::DataType::BOOLEAN);
      { mid->set_logical(common::Logical::EQ); }

      auto right = predicate->add_operators();
      right->mutable_node_type()->set_data_type(common::DataType::INT64);
      {
        auto val = right->mutable_const_();
        val->set_i64(person_id);
      }
    }
    LOG(INFO) << "Building predicate size: " << predicate->operators_size();
  }
  {
    // edge expand to friend
    auto opr = input.add_plan();
    auto edge_node = opr->mutable_opr()->mutable_edge();
    edge_node->set_direction(
        physical::EdgeExpand::Direction::EdgeExpand_Direction_BOTH);
    auto params = edge_node->mutable_params();
    params->add_tables()->set_id(knows_label_id);

    auto meta_data = opr->add_meta_data();
    add_edge_graph_data(meta_data, person_label_id, person_label_id,
                        common::DataType::INT64, "creationDate");
    edge_node->mutable_alias()->set_value(-1);
  }
  {
    // get v
    auto opr = input.add_plan();
    auto get_node = opr->mutable_opr()->mutable_vertex();
    get_node->mutable_alias()->set_value(4);
    get_node->set_opt(physical::GetV::VOpt::GetV_VOpt_Itself);
    get_node->mutable_params()->add_tables()->set_id(person_label_id);
  }
  {
    // edge expand to post
    auto opr = input.add_plan();
    auto edge_node = opr->mutable_opr()->mutable_edge();
    edge_node->mutable_v_tag()->set_value(4);
    edge_node->mutable_alias()->set_value(-1);
    edge_node->set_direction(
        physical::EdgeExpand::Direction::EdgeExpand_Direction_IN);
    auto params = edge_node->mutable_params();
    params->add_tables()->set_id(has_creator_label_id);

    auto meta_data = opr->add_meta_data();
    add_edge_graph_data(meta_data,
                        std::make_pair(post_label_id, person_label_id),
                        std::make_pair(comment_label_id, person_label_id));
  }
  {
    // get post
    auto opr = input.add_plan();
    auto get_node = opr->mutable_opr()->mutable_vertex();
    get_node->mutable_alias()->set_value(5);
    get_node->set_opt(physical::GetV::VOpt::GetV_VOpt_Itself);
    auto params = get_node->mutable_params();
    params->add_tables()->set_id(post_label_id);
    auto predicate = params->mutable_predicate();
    {
      auto left = predicate->add_operators();
      {
        auto var = left->mutable_var();
        var->mutable_node_type()->set_data_type(common::DataType::INT64);
        auto prop = var->mutable_property();
        auto prop_name = prop->mutable_key();
        prop_name->set_name("creationDate");
      }
      auto mid = predicate->add_operators();
      mid->mutable_node_type()->set_data_type(common::DataType::BOOLEAN);
      { mid->set_logical(common::Logical::LT); }

      auto right = predicate->add_operators();
      right->mutable_node_type()->set_data_type(common::DataType::INT64);
      {
        auto val = right->mutable_const_();
        val->set_i64(start_date);
      }
    }
  }
  {
    auto opr = input.add_plan();
    auto edge_node = opr->mutable_opr()->mutable_edge();
    edge_node->mutable_v_tag()->set_value(5);
    edge_node->set_expand_opt(
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_EDGE);
    edge_node->mutable_params()->add_tables()->set_id(has_tag_label_id);
    add_edge_graph_data(opr->add_meta_data(), post_label_id, tag_label_id);
  }
  {
    // get tags
    auto opr = input.add_plan();
    auto get_node = opr->mutable_opr()->mutable_vertex();
    get_node->mutable_alias()->set_value(3);
    get_node->mutable_params()->add_tables()->set_id(tag_label_id);
    get_node->set_opt(physical::GetV::VOpt::GetV_VOpt_END);
  }
}

void make_ic4_query_pb(physical::PhysicalPlan& input, int32_t person_label_id,
                       int32_t post_label_id, int32_t comment_label_id,
                       int32_t tag_label_id, int32_t knows_label_id,
                       int32_t has_creator_label_id, int32_t has_tag_label_id,
                       int64_t person_id, int64_t start_date,
                       int64_t end_date) {
  {
    // first antijoin
    auto antijoin_node = input.add_plan()->mutable_opr()->mutable_join();
    antijoin_node->set_join_kind(physical::Join::JoinKind::Join_JoinKind_ANTI);
    auto lk0 = antijoin_node->add_left_keys();
    lk0->mutable_property()->mutable_id();
    lk0->mutable_tag()->set_id(3);  // tag
    // auto lk1 = antijoin_node->add_left_keys();
    // lk1->mutable_property()->mutable_id();
    // lk1->mutable_tag()->set_id(3);  // tag

    // same for right_keys
    auto rk0 = antijoin_node->add_right_keys();
    rk0->mutable_property()->mutable_id();
    rk0->mutable_tag()->set_id(3);  // tag
    // auto rk1 = antijoin_node->add_right_keys();
    // rk1->mutable_property()->mutable_id();
    // rk1->mutable_tag()->set_id(3);  // tag

    make_ic4_anti_join_left_plan(
        *antijoin_node->mutable_left_plan(), person_label_id, post_label_id,
        comment_label_id, tag_label_id, knows_label_id, has_creator_label_id,
        has_tag_label_id, person_id, start_date, end_date);
    make_ic4_anti_join_right_plan(
        *antijoin_node->mutable_right_plan(), person_label_id, post_label_id,
        comment_label_id, tag_label_id, knows_label_id, has_creator_label_id,
        has_tag_label_id, person_id, start_date, end_date);
    // then group by
    // then sort post cnt and tag.name
  }
  {
    // group by
    auto group_node = input.add_plan()->mutable_opr()->mutable_group_by();
    auto group_key = group_node->add_mappings();
    group_key->mutable_alias()->set_value(6);  // still 3
    group_key->mutable_key()->mutable_tag()->set_id(3);
    group_key->mutable_key()->mutable_property()->mutable_key()->set_name(
        "name");
    group_key->mutable_key()->mutable_node_type()->set_data_type(
        common::DataType::STRING);

    auto agg_func = group_node->add_functions();
    agg_func->set_aggregate(physical::GroupBy::AggFunc::COUNT_DISTINCT);
    agg_func->mutable_alias()->set_value(7);
    auto var = agg_func->add_vars();
    var->mutable_tag()->set_id(2);
    var->mutable_property()->mutable_id();
  }

  {
    // sort by
    auto sort_node = input.add_plan()->mutable_opr()->mutable_order_by();
    // sort_node->mutable_limit()->mutable_lower()->set_value(0);
    // sort_node->mutable_limit()->mutable_upper()->set_value(10);
    sort_node->mutable_limit()->set_lower(0);
    sort_node->mutable_limit()->set_upper(10);
    auto first = sort_node->add_pairs();
    first->mutable_key()->mutable_tag()->set_id(7);
    first->set_order(
        algebra::OrderBy::OrderingPair::Order::OrderBy_OrderingPair_Order_DESC);
    first->mutable_key()->mutable_node_type()->set_data_type(
        common::DataType::INT64);

    auto second = sort_node->add_pairs();
    second->mutable_key()->mutable_tag()->set_id(6);
    second->set_order(
        algebra::OrderBy::OrderingPair::Order::OrderBy_OrderingPair_Order_ASC);
    second->mutable_key()->mutable_node_type()->set_data_type(
        common::DataType::STRING);
  }
  { auto sink_node = input.add_plan()->mutable_opr()->mutable_sink(); }
}

}  // namespace gs

#endif  // TEST_UTILS_H