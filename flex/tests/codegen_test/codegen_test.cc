#include "glog/logging.h"

#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

#include "flex/codegen/building_context.h"
#include "flex/codegen/op_builder/dedup_builder.h"
#include "flex/codegen/op_builder/edge_expand_builder.h"
#include "flex/codegen/op_builder/expr_builder.h"
#include "flex/codegen/op_builder/get_v_builder.h"
#include "flex/codegen/op_builder/group_by_builder.h"
#include "flex/codegen/op_builder/path_expand_builder.h"
#include "flex/codegen/op_builder/project_builder.h"
#include "flex/codegen/op_builder/scan_builder.h"
#include "flex/codegen/op_builder/select_builder.h"
#include "flex/codegen/op_builder/sort_builder.h"
#include "flex/codegen/pb_parser/query_params_parser.h"
#include "flex/codegen/query_generator.h"
#include "flex/codegen/codegen_utils.h"
#include "flex/codegen/util/test_utils.h"

#include "flex/codegen/util/case_when_build.h"
#include "flex/codegen/util/ic2_build.h"
#include "flex/codegen/util/left_outer_join_build.h"
#include "flex/engines/hqps/null_record.h"

namespace gs {
namespace test {
static constexpr int32_t place_label_id = 0;
static constexpr int32_t person_label_id = 1;
static constexpr int32_t comment_label_id = 2;
static constexpr int32_t post_label_id = 3;
// forum label id
static constexpr int32_t forum_label_id = 4;
// organisation label id
static constexpr int32_t organisation_label_id = 5;
// tagClass label id
static constexpr int32_t tagClass_label_id = 6;
// tag label id
static constexpr int32_t tag_label_id = 7;

// hasCreator label id
static constexpr int32_t hasCreator_label_id = 0;
// hasTag label id
static constexpr int32_t hasTag_label_id = 1;
// replyOf label id
static constexpr int32_t replyOf_label_id = 2;
// containerOf label id
static constexpr int32_t containerOf_label_id = 3;
// hasMember label id
static constexpr int32_t hasMember_label_id = 4;
// hasModerator label id
static constexpr int32_t hasModerator_label_id = 5;
// hasInterest label id
static constexpr int32_t hasInterest_label_id = 6;
// islocatedIn label id
static constexpr int32_t isLocatedIn_label_id = 7;
// knows label id
static constexpr int32_t knows_label_id = 8;
// likes
static constexpr int32_t likes_label_id = 9;
// workAt label id
static constexpr int32_t workAt_label_id = 10;
// isPartOf label id
static constexpr int32_t isPartOf_label_id = 11;
// hasType label id
static constexpr int32_t hasType_label_id = 12;
// isSubClassof label id
static constexpr int32_t isSubClassOf_label_id = 13;
// studyAt label id
static constexpr int32_t studyAt_label_id = 11;

}  // namespace test
void test_get_oid_from_expr() {
  common::Expression expr;
  fill_sample_expr(expr);
  int64_t oid;
  CHECK(gs::try_to_get_oid_from_expr(expr, oid));
  CHECK(oid == 1);
}

void test_get_oid_param_from_expr() {
  common::Expression expr;
  fill_oid_param_expr(expr);
  codegen::ParamConst param_const;
  CHECK(gs::try_to_get_oid_param_from_expr(expr, param_const));
  LOG(INFO) << "parse param const: " << param_const.var_name;
}

// test generate scan with oid;
void test_generate_scan_operator() {
  gs::BuildingContext ctx;
  auto scan_op = new physical::Scan();
  auto meta_data = new physical::PhysicalOpr::MetaData();

  gs::make_scan_op_pb(*scan_op, *meta_data, 1);
  auto built_op_str = gs::BuildScanOp(ctx, *scan_op, *meta_data);
  LOG(INFO) << "Generated scan op code: ";
  LOG(INFO) << built_op_str;
  delete scan_op;
}

// test generate code with expression
void test_generate_edge_expand_v_operator_with_single_label() {
  gs::BuildingContext ctx;
  auto edge_expand_op_pb = new physical::EdgeExpand();
  auto meta_data = new physical::PhysicalOpr::MetaData();

  gs::make_edge_expand_v_single_label_op_pb(*edge_expand_op_pb, *meta_data, 1,
                                            0);
  // need meta_data to provide src-edge-dst triplet
  auto built_op_str =
      gs::BuildEdgeExpandOp<uint8_t>(ctx, *edge_expand_op_pb, *meta_data);
  LOG(INFO) << "Generated edge_expand op code: ";
  LOG(INFO) << built_op_str;
  delete edge_expand_op_pb;
  delete meta_data;
}

void test_generate_edge_expand_v_operator_two_label() {
  gs::BuildingContext ctx;

  auto edge_expand_op_pb = new physical::EdgeExpand();
  auto meta_data = new physical::PhysicalOpr::MetaData();

  gs::make_edge_expand_v_two_label_op_pb(*edge_expand_op_pb, *meta_data, 1, 0);
  auto built_op_str =
      gs::BuildEdgeExpandOp<uint8_t>(ctx, *edge_expand_op_pb, *meta_data);
  LOG(INFO) << "Generated edge_expand op code: ";
  LOG(INFO) << built_op_str;
  delete edge_expand_op_pb;
  delete meta_data;
}

void test_generate_edge_expand_e_operator_one_label() {
  gs::BuildingContext ctx;
  auto edge_expand_op_pb = new physical::EdgeExpand();
  auto meta_data = new physical::PhysicalOpr::MetaData();
  // generate op
  gs::make_edge_expand_e_one_label_op_pb(*edge_expand_op_pb, *meta_data, 0, -1);
  auto built_op_str =
      gs::BuildEdgeExpandOp<uint8_t>(ctx, *edge_expand_op_pb, *meta_data);
  LOG(INFO) << "Generated edge_expand op code: ";
  LOG(INFO) << built_op_str;
  delete edge_expand_op_pb;
  delete meta_data;
}

void test_expr_gen() {
  LOG(INFO) << "-------------test_gen_query------------";
  gs::BuildingContext ctx;

  auto exprs = new common::Expression();
  gs::make_sample_exprs(*exprs);
  ExprBuilder expr_builder(ctx);

  auto& expr_oprs = exprs->operators();
  expr_builder.AddAllExprOpr(expr_oprs);
  std::string func_name, func_code;
  std::vector<codegen::ParamConst> func_call_params;
  std::vector<std::string> tag_props;
  common::DataType unused_expr_ret_type;
  std::tie(func_name, func_call_params, tag_props, func_code,
           unused_expr_ret_type) = expr_builder.Build();
  LOG(INFO) << "func_name: " << func_name;
  LOG(INFO) << "func_code: " << func_code;
  for (auto i = 0; i < func_call_params.size(); ++i) {
    LOG(INFO) << "func_call_params: " << i << ", "
              << data_type_2_string(func_call_params[i].type)
              << func_call_params[i].var_name << ", ";
  }

  for (auto i = 0; i < tag_props.size(); ++i) {
    LOG(INFO) << "tag_props: " << i << ", " << tag_props[i];
  }
}

void test_expr_gen_with_params() {
  LOG(INFO) << "-----------test_gen_query_with_params----------";
  gs::BuildingContext ctx;

  auto exprs = new common::Expression();
  gs::make_sample_exprs_with_params(*exprs);
  ExprBuilder expr_builder(ctx);

  auto& expr_oprs = exprs->operators();
  expr_builder.AddAllExprOpr(expr_oprs);
  std::string func_name, func_code;
  std::vector<codegen::ParamConst> func_call_params;
  std::vector<std::string> tag_props;
  common::DataType unused_expr_ret_type;
  std::tie(func_name, func_call_params, tag_props, func_code,
           unused_expr_ret_type) = expr_builder.Build();
  LOG(INFO) << "func_name: " << func_name;
  LOG(INFO) << "func_code: " << func_code;
  for (auto i = 0; i < func_call_params.size(); ++i) {
    LOG(INFO) << "func_call_params: " << i << ", "
              << data_type_2_string(func_call_params[i].type)
              << func_call_params[i].var_name << ", ";
  }

  for (auto i = 0; i < tag_props.size(); ++i) {
    LOG(INFO) << "tag_props: " << i << ", " << tag_props[i];
  }
}

void test_sort_op() {
  LOG(INFO) << "-----------test_sort_op----------";
  gs::BuildingContext ctx;

  auto sort_op_pb = new algebra::OrderBy();
  gs::make_sort_op_pb(*sort_op_pb);
  auto meta_data = new physical::PhysicalOpr::MetaData();
  std::string sort_opt, sort_code;
  std::tie(sort_opt, sort_code) = gs::BuildSortOp(ctx, *sort_op_pb, *meta_data);
  LOG(INFO) << "Generated sort op code: ";
  LOG(INFO) << "sort opt: " << sort_opt;
  LOG(INFO) << "sort code: " << sort_code;
  LOG(INFO) << "Finish sort code generation";
  delete sort_op_pb;
  delete meta_data;
}

void test_select_op() {
  LOG(INFO) << "-----------test_select_op----------";
  gs::BuildingContext ctx;

  auto select_op_pb = new algebra::Select();
  auto meta_data = new physical::PhysicalOpr::MetaData();
  gs::make_select_op_pb(*select_op_pb, *meta_data);
  std::string expr_code, select_code;
  std::tie(expr_code, select_code) =
      gs::BuildSelectOp(ctx, *select_op_pb, *meta_data);
  LOG(INFO) << "Generated select op code: ";
  LOG(INFO) << expr_code;
  LOG(INFO) << select_code;
  LOG(INFO) << "Finish select code generation";
  delete select_op_pb;
  delete meta_data;
}

void test_dedup_op() {
  LOG(INFO) << "-----------test_dedup_op----------";
  gs::BuildingContext ctx;

  auto dedup_op_pb = new algebra::Dedup();
  gs::make_dedup_op_pb(*dedup_op_pb);
  auto meta_data = new physical::PhysicalOpr::MetaData();
  std::string dedup_code;
  dedup_code = gs::BuildDedupOp(ctx, *dedup_op_pb, *meta_data);
  LOG(INFO) << "Generated dedup op code: ";
  LOG(INFO) << dedup_code;
  LOG(INFO) << "Finish dedup code generation";
  delete dedup_op_pb;
  delete meta_data;
}

void test_project_op() {
  LOG(INFO) << "-----------test_project_op----------";
  gs::BuildingContext ctx;

  auto project_op_pb = new physical::Project();
  gs::make_project_op_pb(*project_op_pb);
  auto meta_data = new physical::PhysicalOpr::MetaData();
  std::string project_opt, project_code;
  std::tie(project_opt, project_code) =
      gs::BuildProjectOp(ctx, *project_op_pb, *meta_data);
  LOG(INFO) << "Generated project op code: " << project_code;
  LOG(INFO) << "project opt: " << project_opt;
  LOG(INFO) << "Finish project code generation";
  delete project_op_pb;
  delete meta_data;
}

void test_getv_op() {
  LOG(INFO) << "-----------test_getv_op----------";
  gs::BuildingContext ctx;

  auto getv_op_pb = new physical::GetV();
  gs::make_getv_op_pb(*getv_op_pb);
  auto meta_data = new physical::PhysicalOpr::MetaData();
  // std::string expr_code, getv_opt, getv_code;
  // std::tie(expr_code, getv_opt, getv_code) =
  auto res = gs::BuildGetVOp<uint8_t>(ctx, *getv_op_pb, *meta_data);
  LOG(INFO) << "expr code" << res[0];
  LOG(INFO) << "Generated getv op code: " << res[1];
  LOG(INFO) << "getv opt: " << res[2];
  LOG(INFO) << "Finish getv code generation";
  delete getv_op_pb;
  delete meta_data;
}

void test_path_expand_op() {
  LOG(INFO) << "-----------test_path_expand_op----------";
  gs::BuildingContext ctx;

  auto path_expand_op_pb = new physical::PathExpand();
  google::protobuf::RepeatedPtrField<physical::PhysicalOpr::MetaData> meta_data;
  gs::make_path_expand_op_pb(*path_expand_op_pb, meta_data, -1, 0, 1, 2);
  auto res =
      gs::BuildPathExpandOp<uint8_t>(ctx, *path_expand_op_pb, meta_data, 1);
  LOG(INFO) << "expr code" << res[0];
  LOG(INFO) << "Generated path_expand op code: " << res[1];
  LOG(INFO) << "path_expand opt: " << res[2];
  LOG(INFO) << "Finish path_expand code generation";
  delete path_expand_op_pb;
}

void test_group_count_op() {
  LOG(INFO) << "-----------test_group_count----------";
  gs::BuildingContext ctx;

  auto group_count_op_pb = new physical::GroupBy();
  gs::make_group_count_op_pb(*group_count_op_pb);
  auto meta_data = new physical::PhysicalOpr::MetaData();

  auto res = gs::BuildGroupByOp(ctx, *group_count_op_pb, *meta_data);
  for (auto v : res) {
    LOG(INFO) << "Generated groupBy op code: " << v;
  }
  LOG(INFO) << "Finish group_count code generation";
}

void test_gen_query() {
  gs::BuildingContext ctx;

  auto query = new physical::PhysicalPlan();
  gs::make_query_pb(*query);
  QueryGenerator<uint8_t> query_generator(ctx, *query);
  auto res = query_generator.GenerateQuery();
  LOG(INFO) << "Generated query code: ";
  LOG(INFO) << res;
}

void test_join_op() {
  LOG(INFO) << "-----------test_join_op----------";
  gs::BuildingContext ctx;

  auto join_op_pb = new physical::Join();
  gs::make_join_op_pb(*join_op_pb);
  auto meta_data = new physical::PhysicalOpr::MetaData();

  auto res = gs::BuildJoinOp<uint8_t>(ctx, *join_op_pb);
  for (auto v : res) {
    LOG(INFO) << "Generated join op code: " << v;
  }
  LOG(INFO) << "Finish join code generation";
}

void test_expr_with_label_key() {
  LOG(INFO) << "-----------test_expr_with_label_key----------";
  gs::BuildingContext ctx;

  auto expr_pb = new common::Expression();
  gs::make_expr_with_label_key(*expr_pb);
  auto meta_data = new physical::PhysicalOpr::MetaData();
  LOG(INFO) << "Finish expr code filling";
  gs::ExprBuilder expr_builder(ctx);
  expr_builder.AddAllExprOpr(expr_pb->operators());
  std::string func_name, func_code;
  std::vector<codegen::ParamConst> func_call_params;
  std::vector<std::string> tag_props;
  common::DataType unused_expr_ret_type;
  std::tie(func_name, func_call_params, tag_props, func_code,
           unused_expr_ret_type) = expr_builder.Build();
  LOG(INFO) << "func_name: " << func_name;
  LOG(INFO) << "func_code: " << func_code;
  for (auto i = 0; i < func_call_params.size(); ++i) {
    LOG(INFO) << "func_call_params: " << i << ", "
              << data_type_2_string(func_call_params[i].type)
              << func_call_params[i].var_name << ", ";
  }

  for (auto i = 0; i < tag_props.size(); ++i) {
    LOG(INFO) << "tag_props: " << i << ", " << tag_props[i];
  }
}

void test_ic7() {
  LOG(INFO) << "-----------test_ic7----------";
  gs::BuildingContext ctx;

  auto query = new physical::PhysicalPlan();
  gs::make_ic7_query_pb(*query, 26388279067534, 1, 2, 3, 8, 0, 9);
  QueryGenerator<uint8_t> query_generator(ctx, *query);
  auto res = query_generator.GenerateQuery();
  LOG(INFO) << "Generated query code: ";
  LOG(INFO) << res;
}

void test_ic3() {
  LOG(INFO) << "-----------test_ic3----------";
  gs::BuildingContext ctx;

  auto query = new physical::PhysicalPlan();
  // the generated plan contains dynamicParams
  gs::make_ic3_query_pb(*query, test::place_label_id, test::person_label_id,
                        test::comment_label_id, test::post_label_id,
                        test::knows_label_id, test::hasCreator_label_id,
                        test::isLocatedIn_label_id, test::isPartOf_label_id);
  QueryGenerator<uint8_t> query_generator(ctx, *query);
  auto res = query_generator.GenerateQuery();
  LOG(INFO) << "Generated query code: ";
  LOG(INFO) << res;
}

void test_ic4() {
  LOG(INFO) << "-----------test_ic4----------";
  gs::BuildingContext ctx;

  auto query = new physical::PhysicalPlan();
  // gs::make_ic4_anti_join_left_plan(*query, 1, 3, 2, 7, 8, 0, 1,
  // 10995116278874,
  //  1338508800000, 1340928000000);
  gs::make_ic4_query_pb(*query, 1, 3, 2, 7, 8, 0, 1, 10995116278874,
                        1338508800000, 1340928000000);
  LOG(INFO) << query->DebugString();
  QueryGenerator<uint8_t> query_generator(ctx, *query);
  auto res = query_generator.GenerateQuery();
  LOG(INFO) << "Generated query code: ";
  LOG(INFO) << res;
}

void test_ic2() {
  LOG(INFO) << "-----------test_ic2----------";
  gs::BuildingContext ctx;

  auto query = new physical::PhysicalPlan();
  gs::make_ic2_query_pb(*query, test::person_label_id, test::post_label_id,
                        test::comment_label_id, test::knows_label_id,
                        test::hasCreator_label_id);
  QueryGenerator<uint8_t> query_generator(ctx, *query);
  auto res = query_generator.GenerateQuery();
  LOG(INFO) << "Generated query code: ";
  LOG(INFO) << res;
}

void test_left_outer_join() {
  LOG(INFO) << "-----------test_left_outer_join----------";
  gs::BuildingContext ctx;
  auto query = new physical::PhysicalPlan();
  gs::make_left_outer_join_query_pb(
      *query, test::person_label_id, test::place_label_id,
      test::organisation_label_id, test::knows_label_id,
      test::isLocatedIn_label_id, test::workAt_label_id);
  QueryGenerator<uint8_t> query_generator(ctx, *query);
  auto res = query_generator.GenerateQuery();
  LOG(INFO) << "Generated query code: ";
  LOG(INFO) << res;
}

void test_case_when() {
  LOG(INFO) << "-----------test_case_when----------";
  gs::BuildingContext ctx;
  auto query = new physical::PhysicalPlan();
  gs::make_case_when_query_pb(*query, test::person_label_id,
                              test::place_label_id, test::organisation_label_id,
                              test::knows_label_id, test::isLocatedIn_label_id,
                              test::workAt_label_id);
  QueryGenerator<uint8_t> query_generator(ctx, *query);
  auto res = query_generator.GenerateQuery();
  LOG(INFO) << "Generated query code: ";
  LOG(INFO) << res;
}

void test_agg_first() { LOG(FATAL) << "Not implemented"; }

}  // namespace gs

int main(int argc, char** argv) {
  LOG(INFO) << "Finished";

  // gs::test_get_oid_from_expr();
  // gs::test_get_oid_param_from_expr();
  // gs::test_generate_scan_operator();
  // gs::test_generate_edge_expand_v_operator_with_single_label();
  // gs::test_generate_edge_expand_v_operator_two_label();
  // gs::test_generate_edge_expand_e_operator_one_label();

  // gs::test_expr_gen();
  // gs::test_expr_gen_with_params();
  // gs::test_select_op();

  // gs::test_sort_op();

  // gs::test_dedup_op();

  // gs::test_project_op();

  // gs::test_getv_op();

  // gs::test_path_expand_op();

  // gs::test_group_count_op();

  // gs::test_gen_query();

  // gs::test_join_op();

  // gs::test_expr_with_label_key();

  // gs::test_ic3();

  // gs::test_ic4();
  // gs::test_ic2();

  // gs::test_left_outer_join();

  gs::test_case_when();
  // gs::test_agg_first();

  return 0;
}