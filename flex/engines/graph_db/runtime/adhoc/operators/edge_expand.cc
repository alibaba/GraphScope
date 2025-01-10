/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flex/engines/graph_db/runtime/common/operators/retrieve/edge_expand.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/special_predicates.h"
#include "flex/engines/graph_db/runtime/adhoc/opr_timer.h"
#include "flex/engines/graph_db/runtime/adhoc/predicates.h"
#include "flex/engines/graph_db/runtime/adhoc/utils.h"
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace runtime {

bool tc_fusable(const physical::EdgeExpand& ee_opr0,
                const physical::GroupBy& group_by_opr,
                const physical::EdgeExpand& ee_opr1,
                const physical::GetV& v_opr1,
                const physical::EdgeExpand& ee_opr2,
                const algebra::Select& select_opr) {
  return true;
}

Context eval_tc(const physical::EdgeExpand& ee_opr0,
                const physical::GroupBy& group_by_opr,
                const physical::EdgeExpand& ee_opr1,
                const physical::GetV& v_opr1,
                const physical::EdgeExpand& ee_opr2,
                const algebra::Select& select_opr,
                const GraphReadInterface& graph, Context&& ctx,
                const std::map<std::string, std::string>& params,
                const physical::PhysicalOpr_MetaData& meta0,
                const physical::PhysicalOpr_MetaData& meta1,
                const physical::PhysicalOpr_MetaData& meta2) {
  CHECK(!ee_opr0.is_optional());
  CHECK(!ee_opr1.is_optional());
  CHECK(!ee_opr2.is_optional());

  int input_tag = -1;
  if (ee_opr0.has_v_tag()) {
    input_tag = ee_opr0.v_tag().value();
  }

  Direction dir0 = parse_direction(ee_opr0.direction());
  Direction dir1 = parse_direction(ee_opr1.direction());
  Direction dir2 = parse_direction(ee_opr2.direction());

  std::shared_ptr<IVertexColumn> input_vertex_list =
      std::dynamic_pointer_cast<IVertexColumn>(ctx.get(input_tag));
  CHECK(input_vertex_list->vertex_column_type() == VertexColumnType::kSingle);
  auto casted_input_vertex_list =
      std::dynamic_pointer_cast<SLVertexColumn>(input_vertex_list);
  label_t input_label = casted_input_vertex_list->label();

  label_t d0_nbr_label, d0_e_label, d1_nbr_label, d1_e_label, d2_nbr_label,
      d2_e_label;
  PropertyType d0_ep, d1_ep, d2_ep;
  {
    auto labels0 = parse_label_triplets(meta0);
    CHECK_EQ(labels0.size(), 1);
    d0_e_label = labels0[0].edge_label;
    if (dir0 == Direction::kOut) {
      CHECK_EQ(labels0[0].src_label, input_label);
      d0_nbr_label = labels0[0].dst_label;
    } else if (dir0 == Direction::kIn) {
      CHECK_EQ(labels0[0].dst_label, input_label);
      d0_nbr_label = labels0[0].src_label;
    } else {
      LOG(FATAL) << "both direction not supported";
    }

    const auto& properties0 = graph.schema().get_edge_properties(
        labels0[0].src_label, labels0[0].dst_label, labels0[0].edge_label);
    if (properties0.empty()) {
      d0_ep = PropertyType::Empty();
    } else {
      CHECK_EQ(1, properties0.size());
      d0_ep = properties0[0];
    }

    auto labels1 = parse_label_triplets(meta1);
    CHECK_EQ(labels1.size(), 1);
    d1_e_label = labels1[0].edge_label;
    if (dir1 == Direction::kOut) {
      CHECK_EQ(labels1[0].src_label, input_label);
      d1_nbr_label = labels1[0].dst_label;
    } else if (dir1 == Direction::kIn) {
      CHECK_EQ(labels1[0].dst_label, input_label);
      d1_nbr_label = labels1[0].src_label;
    } else {
      LOG(FATAL) << "both direction not supported";
    }

    const auto& properties1 = graph.schema().get_edge_properties(
        labels1[0].src_label, labels1[0].dst_label, labels1[0].edge_label);
    if (properties1.empty()) {
      d1_ep = PropertyType::Empty();
    } else {
      CHECK_EQ(1, properties1.size());
      d1_ep = properties1[0];
    }

    auto labels2 = parse_label_triplets(meta2);
    CHECK_EQ(labels2.size(), 1);
    d2_e_label = labels2[0].edge_label;
    if (dir2 == Direction::kOut) {
      CHECK_EQ(labels2[0].src_label, d1_nbr_label);
      d2_nbr_label = labels2[0].dst_label;
    } else if (dir1 == Direction::kIn) {
      CHECK_EQ(labels2[0].dst_label, d1_nbr_label);
      d2_nbr_label = labels2[0].src_label;
    } else {
      LOG(FATAL) << "both direction not supported";
    }

    const auto& properties2 = graph.schema().get_edge_properties(
        labels2[0].src_label, labels2[0].dst_label, labels2[0].edge_label);
    if (properties2.empty()) {
      d2_ep = PropertyType::Empty();
    } else {
      CHECK_EQ(1, properties2.size());
      d2_ep = properties2[0];
    }
  }
  CHECK(d0_ep == PropertyType::Date());
  CHECK(d1_ep == PropertyType::Date());
  CHECK(d2_ep == PropertyType::Empty());
  auto csr0 = (dir0 == Direction::kOut)
                  ? graph.GetOutgoingGraphView<Date>(input_label, d0_nbr_label,
                                                     d0_e_label)
                  : graph.GetIncomingGraphView<Date>(input_label, d0_nbr_label,
                                                     d0_e_label);
  auto csr1 = (dir1 == Direction::kOut)
                  ? graph.GetOutgoingGraphView<Date>(input_label, d1_nbr_label,
                                                     d1_e_label)
                  : graph.GetIncomingGraphView<Date>(input_label, d1_nbr_label,
                                                     d1_e_label);
  auto csr2 = (dir2 == Direction::kOut)
                  ? graph.GetOutgoingGraphView<grape::EmptyType>(
                        d1_nbr_label, d2_nbr_label, d2_e_label)
                  : graph.GetIncomingGraphView<grape::EmptyType>(
                        d1_nbr_label, d2_nbr_label, d2_e_label);

  const algebra::QueryParams& ee_opr0_qp = ee_opr0.params();
  std::string param_name = ee_opr0_qp.predicate().operators(2).param().name();
  std::string param_value = params.at(param_name);

  Date min_date(std::stoll(param_value));

  SLVertexColumnBuilder builder1(d1_nbr_label);
  SLVertexColumnBuilder builder2(d2_nbr_label);
  std::vector<size_t> offsets;

  size_t idx = 0;
  static thread_local GraphReadInterface::vertex_array_t<bool> d0_set;
  static thread_local std::vector<vid_t> d0_vec;

  d0_set.Init(graph.GetVertexSet(d0_nbr_label), false);
  for (auto v : casted_input_vertex_list->vertices()) {
    csr0.foreach_edges_gt(v, min_date, [&](vid_t u, const Date& date) {
      d0_set[u] = true;
      d0_vec.push_back(u);
    });
    for (auto& e1 : csr1.get_edges(v)) {
      auto nbr1 = e1.get_neighbor();
      for (auto& e2 : csr2.get_edges(nbr1)) {
        auto nbr2 = e2.get_neighbor();
        if (d0_set[nbr2]) {
          builder1.push_back_opt(nbr1);
          builder2.push_back_opt(nbr2);
          offsets.push_back(idx);
        }
      }
    }
    for (auto u : d0_vec) {
      d0_set[u] = false;
    }
    d0_vec.clear();
    ++idx;
  }

  int alias1 = -1;
  if (ee_opr1.has_alias()) {
    alias1 = ee_opr1.alias().value();
  }
  if (v_opr1.has_alias()) {
    alias1 = v_opr1.alias().value();
  }
  int alias2 = -1;
  if (ee_opr2.has_alias()) {
    alias2 = ee_opr2.alias().value();
  }

  std::shared_ptr<IContextColumn> col1 = builder1.finish();
  std::shared_ptr<IContextColumn> col2 = builder2.finish();
  ctx.set_with_reshuffle(alias1, col1, offsets);
  ctx.set(alias2, col2);
  return ctx;
}

}  // namespace runtime

}  // namespace gs