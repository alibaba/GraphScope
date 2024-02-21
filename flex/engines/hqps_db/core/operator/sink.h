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
#ifndef ENGINES_HQPS_ENGINE_OPERATOR_SINK_H_
#define ENGINES_HQPS_ENGINE_OPERATOR_SINK_H_

#include <numeric>
#include <queue>
#include <string>

#include "flex/engines/hqps_db/core/context.h"
#include "flex/engines/hqps_db/core/params.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/engines/hqps_db/core/utils/props.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"
#include "flex/engines/hqps_db/structures/multi_edge_set/flat_edge_set.h"
#include "flex/engines/hqps_db/structures/multi_edge_set/general_edge_set.h"
#include "flex/engines/hqps_db/structures/multi_edge_set/untyped_edge_set.h"
#include "flex/engines/hqps_db/structures/path.h"

#include "flex/proto_generated_gie/results.pb.h"

namespace gs {

template <typename T>
size_t SizeOf(const T& t) {
  return sizeof(T);
}

template <typename>
size_t SizeOf(const std::string& t) {
  return t.size();
}

template <typename>
size_t SizeOf(const std::string_view& t) {
  return t.size();
}

template <typename T>
size_t SizeOf(const std::vector<T>& t) {
  return t.size() * sizeof(T);
}

template <size_t Id = 0, typename... T>
size_t SizeOfImpl(const std::tuple<T...>& t) {
  if constexpr (Id < sizeof...(T)) {
    return SizeOf(std::get<Id>(t)) + SizeOfImpl<Id + 1>(t);
  } else {
    return 0;
  }
}

template <typename... T>
size_t SizeOf(const std::tuple<T...>& t) {
  return SizeOfImpl<0>(t);
}

template <size_t Is = 0, typename... T>
void SumEleSize(std::tuple<T...>& t, size_t& size) {
  if constexpr (Is < sizeof...(T)) {
    size += SizeOf(std::get<Is>(t));
    if constexpr (Is + 1 < sizeof...(T)) {
      SumEleSize<Is + 1>(t, size);
    }
  }
}

template <typename T, typename std::enable_if<
                          (std::is_same_v<T, int32_t>)>::type* = nullptr>
void template_set_value(common::Value* value, T v) {
  value->set_i32(v);
}
template <typename T, typename std::enable_if<
                          (std::is_same_v<T, uint32_t>)>::type* = nullptr>
void template_set_value(common::Value* value, T v) {
  value->set_i32(v);
}

template <typename T,
          typename std::enable_if<(std::is_same_v<T, bool>)>::type* = nullptr>
void template_set_value(common::Value* value, T v) {
  value->set_boolean(v);
}

template <typename T, typename std::enable_if<
                          (std::is_same_v<T, unsigned long>)>::type* = nullptr>
void template_set_value(common::Value* value, T v) {
  if constexpr (sizeof(T) == 8) {
    value->set_i64(v);
  } else {
    value->set_i32(v);
  }
}

template <typename T,
          typename std::enable_if<(std::is_same_v<T, uint64_t>) &&(
              !std::is_same_v<uint64_t, unsigned long>)>::type* = nullptr>
void template_set_value(common::Value* value, T v) {
  value->set_i64(v);
}

template <typename T, typename std::enable_if<
                          (std::is_same_v<T, int64_t>)>::type* = nullptr>
void template_set_value(common::Value* value, T v) {
  value->set_i64(v);
}

template <typename T,
          typename std::enable_if<
              (std::is_same_v<T, std::string_view>)>::type* = nullptr>
void template_set_value(common::Value* value, T v) {
  value->mutable_str()->assign(v.data(), v.size());
}

template <typename T, typename std::enable_if<
                          (std::is_same_v<T, std::string>)>::type* = nullptr>
void template_set_value(common::Value* value, T v) {
  value->set_str(v.data(), v.size());
}

template <typename T,
          typename std::enable_if<(std::is_same_v<T, double>)>::type* = nullptr>
void template_set_value(common::Value* value, T v) {
  value->set_f64(v);
}

template <typename T, typename std::enable_if<
                          (std::is_same_v<T, gs::Date>)>::type* = nullptr>
void template_set_value(common::Value* value, gs::Date v) {
  value->set_i64(v.milli_second);
}

template <size_t Is = 0, typename... T>
void template_set_tuple_value_impl(results::Collection* collection,
                                   const std::tuple<T...>& t) {
  if constexpr (Is < sizeof...(T)) {
    auto cur_ele = collection->add_collection()->mutable_object();
    template_set_value(cur_ele, std::get<Is>(t));
    if constexpr (Is + 1 < sizeof...(T)) {
      template_set_tuple_value_impl<Is + 1>(collection, t);
    }
  }
}

template <typename... T>
void template_set_tuple_value(results::Collection* collection,
                              const std::tuple<T...>& t) {
  template_set_tuple_value_impl(collection, t);
}

template <typename T>
void template_set_tuple_value(results::Collection* collection,
                              const std::vector<T>& t) {
  for (size_t i = 0; i < t.size(); ++i) {
    auto cur_ele = collection->add_collection()->mutable_object();
    // if is tuple
    if constexpr (gs::is_tuple<T>::value) {
      LOG(WARNING) << "PLEASE FIXME: tuple in vector is not supported "
                      "yet.";
      template_set_value(cur_ele, gs::to_string(t[i]));
    } else {
      template_set_value(cur_ele, t[i]);
    }
  }
}

/////Sink Any

void set_any_to_common_value(const Any& any, common::Value* value) {
  if (any.type == PropertyType::Bool()) {
    value->set_boolean(any.value.b);
  } else if (any.type == PropertyType::Int32()) {
    value->set_i32(any.value.i);
  } else if (any.type == PropertyType::UInt32()) {
    // FIXME(zhanglei): temporarily use i32, fix this after common.proto is
    // changed
    value->set_i32(any.value.ui);
  } else if (any.type == PropertyType::Int64()) {
    value->set_i64(any.value.l);
  } else if (any.type == PropertyType::UInt64()) {
    // FIXME(zhanglei): temporarily use i64, fix this after common.proto is
    // changed
    value->set_i64(any.value.ul);
  } else if (any.type == PropertyType::Double()) {
    value->set_f64(any.value.db);
  } else if (any.type == PropertyType::Float()) {
    value->set_f64(any.value.f);
  } else if (any.type == PropertyType::Date()) {
    value->set_i64(any.value.d.milli_second);
  } else if (any.type == PropertyType::String()) {
    value->mutable_str()->assign(any.value.s.data(), any.value.s.size());
  } else {
    LOG(WARNING) << "Unexpected property type: "
                 << static_cast<int>(any.type.type_enum);
  }
}

// set edge value
void set_edge_property(results::Edge* edge, const std::string& prop_name,
                       const std::tuple<int32_t>& value) {
  auto prop = edge->add_properties();
  prop->mutable_value()->set_i64(std::get<0>(value));
  prop->mutable_key()->set_name(prop_name);
}

void set_edge_property(results::Edge* edge, const std::string& prop_name,
                       const std::tuple<int64_t>& value) {
  auto prop = edge->add_properties();
  prop->mutable_value()->set_i64(std::get<0>(value));
  prop->mutable_key()->set_name(prop_name);
}

// set double
void set_edge_property(results::Edge* edge, const std::string& prop_name,
                       const std::tuple<double>& value) {
  auto prop = edge->add_properties();
  prop->mutable_value()->set_f64(std::get<0>(value));
  prop->mutable_key()->set_name(prop_name);
}
// set date
void set_edge_property(results::Edge* edge, const std::string& prop_name,
                       const std::tuple<gs::Date>& value) {
  auto prop = edge->add_properties();
  prop->mutable_value()->set_i64(std::get<0>(value).milli_second);
  prop->mutable_key()->set_name(prop_name);
}

// set string
void set_edge_property(results::Edge* edge, const std::string& prop_name,
                       const std::tuple<std::string_view>& value) {
  auto prop = edge->add_properties();
  prop->mutable_value()->mutable_str()->assign(std::get<0>(value).data(),
                                               std::get<0>(value).size());
  prop->mutable_key()->set_name(prop_name);
}

// set grape::EmptyType
void set_edge_property(results::Edge* edge, const std::string& prop_name,
                       const std::tuple<grape::EmptyType>& value) {
  // just skip
}

void set_edge_property(results::Edge* edge, const std::string& prop_name,
                       const Any& value) {
  if (value.type == PropertyType::kEmpty) {
    return;
  }
  auto prop = edge->add_properties();
  set_any_to_common_value(value, prop->mutable_value());
  prop->mutable_key()->set_name(prop_name);
}

template <typename GRAPH_INTERFACE>
class SinkOp {
 public:
  using vid_t = typename GRAPH_INTERFACE::vertex_id_t;
  using label_id_t = typename GRAPH_INTERFACE::label_id_t;
  // sink current context to results_pb defined in results.proto
  // return results::CollectiveResults
  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV_T>
  static results::CollectiveResults Sink(
      const GRAPH_INTERFACE& graph,
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV_T...>& ctx,
      std::array<int32_t, Context<CTX_HEAD_T, cur_alias, base_tag,
                                  CTX_PREV_T...>::col_num>
          tag_ids) {
    // prepare enough record rows.
    auto size = ctx.GetHead().Size();
    // std::vector<results::Results> results_vec(size);
    results::CollectiveResults results_vec;
    for (size_t i = 0; i < size; ++i) {
      results_vec.add_results();
    }
    LOG(INFO) << "reserve " << size << " records";
    sink_column<0>(graph, results_vec, ctx, tag_ids);
    sink_head(graph, results_vec, ctx, tag_ids[tag_ids.size() - 1]);
    return results_vec;
  }

  template <
      size_t I, typename CTX_T,
      typename std::enable_if<(I >= CTX_T::prev_alias_num)>::type* = nullptr>
  static void sink_column(const GRAPH_INTERFACE& graph,
                          results::CollectiveResults& record, CTX_T& ctx,
                          const std::array<int32_t, CTX_T::col_num>& tag_ids) {
    LOG(INFO) << "no prev columns to sink";
  }

  template <
      size_t I, typename CTX_T,
      typename std::enable_if<(I < CTX_T::prev_alias_num)>::type* = nullptr>
  static void sink_column(const GRAPH_INTERFACE& graph,
                          results::CollectiveResults& record, CTX_T& ctx,
                          const std::array<int32_t, CTX_T::col_num>& tag_ids) {
    if constexpr (I < CTX_T::prev_alias_num) {
      LOG(INFO) << "Projecting col: " << I;
      static constexpr size_t act_tag_id = CTX_T::base_tag_id + I;
      auto offset_array = ctx.ObtainOffsetFromTag(act_tag_id);
      auto repeat_array = offset_array_to_repeat_array(std::move(offset_array));
      sink_col_impl<I, act_tag_id>(graph, record,
                                   ctx.template GetNode<act_tag_id>(),
                                   repeat_array, tag_ids[I]);
    }
    if constexpr (I + 1 < CTX_T::prev_alias_num) {
      sink_column<I + 1>(graph, record, ctx, tag_ids);
    }
  }

  template <typename CTX_T>
  static void sink_head(const GRAPH_INTERFACE& graph,
                        results::CollectiveResults& record, CTX_T& ctx,
                        int32_t tag_id) {
    auto& head = ctx.GetHead();
    sink_col_impl<CTX_T::prev_alias_num, CTX_T::max_tag_id>(graph, record, head,
                                                            {}, tag_id);
  }

  template <size_t Ind, size_t act_tag_id, typename LabelT,
            typename vertex_id_t, typename... T>
  static void sink_col_impl(
      const GRAPH_INTERFACE& graph, results::CollectiveResults& results_vec,
      const RowVertexSetImpl<LabelT, vertex_id_t, T...>& vertex_set,
      const std::vector<size_t>& repeat_offsets, int32_t tag_id) {
    auto label = vertex_set.GetLabel();
    auto& vids = vertex_set.GetVertices();
    return sink_col_impl_for_vertex_set<Ind, act_tag_id>(
        graph, label, vids, results_vec, repeat_offsets, tag_id);
  }

  template <size_t Ind, size_t act_tag_id, typename LabelT, typename KEY_T,
            typename vertex_id_t>
  static void sink_col_impl(
      const GRAPH_INTERFACE& graph, results::CollectiveResults& results_vec,
      const KeyedRowVertexSetImpl<LabelT, KEY_T, vertex_id_t, grape::EmptyType>&
          vertex_set,
      const std::vector<size_t>& repeat_offsets, int32_t tag_id) {
    auto label = vertex_set.GetLabel();
    auto& vids = vertex_set.GetVertices();
    return sink_col_impl_for_vertex_set<Ind, act_tag_id>(
        graph, label, vids, results_vec, repeat_offsets, tag_id);
  }

  // Sink two label vertex set, currently only print the id
  template <size_t Ind, size_t act_tag_id, typename LabelT,
            typename vertex_id_t>
  static void sink_col_impl(
      const GRAPH_INTERFACE& graph, results::CollectiveResults& results_vec,
      const TwoLabelVertexSetImpl<vertex_id_t, LabelT, grape::EmptyType>&
          vertex_set,
      const std::vector<size_t>& repeat_offsets, int32_t tag_id) {
    auto labels = vertex_set.GetLabels();
    auto& vids = vertex_set.GetVertices();
    auto& bitset = vertex_set.GetBitset();
    // get all property for two labels vertex
    auto& schema = graph.schema();
    std::array<std::vector<std::string>, 2> prop_names;
    prop_names[0] = schema.get_vertex_property_names(labels[0]);
    prop_names[1] = schema.get_vertex_property_names(labels[1]);
    // get all properties
    std::array<std::vector<std::shared_ptr<RefColumnBase>>, 2> column_ptrs;
    for (size_t i = 0; i < prop_names[0].size(); ++i) {
      column_ptrs[0].emplace_back(
          graph.GetRefColumnBase(labels[0], prop_names[0][i]));
    }
    for (size_t i = 0; i < prop_names[1].size(); ++i) {
      column_ptrs[1].emplace_back(
          graph.GetRefColumnBase(labels[1], prop_names[1][i]));
    }

    label_t label;
    if (repeat_offsets.empty()) {
      for (size_t i = 0; i < vids.size(); ++i) {
        auto row = results_vec.mutable_results(i);
        CHECK(row->record().columns_size() == Ind);
        auto record = row->mutable_record();
        auto new_col = record->add_columns();
        new_col->mutable_name_or_id()->set_id(tag_id);
        auto vertex =
            new_col->mutable_entry()->mutable_element()->mutable_vertex();
        if (bitset.get_bit(i)) {
          label = labels[0];
        } else {
          label = labels[1];
        }
        vertex->mutable_label()->set_id(label);
        vertex->set_id(encode_unique_vertex_id(label, vids[i]));
        // set properties.
        auto columns = column_ptrs[label];
        for (size_t j = 0; j < columns.size(); ++j) {
          auto& column_ptr = columns[j];
          // Only set non-none properties.
          if (column_ptr) {
            auto new_prop = vertex->add_properties();
            new_prop->mutable_key()->set_name(prop_names[label][j]);
            set_any_to_common_value(column_ptr->get(vids[i]),
                                    new_prop->mutable_value());
          }
        }
      }
    } else {
      CHECK(repeat_offsets.size() == vids.size());
      {
        size_t num_rows = 0;
        for (size_t i : repeat_offsets) {
          num_rows += i;
        }
        CHECK(num_rows == results_vec.results_size())
            << num_rows << " " << results_vec.results_size();
      }
      size_t cur_ind = 0;
      for (size_t i = 0; i < vids.size(); ++i) {
        if (bitset.get_bit(i)) {
          label = labels[0];
        } else {
          label = labels[1];
        }
        for (size_t j = 0; j < repeat_offsets[i]; ++j) {
          auto row = results_vec.mutable_results(cur_ind++);
          auto record = row->mutable_record();
          auto new_col = record->add_columns();

          new_col->mutable_name_or_id()->set_id(tag_id);
          auto vertex =
              new_col->mutable_entry()->mutable_element()->mutable_vertex();
          vertex->set_id(encode_unique_vertex_id(label, vids[i]));
          vertex->mutable_label()->set_id(label);
          // set properties.
          auto columns = column_ptrs[label];
          for (size_t j = 0; j < columns.size(); ++j) {
            auto& column_ptr = columns[j];
            // Only set non-none properties.
            if (column_ptr) {
              auto new_prop = vertex->add_properties();
              new_prop->mutable_key()->set_name(prop_names[label][j]);
              set_any_to_common_value(column_ptr->get(vids[i]),
                                      new_prop->mutable_value());
            }
          }
        }
      }
    }
  }

  // sink row vertex set, if offsets is empty, we sink all vertices
  // if offsets is set, we use offset to repeat
  template <size_t Ind, size_t act_tag_id, typename LabelT,
            typename vertex_id_t>
  static void sink_col_impl_for_vertex_set(
      const GRAPH_INTERFACE& graph, LabelT label,
      const std::vector<vertex_id_t>& vids,
      results::CollectiveResults& results_vec,
      const std::vector<size_t>& repeat_offsets, int32_t tag_id) {
    auto& schema = graph.schema();
    auto prop_names = schema.get_vertex_property_names(label);
    // get all properties
    std::vector<std::shared_ptr<RefColumnBase>> column_ptrs;
    for (size_t i = 0; i < prop_names.size(); ++i) {
      column_ptrs.emplace_back(graph.GetRefColumnBase(label, prop_names[i]));
    }
    VLOG(10) << "PropNames: " << prop_names.size();
    if (repeat_offsets.empty()) {
      for (size_t i = 0; i < vids.size(); ++i) {
        auto row = results_vec.mutable_results(i);
        CHECK(row->record().columns_size() == Ind);
        auto record = row->mutable_record();
        auto new_col = record->add_columns();
        new_col->mutable_name_or_id()->set_id(tag_id);
        auto vertex =
            new_col->mutable_entry()->mutable_element()->mutable_vertex();
        vertex->set_id(encode_unique_vertex_id(label, vids[i]));
        vertex->mutable_label()->set_id(label);
        for (size_t j = 0; j < column_ptrs.size(); ++j) {
          auto& column_ptr = column_ptrs[j];
          // Only set non-none properties.
          if (column_ptr) {
            auto new_prop = vertex->add_properties();
            new_prop->mutable_key()->set_name(prop_names[j]);
            set_any_to_common_value(column_ptr->get(vids[i]),
                                    new_prop->mutable_value());
          }
        }
      }
    } else {
      CHECK(repeat_offsets.size() == vids.size());
      {
        int32_t num_rows = 0;
        for (size_t i : repeat_offsets) {
          num_rows += i;
        }
        CHECK(num_rows == results_vec.results_size())
            << num_rows << " " << results_vec.results_size();
      }
      size_t cur_ind = 0;
      for (size_t i = 0; i < vids.size(); ++i) {
        for (size_t j = 0; j < repeat_offsets[i]; ++j) {
          auto row = results_vec.mutable_results(cur_ind++);
          auto record = row->mutable_record();
          auto new_col = record->add_columns();
          new_col->mutable_name_or_id()->set_id(tag_id);
          auto vertex =
              new_col->mutable_entry()->mutable_element()->mutable_vertex();
          vertex->set_id(encode_unique_vertex_id(label, vids[i]));
          vertex->mutable_label()->set_id(label);
          for (size_t j = 0; j < column_ptrs.size(); ++j) {
            auto& column_ptr = column_ptrs[j];
            // Only set non-none properties.
            if (column_ptr) {
              auto new_prop = vertex->add_properties();
              new_prop->mutable_key()->set_name(prop_names[j]);
              set_any_to_common_value(column_ptr->get(vids[i]),
                                      new_prop->mutable_value());
            }
          }
        }
      }
    }
  }

  // sink collection of pod type
  template <size_t Ind, size_t act_tag_id, typename T,
            typename std::enable_if<
                (!gs::is_vector<T>::value) && (!gs::is_tuple<T>::value) &&
                (!std::is_same<T, LabelKey>::value)>::type* = nullptr>
  static void sink_col_impl(const GRAPH_INTERFACE& graph,
                            results::CollectiveResults& results_vec,
                            const Collection<T>& collection,
                            const std::vector<size_t>& repeat_offsets,
                            int32_t tag_id) {
    if (repeat_offsets.empty()) {
      CHECK((int32_t) collection.Size() == results_vec.results_size())
          << "size neq " << collection.Size() << " "
          << results_vec.results_size();
      for (size_t i = 0; i < collection.Size(); ++i) {
        auto row = results_vec.mutable_results(i);
        CHECK(row->record().columns_size() == Ind);
        auto record = row->mutable_record();
        auto new_col = record->add_columns();
        new_col->mutable_name_or_id()->set_id(tag_id);
        auto common_value_ptr =
            new_col->mutable_entry()->mutable_element()->mutable_object();
        template_set_value<T>(common_value_ptr, collection.Get(i));
      }
    } else {
      CHECK(repeat_offsets.size() == collection.Size());
      size_t cur_ind = 0;
      for (size_t i = 0; i < collection.Size(); ++i) {
        for (size_t j = 0; j < repeat_offsets[i]; ++j) {
          auto row = results_vec.mutable_results(cur_ind++);
          auto record = row->mutable_record();
          auto new_col = record->add_columns();
          new_col->mutable_name_or_id()->set_id(tag_id);
          auto common_value_ptr =
              new_col->mutable_entry()->mutable_element()->mutable_object();
          template_set_value<T>(common_value_ptr, collection.Get(i));
        }
      }
    }
  }

  // sink collection of LabelKey
  template <size_t Ind, size_t act_tag_id>
  static void sink_col_impl(const GRAPH_INTERFACE& graph,
                            results::CollectiveResults& results_vec,
                            const Collection<LabelKey>& collection,
                            const std::vector<size_t>& repeat_offsets,
                            int32_t tag_id) {
    if (repeat_offsets.empty()) {
      CHECK((int32_t) collection.Size() == results_vec.results_size())
          << "size neq " << collection.Size() << " "
          << results_vec.results_size();
      for (size_t i = 0; i < collection.Size(); ++i) {
        auto row = results_vec.mutable_results(i);
        CHECK(row->record().columns_size() == Ind);
        auto record = row->mutable_record();
        auto new_col = record->add_columns();
        new_col->mutable_name_or_id()->set_id(tag_id);
        auto obj =
            new_col->mutable_entry()->mutable_element()->mutable_object();
        obj->set_i32(collection.Get(i).label_id);
      }
    } else {
      CHECK(repeat_offsets.size() == collection.Size());
      size_t cur_ind = 0;
      for (size_t i = 0; i < collection.Size(); ++i) {
        for (size_t j = 0; j < repeat_offsets[i]; ++j) {
          auto row = results_vec.mutable_results(cur_ind++);
          auto record = row->mutable_record();
          auto new_col = record->add_columns();
          new_col->mutable_name_or_id()->set_id(tag_id);
          auto obj =
              new_col->mutable_entry()->mutable_element()->mutable_object();
          obj->set_i32(collection.Get(i).label_id);
        }
      }
    }
  }

  // sink for tuple with one element
  template <size_t Ind, size_t act_tag_id, typename T,
            typename std::enable_if<
                (!gs::is_vector<T>::value) && (gs::is_tuple<T>::value) &&
                (gs::tuple_size<T>::value == 1)>::type* = nullptr>
  static void sink_col_impl(const GRAPH_INTERFACE& graph,
                            results::CollectiveResults& results_vec,
                            const Collection<T>& collection,
                            const std::vector<size_t>& repeat_offsets,
                            int32_t tag_id) {
    if (repeat_offsets.empty()) {
      CHECK(collection.Size() == results_vec.results_size())
          << "size neq " << collection.Size() << " "
          << results_vec.results_size();
      for (size_t i = 0; i < collection.Size(); ++i) {
        auto row = results_vec.mutable_results(i);
        CHECK(row->record().columns_size() == Ind)
            << "record column size: " << row->record().columns_size()
            << ", ind: " << Ind;
        auto record = row->mutable_record();
        auto new_col = record->add_columns();
        new_col->mutable_name_or_id()->set_id(tag_id);
        auto mutable_ele =
            new_col->mutable_entry()->mutable_element()->mutable_object();
        template_set_value<typename std::tuple_element_t<0, T>>(
            mutable_ele, std::get<0>(collection.Get(i)));
      }
    } else {
      CHECK(repeat_offsets.size() == collection.Size());
      size_t cur_ind = 0;
      for (size_t i = 0; i < collection.Size(); ++i) {
        for (size_t j = 0; j < repeat_offsets[i]; ++j) {
          auto row = results_vec.mutable_results(cur_ind++);
          auto record = row->mutable_record();
          auto new_col = record->add_columns();
          new_col->mutable_name_or_id()->set_id(tag_id);
          auto mutable_ele =
              new_col->mutable_entry()->mutable_element()->mutable_object();
          template_set_value<typename std::tuple_element_t<0, T>>(
              mutable_ele, std::get<0>(collection.Get(i)));
        }
      }
    }
  }

  // sink for tuple, with more than one element
  template <size_t Ind, size_t act_tag_id, typename T,
            typename std::enable_if<
                (!gs::is_vector<T>::value) && (gs::is_tuple<T>::value) &&
                ((gs::tuple_size<T>::value) > 1)>::type* = nullptr>
  static void sink_col_impl(const GRAPH_INTERFACE& graph,
                            results::CollectiveResults& results_vec,
                            const Collection<T>& collection,
                            const std::vector<size_t>& repeat_offsets,
                            int32_t tag_id) {
    if (repeat_offsets.empty()) {
      CHECK(collection.Size() == results_vec.results_size())
          << "size neq " << collection.Size() << " "
          << results_vec.results_size();
      for (size_t i = 0; i < collection.Size(); ++i) {
        auto row = results_vec.mutable_results(i);
        CHECK(row->record().columns_size() == Ind)
            << "record column size: " << row->record().columns_size()
            << ", ind: " << Ind;
        auto record = row->mutable_record();
        auto new_col = record->add_columns();
        new_col->mutable_name_or_id()->set_id(tag_id);
        auto mutable_collection =
            new_col->mutable_entry()->mutable_collection();
        template_set_tuple_value(mutable_collection, collection.Get(i));
      }
    } else {
      CHECK(repeat_offsets.size() == collection.Size());
      size_t cur_ind = 0;
      for (size_t i = 0; i < collection.Size(); ++i) {
        for (size_t j = 0; j < repeat_offsets[i]; ++j) {
          auto row = results_vec.mutable_results(cur_ind++);
          auto record = row->mutable_record();
          auto new_col = record->add_columns();
          new_col->mutable_name_or_id()->set_id(tag_id);
          auto mutable_collection =
              new_col->mutable_entry()->mutable_collection();
          template_set_tuple_value(mutable_collection, collection.Get(i));
        }
      }
    }
  }

  // sink for collection of vector.
  template <size_t Ind, size_t act_tag_id, typename T,
            typename std::enable_if<(gs::is_vector<T>::value)>::type* = nullptr>
  static void sink_col_impl(const GRAPH_INTERFACE& graph,
                            results::CollectiveResults& results_vec,
                            const Collection<T>& collection,
                            const std::vector<size_t>& repeat_offsets,
                            int32_t tag_id) {
    if (repeat_offsets.empty()) {
      CHECK(collection.Size() == results_vec.results_size())
          << "size neq " << collection.Size() << " "
          << results_vec.results_size();
      for (size_t i = 0; i < collection.Size(); ++i) {
        // auto& row = results_vec[i];
        auto row = results_vec.mutable_results(i);
        CHECK(row->record().columns_size() == Ind)
            << "record column size: " << row->record().columns_size()
            << ", ind: " << Ind;
        auto record = row->mutable_record();
        auto new_col = record->add_columns();
        new_col->mutable_name_or_id()->set_id(tag_id);
        auto mutable_collection =
            new_col->mutable_entry()->mutable_collection();
        template_set_tuple_value(mutable_collection, collection.Get(i));
      }
    } else {
      CHECK(repeat_offsets.size() == collection.Size());
      size_t cur_ind = 0;
      for (size_t i = 0; i < collection.Size(); ++i) {
        for (size_t j = 0; j < repeat_offsets[i]; ++j) {
          auto row = results_vec.mutable_results(cur_ind++);
          auto record = row->mutable_record();
          auto new_col = record->add_columns();
          new_col->mutable_name_or_id()->set_id(tag_id);
          auto mutable_collection =
              new_col->mutable_entry()->mutable_collection();
          template_set_tuple_value(mutable_collection, collection.Get(i));
        }
      }
    }
  }

  // sink general vertex, we only return vertex ids.
  template <size_t Ind, size_t act_tag_id, typename VID_T, typename LabelT,
            typename... SET_T>
  static void sink_col_impl(
      const GRAPH_INTERFACE& graph, results::CollectiveResults& results_vec,
      const GeneralVertexSet<VID_T, LabelT, SET_T...>& vertex_set,
      const std::vector<size_t>& repeat_offsets, int32_t tag_id) {
    auto& schema = graph.schema();
    auto vertices_vec = vertex_set.GetVertices();
    auto labels_vec = vertex_set.GetLabels();
    auto& bitsets = vertex_set.GetBitsets();
    CHECK(bitsets.size() == labels_vec.size());
    std::vector<std::vector<std::string>> prop_names;
    for (size_t i = 0; i < labels_vec.size(); ++i) {
      prop_names.emplace_back(schema.get_vertex_property_names(labels_vec[i]));
    }
    // get all properties
    std::vector<std::vector<std::shared_ptr<RefColumnBase>>> column_ptrs(
        labels_vec.size());
    if (labels_vec.size() > 0) {
      for (size_t i = 0; i < labels_vec.size(); ++i) {
        for (size_t j = 0; j < prop_names[i].size(); ++j) {
          column_ptrs[i].emplace_back(
              graph.GetRefColumnBase(labels_vec[i], prop_names[i][j]));
        }
      }
    }

    label_t label;
    size_t label_vec_ind;
    if (repeat_offsets.empty()) {
      CHECK(vertex_set.Size() == results_vec.results_size())
          << "size neq " << vertex_set.Size() << " "
          << results_vec.results_size();

      for (size_t i = 0; i < vertices_vec.size(); ++i) {
        // auto& row = results_vec[i];
        auto row = results_vec.mutable_results(i);
        CHECK(row->record().columns_size() == Ind)
            << "record column size: " << row->record().columns_size()
            << ", ind: " << Ind;
        auto record = row->mutable_record();
        auto new_col = record->add_columns();
        new_col->mutable_name_or_id()->set_id(tag_id);
        auto mutable_vertex =
            new_col->mutable_entry()->mutable_element()->mutable_vertex();
        // todo: set properties.
        for (size_t j = 0; j < bitsets.size(); ++j) {
          if (bitsets[j].get_bit(i)) {
            label = labels_vec[j];
            label_vec_ind = j;
            break;
          }
        }
        mutable_vertex->mutable_label()->set_id(label);
        mutable_vertex->set_id(encode_unique_vertex_id(label, vertices_vec[i]));
        // label must be set
        auto cur_column_ptrs = column_ptrs[label_vec_ind];
        for (size_t j = 0; j < cur_column_ptrs.size(); ++j) {
          auto& column_ptr = cur_column_ptrs[j];
          // Only set non-none properties.
          if (column_ptr) {
            auto new_prop = mutable_vertex->add_properties();
            new_prop->mutable_key()->set_name(prop_names[label_vec_ind][j]);
            set_any_to_common_value(column_ptr->get(vertices_vec[i]),
                                    new_prop->mutable_value());
          }
        }
      }
    } else {
      CHECK(repeat_offsets.size() == vertices_vec.size());
      size_t cur_ind = 0;
      for (size_t i = 0; i < vertices_vec.size(); ++i) {
        for (size_t j = 0; j < repeat_offsets[i]; ++j) {
          auto row = results_vec.mutable_results(cur_ind++);
          auto record = row->mutable_record();
          auto new_col = record->add_columns();
          new_col->mutable_name_or_id()->set_id(tag_id);
          auto mutable_vertex =
              new_col->mutable_entry()->mutable_element()->mutable_vertex();
          for (size_t j = 0; j < bitsets.size(); ++j) {
            if (bitsets[j].get_bit(i)) {
              label = labels_vec[j];
              label_vec_ind = j;
              break;
            }
          }
          mutable_vertex->mutable_label()->set_id(label);
          mutable_vertex->set_id(
              encode_unique_vertex_id(label, vertices_vec[i]));
          // label must be set
          auto cur_column_ptrs = column_ptrs[label_vec_ind];
          for (size_t j = 0; j < cur_column_ptrs.size(); ++j) {
            auto& column_ptr = cur_column_ptrs[j];
            // Only set non-none properties.
            if (column_ptr) {
              auto new_prop = mutable_vertex->add_properties();
              new_prop->mutable_key()->set_name(prop_names[label_vec_ind][j]);
              set_any_to_common_value(column_ptr->get(vertices_vec[i]),
                                      new_prop->mutable_value());
            }
          }
        }
      }
    }
  }

  template <size_t Ind, size_t act_tag_id, typename EDGE_SET_T,
            typename std::enable_if<EDGE_SET_T::is_edge_set>::type* = nullptr>
  static void sink_col_impl(const GRAPH_INTERFACE& graph,
                            results::CollectiveResults& results_vec,
                            const EDGE_SET_T& edge_set,
                            const std::vector<size_t>& repeat_offsets,
                            int32_t tag_id) {
    if (repeat_offsets.empty()) {
      CHECK(edge_set.Size() == results_vec.results_size())
          << "size neq " << edge_set.Size() << " "
          << results_vec.results_size();
      auto iter = edge_set.begin();
      auto end_iter = edge_set.end();
      for (size_t i = 0; i < results_vec.results_size(); ++i) {
        auto row = results_vec.mutable_results(i);
        CHECK(row->record().columns_size() == Ind)
            << "record column size: " << row->record().columns_size()
            << ", ind: " << Ind;
        auto record = row->mutable_record();
        auto new_col = record->add_columns();
        new_col->mutable_name_or_id()->set_id(tag_id);
        auto mutable_edge =
            new_col->mutable_entry()->mutable_element()->mutable_edge();
        CHECK(iter != end_iter);
        auto unique_edge_label = generate_edge_label_id(
            iter.GetSrcLabel(), iter.GetDstLabel(), iter.GetEdgeLabel());
        mutable_edge->mutable_label()->set_id(unique_edge_label);
        mutable_edge->set_id(encode_unique_edge_id(unique_edge_label, i));
        mutable_edge->set_src_id(
            encode_unique_vertex_id(iter.GetSrcLabel(), iter.GetSrc()));
        mutable_edge->mutable_src_label()->set_id(iter.GetSrcLabel());
        mutable_edge->set_dst_id(
            encode_unique_vertex_id(iter.GetDstLabel(), iter.GetDst()));
        mutable_edge->mutable_dst_label()->set_id(iter.GetDstLabel());

        auto prop_names = iter.GetPropNames();
        if (prop_names.size() > 0) {
          set_edge_property(mutable_edge, prop_names[0], iter.GetData());
        }
        ++iter;
        // todo: set properties.
      }
    } else {
      CHECK(repeat_offsets.size() == edge_set.Size());
      size_t cur_ind = 0;
      auto iter = edge_set.begin();
      auto end_iter = edge_set.end();
      for (size_t i = 0; i < repeat_offsets.size(); ++i) {
        CHECK(iter != end_iter);
        for (size_t j = 0; j < repeat_offsets[i]; ++j) {
          auto row = results_vec.mutable_results(cur_ind++);
          CHECK(row->record().columns_size() == Ind)
              << "record column size: " << row->record().columns_size()
              << ", ind: " << Ind;
          auto record = row->mutable_record();
          auto new_col = record->add_columns();
          new_col->mutable_name_or_id()->set_id(tag_id);
          auto mutable_edge =
              new_col->mutable_entry()->mutable_element()->mutable_edge();
          auto unique_edge_label = generate_edge_label_id(
              iter.GetSrcLabel(), iter.GetDstLabel(), iter.GetEdgeLabel());
          mutable_edge->mutable_label()->set_id(unique_edge_label);
          mutable_edge->set_id(
              encode_unique_edge_id(unique_edge_label, cur_ind - 1));
          mutable_edge->set_src_id(
              encode_unique_vertex_id(iter.GetSrcLabel(), iter.GetSrc()));
          mutable_edge->mutable_src_label()->set_id(iter.GetSrcLabel());
          mutable_edge->set_dst_id(
              encode_unique_vertex_id(iter.GetDstLabel(), iter.GetDst()));
          mutable_edge->mutable_dst_label()->set_id(iter.GetDstLabel());
          auto prop_names = iter.GetPropNames();
          if (prop_names.size() > 0) {
            set_edge_property(mutable_edge, prop_names[0], iter.GetData());
          }
        }
        ++iter;
      }
    }
  }

  // sink for compressed path set
  template <size_t Ind, size_t act_tag_id, typename PATH_SET_T,
            typename std::enable_if_t<(PATH_SET_T::is_path_set)>* = nullptr>
  static void sink_col_impl(const GRAPH_INTERFACE& graph,
                            results::CollectiveResults& results_vec,
                            const PATH_SET_T& path_set,
                            const std::vector<size_t>& repeat_offsets,
                            int32_t tag_id) {
    if (repeat_offsets.empty()) {
      CHECK(path_set.Size() == results_vec.results_size())
          << "size neq " << path_set.Size() << " "
          << results_vec.results_size();
      auto iter = path_set.begin();
      auto end_iter = path_set.end();
      for (size_t i = 0; i < results_vec.results_size(); ++i) {
        // auto& row = results_vec[i];
        auto row = results_vec.mutable_results(i);
        CHECK(row->record().columns_size() == Ind)
            << "record column size: " << row->record().columns_size()
            << ", ind: " << Ind;
        auto record = row->mutable_record();
        auto new_col = record->add_columns();
        new_col->mutable_name_or_id()->set_id(tag_id);
        auto mutable_path =
            new_col->mutable_entry()->mutable_element()->mutable_graph_path();
        CHECK(iter != end_iter);
        auto cur_path = iter.GetElement();
        add_path_to_pb(cur_path, *mutable_path);
        ++iter;
      }
    } else {
      CHECK(repeat_offsets.size() == path_set.Size());
      size_t cur_ind = 0;
      auto iter = path_set.begin();
      auto end_iter = path_set.end();
      for (size_t i = 0; i < repeat_offsets.size(); ++i) {
        CHECK(iter != end_iter);
        for (size_t j = 0; j < repeat_offsets[i]; ++j) {
          // auto& row = results_vec[i];
          auto row = results_vec.mutable_results(cur_ind++);
          CHECK(row->record().columns_size() == Ind)
              << "record column size: " << row->record().columns_size()
              << ", ind: " << Ind;
          auto record = row->mutable_record();
          auto new_col = record->add_columns();
          new_col->mutable_name_or_id()->set_id(tag_id);
          auto mutable_path =
              new_col->mutable_entry()->mutable_element()->mutable_graph_path();
          auto cur_path = iter.GetElement();
          add_path_to_pb(cur_path, *mutable_path);
          // todo: set properties.
        }
        ++iter;
      }
    }
  }

  // TODO(zhanglei): This is temporary solution for sink path to results.
  // Out physical plan will only generate EndV option, so we can only sink the
  // end vertex to results.
  // If we sink all vertices to results, cypher driver seems failed to parse
  // paths of different lengths or what:
  // <Tried to construct a path that is not built like a path: even number of
  // elements>
  static void add_path_to_pb(const Path<vid_t, label_id_t>& path,
                             results::GraphPath& mutable_path) {
    if (path.length() <= 0) {
      return;
    }
    vid_t vid;
    label_id_t label;
    std::tie(label, vid) = path.GetNode(path.length() - 1);
    auto vertex = mutable_path.add_path()->mutable_vertex();
    vertex->set_id(encode_unique_vertex_id(label, vid));
    vertex->mutable_label()->set_id(label);
  }

  static vid_t encode_unique_vertex_id(label_id_t label_id, vid_t vid) {
    // encode label_id and vid to a unique vid
    vid_t unique_vid = label_id;
    static constexpr int num_bits = sizeof(vid_t) * 8 - sizeof(label_id_t) * 8;
    unique_vid = unique_vid << num_bits;
    unique_vid = unique_vid | vid;
    return unique_vid;
  }

  static int64_t encode_unique_edge_id(label_id_t label_id, size_t index) {
    // encode label_id and vid to a unique vid
    int64_t unique_edge_id = label_id;
    static constexpr int num_bits =
        sizeof(int64_t) * 8 - sizeof(label_id_t) * 8;
    unique_edge_id = unique_edge_id << num_bits;
    unique_edge_id = unique_edge_id | index;
    return unique_edge_id;
  }

  static label_id_t generate_edge_label_id(label_id_t src_label_id,
                                           label_id_t dst_label_id,
                                           label_id_t edge_label_id) {
    label_id_t unique_edge_label_id = src_label_id;
    static constexpr int num_bits = sizeof(label_id_t) * 8;
    unique_edge_label_id = unique_edge_label_id << num_bits;
    unique_edge_label_id = unique_edge_label_id | dst_label_id;
    unique_edge_label_id = unique_edge_label_id << num_bits;
    unique_edge_label_id = unique_edge_label_id | edge_label_id;
    return unique_edge_label_id;
  }
};
}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_OPERATOR_SINK_H_
