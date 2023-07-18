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
#include "flex/engines/hqps_db/structures/multi_edge_set/flat_edge_set.h"
#include "flex/engines/hqps_db/structures/multi_edge_set/general_edge_set.h"

#include "proto_generated_gie/results.pb.h"

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
  value->set_str(v.data(), v.size());
}

template <typename T,
          typename std::enable_if<(std::is_same_v<T, double>)>::type* = nullptr>
void template_set_value(common::Value* value, T v) {
  value->set_f64(v);
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
  for (auto i = 0; i < t.size(); ++i) {
    auto cur_ele = collection->add_collection()->mutable_object();
    template_set_value(cur_ele, t[i]);
  }
}

class SinkOp {
 public:
  // sink current context to results_pb defined in results.proto
  // return results::CollectiveResults
  template <typename CTX_HEAD_T, int cur_alias, int base_tag,
            typename... CTX_PREV_T>
  static results::CollectiveResults Sink(
      Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV_T...>& ctx,
      std::array<int32_t, Context<CTX_HEAD_T, cur_alias, base_tag,
                                  CTX_PREV_T...>::col_num>
          tag_ids) {
    using CTX_T = Context<CTX_HEAD_T, cur_alias, base_tag, CTX_PREV_T...>;

    // prepare enough record rows.
    auto size = ctx.GetHead().Size();
    // std::vector<results::Results> results_vec(size);
    results::CollectiveResults results_vec;
    for (auto i = 0; i < size; ++i) {
      results_vec.add_results();
    }
    LOG(INFO) << "reserve " << size << " records";
    sink_column<0>(results_vec, ctx, tag_ids);
    sink_head(results_vec, ctx, tag_ids[tag_ids.size() - 1]);
    return results_vec;
  }

  template <
      size_t I, typename CTX_T,
      typename std::enable_if<(I >= CTX_T::prev_alias_num)>::type* = nullptr>
  static void sink_column(results::CollectiveResults& record, CTX_T& ctx,
                          const std::array<int32_t, CTX_T::col_num>& tag_ids) {
    LOG(INFO) << "no prev columns to sink";
  }

  template <
      size_t I, typename CTX_T,
      typename std::enable_if<(I < CTX_T::prev_alias_num)>::type* = nullptr>
  static void sink_column(results::CollectiveResults& record, CTX_T& ctx,
                          const std::array<int32_t, CTX_T::col_num>& tag_ids) {
    if constexpr (I < CTX_T::prev_alias_num) {
      LOG(INFO) << "Projecting col: " << I;
      static constexpr size_t act_tag_id = CTX_T::base_tag_id + I;
      auto offset_array = ctx.ObtainOffsetFromTag(act_tag_id);
      auto repeat_array = offset_array_to_repeat_array(std::move(offset_array));
      sink_col_impl<I, act_tag_id>(record, ctx.template GetNode<act_tag_id>(),
                                   repeat_array, tag_ids[I]);
    }
    if constexpr (I + 1 < CTX_T::prev_alias_num) {
      sink_column<I + 1>(record, ctx, tag_ids);
    }
  }

  template <typename CTX_T>
  static void sink_head(results::CollectiveResults& record, CTX_T& ctx,
                        int32_t tag_id) {
    auto& head = ctx.GetHead();
    sink_col_impl<CTX_T::prev_alias_num, CTX_T::max_tag_id>(record, head, {},
                                                            tag_id);
  }

  template <size_t Ind, size_t act_tag_id, typename LabelT,
            typename vertex_id_t, typename... T>
  static void sink_col_impl(
      results::CollectiveResults& results_vec,
      const RowVertexSetImpl<LabelT, vertex_id_t, T...>& vertex_set,
      const std::vector<size_t>& repeat_offsets, int32_t tag_id) {
    auto label = vertex_set.GetLabel();
    auto& vids = vertex_set.GetVertices();
    return sink_col_impl_for_vertex_set<Ind, act_tag_id>(
        label, vids, results_vec, repeat_offsets, tag_id);
  }

  template <size_t Ind, size_t act_tag_id, typename LabelT, typename KEY_T,
            typename vertex_id_t>
  static void sink_col_impl(
      results::CollectiveResults& results_vec,
      const KeyedRowVertexSetImpl<LabelT, KEY_T, vertex_id_t, grape::EmptyType>&
          vertex_set,
      const std::vector<size_t>& repeat_offsets, int32_t tag_id) {
    auto label = vertex_set.GetLabel();
    auto& vids = vertex_set.GetVertices();
    return sink_col_impl_for_vertex_set<Ind, act_tag_id>(
        label, vids, results_vec, repeat_offsets, tag_id);
  }
  // sink row vertex set, if offsets is empty, we sink all vertices
  // if offsets is set, we use offset to repeat
  template <size_t Ind, size_t act_tag_id, typename LabelT,
            typename vertex_id_t>
  static void sink_col_impl_for_vertex_set(
      LabelT label, const std::vector<vertex_id_t>& vids,
      results::CollectiveResults& results_vec,
      const std::vector<size_t>& repeat_offsets, int32_t tag_id) {
    if (repeat_offsets.empty()) {
      for (auto i = 0; i < vids.size(); ++i) {
        auto row = results_vec.mutable_results(i);
        CHECK(row->record().columns_size() == Ind);
        auto record = row->mutable_record();
        auto new_col = record->add_columns();
        new_col->mutable_name_or_id()->set_id(tag_id);
        new_col->mutable_entry()->mutable_element()->mutable_vertex()->set_id(
            vids[i]);
        new_col->mutable_entry()
            ->mutable_element()
            ->mutable_vertex()
            ->mutable_label()
            ->set_id(label);
      }
    } else {
      CHECK(repeat_offsets.size() == vids.size());
      {
        int32_t num_rows = 0;
        for (auto i : repeat_offsets) {
          num_rows += i;
        }
        CHECK(num_rows == results_vec.results_size());
      }
      size_t cur_ind = 0;
      for (auto i = 0; i < vids.size(); ++i) {
        for (auto j = 0; j < repeat_offsets[i]; ++j) {
          auto row = results_vec.mutable_results(cur_ind++);
          auto record = row->mutable_record();
          auto new_col = record->add_columns();
          new_col->mutable_name_or_id()->set_id(tag_id);
          new_col->mutable_entry()->mutable_element()->mutable_vertex()->set_id(
              vids[i]);
          new_col->mutable_entry()
              ->mutable_element()
              ->mutable_vertex()
              ->mutable_label()
              ->set_id(label);
        }
      }
    }
  }

  // sink collection of pod
  template <size_t Ind, size_t act_tag_id, typename T,
            typename std::enable_if<(!gs::is_vector<T>::value) &&
                                    (!gs::is_tuple<T>::value)>::type* = nullptr>
  static void sink_col_impl(results::CollectiveResults& results_vec,
                            const Collection<T>& collection,
                            const std::vector<size_t>& repeat_offsets,
                            int32_t tag_id) {
    if (repeat_offsets.empty()) {
      CHECK(collection.Size() == results_vec.results_size())
          << "size neq " << collection.Size() << " "
          << results_vec.results_size();
      for (auto i = 0; i < collection.Size(); ++i) {
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
      for (auto i = 0; i < collection.Size(); ++i) {
        for (auto j = 0; j < repeat_offsets[i]; ++j) {
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

  // sinke for tuple with one element
  template <size_t Ind, size_t act_tag_id, typename T,
            typename std::enable_if<
                (!gs::is_vector<T>::value) && (gs::is_tuple<T>::value) &&
                (gs::tuple_size<T>::value == 1)>::type* = nullptr>
  static void sink_col_impl(results::CollectiveResults& results_vec,
                            const Collection<T>& collection,
                            const std::vector<size_t>& repeat_offsets,
                            int32_t tag_id) {
    if (repeat_offsets.empty()) {
      CHECK(collection.Size() == results_vec.results_size())
          << "size neq " << collection.Size() << " "
          << results_vec.results_size();
      for (auto i = 0; i < collection.Size(); ++i) {
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
      for (auto i = 0; i < collection.Size(); ++i) {
        for (auto j = 0; j < repeat_offsets[i]; ++j) {
          // auto& row = results_vec[cur_ind++];
          auto row = results_vec.mutable_results(i);
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
  static void sink_col_impl(results::CollectiveResults& results_vec,
                            const Collection<T>& collection,
                            const std::vector<size_t>& repeat_offsets,
                            int32_t tag_id) {
    if (repeat_offsets.empty()) {
      CHECK(collection.Size() == results_vec.results_size())
          << "size neq " << collection.Size() << " "
          << results_vec.results_size();
      for (auto i = 0; i < collection.Size(); ++i) {
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
      for (auto i = 0; i < collection.Size(); ++i) {
        for (auto j = 0; j < repeat_offsets[i]; ++j) {
          // auto& row = results_vec[cur_ind++];
          auto row = results_vec.mutable_results(i);
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
  static void sink_col_impl(results::CollectiveResults& results_vec,
                            const Collection<T>& collection,
                            const std::vector<size_t>& repeat_offsets,
                            int32_t tag_id) {
    if (repeat_offsets.empty()) {
      CHECK(collection.Size() == results_vec.results_size())
          << "size neq " << collection.Size() << " "
          << results_vec.results_size();
      for (auto i = 0; i < collection.Size(); ++i) {
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
      for (auto i = 0; i < collection.Size(); ++i) {
        for (auto j = 0; j < repeat_offsets[i]; ++j) {
          // auto& row = results_vec[cur_ind++];
          auto row = results_vec.mutable_results(i);
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
};
}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_OPERATOR_SINK_H_
