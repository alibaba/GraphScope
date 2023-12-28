
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

#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_ABSTRACT_ARROW_FRAGMENT_LOADER_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_ABSTRACT_ARROW_FRAGMENT_LOADER_H_

#include "flex/storages/rt_mutable_graph/loader/basic_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loader/i_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include "arrow/util/value_parsing.h"

#include "grape/util.h"

namespace gs {

// The interface providing visitor pattern for RecordBatch.
class IRecordBatchSupplier {
 public:
  virtual std::shared_ptr<arrow::RecordBatch> GetNextBatch() = 0;
};

bool check_primary_key_type(std::shared_ptr<arrow::DataType> data_type);

// For Primitive types.
template <typename COL_T>
void set_single_vertex_column(gs::ColumnBase* col,
                              std::shared_ptr<arrow::ChunkedArray> array,
                              const std::vector<vid_t>& vids) {
  using arrow_array_type = typename gs::TypeConverter<COL_T>::ArrowArrayType;
  auto array_type = array->type();
  auto arrow_type = gs::TypeConverter<COL_T>::ArrowTypeValue();
  // CHECK(array_type->Equals(arrow_type))
  //     << "Inconsistent data type, expect " << arrow_type->ToString()
  //     << ", but got " << array_type->ToString();
  size_t cur_ind = 0;
  // We temporally support load
  // 1. int64_t to uint8_t,
  // 2. load double to uint8_t,
  // 3. load int64_t to uint16_t
  if (array_type->Equals(arrow_type)) {
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted = std::static_pointer_cast<arrow_array_type>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        if (vids[cur_ind] == std::numeric_limits<vid_t>::max()) {
          ++cur_ind;
        } else {
          col->set_any(
              vids[cur_ind++],
              std::move(AnyConverter<COL_T>::to_any(casted->Value(k))));
        }
      }
    }
  } else {
    if (arrow_type->Equals(arrow::uint8()) &&
        array_type->Equals(arrow::int64())) {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::Int64Array>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          if (vids[cur_ind] == std::numeric_limits<vid_t>::max()) {
            ++cur_ind;
          } else {
            uint8_t val = casted->Value(k);
            col->set_any(vids[cur_ind++],
                         std::move(AnyConverter<COL_T>::to_any(val)));
          }
        }
      }
    } else if (arrow_type->Equals(arrow::uint16()) &&
               array_type->Equals(arrow::int64())) {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::Int64Array>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          if (vids[cur_ind] == std::numeric_limits<vid_t>::max()) {
            ++cur_ind;
          } else {
            uint16_t val = casted->Value(k);
            col->set_any(vids[cur_ind++],
                         std::move(AnyConverter<COL_T>::to_any(val)));
          }
        }
      }
    } else if (arrow_type->Equals(arrow::uint8()) &&
               array_type->Equals(arrow::float64())) {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::DoubleArray>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          if (vids[cur_ind] == std::numeric_limits<vid_t>::max()) {
            ++cur_ind;
          } else {
            uint8_t val = casted->Value(k);
            col->set_any(vids[cur_ind++],
                         std::move(AnyConverter<COL_T>::to_any(val)));
          }
        }
      }
    } else {
      LOG(FATAL) << "Not support type: " << array_type->ToString() << ", "
                 << arrow_type->ToString();
    }
  }
}

// For String types.
void set_vertex_column_from_string_array(
    gs::ColumnBase* col, std::shared_ptr<arrow::ChunkedArray> array,
    const std::vector<vid_t>& vids);

void set_vertex_column_from_timestamp_array(
    gs::ColumnBase* col, std::shared_ptr<arrow::ChunkedArray> array,
    const std::vector<vid_t>& vids);

void set_vertex_properties(gs::ColumnBase* col,
                           std::shared_ptr<arrow::ChunkedArray> array,
                           const std::vector<vid_t>& vids);

void check_edge_invariant(
    const Schema& schema,
    const std::vector<std::tuple<size_t, std::string, std::string>>&
        column_mappings,
    size_t src_col_ind, size_t dst_col_ind, label_t src_label_i,
    label_t dst_label_i, label_t edge_label_i);

template <typename KEY_T>
struct _add_vertex {
  void operator()(const std::shared_ptr<arrow::Array>& col,
                  IdIndexer<KEY_T, vid_t>& indexer, std::vector<vid_t>& vids) {
    size_t row_num = col->length();
    vid_t vid;
    if constexpr (!std::is_same<std::string_view, KEY_T>::value) {
      // for non-string value
      auto expected_type = gs::TypeConverter<KEY_T>::ArrowTypeValue();
      using arrow_array_t = typename gs::TypeConverter<KEY_T>::ArrowArrayType;
      if (!col->type()->Equals(expected_type)) {
        LOG(FATAL) << "Inconsistent data type, expect "
                   << expected_type->ToString() << ", but got "
                   << col->type()->ToString();
      }
      auto casted_array = std::static_pointer_cast<arrow_array_t>(col);
      for (auto i = 0; i < row_num; ++i) {
        if (!indexer.add(casted_array->Value(i), vid)) {
          LOG(ERROR) << "Duplicate vertex id: " << casted_array->Value(i)
                     << "..";
          vids.emplace_back(std::numeric_limits<vid_t>::max());
        } else {
          vids.emplace_back(vid);
        }
      }
    } else {
      if (col->type()->Equals(arrow::utf8())) {
        auto casted_array = std::static_pointer_cast<arrow::StringArray>(col);
        for (auto i = 0; i < row_num; ++i) {
          auto str = casted_array->GetView(i);
          std::string_view str_view(str.data(), str.size());
          if (!indexer.add(str_view, vid)) {
            LOG(ERROR) << "Duplicate vertex id: " << str_view << "..";
            vids.emplace_back(std::numeric_limits<vid_t>::max());
          } else {
            vids.emplace_back(vid);
          }
        }
      } else if (col->type()->Equals(arrow::large_utf8())) {
        auto casted_array =
            std::static_pointer_cast<arrow::LargeStringArray>(col);
        for (auto i = 0; i < row_num; ++i) {
          auto str = casted_array->GetView(i);
          std::string_view str_view(str.data(), str.size());
          if (!indexer.add(str_view, vid)) {
            LOG(ERROR) << "Duplicate vertex id: " << str_view << "..";
            vids.emplace_back(std::numeric_limits<vid_t>::max());
          } else {
            vids.emplace_back(vid);
          }
        }
      } else {
        LOG(FATAL) << "Not support type: " << col->type()->ToString();
      }
    }
  }
};

static void indexer_check_lambda(const LFIndexer<vid_t>& cur_indexer,
                                 const std::shared_ptr<arrow::Array>& cur_col) {
  if (cur_indexer.get_type() == PropertyType::kInt64) {
    CHECK(cur_col->type()->Equals(arrow::int64()));
  } else if (cur_indexer.get_type() == PropertyType::kString) {
    CHECK(cur_col->type()->Equals(arrow::utf8()) ||
          cur_col->type()->Equals(arrow::large_utf8()));
  } else if (cur_indexer.get_type() == PropertyType::kInt32) {
    CHECK(cur_col->type()->Equals(arrow::int32()));
  } else if (cur_indexer.get_type() == PropertyType::kUInt32) {
    CHECK(cur_col->type()->Equals(arrow::uint32()));
  } else if (cur_indexer.get_type() == PropertyType::kUInt64) {
    CHECK(cur_col->type()->Equals(arrow::uint64()));
  }
}

template <typename PK_T, typename EDATA_T>
static void append_src_or_dst(
    bool is_dst, size_t old_size, const std::shared_ptr<arrow::Array> col,
    const LFIndexer<vid_t>& indexer,
    std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& parsed_edges,
    std::vector<int32_t>& ie_degree, std::vector<int32_t>& oe_degree) {
  size_t cur_ind = old_size;
  auto invalid_vid = std::numeric_limits<vid_t>::max();
  if constexpr (std::is_same_v<PK_T, std::string_view>) {
    if (col->type()->Equals(arrow::utf8())) {
      auto casted = std::static_pointer_cast<arrow::StringArray>(col);
      for (auto j = 0; j < casted->length(); ++j) {
        auto str = casted->GetView(j);
        std::string_view str_view(str.data(), str.size());
        auto vid = indexer.get_index(Any::From(str_view));
        if (is_dst) {
          std::get<1>(parsed_edges[cur_ind++]) = vid;
        } else {
          std::get<0>(parsed_edges[cur_ind++]) = vid;
        }
        if (vid != invalid_vid) {
          is_dst ? ie_degree[vid]++ : oe_degree[vid]++;
        }
      }
    } else {
      // must be large utf8
      auto casted = std::static_pointer_cast<arrow::LargeStringArray>(col);
      for (auto j = 0; j < casted->length(); ++j) {
        auto str = casted->GetView(j);
        std::string_view str_view(str.data(), str.size());
        auto vid = indexer.get_index(Any::From(str_view));
        if (is_dst) {
          std::get<1>(parsed_edges[cur_ind++]) = vid;
        } else {
          std::get<0>(parsed_edges[cur_ind++]) = vid;
        }
        if (vid != invalid_vid) {
          is_dst ? ie_degree[vid]++ : oe_degree[vid]++;
        }
      }
    }
  } else {
    using arrow_array_type = typename gs::TypeConverter<PK_T>::ArrowArrayType;
    auto casted = std::static_pointer_cast<arrow_array_type>(col);
    for (auto j = 0; j < casted->length(); ++j) {
      auto vid = indexer.get_index(Any::From(casted->Value(j)));
      if (is_dst) {
        std::get<1>(parsed_edges[cur_ind++]) = vid;
      } else {
        std::get<0>(parsed_edges[cur_ind++]) = vid;
      }
      if (vid != invalid_vid) {
        is_dst ? ie_degree[vid]++ : oe_degree[vid]++;
      }
    }
  }
}

template <typename PK_T, typename EDATA_T>
static void append_edges(
    std::shared_ptr<arrow::Array> src_col,
    std::shared_ptr<arrow::Array> dst_col, const LFIndexer<vid_t>& src_indexer,
    const LFIndexer<vid_t>& dst_indexer,
    std::vector<std::shared_ptr<arrow::Array>>& edata_cols,
    const std::vector<PropertyType>& edge_props,
    std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& parsed_edges,
    std::vector<int32_t>& ie_degree, std::vector<int32_t>& oe_degree) {
  CHECK(src_col->length() == dst_col->length());

  indexer_check_lambda(src_indexer, src_col);
  indexer_check_lambda(dst_indexer, dst_col);
  auto old_size = parsed_edges.size();
  parsed_edges.resize(old_size + src_col->length());
  VLOG(10) << "resize parsed_edges from" << old_size << " to "
           << parsed_edges.size();

  auto src_col_thread = std::thread([&]() {
    append_src_or_dst<PK_T, EDATA_T>(false, old_size, src_col, src_indexer,
                                     parsed_edges, ie_degree, oe_degree);
  });
  auto dst_col_thread = std::thread([&]() {
    append_src_or_dst<PK_T, EDATA_T>(true, old_size, dst_col, dst_indexer,
                                     parsed_edges, ie_degree, oe_degree);
  });
  // if EDATA_T is grape::EmptyType, no need to read columns
  auto edata_col_thread = std::thread([&]() {
    if constexpr (!std::is_same<EDATA_T, grape::EmptyType>::value) {
      CHECK(edata_cols.size() == 1);
      auto edata_col = edata_cols[0];
      CHECK(src_col->length() == edata_col->length());
      size_t cur_ind = old_size;
      auto type = edata_col->type();
      if (!type->Equals(TypeConverter<EDATA_T>::ArrowTypeValue())) {
        LOG(FATAL) << "Inconsistent data type, expect "
                   << TypeConverter<EDATA_T>::ArrowTypeValue()->ToString()
                   << ", but got " << type->ToString();
      }

      using arrow_array_type =
          typename gs::TypeConverter<EDATA_T>::ArrowArrayType;
      // cast chunk to EDATA_T array
      auto data = std::static_pointer_cast<arrow_array_type>(edata_col);
      for (auto j = 0; j < edata_col->length(); ++j) {
        if constexpr (std::is_same<arrow_array_type,
                                   arrow::StringArray>::value ||
                      std::is_same<arrow_array_type,
                                   arrow::LargeStringArray>::value) {
          auto str = data->GetView(j);
          std::string_view str_view(str.data(), str.size());
          std::get<2>(parsed_edges[cur_ind++]) = str_view;
        } else {
          std::get<2>(parsed_edges[cur_ind++]) = data->Value(j);
        }
      }
      VLOG(10) << "Finish inserting:  " << src_col->length() << " edges";
    }
  });

  src_col_thread.join();
  dst_col_thread.join();
  edata_col_thread.join();
}

template <typename T>
static void cast_array_and_append_impl(
    size_t old_size, size_t offset, PropertyType property_type,
    std::shared_ptr<arrow::Array> array,
    std::vector<std::tuple<vid_t, vid_t, std::vector<char>>>& parsed_edges) {
  using arrow_array_type = typename gs::TypeConverter<T>::ArrowArrayType;
  auto array_type = array->type();
  auto arrow_type = gs::TypeConverter<T>::ArrowTypeValue();
  if (arrow_type->Equals(array_type)) {
    auto casted = std::static_pointer_cast<arrow_array_type>(array);
    auto cur_ind = old_size;
    for (auto j = 0; j < casted->length(); ++j) {
      // must be primitive type, no string
      // std::get<2>(parsed_edges[cur_ind++]) = casted->Value(j);
      // set casted->Value(j); to std::get<2>(parsed_edges[cur_ind++]) from
      // offset to offset + sizeof(T)
      auto& vec = std::get<2>(parsed_edges[cur_ind++]);
      auto value = casted->Value(j);
      CHECK(vec.size() >= offset + sizeof(T));
      memcpy(vec.data() + offset, &value, sizeof(T));
    }
  } else {
    if (array_type->Equals(arrow::int64()) &&
        arrow_type->Equals(arrow::uint8())) {
      auto casted = std::static_pointer_cast<arrow::Int64Array>(array);
      auto cur_ind = old_size;
      for (auto j = 0; j < casted->length(); ++j) {
        auto value = casted->Value(j);
        auto& vec = std::get<2>(parsed_edges[cur_ind++]);
        CHECK(vec.size() >= offset + sizeof(uint8_t));
        memcpy(vec.data() + offset, &value, sizeof(uint8_t));
      }
    } else if (array_type->Equals(arrow::int64()) &&
               arrow_type->Equals(arrow::uint16())) {
      auto casted = std::static_pointer_cast<arrow::Int64Array>(array);
      auto cur_ind = old_size;
      for (auto j = 0; j < casted->length(); ++j) {
        auto value = casted->Value(j);
        auto& vec = std::get<2>(parsed_edges[cur_ind++]);
        CHECK(vec.size() >= offset + sizeof(uint16_t));
        memcpy(vec.data() + offset, &value, sizeof(uint16_t));
      }
    } else if (array_type->Equals(arrow::float64()) &&
               arrow_type->Equals(arrow::uint8())) {
      auto casted = std::static_pointer_cast<arrow::DoubleArray>(array);
      auto cur_ind = old_size;
      for (auto j = 0; j < casted->length(); ++j) {
        auto value = casted->Value(j);
        auto& vec = std::get<2>(parsed_edges[cur_ind++]);
        CHECK(vec.size() >= offset + sizeof(uint8_t));
        memcpy(vec.data() + offset, &value, sizeof(uint8_t));
      }
    } else {
      LOG(FATAL) << "Not support type: " << array_type->ToString() << ", "
                 << arrow_type->ToString();
    }
  }
}

static void cast_array_and_append(
    size_t old_size, size_t offset, PropertyType property_type,
    std::shared_ptr<arrow::Array> array,
    std::vector<std::tuple<vid_t, vid_t, std::vector<char>>>& parsed_edges) {
  auto type = array->type();
  if (type->Equals(arrow::int32())) {
    cast_array_and_append_impl<int32_t>(old_size, offset, property_type, array,
                                        parsed_edges);
  } else if (type->Equals(arrow::int64())) {
    cast_array_and_append_impl<int64_t>(old_size, offset, property_type, array,
                                        parsed_edges);
  } else if (type->Equals(arrow::uint32())) {
    cast_array_and_append_impl<uint32_t>(old_size, offset, property_type, array,
                                         parsed_edges);
  } else if (type->Equals(arrow::uint64())) {
    cast_array_and_append_impl<uint64_t>(old_size, offset, property_type, array,
                                         parsed_edges);
  } else if (type->Equals(arrow::float32())) {
    cast_array_and_append_impl<float>(old_size, offset, property_type, array,
                                      parsed_edges);
  } else if (type->Equals(arrow::float64())) {
    cast_array_and_append_impl<double>(old_size, offset, property_type, array,
                                       parsed_edges);
  } else if (type->Equals(arrow::utf8())) {
    cast_array_and_append_impl<std::string_view>(
        old_size, offset, property_type, array, parsed_edges);
  } else if (type->Equals(arrow::large_utf8())) {
    cast_array_and_append_impl<std::string_view>(
        old_size, offset, property_type, array, parsed_edges);
  } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::MILLI))) {
    cast_array_and_append_impl<Date>(old_size, offset, property_type, array,
                                     parsed_edges);
  } else {
    LOG(FATAL) << "Not support type: " << type->ToString();
  }
}

template <typename PK_T, typename EDATA_T>
static void append_edges_for_multiple_props(
    std::shared_ptr<arrow::Array> src_col,
    std::shared_ptr<arrow::Array> dst_col, const LFIndexer<vid_t>& src_indexer,
    const LFIndexer<vid_t>& dst_indexer,
    std::vector<std::shared_ptr<arrow::Array>>& edata_cols,
    const std::vector<PropertyType>& edge_props,
    std::vector<std::tuple<vid_t, vid_t, std::vector<char>>>& parsed_edges,
    std::vector<int32_t>& ie_degree, std::vector<int32_t>& oe_degree,
    const std::vector<size_t>& offset_vec) {
  CHECK(src_col->length() == dst_col->length());

  indexer_check_lambda(src_indexer, src_col);
  indexer_check_lambda(dst_indexer, dst_col);
  auto old_size = parsed_edges.size();
  parsed_edges.resize(old_size + src_col->length());
  VLOG(10) << "resize parsed_edges from" << old_size << " to "
           << parsed_edges.size();

  auto src_col_thread = std::thread([&]() {
    append_src_or_dst<PK_T, std::vector<char>>(false, old_size, src_col,
                                               src_indexer, parsed_edges,
                                               ie_degree, oe_degree);
  });
  auto dst_col_thread = std::thread([&]() {
    append_src_or_dst<PK_T, std::vector<char>>(true, old_size, dst_col,
                                               dst_indexer, parsed_edges,
                                               ie_degree, oe_degree);
  });
  auto edata_col_thread = std::thread([&]() {
    CHECK(edata_cols.size() == edge_props.size());
    // first check type consistency
    // for (auto i = 0; i < edata_cols.size(); ++i) {
    //   CHECK(src_col->length() == edata_cols[i]->length());
    //   auto expected_arrow_type = PropertyTypeToArrowType(edge_props[i]);
    //   if (!edata_cols[i]->type()->Equals(expected_arrow_type)) {
    //     LOG(FATAL) << "Inconsistent data type, expect "
    //                << expected_arrow_type->ToString() << ", but got "
    //                << edata_cols[i]->type()->ToString();
    //   }
    // }

    for (auto i = old_size; i < parsed_edges.size(); ++i) {
      std::get<2>(parsed_edges[i]).resize(offset_vec.back());
    }

    for (auto i = 0; i < edata_cols.size(); ++i) {
      auto edata_col = edata_cols[i];
      auto type = edata_col->type();
      cast_array_and_append(old_size, offset_vec[i], edge_props[i], edata_col,
                            parsed_edges);
    }
    VLOG(10) << "Finish inserting:  " << src_col->length() << " edges";
  });

  src_col_thread.join();
  dst_col_thread.join();
  edata_col_thread.join();
}

// A AbstractArrowFragmentLoader with can load fragment from arrow::table.
// Can not be used directly, should be inherited.
class AbstractArrowFragmentLoader : public IFragmentLoader {
 public:
  AbstractArrowFragmentLoader(const std::string& work_dir, const Schema& schema,
                              const LoadingConfig& loading_config,
                              int32_t thread_num)
      : loading_config_(loading_config),
        schema_(schema),
        thread_num_(thread_num),
        basic_fragment_loader_(schema_, work_dir) {
    vertex_label_num_ = schema_.vertex_label_num();
    edge_label_num_ = schema_.edge_label_num();
  }

  ~AbstractArrowFragmentLoader() {}

  void AddVerticesRecordBatch(
      label_t v_label_id, const std::vector<std::string>& input_paths,
      std::function<std::shared_ptr<IRecordBatchSupplier>(
          label_t, const std::string&, const LoadingConfig&)>
          supplier_creator);

  // Add edges in record batch to output_parsed_edges, output_ie_degrees and
  // output_oe_degrees.
  void AddEdgesRecordBatch(
      label_t src_label_id, label_t dst_label_id, label_t edge_label_id,
      const std::vector<std::string>& input_paths,
      std::function<std::shared_ptr<IRecordBatchSupplier>(
          label_t, label_t, label_t, const std::string&, const LoadingConfig&)>
          supplier_creator);

 protected:
  template <typename KEY_T>
  void addVertexBatchFromArray(
      label_t v_label_id, IdIndexer<KEY_T, vid_t>& indexer,
      std::shared_ptr<arrow::Array>& primary_key_col,
      const std::vector<std::shared_ptr<arrow::Array>>& property_cols) {
    size_t row_num = primary_key_col->length();
    auto col_num = property_cols.size();
    for (size_t i = 0; i < col_num; ++i) {
      CHECK_EQ(property_cols[i]->length(), row_num);
    }

    double t = -grape::GetCurrentTime();
    std::vector<vid_t> vids;
    vids.reserve(row_num);

    _add_vertex<KEY_T>()(primary_key_col, indexer, vids);

    for (auto j = 0; j < property_cols.size(); ++j) {
      auto array = property_cols[j];
      auto chunked_array = std::make_shared<arrow::ChunkedArray>(array);
      set_vertex_properties(
          basic_fragment_loader_.GetVertexTable(v_label_id).column_ptrs()[j],
          chunked_array, vids);
    }

    VLOG(10) << "Insert rows: " << row_num;
  }

  template <typename KEY_T>
  void addVertexRecordBatchImpl(
      label_t v_label_id, const std::vector<std::string>& v_files,
      std::function<std::shared_ptr<IRecordBatchSupplier>(
          label_t, const std::string&, const LoadingConfig&)>
          supplier_creator) {
    std::string v_label_name = schema_.get_vertex_label_name(v_label_id);
    VLOG(10) << "Parsing vertex file:" << v_files.size() << " for label "
             << v_label_name;
    auto primary_key = schema_.get_vertex_primary_key(v_label_id)[0];
    auto primary_key_name = std::get<1>(primary_key);
    size_t primary_key_ind = std::get<2>(primary_key);
    IdIndexer<KEY_T, vid_t> indexer;

    for (auto& v_file : v_files) {
      VLOG(10) << "Parsing vertex file:" << v_file << " for label "
               << v_label_name;
      auto record_batch_supplier =
          supplier_creator(v_label_id, v_file, loading_config_);

      bool first_batch = true;
      while (true) {
        auto batch = record_batch_supplier->GetNextBatch();
        if (!batch) {
          break;
        }
        if (first_batch) {
          auto header = batch->schema()->field_names();
          auto schema_column_names =
              schema_.get_vertex_property_names(v_label_id);
          CHECK(schema_column_names.size() + 1 == header.size())
              << "File header of size: " << header.size()
              << " does not match schema column size: "
              << schema_column_names.size() + 1;
          first_batch = false;
        }
        auto columns = batch->columns();
        CHECK(primary_key_ind < columns.size());
        auto primary_key_column = columns[primary_key_ind];
        auto other_columns_array = columns;
        other_columns_array.erase(other_columns_array.begin() +
                                  primary_key_ind);
        addVertexBatchFromArray(v_label_id, indexer, primary_key_column,
                                other_columns_array);
      }
      VLOG(10) << "Finish parsing vertex file:" << v_file << " for label "
               << v_label_name;
    }

    VLOG(10) << "Finish parsing vertex file:" << v_files.size() << " for label "
             << v_label_name;
    if (indexer.bucket_count() == 0) {
      indexer._rehash(schema_.get_max_vnum(v_label_name));
    }
    basic_fragment_loader_.FinishAddingVertex<KEY_T>(v_label_id, indexer);
  }

  template <typename EDATA_T,
            typename std::enable_if<
                (!std::is_same_v<EDATA_T, FixedChar>)>::type* = nullptr>
  void addEdgesRecordBatchImpl(
      label_t src_label_id, label_t dst_label_id, label_t e_label_id,
      const std::vector<std::string>& e_files,
      std::function<std::shared_ptr<IRecordBatchSupplier>(
          label_t, label_t, label_t, const std::string&, const LoadingConfig&)>
          supplier_creator) {
    auto src_label_name = schema_.get_vertex_label_name(src_label_id);
    auto dst_label_name = schema_.get_vertex_label_name(dst_label_id);
    auto edge_label_name = schema_.get_edge_label_name(e_label_id);
    auto edge_column_mappings = loading_config_.GetEdgeColumnMappings(
        src_label_id, dst_label_id, e_label_id);
    auto src_dst_col_pair = loading_config_.GetEdgeSrcDstCol(
        src_label_id, dst_label_id, e_label_id);
    if (src_dst_col_pair.first.size() != 1 ||
        src_dst_col_pair.second.size() != 1) {
      LOG(FATAL) << "We currently only support one src primary key and one "
                    "dst primary key";
    }
    size_t src_col_ind = src_dst_col_pair.first[0].second;
    size_t dst_col_ind = src_dst_col_pair.second[0].second;
    CHECK(src_col_ind != dst_col_ind);

    check_edge_invariant(schema_, edge_column_mappings, src_col_ind,
                         dst_col_ind, src_label_id, dst_label_id, e_label_id);

    std::vector<std::tuple<vid_t, vid_t, EDATA_T>> parsed_edges;
    std::vector<int32_t> ie_degree, oe_degree;
    const auto& src_indexer = basic_fragment_loader_.GetLFIndexer(src_label_id);
    const auto& dst_indexer = basic_fragment_loader_.GetLFIndexer(dst_label_id);
    ie_degree.resize(dst_indexer.size());
    oe_degree.resize(src_indexer.size());
    VLOG(10) << "src indexer size: " << src_indexer.size()
             << " dst indexer size: " << dst_indexer.size();
    std::vector<std::shared_ptr<arrow::Array>> property_str_types;
    for (auto filename : e_files) {
      auto record_batch_supplier = supplier_creator(
          src_label_id, dst_label_id, e_label_id, filename, loading_config_);
      bool first_batch = true;
      while (true) {
        auto record_batch = record_batch_supplier->GetNextBatch();
        if (!record_batch) {
          break;
        }
        if (first_batch) {
          auto header = record_batch->schema()->field_names();
          auto schema_column_names = schema_.get_edge_property_names(
              src_label_id, dst_label_id, e_label_id);
          auto schema_column_types = schema_.get_edge_properties(
              src_label_id, dst_label_id, e_label_id);
          CHECK(schema_column_names.size() + 2 == header.size())
              << "schema size: " << schema_column_names.size()
              << " neq header size: " << header.size();
        }
        // copy the table to csr.
        auto columns = record_batch->columns();
        // We assume the src_col and dst_col will always be put at front.
        CHECK(columns.size() >= 2);
        auto src_col = columns[0];
        auto dst_col = columns[1];
        auto src_col_type = src_col->type();
        auto dst_col_type = dst_col->type();
        CHECK(check_primary_key_type(src_col_type))
            << "unsupported src_col type: " << src_col_type->ToString();
        CHECK(check_primary_key_type(dst_col_type))
            << "unsupported dst_col type: " << dst_col_type->ToString();
        CHECK(src_col_type->Equals(dst_col_type))
            << "src_col type: " << src_col_type->ToString()
            << " neq dst_col type: " << dst_col_type->ToString();

        std::vector<std::shared_ptr<arrow::Array>> property_cols;
        for (auto i = 2; i < columns.size(); ++i) {
          if (columns[i]->type()->Equals(arrow::large_utf8()) ||
              columns[i]->type()->Equals(arrow::utf8())) {
            property_str_types.emplace_back(columns[i]);
          }
          property_cols.emplace_back(columns[i]);
        }
        // CHECK(property_cols.size() <= 1)
        //     << "Currently only support at most one property on edge";
        auto edge_properties =
            schema_.get_edge_properties(src_label_id, dst_label_id, e_label_id);
        // add edges to vector
        CHECK(src_col->length() == dst_col->length());
        if (src_col_type->Equals(arrow::int64())) {
          append_edges<int64_t, EDATA_T>(
              src_col, dst_col, src_indexer, dst_indexer, property_cols,
              edge_properties, parsed_edges, ie_degree, oe_degree);
        } else if (src_col_type->Equals(arrow::uint64())) {
          append_edges<uint64_t, EDATA_T>(
              src_col, dst_col, src_indexer, dst_indexer, property_cols,
              edge_properties, parsed_edges, ie_degree, oe_degree);
        } else if (src_col_type->Equals(arrow::int32())) {
          append_edges<int32_t, EDATA_T>(
              src_col, dst_col, src_indexer, dst_indexer, property_cols,
              edge_properties, parsed_edges, ie_degree, oe_degree);
        } else if (src_col_type->Equals(arrow::uint32())) {
          append_edges<uint32_t, EDATA_T>(
              src_col, dst_col, src_indexer, dst_indexer, property_cols,
              edge_properties, parsed_edges, ie_degree, oe_degree);
        } else {
          // must be string
          append_edges<std::string_view, EDATA_T>(
              src_col, dst_col, src_indexer, dst_indexer, property_cols,
              edge_properties, parsed_edges, ie_degree, oe_degree);
        }
        first_batch = false;
      }
      VLOG(10) << "Finish parsing edge file:" << filename << " for label "
               << src_label_name << " -> " << dst_label_name << " -> "
               << edge_label_name;
    }
    VLOG(10) << "Finish parsing edge file:" << e_files.size() << " for label "
             << src_label_name << " -> " << dst_label_name << " -> "
             << edge_label_name;

    basic_fragment_loader_.PutEdges(src_label_id, dst_label_id, e_label_id,
                                    parsed_edges, ie_degree, oe_degree);
    property_str_types.clear();
    VLOG(10) << "Finish putting: " << parsed_edges.size() << " edges";
  }

  template <typename EDATA_T,
            typename std::enable_if<std::is_same_v<EDATA_T, FixedChar>>::type* =
                nullptr>
  void addEdgesRecordBatchImpl(
      label_t src_label_id, label_t dst_label_id, label_t e_label_id,
      const std::vector<std::string>& e_files,
      std::function<std::shared_ptr<IRecordBatchSupplier>(
          label_t, label_t, label_t, const std::string&, const LoadingConfig&)>
          supplier_creator) {
    auto src_label_name = schema_.get_vertex_label_name(src_label_id);
    auto dst_label_name = schema_.get_vertex_label_name(dst_label_id);
    auto edge_label_name = schema_.get_edge_label_name(e_label_id);
    auto edge_column_mappings = loading_config_.GetEdgeColumnMappings(
        src_label_id, dst_label_id, e_label_id);
    auto src_dst_col_pair = loading_config_.GetEdgeSrcDstCol(
        src_label_id, dst_label_id, e_label_id);
    if (src_dst_col_pair.first.size() != 1 ||
        src_dst_col_pair.second.size() != 1) {
      LOG(FATAL) << "We currently only support one src primary key and one "
                    "dst primary key";
    }
    size_t src_col_ind = src_dst_col_pair.first[0].second;
    size_t dst_col_ind = src_dst_col_pair.second[0].second;
    CHECK(src_col_ind != dst_col_ind);

    check_edge_invariant(schema_, edge_column_mappings, src_col_ind,
                         dst_col_ind, src_label_id, dst_label_id, e_label_id);

    // Since edge contains multiple properties, we need spaces to store them.
    std::vector<std::tuple<vid_t, vid_t, std::vector<char>>> parsed_edges;
    std::vector<int32_t> ie_degree, oe_degree;
    const auto& src_indexer = basic_fragment_loader_.GetLFIndexer(src_label_id);
    const auto& dst_indexer = basic_fragment_loader_.GetLFIndexer(dst_label_id);
    ie_degree.resize(dst_indexer.size());
    oe_degree.resize(src_indexer.size());
    VLOG(10) << "src indexer size: " << src_indexer.size()
             << " dst indexer size: " << dst_indexer.size();
    // first get offset vector
    auto edge_properties =
        schema_.get_edge_properties(src_label_id, dst_label_id, e_label_id);
    std::vector<size_t> offset_vec(edge_properties.size(), 0);
    uint16_t cur_offset = 0;
    {
      for (auto i = 0; i < edge_properties.size(); ++i) {
        offset_vec[i] = cur_offset;
        cur_offset += edge_properties[i].NumBytes();
      }
      offset_vec.emplace_back(cur_offset);
      VLOG(10) << "offset_vec: " << gs::to_string(offset_vec)
               << ", total size: " << cur_offset;
    }

    for (auto filename : e_files) {
      auto record_batch_supplier = supplier_creator(
          src_label_id, dst_label_id, e_label_id, filename, loading_config_);
      bool first_batch = true;
      while (true) {
        auto record_batch = record_batch_supplier->GetNextBatch();
        if (!record_batch) {
          break;
        }
        if (first_batch) {
          auto header = record_batch->schema()->field_names();
          auto schema_column_names = schema_.get_edge_property_names(
              src_label_id, dst_label_id, e_label_id);
          auto schema_column_types = schema_.get_edge_properties(
              src_label_id, dst_label_id, e_label_id);
          CHECK(schema_column_names.size() + 2 == header.size())
              << "schema size: " << schema_column_names.size()
              << " neq header size: " << header.size();
        }
        // copy the table to csr.
        auto columns = record_batch->columns();
        // We assume the src_col and dst_col will always be put at front.
        CHECK(columns.size() >= 2);
        auto src_col = columns[0];
        auto dst_col = columns[1];
        auto src_col_type = src_col->type();
        auto dst_col_type = dst_col->type();
        CHECK(check_primary_key_type(src_col_type))
            << "unsupported src_col type: " << src_col_type->ToString();
        CHECK(check_primary_key_type(dst_col_type))
            << "unsupported dst_col type: " << dst_col_type->ToString();
        CHECK(src_col_type->Equals(dst_col_type))
            << "src_col type: " << src_col_type->ToString()
            << " neq dst_col type: " << dst_col_type->ToString();

        std::vector<std::shared_ptr<arrow::Array>> property_cols;
        for (auto i = 2; i < columns.size(); ++i) {
          property_cols.emplace_back(columns[i]);
        }
        // CHECK(property_cols.size() <= 1)
        //     << "Currently only support at most one property on edge";

        // add edges to vector
        CHECK(src_col->length() == dst_col->length());
        if (src_col_type->Equals(arrow::int64())) {
          append_edges_for_multiple_props<int64_t, EDATA_T>(
              src_col, dst_col, src_indexer, dst_indexer, property_cols,
              edge_properties, parsed_edges, ie_degree, oe_degree, offset_vec);
        } else if (src_col_type->Equals(arrow::uint64())) {
          append_edges_for_multiple_props<uint64_t, EDATA_T>(
              src_col, dst_col, src_indexer, dst_indexer, property_cols,
              edge_properties, parsed_edges, ie_degree, oe_degree, offset_vec);
        } else if (src_col_type->Equals(arrow::int32())) {
          append_edges_for_multiple_props<int32_t, EDATA_T>(
              src_col, dst_col, src_indexer, dst_indexer, property_cols,
              edge_properties, parsed_edges, ie_degree, oe_degree, offset_vec);
        } else if (src_col_type->Equals(arrow::uint32())) {
          append_edges_for_multiple_props<uint32_t, EDATA_T>(
              src_col, dst_col, src_indexer, dst_indexer, property_cols,
              edge_properties, parsed_edges, ie_degree, oe_degree, offset_vec);
        } else {
          // must be string
          append_edges_for_multiple_props<std::string_view, EDATA_T>(
              src_col, dst_col, src_indexer, dst_indexer, property_cols,
              edge_properties, parsed_edges, ie_degree, oe_degree, offset_vec);
        }
        first_batch = false;
      }
      VLOG(10) << "Finish parsing edge file:" << filename << " for label "
               << src_label_name << " -> " << dst_label_name << " -> "
               << edge_label_name;
    }
    VLOG(10) << "Finish parsing edge file:" << e_files.size() << " for label "
             << src_label_name << " -> " << dst_label_name << " -> "
             << edge_label_name;

    basic_fragment_loader_.PutMultiPropEdges(src_label_id, dst_label_id,
                                             e_label_id, parsed_edges,
                                             ie_degree, oe_degree, offset_vec);
    VLOG(10) << "Finish putting: " << parsed_edges.size() << " edges";
  }

  const LoadingConfig& loading_config_;
  const Schema& schema_;
  size_t vertex_label_num_, edge_label_num_;
  int32_t thread_num_;

  mutable BasicFragmentLoader basic_fragment_loader_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_ABSTRACT_ARROW_FRAGMENT_LOADER_H_