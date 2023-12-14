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

#include "flex/storages/rt_mutable_graph/loader/abstract_arrow_fragment_loader.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"

namespace gs {

bool check_primary_key_type(std::shared_ptr<arrow::DataType> data_type) {
  if (data_type->Equals(arrow::int64()) || data_type->Equals(arrow::uint64()) ||
      data_type->Equals(arrow::int32()) || data_type->Equals(arrow::uint32()) ||
      data_type->Equals(arrow::utf8()) ||
      data_type->Equals(arrow::large_utf8())) {
    return true;
  } else {
    return false;
  }
}

void set_vertex_properties(gs::ColumnBase* col,
                           std::shared_ptr<arrow::ChunkedArray> array,
                           const std::vector<vid_t>& vids) {
  auto type = array->type();
  auto col_type = col->type();
  size_t cur_ind = 0;
  if (col_type == PropertyType::kBool) {
    CHECK(type->Equals(arrow::boolean()))
        << "Inconsistent data type, expect bool, but got " << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::BooleanArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        col->set_any(vids[cur_ind++],
                     std::move(AnyConverter<bool>::to_any(casted->Value(k))));
      }
    }
  } else if (col_type == PropertyType::kInt64) {
    CHECK(type->Equals(arrow::int64()))
        << "Inconsistent data type, expect int64, but got " << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::Int64Array>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        col->set_any(
            vids[cur_ind++],
            std::move(AnyConverter<int64_t>::to_any(casted->Value(k))));
      }
    }
  } else if (col_type == PropertyType::kInt32) {
    CHECK(type->Equals(arrow::int32()))
        << "Inconsistent data type, expect int32, but got " << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::Int32Array>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        col->set_any(
            vids[cur_ind++],
            std::move(AnyConverter<int32_t>::to_any(casted->Value(k))));
      }
    }
  } else if (col_type == PropertyType::kUInt64) {
    CHECK(type->Equals(arrow::uint64()))
        << "Inconsistent data type, expect uint64, but got "
        << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::UInt64Array>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        col->set_any(
            vids[cur_ind++],
            std::move(AnyConverter<uint64_t>::to_any(casted->Value(k))));
      }
    }
  } else if (col_type == PropertyType::kUInt32) {
    CHECK(type->Equals(arrow::uint32()))
        << "Inconsistent data type, expect uint32, but got "
        << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::UInt32Array>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        col->set_any(
            vids[cur_ind++],
            std::move(AnyConverter<uint32_t>::to_any(casted->Value(k))));
      }
    }
  } else if (col_type == PropertyType::kDouble) {
    CHECK(type->Equals(arrow::float64()))
        << "Inconsistent data type, expect double, but got "
        << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::DoubleArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        col->set_any(vids[cur_ind++],
                     std::move(AnyConverter<double>::to_any(casted->Value(k))));
      }
    }
  } else if (col_type == PropertyType::kFloat) {
    CHECK(type->Equals(arrow::float32()))
        << "Inconsistent data type, expect float, but got " << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::FloatArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        col->set_any(vids[cur_ind++],
                     std::move(AnyConverter<float>::to_any(casted->Value(k))));
      }
    }
  } else if (col_type == PropertyType::kString ||
             col_type == PropertyType::kStringMap) {
    CHECK(type->Equals(arrow::large_utf8()) || type->Equals(arrow::utf8()))
        << "Inconsistent data type, expect string, but got "
        << type->ToString();
    if (type->Equals(arrow::large_utf8())) {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::LargeStringArray>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          auto str = casted->GetView(k);
          std::string_view str_view(str.data(), str.size());
          col->set_any(
              vids[cur_ind++],
              std::move(AnyConverter<std::string_view>::to_any(str_view)));
        }
      }
    } else {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::StringArray>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          auto str = casted->GetView(k);
          std::string_view str_view(str.data(), str.size());
          col->set_any(
              vids[cur_ind++],
              std::move(AnyConverter<std::string_view>::to_any(str_view)));
        }
      }
    }
  } else if (col_type == PropertyType::kDate) {
    if (type->Equals(arrow::timestamp(arrow::TimeUnit::type::MILLI))) {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::TimestampArray>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          col->set_any(vids[cur_ind++],
                       std::move(AnyConverter<Date>::to_any(casted->Value(k))));
        }
      }
    } else {
      LOG(FATAL) << "Not implemented: converting " << type->ToString() << " to "
                 << col_type;
    }
  } else {
    LOG(FATAL) << "Not support type: " << type->ToString();
  }
}

void check_edge_invariant(
    const Schema& schema,
    const std::vector<std::tuple<size_t, std::string, std::string>>&
        column_mappings,
    size_t src_col_ind, size_t dst_col_ind, label_t src_label_i,
    label_t dst_label_i, label_t edge_label_i) {
  // TODO(zhanglei): Check column mappings after multiple property on edge is
  // supported
  if (column_mappings.size() > 1) {
    LOG(FATAL) << "Edge column mapping must be less than 1";
  }
  if (column_mappings.size() > 0) {
    auto& mapping = column_mappings[0];
    if (std::get<0>(mapping) == src_col_ind ||
        std::get<0>(mapping) == dst_col_ind) {
      LOG(FATAL) << "Edge column mappings must not contain src_col_ind or "
                    "dst_col_ind";
    }
    auto src_label_name = schema.get_vertex_label_name(src_label_i);
    auto dst_label_name = schema.get_vertex_label_name(dst_label_i);
    auto edge_label_name = schema.get_edge_label_name(edge_label_i);
    // check property exists in schema
    if (!schema.edge_has_property(src_label_name, dst_label_name,
                                  edge_label_name, std::get<2>(mapping))) {
      LOG(FATAL) << "property " << std::get<2>(mapping)
                 << " not exists in schema for edge triplet " << src_label_name
                 << " -> " << edge_label_name << " -> " << dst_label_name;
    }
  }
}

void AbstractArrowFragmentLoader::AddVerticesRecordBatch(
    label_t v_label_id, const std::vector<std::string>& v_files,
    std::function<std::shared_ptr<IRecordBatchSupplier>(
        label_t, const std::string&, const LoadingConfig&)>
        supplier_creator) {
  auto primary_keys = schema_.get_vertex_primary_key(v_label_id);

  if (primary_keys.size() != 1) {
    LOG(FATAL) << "Only support one primary key for vertex.";
  }
  auto type = std::get<0>(primary_keys[0]);
  if (type != PropertyType::kInt64 && type != PropertyType::kString &&
      type != PropertyType::kInt32 && type != PropertyType::kUInt32 &&
      type != PropertyType::kUInt64) {
    LOG(FATAL)
        << "Only support int64_t, uint64_t, int32_t, uint32_t and string "
           "primary key for vertex.";
  }
  std::string v_label_name = schema_.get_vertex_label_name(v_label_id);
  VLOG(10) << "Start init vertices for label " << v_label_name << " with "
           << v_files.size() << " files.";

  if (type == PropertyType::kInt64) {
    addVertexRecordBatchImpl<int64_t>(v_label_id, v_files, supplier_creator);
  } else if (type == PropertyType::kString) {
    addVertexRecordBatchImpl<std::string_view>(v_label_id, v_files,
                                               supplier_creator);
  } else if (type == PropertyType::kInt32) {
    addVertexRecordBatchImpl<int32_t>(v_label_id, v_files, supplier_creator);
  } else if (type == PropertyType::kUInt32) {
    addVertexRecordBatchImpl<uint32_t>(v_label_id, v_files, supplier_creator);
  } else if (type == PropertyType::kUInt64) {
    addVertexRecordBatchImpl<uint64_t>(v_label_id, v_files, supplier_creator);
  }
  VLOG(10) << "Finish init vertices for label " << v_label_name;
}

void AbstractArrowFragmentLoader::AddEdgesRecordBatch(
    label_t src_label_i, label_t dst_label_i, label_t edge_label_i,
    const std::vector<std::string>& filenames,
    std::function<std::shared_ptr<IRecordBatchSupplier>(
        label_t, label_t, label_t, const std::string&, const LoadingConfig&)>
        supplier_creator) {
  auto src_label_name = schema_.get_vertex_label_name(src_label_i);
  auto dst_label_name = schema_.get_vertex_label_name(dst_label_i);
  auto edge_label_name = schema_.get_edge_label_name(edge_label_i);
  if (filenames.size() <= 0) {
    LOG(FATAL) << "No edge files found for src label: " << src_label_name
               << " dst label: " << dst_label_name
               << " edge label: " << edge_label_name;
  }
  if (filenames.size() <= 0) {
    LOG(FATAL) << "No edge files found for src label: " << src_label_name
               << " dst label: " << dst_label_name
               << " edge label: " << edge_label_name;
  }
  VLOG(10) << "Init edges src label: " << src_label_name
           << " dst label: " << dst_label_name
           << " edge label: " << edge_label_name
           << " filenames: " << filenames.size();
  auto& property_types = schema_.get_edge_properties(
      src_label_name, dst_label_name, edge_label_name);
  size_t col_num = property_types.size();
  CHECK_LE(col_num, 1) << "Only single or no property is supported for edge.";

  if (col_num == 0) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<grape::EmptyType>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesRecordBatchImpl<grape::EmptyType>(
          src_label_i, dst_label_i, edge_label_i, filenames, supplier_creator);
    }
  } else if (property_types[0] == PropertyType::kBool) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<bool>(src_label_i, dst_label_i,
                                                      edge_label_i);
    } else {
      addEdgesRecordBatchImpl<bool>(src_label_i, dst_label_i, edge_label_i,
                                    filenames, supplier_creator);
    }
  } else if (property_types[0] == PropertyType::kDate) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<Date>(src_label_i, dst_label_i,
                                                      edge_label_i);
    } else {
      addEdgesRecordBatchImpl<Date>(src_label_i, dst_label_i, edge_label_i,
                                    filenames, supplier_creator);
    }
  } else if (property_types[0] == PropertyType::kInt32) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<int32_t>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesRecordBatchImpl<int32_t>(src_label_i, dst_label_i, edge_label_i,
                                       filenames, supplier_creator);
    }
  } else if (property_types[0] == PropertyType::kUInt32) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<uint32_t>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesRecordBatchImpl<uint32_t>(src_label_i, dst_label_i, edge_label_i,
                                        filenames, supplier_creator);
    }
  } else if (property_types[0] == PropertyType::kInt64) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<int64_t>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesRecordBatchImpl<int64_t>(src_label_i, dst_label_i, edge_label_i,
                                       filenames, supplier_creator);
    }
  } else if (property_types[0] == PropertyType::kUInt64) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<uint64_t>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesRecordBatchImpl<uint64_t>(src_label_i, dst_label_i, edge_label_i,
                                        filenames, supplier_creator);
    }
  } else if (property_types[0] == PropertyType::kString) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<std::string_view>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesRecordBatchImpl<std::string_view>(
          src_label_i, dst_label_i, edge_label_i, filenames, supplier_creator);
    }
  } else if (property_types[0] == PropertyType::kDouble) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<double>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesRecordBatchImpl<double>(src_label_i, dst_label_i, edge_label_i,
                                      filenames, supplier_creator);
    }
  } else if (property_types[0] == PropertyType::kFloat) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<float>(src_label_i, dst_label_i,
                                                       edge_label_i);
    } else {
      addEdgesRecordBatchImpl<float>(src_label_i, dst_label_i, edge_label_i,
                                     filenames, supplier_creator);
    }
  } else {
    LOG(FATAL) << "Unsupported edge property type." << property_types[0];
  }
}

}  // namespace gs
