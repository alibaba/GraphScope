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
#include <sys/statvfs.h>

#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/storages/rt_mutable_graph/loader/abstract_arrow_fragment_loader.h"

namespace gs {

void printDiskRemaining(const std::string& path) {
  struct statvfs buf;
  if (statvfs(path.c_str(), &buf) == 0) {
    LOG(INFO) << "Disk remaining: " << buf.f_bsize * buf.f_bavail / 1024 / 1024
              << "MB";
  }
}

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

void set_column_from_string_array(gs::ColumnBase* col,
                                  std::shared_ptr<arrow::ChunkedArray> array,
                                  const std::vector<size_t>& offset) {
  auto type = array->type();
  auto size = col->size();
  CHECK(type->Equals(arrow::large_utf8()) || type->Equals(arrow::utf8()))
      << "Inconsistent data type, expect string, but got " << type->ToString();
  size_t cur_ind = 0;
  if (type->Equals(arrow::large_utf8())) {
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::LargeStringArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        auto str = casted->GetView(k);
        std::string_view sw;
        if (casted->IsNull(k)) {
          VLOG(1) << "Found null string in vertex property.";
          sw = "";
        } else {
          sw = std::string_view(str.data(), str.size());
        }
        if (offset[cur_ind] >= size) {
          cur_ind++;
        } else {
          col->set_any(offset[cur_ind++], std::move(sw));
        }
      }
    }
  } else {
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::StringArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        auto str = casted->GetView(k);
        std::string_view sw(str.data(), str.size());
        if (offset[cur_ind] >= size) {
          cur_ind++;
        } else {
          col->set_any(offset[cur_ind++], std::move(sw));
        }
      }
    }
  }
}

void set_properties_column(gs::ColumnBase* col,
                           std::shared_ptr<arrow::ChunkedArray> array,
                           const std::vector<size_t>& offset) {
  auto type = array->type();
  auto col_type = col->type();

  // TODO(zhanglei): reduce the dummy code here with a template function.
  if (col_type == PropertyType::kBool) {
    set_column<bool>(col, array, offset);
  } else if (col_type == PropertyType::kInt64) {
    set_column<int64_t>(col, array, offset);
  } else if (col_type == PropertyType::kInt32) {
    set_column<int32_t>(col, array, offset);
  } else if (col_type == PropertyType::kUInt64) {
    set_column<uint64_t>(col, array, offset);
  } else if (col_type == PropertyType::kUInt32) {
    set_column<uint32_t>(col, array, offset);
  } else if (col_type == PropertyType::kDouble) {
    set_column<double>(col, array, offset);
  } else if (col_type == PropertyType::kFloat) {
    set_column<float>(col, array, offset);
  } else if (col_type == PropertyType::kDate) {
    set_column_from_timestamp_array(col, array, offset);
  } else if (col_type == PropertyType::kDay) {
    set_column_from_timestamp_array_to_day(col, array, offset);
  } else if (col_type == PropertyType::kStringMap) {
    set_column_from_string_array(col, array, offset);
  } else if (col_type.type_enum == impl::PropertyTypeImpl::kVarChar) {
    set_column_from_string_array(col, array, offset);
  } else if (col_type == PropertyType::kStringView) {
    set_column_from_string_array(col, array, offset);
  } else {
    LOG(FATAL) << "Not support type: " << type->ToString();
  }
}

void set_column_from_timestamp_array(gs::ColumnBase* col,
                                     std::shared_ptr<arrow::ChunkedArray> array,
                                     const std::vector<size_t>& offset) {
  auto type = array->type();
  auto col_type = col->type();
  auto size = col->size();
  size_t cur_ind = 0;
  if (type->Equals(arrow::timestamp(arrow::TimeUnit::type::MILLI))) {
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::TimestampArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        if (offset[cur_ind] >= size) {
          cur_ind++;
        } else {
          col->set_any(offset[cur_ind++],
                       std::move(AnyConverter<Date>::to_any(casted->Value(k))));
        }
      }
    }
  } else {
    LOG(FATAL) << "Not implemented: converting " << type->ToString() << " to "
               << col_type;
  }
}

void set_column_from_timestamp_array_to_day(
    gs::ColumnBase* col, std::shared_ptr<arrow::ChunkedArray> array,
    const std::vector<size_t>& offset) {
  auto type = array->type();
  auto col_type = col->type();
  auto size = col->size();
  size_t cur_ind = 0;
  if (type->Equals(arrow::timestamp(arrow::TimeUnit::type::MILLI))) {
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::TimestampArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        if (offset[cur_ind] >= size) {
          cur_ind++;
        } else {
          col->set_any(offset[cur_ind++],
                       std::move(AnyConverter<Day>::to_any(casted->Value(k))));
        }
      }
    }
  } else {
    LOG(FATAL) << "Not implemented: converting " << type->ToString() << " to "
               << col_type;
  }
}

void check_edge_invariant(
    const Schema& schema,
    const std::vector<std::tuple<size_t, std::string, std::string>>&
        column_mappings,
    size_t src_col_ind, size_t dst_col_ind, label_t src_label_i,
    label_t dst_label_i, label_t edge_label_i) {
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
    std::function<std::vector<std::shared_ptr<IRecordBatchSupplier>>(
        label_t, const std::string&, const LoadingConfig&, int)>
        supplier_creator) {
  auto primary_keys = schema_.get_vertex_primary_key(v_label_id);

  if (primary_keys.size() != 1) {
    LOG(FATAL) << "Only support one primary key for vertex.";
  }
  auto type = std::get<0>(primary_keys[0]);
  if (type != PropertyType::kInt64 && type != PropertyType::kStringView &&
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
  } else if (type == PropertyType::kInt32) {
    addVertexRecordBatchImpl<int32_t>(v_label_id, v_files, supplier_creator);
  } else if (type == PropertyType::kUInt32) {
    addVertexRecordBatchImpl<uint32_t>(v_label_id, v_files, supplier_creator);
  } else if (type == PropertyType::kUInt64) {
    addVertexRecordBatchImpl<uint64_t>(v_label_id, v_files, supplier_creator);
  } else if (type.type_enum == impl::PropertyTypeImpl::kVarChar ||
             type.type_enum == impl::PropertyTypeImpl::kStringView) {
    addVertexRecordBatchImpl<std::string_view>(v_label_id, v_files,
                                               supplier_creator);
  } else {
    LOG(FATAL) << "Unsupported primary key type for vertex, type: " << type
               << ", label: " << v_label_name;
  }
  VLOG(10) << "Finish init vertices for label " << v_label_name;
}

void AbstractArrowFragmentLoader::AddEdgesRecordBatch(
    label_t src_label_i, label_t dst_label_i, label_t edge_label_i,
    const std::vector<std::string>& filenames,
    std::function<std::vector<std::shared_ptr<IRecordBatchSupplier>>(
        label_t, label_t, label_t, const std::string&, const LoadingConfig&,
        int)>
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
  EdgeStrategy oe_strategy = schema_.get_outgoing_edge_strategy(
      src_label_name, dst_label_name, edge_label_name);
  EdgeStrategy ie_strategy = schema_.get_incoming_edge_strategy(
      src_label_name, dst_label_name, edge_label_name);
  bool oe_mutable = schema_.outgoing_edge_mutable(
      src_label_name, dst_label_name, edge_label_name);
  bool ie_mutable = schema_.incoming_edge_mutable(
      src_label_name, dst_label_name, edge_label_name);
  if (col_num == 0) {
    auto dual_csr = new DualCsr<grape::EmptyType>(oe_strategy, ie_strategy,
                                                  oe_mutable, ie_mutable);
    basic_fragment_loader_.set_csr(src_label_i, dst_label_i, edge_label_i,
                                   dual_csr);
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<grape::EmptyType>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesRecordBatchImpl<grape::EmptyType>(
          src_label_i, dst_label_i, edge_label_i, filenames, supplier_creator);
    }
  } else if (col_num == 1) {
    if (property_types[0] == PropertyType::kBool) {
      auto dual_csr =
          new DualCsr<bool>(oe_strategy, ie_strategy, oe_mutable, ie_mutable);
      basic_fragment_loader_.set_csr(src_label_i, dst_label_i, edge_label_i,
                                     dual_csr);
      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<bool>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<bool>(src_label_i, dst_label_i, edge_label_i,
                                      filenames, supplier_creator);
      }
    } else if (property_types[0] == PropertyType::kDate) {
      auto dual_csr =
          new DualCsr<Date>(oe_strategy, ie_strategy, oe_mutable, ie_mutable);
      basic_fragment_loader_.set_csr(src_label_i, dst_label_i, edge_label_i,
                                     dual_csr);

      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<Date>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<Date>(src_label_i, dst_label_i, edge_label_i,
                                      filenames, supplier_creator);
      }
    } else if (property_types[0] == PropertyType::kInt32) {
      auto dual_csr = new DualCsr<int32_t>(oe_strategy, ie_strategy, oe_mutable,
                                           ie_mutable);
      basic_fragment_loader_.set_csr(src_label_i, dst_label_i, edge_label_i,
                                     dual_csr);

      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<int32_t>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<int32_t>(src_label_i, dst_label_i, edge_label_i,
                                         filenames, supplier_creator);
      }
    } else if (property_types[0] == PropertyType::kUInt32) {
      auto dual_csr = new DualCsr<uint32_t>(oe_strategy, ie_strategy,
                                            oe_mutable, ie_mutable);
      basic_fragment_loader_.set_csr(src_label_i, dst_label_i, edge_label_i,
                                     dual_csr);

      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<uint32_t>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<uint32_t>(src_label_i, dst_label_i,
                                          edge_label_i, filenames,
                                          supplier_creator);
      }
    } else if (property_types[0] == PropertyType::kInt64) {
      auto dual_csr = new DualCsr<int64_t>(oe_strategy, ie_strategy, oe_mutable,
                                           ie_mutable);
      basic_fragment_loader_.set_csr(src_label_i, dst_label_i, edge_label_i,
                                     dual_csr);

      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<int64_t>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<int64_t>(src_label_i, dst_label_i, edge_label_i,
                                         filenames, supplier_creator);
      }
    } else if (property_types[0] == PropertyType::kUInt64) {
      auto dual_csr = new DualCsr<uint64_t>(oe_strategy, ie_strategy,
                                            oe_mutable, ie_mutable);
      basic_fragment_loader_.set_csr(src_label_i, dst_label_i, edge_label_i,
                                     dual_csr);

      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<uint64_t>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<uint64_t>(src_label_i, dst_label_i,
                                          edge_label_i, filenames,
                                          supplier_creator);
      }
    } else if (property_types[0] == PropertyType::kDouble) {
      auto dual_csr =
          new DualCsr<double>(oe_strategy, ie_strategy, oe_mutable, ie_mutable);
      basic_fragment_loader_.set_csr(src_label_i, dst_label_i, edge_label_i,
                                     dual_csr);
      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<double>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<double>(src_label_i, dst_label_i, edge_label_i,
                                        filenames, supplier_creator);
      }
    } else if (property_types[0] == PropertyType::kFloat) {
      auto dual_csr =
          new DualCsr<float>(oe_strategy, ie_strategy, oe_mutable, ie_mutable);
      basic_fragment_loader_.set_csr(src_label_i, dst_label_i, edge_label_i,
                                     dual_csr);

      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<float>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<float>(src_label_i, dst_label_i, edge_label_i,
                                       filenames, supplier_creator);
      }
    } else if (property_types[0].type_enum ==
                   impl::PropertyTypeImpl::kVarChar ||
               property_types[0].type_enum ==
                   impl::PropertyTypeImpl::kStringView) {
      // Both varchar and string are treated as string. For String, we use the
      // default max length defined in PropertyType::STRING_DEFAULT_MAX_LENGTH
      uint16_t max_length = PropertyType::STRING_DEFAULT_MAX_LENGTH;
      if (property_types[0].type_enum == impl::PropertyTypeImpl::kVarChar) {
        max_length = property_types[0].additional_type_info.max_length;
      }
      auto dual_csr =
          new DualCsr<std::string_view>(oe_strategy, ie_strategy, max_length);
      basic_fragment_loader_.set_csr(src_label_i, dst_label_i, edge_label_i,
                                     dual_csr);
      if (filenames.empty()) {
        basic_fragment_loader_.AddNoPropEdgeBatch<std::string_view>(
            src_label_i, dst_label_i, edge_label_i);
      } else {
        addEdgesRecordBatchImpl<std::string_view>(src_label_i, dst_label_i,
                                                  edge_label_i, filenames,
                                                  supplier_creator);
      }
    } else {
      LOG(FATAL) << "Unsupported edge property type." << property_types[0];
    }
  } else {
    const auto& props = schema_.get_edge_properties(
        src_label_name, dst_label_name, edge_label_name);
    const auto& prop_names = schema_.get_edge_property_names(
        src_label_name, dst_label_name, edge_label_name);
    auto dual_csr = new DualCsr<RecordView>(oe_strategy, ie_strategy,
                                            prop_names, props, {});
    basic_fragment_loader_.set_csr(src_label_i, dst_label_i, edge_label_i,
                                   dual_csr);
    if (filenames.empty()) {
      LOG(FATAL) << "No edge files found for src label: " << src_label_name
                 << " dst label: " << dst_label_name
                 << " edge label: " << edge_label_name;
    } else {
      addEdgesRecordBatchImpl<RecordView>(
          src_label_i, dst_label_i, edge_label_i, filenames, supplier_creator);
    }
  }
}

}  // namespace gs
