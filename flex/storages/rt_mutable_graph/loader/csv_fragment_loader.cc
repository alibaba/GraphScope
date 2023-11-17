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

#include "flex/storages/rt_mutable_graph/loader/csv_fragment_loader.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"

namespace gs {

static std::vector<std::string> read_header(const std::string& file_name,
                                            char delimiter) {
  // read the header line of the file, and split into vector to string by
  // delimiter
  std::vector<std::string> res_vec;
  std::ifstream file(file_name);
  std::string line;
  if (file.is_open()) {
    if (std::getline(file, line)) {
      std::stringstream ss(line);
      std::string token;
      while (std::getline(ss, token, delimiter)) {
        // trim the token
        token.erase(token.find_last_not_of(" \n\r\t") + 1);
        res_vec.push_back(token);
      }
    } else {
      LOG(FATAL) << "Fail to read header line of file: " << file_name;
    }
    file.close();
  } else {
    LOG(FATAL) << "Fail to open file: " << file_name;
  }
  return res_vec;
}

static bool check_primary_key_type(std::shared_ptr<arrow::DataType> data_type) {
  if (data_type->Equals(arrow::int64()) || data_type->Equals(arrow::uint64()) ||
      data_type->Equals(arrow::int32()) || data_type->Equals(arrow::uint32()) ||
      data_type->Equals(arrow::utf8()) ||
      data_type->Equals(arrow::large_utf8())) {
    return true;
  } else {
    return false;
  }
}

static void put_delimiter_option(const LoadingConfig& loading_config,
                                 arrow::csv::ParseOptions& parse_options) {
  auto delimiter_str = loading_config.GetDelimiter();
  if (delimiter_str.size() != 1) {
    LOG(FATAL) << "Delimiter should be a single character";
  }
  parse_options.delimiter = delimiter_str[0];
}

static bool put_skip_rows_option(const LoadingConfig& loading_config,
                                 arrow::csv::ReadOptions& read_options) {
  bool header_row = loading_config.GetHasHeaderRow();
  if (header_row) {
    read_options.skip_rows = 1;
  } else {
    read_options.skip_rows = 0;
  }
  return header_row;
}

static void put_escape_char_option(const LoadingConfig& loading_config,
                                   arrow::csv::ParseOptions& parse_options) {
  auto escape_str = loading_config.GetEscapeChar();
  if (escape_str.size() != 1) {
    LOG(FATAL) << "Escape char should be a single character";
  }
  parse_options.escape_char = escape_str[0];
  parse_options.escaping = loading_config.GetIsEscaping();
}

static void put_block_size_option(const LoadingConfig& loading_config,
                                  arrow::csv::ReadOptions& read_options) {
  auto batch_size = loading_config.GetBatchSize();
  if (batch_size <= 0) {
    LOG(FATAL) << "Block size should be positive";
  }
  read_options.block_size = batch_size;
}

static void put_quote_char_option(const LoadingConfig& loading_config,
                                  arrow::csv::ParseOptions& parse_options) {
  auto quoting_str = loading_config.GetQuotingChar();
  if (quoting_str.size() != 1) {
    LOG(FATAL) << "Quote char should be a single character";
  }
  parse_options.quote_char = quoting_str[0];
  parse_options.quoting = loading_config.GetIsQuoting();
  parse_options.double_quote = loading_config.GetIsDoubleQuoting();
}

static void put_boolean_option(arrow::csv::ConvertOptions& convert_options) {
  convert_options.true_values.emplace_back("True");
  convert_options.true_values.emplace_back("true");
  convert_options.true_values.emplace_back("TRUE");
  convert_options.false_values.emplace_back("False");
  convert_options.false_values.emplace_back("false");
  convert_options.false_values.emplace_back("FALSE");
}

static void put_column_names_option(const LoadingConfig& loading_config,
                                    bool header_row,
                                    const std::string& file_path,
                                    char delimiter,
                                    arrow::csv::ReadOptions& read_options) {
  std::vector<std::string> all_column_names;
  if (header_row) {
    all_column_names = read_header(file_path, delimiter);
    // It is possible that there exists duplicate column names in the header,
    // transform them to unique names
    std::unordered_map<std::string, int> name_count;
    for (auto& name : all_column_names) {
      if (name_count.find(name) == name_count.end()) {
        name_count[name] = 1;
      } else {
        name_count[name]++;
      }
    }
    VLOG(10) << "before Got all column names: " << all_column_names.size()
             << gs::to_string(all_column_names);
    for (auto i = 0; i < all_column_names.size(); ++i) {
      auto& name = all_column_names[i];
      if (name_count[name] > 1) {
        auto cur_cnt = name_count[name];
        name_count[name] -= 1;
        all_column_names[i] = name + "_" + std::to_string(cur_cnt);
      }
    }
    VLOG(10) << "Got all column names: " << all_column_names.size()
             << gs::to_string(all_column_names);
  } else {
    // just get the number of columns.
    size_t num_cols = 0;
    {
      auto tmp = read_header(file_path, delimiter);
      num_cols = tmp.size();
    }
    all_column_names.resize(num_cols);
    for (auto i = 0; i < all_column_names.size(); ++i) {
      all_column_names[i] = std::string("f") + std::to_string(i);
    }
  }
  read_options.column_names = all_column_names;
  VLOG(10) << "Got all column names: " << all_column_names.size()
           << gs::to_string(all_column_names);
}

static void check_edge_invariant(
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

static void set_vertex_properties(gs::ColumnBase* col,
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
  } else if (col_type == PropertyType::kString) {
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

template <typename PK_T, typename EDATA_T>
static void append_edges(
    std::shared_ptr<arrow::Array> src_col,
    std::shared_ptr<arrow::Array> dst_col, const LFIndexer<vid_t>& src_indexer,
    const LFIndexer<vid_t>& dst_indexer,
    std::vector<std::shared_ptr<arrow::Array>>& edata_cols,
    std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& parsed_edges,
    std::vector<int32_t>& ie_degree, std::vector<int32_t>& oe_degree) {
  CHECK(src_col->length() == dst_col->length());
  auto indexer_check_lambda = [](const LFIndexer<vid_t>& cur_indexer,
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
  };

  indexer_check_lambda(src_indexer, src_col);
  indexer_check_lambda(dst_indexer, dst_col);
  auto old_size = parsed_edges.size();
  parsed_edges.resize(old_size + src_col->length());
  VLOG(10) << "resize parsed_edges from" << old_size << " to "
           << parsed_edges.size();

  auto _append = [&](bool is_dst) {
    size_t cur_ind = old_size;
    const auto& col = is_dst ? dst_col : src_col;
    const auto& indexer = is_dst ? dst_indexer : src_indexer;
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
          is_dst ? ie_degree[vid]++ : oe_degree[vid]++;
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
          is_dst ? ie_degree[vid]++ : oe_degree[vid]++;
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
        is_dst ? ie_degree[vid]++ : oe_degree[vid]++;
      }
    }
  };

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
          std::get<2>(parsed_edges[cur_ind++]) = data->GetView(j);
        } else {
          std::get<2>(parsed_edges[cur_ind++]) = data->Value(j);
        }
      }
      VLOG(10) << "Finish inserting:  " << src_col->length() << " edges";
    }
  });
  auto src_col_thread = std::thread([&]() { _append(false); });
  auto dst_col_thread = std::thread([&]() { _append(true); });
  src_col_thread.join();
  dst_col_thread.join();
  edata_col_thread.join();
}

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
          LOG(FATAL) << "Duplicate vertex id: " << casted_array->Value(i)
                     << "..";
        }
        vids.emplace_back(vid);
      }
    } else {
      if (col->type()->Equals(arrow::utf8())) {
        auto casted_array = std::static_pointer_cast<arrow::StringArray>(col);
        for (auto i = 0; i < row_num; ++i) {
          auto str = casted_array->GetView(i);
          std::string_view str_view(str.data(), str.size());
          if (!indexer.add(str_view, vid)) {
            LOG(FATAL) << "Duplicate vertex id: " << str_view << "..";
          }
          vids.emplace_back(vid);
        }
      } else if (col->type()->Equals(arrow::large_utf8())) {
        auto casted_array =
            std::static_pointer_cast<arrow::LargeStringArray>(col);
        for (auto i = 0; i < row_num; ++i) {
          auto str = casted_array->GetView(i);
          std::string_view str_view(str.data(), str.size());
          if (!indexer.add(str_view, vid)) {
            LOG(FATAL) << "Duplicate vertex id: " << str_view << "..";
          }
          vids.emplace_back(vid);
        }
      } else {
        LOG(FATAL) << "Not support type: " << col->type()->ToString();
      }
    }
  }
};

template <typename KEY_T>
void CSVFragmentLoader::addVertexBatch(
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

// Iterate over all record batches read from file.
void ForEachRecordBatch(
    const std::string& path, const arrow::csv::ConvertOptions& convert_options,
    const arrow::csv::ReadOptions& read_options,
    const arrow::csv::ParseOptions& parse_options,
    std::function<void(std::shared_ptr<arrow::RecordBatch>, bool)> func,
    bool stream) {
  auto read_result = arrow::io::ReadableFile::Open(path);
  if (!read_result.ok()) {
    LOG(FATAL) << "Failed to open file: " << path
               << " error: " << read_result.status().message();
  }
  std::shared_ptr<arrow::io::ReadableFile> file = read_result.ValueOrDie();
  bool first_batch = true;
  if (stream) {
    auto res = arrow::csv::StreamingReader::Make(
        arrow::io::default_io_context(), file, read_options, parse_options,
        convert_options);
    if (!res.ok()) {
      LOG(FATAL) << "Failed to create streaming reader for file: " << path
                 << " error: " << res.status().message();
    }
    auto reader = res.ValueOrDie();
    while (true) {
      std::shared_ptr<arrow::RecordBatch> batch;
      auto status = reader->ReadNext(&batch);
      if (!status.ok()) {
        LOG(FATAL) << "Failed to read batch from file: " << path
                   << " error: " << status.message();
      }
      if (batch == nullptr) {
        break;
      }

      func(batch, first_batch);
      if (first_batch) {
        first_batch = false;
      }
    }
  } else {
    auto res = arrow::csv::TableReader::Make(arrow::io::default_io_context(),
                                             file, read_options, parse_options,
                                             convert_options);

    if (!res.ok()) {
      LOG(FATAL) << "Failed to create table reader for file: " << path
                 << " error: " << res.status().message();
    }
    auto reader = res.ValueOrDie();

    auto result = reader->Read();
    auto status = result.status();
    if (!status.ok()) {
      LOG(FATAL) << "Failed to read table from file: " << path
                 << " error: " << status.message();
    }
    std::shared_ptr<arrow::Table> table = result.ValueOrDie();

    arrow::TableBatchReader batch_reader(*table);
    while (true) {
      std::shared_ptr<arrow::RecordBatch> batch;
      auto status = batch_reader.ReadNext(&batch);
      if (!status.ok()) {
        LOG(FATAL) << "Failed to read batch from file: " << path
                   << " error: " << status.message();
      }
      if (batch == nullptr) {
        break;
      }
      func(batch, first_batch);
      if (first_batch) {
        first_batch = false;
      }
    }
  }
}

template <typename KEY_T>
void CSVFragmentLoader::addVerticesImpl(label_t v_label_id,
                                        const std::string& v_label_name,
                                        const std::vector<std::string> v_files,
                                        IdIndexer<KEY_T, vid_t>& indexer) {
  VLOG(10) << "Parsing vertex file:" << v_files.size() << " for label "
           << v_label_name;

  for (auto& v_file : v_files) {
    arrow::csv::ConvertOptions convert_options;
    arrow::csv::ReadOptions read_options;
    arrow::csv::ParseOptions parse_options;
    fillVertexReaderMeta(read_options, parse_options, convert_options, v_file,
                         v_label_id);
    auto primary_key = schema_.get_vertex_primary_key(v_label_id)[0];
    auto primary_key_name = std::get<1>(primary_key);
    size_t primary_key_ind = std::get<2>(primary_key);
    ForEachRecordBatch(
        v_file, convert_options, read_options, parse_options,
        [&](std::shared_ptr<arrow::RecordBatch> batch, bool first_batch) {
          if (first_batch) {
            auto header = batch->schema()->field_names();
            auto schema_column_names =
                schema_.get_vertex_property_names(v_label_id);
            CHECK(schema_column_names.size() + 1 == header.size())
                << "File header of size: " << header.size()
                << " does not match schema column size: "
                << schema_column_names.size() + 1;
          }
          auto columns = batch->columns();
          CHECK(primary_key_ind < columns.size());
          auto primary_key_column = columns[primary_key_ind];
          auto other_columns_array = columns;
          other_columns_array.erase(other_columns_array.begin() +
                                    primary_key_ind);
          addVertexBatch(v_label_id, indexer, primary_key_column,
                         other_columns_array);
        },
        loading_config_.GetIsBatchReader());
  }

  VLOG(10) << "Finish parsing vertex file:" << v_files.size() << " for label "
           << v_label_name;
}

void CSVFragmentLoader::addVertices(label_t v_label_id,
                                    const std::vector<std::string>& v_files) {
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
    IdIndexer<int64_t, vid_t> indexer;
    addVerticesImpl<int64_t>(v_label_id, v_label_name, v_files, indexer);
    if (indexer.bucket_count() == 0) {
      indexer._rehash(schema_.get_max_vnum(v_label_name));
    }
    basic_fragment_loader_.FinishAddingVertex<int64_t>(v_label_id, indexer);
  } else if (type == PropertyType::kString) {
    IdIndexer<std::string_view, vid_t> indexer;
    addVerticesImpl<std::string_view>(v_label_id, v_label_name, v_files,
                                      indexer);
    if (indexer.bucket_count() == 0) {
      indexer._rehash(schema_.get_max_vnum(v_label_name));
    }
    basic_fragment_loader_.FinishAddingVertex<std::string_view>(v_label_id,
                                                                indexer);
  } else if (type == PropertyType::kInt32) {
    IdIndexer<int32_t, vid_t> indexer;
    addVerticesImpl<int32_t>(v_label_id, v_label_name, v_files, indexer);
    if (indexer.bucket_count() == 0) {
      indexer._rehash(schema_.get_max_vnum(v_label_name));
    }
    basic_fragment_loader_.FinishAddingVertex<int32_t>(v_label_id, indexer);
  } else if (type == PropertyType::kUInt32) {
    IdIndexer<uint32_t, vid_t> indexer;
    addVerticesImpl<uint32_t>(v_label_id, v_label_name, v_files, indexer);
    if (indexer.bucket_count() == 0) {
      indexer._rehash(schema_.get_max_vnum(v_label_name));
    }
    basic_fragment_loader_.FinishAddingVertex<uint32_t>(v_label_id, indexer);
  } else if (type == PropertyType::kUInt64) {
    IdIndexer<uint64_t, vid_t> indexer;
    addVerticesImpl<uint64_t>(v_label_id, v_label_name, v_files, indexer);
    if (indexer.bucket_count() == 0) {
      indexer._rehash(schema_.get_max_vnum(v_label_name));
    }
    basic_fragment_loader_.FinishAddingVertex<uint64_t>(v_label_id, indexer);
  }
  VLOG(10) << "Finish init vertices for label " << v_label_name;
}

template <typename EDATA_T>
void CSVFragmentLoader::addEdgesImpl(label_t src_label_id, label_t dst_label_id,
                                     label_t e_label_id,
                                     const std::vector<std::string>& e_files) {
  auto edge_column_mappings = loading_config_.GetEdgeColumnMappings(
      src_label_id, dst_label_id, e_label_id);
  auto src_dst_col_pair =
      loading_config_.GetEdgeSrcDstCol(src_label_id, dst_label_id, e_label_id);
  if (src_dst_col_pair.first.size() != 1 ||
      src_dst_col_pair.second.size() != 1) {
    LOG(FATAL) << "We currently only support one src primary key and one "
                  "dst primary key";
  }
  size_t src_col_ind = src_dst_col_pair.first[0];
  size_t dst_col_ind = src_dst_col_pair.second[0];
  CHECK(src_col_ind != dst_col_ind);

  check_edge_invariant(schema_, edge_column_mappings, src_col_ind, dst_col_ind,
                       src_label_id, dst_label_id, e_label_id);

  std::vector<std::tuple<vid_t, vid_t, EDATA_T>> parsed_edges;
  std::vector<int32_t> ie_degree, oe_degree;
  const auto& src_indexer = basic_fragment_loader_.GetLFIndexer(src_label_id);
  const auto& dst_indexer = basic_fragment_loader_.GetLFIndexer(dst_label_id);
  ie_degree.resize(dst_indexer.size());
  oe_degree.resize(src_indexer.size());
  VLOG(10) << "src indexer size: " << src_indexer.size()
           << " dst indexer size: " << dst_indexer.size();

  for (auto filename : e_files) {
    arrow::csv::ConvertOptions convert_options;
    arrow::csv::ReadOptions read_options;
    arrow::csv::ParseOptions parse_options;
    fillEdgeReaderMeta(read_options, parse_options, convert_options, filename,
                       src_label_id, dst_label_id, e_label_id);
    ForEachRecordBatch(
        filename, convert_options, read_options, parse_options,
        [&](std::shared_ptr<arrow::RecordBatch> batch, bool first_batch) {
          if (first_batch) {
            auto header = batch->schema()->field_names();
            auto schema_column_names = schema_.get_edge_property_names(
                src_label_id, dst_label_id, e_label_id);
            auto schema_column_types = schema_.get_edge_properties(
                src_label_id, dst_label_id, e_label_id);
            CHECK(schema_column_names.size() + 2 == header.size())
                << "schema size: " << schema_column_names.size()
                << " neq header size: " << header.size();
          }
          // copy the table to csr.
          auto columns = batch->columns();
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
          CHECK(property_cols.size() <= 1)
              << "Currently only support at most one property on edge";

          // add edges to vector
          CHECK(src_col->length() == dst_col->length());
          if (src_col_type->Equals(arrow::int64())) {
            append_edges<int64_t, EDATA_T>(src_col, dst_col, src_indexer,
                                           dst_indexer, property_cols,
                                           parsed_edges, ie_degree, oe_degree);
          } else if (src_col_type->Equals(arrow::uint64())) {
            append_edges<uint64_t, EDATA_T>(src_col, dst_col, src_indexer,
                                            dst_indexer, property_cols,
                                            parsed_edges, ie_degree, oe_degree);
          } else if (src_col_type->Equals(arrow::int32())) {
            append_edges<int32_t, EDATA_T>(src_col, dst_col, src_indexer,
                                           dst_indexer, property_cols,
                                           parsed_edges, ie_degree, oe_degree);
          } else if (src_col_type->Equals(arrow::uint32())) {
            append_edges<uint32_t, EDATA_T>(src_col, dst_col, src_indexer,
                                            dst_indexer, property_cols,
                                            parsed_edges, ie_degree, oe_degree);
          } else {
            // must be string
            append_edges<std::string_view, EDATA_T>(
                src_col, dst_col, src_indexer, dst_indexer, property_cols,
                parsed_edges, ie_degree, oe_degree);
          }
        },
        loading_config_.GetIsBatchReader());
  }

  basic_fragment_loader_.PutEdges(src_label_id, dst_label_id, e_label_id,
                                  parsed_edges, ie_degree, oe_degree);

  VLOG(10) << "Finish putting: " << parsed_edges.size() << " edges";
}

void CSVFragmentLoader::addEdges(label_t src_label_i, label_t dst_label_i,
                                 label_t edge_label_i,
                                 const std::vector<std::string>& filenames) {
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
      addEdgesImpl<grape::EmptyType>(src_label_i, dst_label_i, edge_label_i,
                                     filenames);
    }
  } else if (property_types[0] == PropertyType::kBool) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<bool>(src_label_i, dst_label_i,
                                                      edge_label_i);
    } else {
      addEdgesImpl<bool>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else if (property_types[0] == PropertyType::kDate) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<Date>(src_label_i, dst_label_i,
                                                      edge_label_i);
    } else {
      addEdgesImpl<Date>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else if (property_types[0] == PropertyType::kInt32) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<int32_t>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesImpl<int32_t>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else if (property_types[0] == PropertyType::kUInt32) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<uint32_t>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesImpl<uint32_t>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else if (property_types[0] == PropertyType::kInt64) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<int64_t>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesImpl<int64_t>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else if (property_types[0] == PropertyType::kUInt64) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<uint64_t>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesImpl<uint64_t>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else if (property_types[0] == PropertyType::kString) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<std::string>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      LOG(FATAL) << "Unsupported edge property type.";
    }
  } else if (property_types[0] == PropertyType::kDouble) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<double>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesImpl<double>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else if (property_types[0] == PropertyType::kFloat) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<float>(src_label_i, dst_label_i,
                                                       edge_label_i);
    } else {
      addEdgesImpl<float>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else {
    LOG(FATAL) << "Unsupported edge property type." << property_types[0];
  }
}

void CSVFragmentLoader::loadVertices() {
  auto vertex_sources = loading_config_.GetVertexLoadingMeta();
  if (vertex_sources.empty()) {
    LOG(INFO) << "Skip loading vertices since no vertex source is specified.";
    return;
  }

  if (thread_num_ == 1) {
    LOG(INFO) << "Loading vertices with single thread...";
    for (auto iter = vertex_sources.begin(); iter != vertex_sources.end();
         ++iter) {
      auto v_label_id = iter->first;
      auto v_files = iter->second;
      addVertices(v_label_id, v_files);
    }
  } else {
    // copy vertex_sources and edge sources to vector, since we need to
    // use multi-thread loading.
    std::vector<std::pair<label_t, std::vector<std::string>>> vertex_files;
    for (auto iter = vertex_sources.begin(); iter != vertex_sources.end();
         ++iter) {
      vertex_files.emplace_back(iter->first, iter->second);
    }
    LOG(INFO) << "Parallel loading with " << thread_num_ << " threads, "
              << " " << vertex_files.size() << " vertex files, ";
    std::atomic<size_t> v_ind(0);
    std::vector<std::thread> threads(thread_num_);
    for (int i = 0; i < thread_num_; ++i) {
      threads[i] = std::thread([&]() {
        while (true) {
          size_t cur = v_ind.fetch_add(1);
          if (cur >= vertex_files.size()) {
            break;
          }
          auto v_label_id = vertex_files[cur].first;
          addVertices(v_label_id, vertex_files[cur].second);
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }

    LOG(INFO) << "Finished loading vertices";
  }
}

void CSVFragmentLoader::fillVertexReaderMeta(
    arrow::csv::ReadOptions& read_options,
    arrow::csv::ParseOptions& parse_options,
    arrow::csv::ConvertOptions& convert_options, const std::string& v_file,
    label_t v_label) const {
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCTimeStampParser>());
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCLongDateParser>());
  convert_options.timestamp_parsers.emplace_back(
      arrow::TimestampParser::MakeISO8601());
  // BOOLEAN parser
  put_boolean_option(convert_options);

  put_delimiter_option(loading_config_, parse_options);
  bool header_row = put_skip_rows_option(loading_config_, read_options);
  put_column_names_option(loading_config_, header_row, v_file,
                          parse_options.delimiter, read_options);
  put_escape_char_option(loading_config_, parse_options);
  put_quote_char_option(loading_config_, parse_options);
  put_block_size_option(loading_config_, read_options);

  // parse all column_names

  std::vector<std::string> included_col_names;
  std::vector<size_t> included_col_indices;
  std::vector<std::string> mapped_property_names;

  auto cur_label_col_mapping = loading_config_.GetVertexColumnMappings(v_label);
  auto primary_keys = schema_.get_vertex_primary_key(v_label);
  CHECK(primary_keys.size() == 1);
  auto primary_key = primary_keys[0];

  if (cur_label_col_mapping.size() == 0) {
    // use default mapping, we assume the order of the columns in the file is
    // the same as the order of the properties in the schema, except for
    // primary key.
    auto primary_key_name = std::get<1>(primary_key);
    auto primary_key_ind = std::get<2>(primary_key);
    auto property_names = schema_.get_vertex_property_names(v_label);
    // for example, schema is : (name,age)
    // file header is (id,name,age), the primary key is id.
    // so, the mapped_property_names are: (id,name,age)
    CHECK(property_names.size() + 1 == read_options.column_names.size())
        << gs::to_string(property_names)
        << ", read options: " << gs::to_string(read_options.column_names);
    // insert primary_key to property_names
    property_names.insert(property_names.begin() + primary_key_ind,
                          primary_key_name);

    for (auto i = 0; i < read_options.column_names.size(); ++i) {
      included_col_names.emplace_back(read_options.column_names[i]);
      included_col_indices.emplace_back(i);
      // We assume the order of the columns in the file is the same as the
      // order of the properties in the schema, except for primary key.
      mapped_property_names.emplace_back(property_names[i]);
    }
  } else {
    for (auto i = 0; i < cur_label_col_mapping.size(); ++i) {
      auto& [col_id, col_name, property_name] = cur_label_col_mapping[i];
      if (col_name.empty()) {
        // use default mapping
        col_name = read_options.column_names[col_id];
      }
      included_col_names.emplace_back(col_name);
      included_col_indices.emplace_back(col_id);
      mapped_property_names.emplace_back(property_name);
    }
  }

  VLOG(10) << "Include columns: " << included_col_names.size()
           << gs::to_string(included_col_names);
  // if empty, then means need all columns
  convert_options.include_columns = included_col_names;

  // put column_types, col_name : col_type
  std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> arrow_types;
  {
    auto property_types = schema_.get_vertex_properties(v_label);
    auto property_names = schema_.get_vertex_property_names(v_label);
    CHECK(property_types.size() == property_names.size());

    for (auto i = 0; i < property_types.size(); ++i) {
      // for each schema' property name, get the index of the column in
      // vertex_column mapping, and bind the type with the column name
      auto property_type = property_types[i];
      auto property_name = property_names[i];
      size_t ind = mapped_property_names.size();
      for (auto i = 0; i < mapped_property_names.size(); ++i) {
        if (mapped_property_names[i] == property_name) {
          ind = i;
          break;
        }
      }
      if (ind == mapped_property_names.size()) {
        LOG(FATAL) << "The specified property name: " << property_name
                   << " does not exist in the vertex column mapping for "
                      "vertex label: "
                   << schema_.get_vertex_label_name(v_label)
                   << " please "
                      "check your configuration";
      }
      VLOG(10) << "vertex_label: " << schema_.get_vertex_label_name(v_label)
               << " property_name: " << property_name
               << " property_type: " << property_type << " ind: " << ind;
      arrow_types.insert(
          {included_col_names[ind], PropertyTypeToArrowType(property_type)});
    }
    {
      // add primary key types;
      auto primary_key_name = std::get<1>(primary_key);
      auto primary_key_type = std::get<0>(primary_key);
      size_t ind = mapped_property_names.size();
      for (auto i = 0; i < mapped_property_names.size(); ++i) {
        if (mapped_property_names[i] == primary_key_name) {
          ind = i;
          break;
        }
      }
      if (ind == mapped_property_names.size()) {
        LOG(FATAL) << "The specified property name: " << primary_key_name
                   << " does not exist in the vertex column mapping, please "
                      "check your configuration";
      }
      arrow_types.insert(
          {included_col_names[ind], PropertyTypeToArrowType(primary_key_type)});
    }

    convert_options.column_types = arrow_types;
  }
}

void CSVFragmentLoader::fillEdgeReaderMeta(
    arrow::csv::ReadOptions& read_options,
    arrow::csv::ParseOptions& parse_options,
    arrow::csv::ConvertOptions& convert_options, const std::string& e_file,
    label_t src_label_id, label_t dst_label_id, label_t label_id) const {
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCTimeStampParser>());
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCLongDateParser>());
  convert_options.timestamp_parsers.emplace_back(
      arrow::TimestampParser::MakeISO8601());
  put_boolean_option(convert_options);

  put_delimiter_option(loading_config_, parse_options);
  bool header_row = put_skip_rows_option(loading_config_, read_options);
  put_column_names_option(loading_config_, header_row, e_file,
                          parse_options.delimiter, read_options);
  put_escape_char_option(loading_config_, parse_options);
  put_quote_char_option(loading_config_, parse_options);
  put_block_size_option(loading_config_, read_options);

  auto src_dst_cols =
      loading_config_.GetEdgeSrcDstCol(src_label_id, dst_label_id, label_id);

  // parse all column_names
  // Get all column names(header, and always skip the first row)
  std::vector<std::string> included_col_names;
  std::vector<std::string> mapped_property_names;

  {
    // add src and dst primary col, to included_columns, put src_col and
    // dst_col at the first of included_columns.
    CHECK(src_dst_cols.first.size() == 1 && src_dst_cols.second.size() == 1);
    auto src_col_ind = src_dst_cols.first[0];
    auto dst_col_ind = src_dst_cols.second[0];
    CHECK(src_col_ind >= 0 && src_col_ind < read_options.column_names.size());
    CHECK(dst_col_ind >= 0 && dst_col_ind < read_options.column_names.size());

    included_col_names.emplace_back(read_options.column_names[src_col_ind]);
    included_col_names.emplace_back(read_options.column_names[dst_col_ind]);
  }

  auto cur_label_col_mapping = loading_config_.GetEdgeColumnMappings(
      src_label_id, dst_label_id, label_id);
  if (cur_label_col_mapping.empty()) {
    // use default mapping, we assume the order of the columns in the file is
    // the same as the order of the properties in the schema,
    auto edge_prop_names =
        schema_.get_edge_property_names(src_label_id, dst_label_id, label_id);
    for (auto i = 0; i < edge_prop_names.size(); ++i) {
      auto property_name = edge_prop_names[i];
      included_col_names.emplace_back(property_name);
      mapped_property_names.emplace_back(property_name);
    }
  } else {
    // add the property columns into the included columns
    for (auto i = 0; i < cur_label_col_mapping.size(); ++i) {
      // TODO: make the property column's names are in same order with schema.
      auto& [col_id, col_name, property_name] = cur_label_col_mapping[i];
      if (col_name.empty()) {
        // use default mapping
        col_name = read_options.column_names[col_id];
      }
      included_col_names.emplace_back(col_name);
      mapped_property_names.emplace_back(property_name);
    }
  }

  VLOG(10) << "Include Edge columns: " << gs::to_string(included_col_names);
  // if empty, then means need all columns
  convert_options.include_columns = included_col_names;

  // put column_types, col_name : col_type
  std::unordered_map<std::string, std::shared_ptr<arrow::DataType>> arrow_types;
  {
    auto property_types =
        schema_.get_edge_properties(src_label_id, dst_label_id, label_id);
    auto property_names =
        schema_.get_edge_property_names(src_label_id, dst_label_id, label_id);
    CHECK(property_types.size() == property_names.size());
    for (auto i = 0; i < property_types.size(); ++i) {
      // for each schema' property name, get the index of the column in
      // vertex_column mapping, and bind the type with the column name
      auto property_type = property_types[i];
      auto property_name = property_names[i];
      size_t ind = mapped_property_names.size();
      for (auto i = 0; i < mapped_property_names.size(); ++i) {
        if (mapped_property_names[i] == property_name) {
          ind = i;
          break;
        }
      }
      if (ind == mapped_property_names.size()) {
        LOG(FATAL) << "The specified property name: " << property_name
                   << " does not exist in the vertex column mapping, please "
                      "check your configuration";
      }
      VLOG(10) << "edge_label: " << schema_.get_edge_label_name(label_id)
               << " property_name: " << property_name
               << " property_type: " << property_type << " ind: " << ind;
      arrow_types.insert({included_col_names[ind + 2],
                          PropertyTypeToArrowType(property_type)});
    }
    {
      // add src and dst primary col, to included_columns and column types.
      auto src_dst_cols = loading_config_.GetEdgeSrcDstCol(
          src_label_id, dst_label_id, label_id);
      CHECK(src_dst_cols.first.size() == 1 && src_dst_cols.second.size() == 1);
      auto src_col_ind = src_dst_cols.first[0];
      auto dst_col_ind = src_dst_cols.second[0];
      CHECK(src_col_ind >= 0 && src_col_ind < read_options.column_names.size());
      CHECK(dst_col_ind >= 0 && dst_col_ind < read_options.column_names.size());
      PropertyType src_col_type, dst_col_type;
      {
        auto src_primary_keys = schema_.get_vertex_primary_key(src_label_id);
        CHECK(src_primary_keys.size() == 1);
        src_col_type = std::get<0>(src_primary_keys[0]);
        arrow_types.insert({read_options.column_names[src_col_ind],
                            PropertyTypeToArrowType(src_col_type)});
      }
      {
        auto dst_primary_keys = schema_.get_vertex_primary_key(dst_label_id);
        CHECK(dst_primary_keys.size() == 1);
        dst_col_type = std::get<0>(dst_primary_keys[0]);
        arrow_types.insert({read_options.column_names[dst_col_ind],
                            PropertyTypeToArrowType(dst_col_type)});
      }
    }

    convert_options.column_types = arrow_types;

    VLOG(10) << "Column types: ";
    for (auto iter : arrow_types) {
      VLOG(10) << iter.first << " : " << iter.second->ToString();
    }
  }
}

void CSVFragmentLoader::loadEdges() {
  auto& edge_sources = loading_config_.GetEdgeLoadingMeta();

  if (edge_sources.empty()) {
    LOG(INFO) << "Skip loading edges since no edge source is specified.";
    return;
  }

  if (thread_num_ == 1) {
    LOG(INFO) << "Loading edges with single thread...";
    for (auto iter = edge_sources.begin(); iter != edge_sources.end(); ++iter) {
      auto& src_label_id = std::get<0>(iter->first);
      auto& dst_label_id = std::get<1>(iter->first);
      auto& e_label_id = std::get<2>(iter->first);
      auto& e_files = iter->second;

      addEdges(src_label_id, dst_label_id, e_label_id, e_files);
    }
  } else {
    std::vector<std::pair<typename LoadingConfig::edge_triplet_type,
                          std::vector<std::string>>>
        edge_files;
    for (auto iter = edge_sources.begin(); iter != edge_sources.end(); ++iter) {
      edge_files.emplace_back(iter->first, iter->second);
    }
    LOG(INFO) << "Parallel loading with " << thread_num_ << " threads, "
              << edge_files.size() << " edge files.";
    std::atomic<size_t> e_ind(0);
    std::vector<std::thread> threads(thread_num_);
    for (int i = 0; i < thread_num_; ++i) {
      threads[i] = std::thread([&]() {
        while (true) {
          size_t cur = e_ind.fetch_add(1);
          if (cur >= edge_files.size()) {
            break;
          }
          auto& edge_file = edge_files[cur];
          auto src_label_id = std::get<0>(edge_file.first);
          auto dst_label_id = std::get<1>(edge_file.first);
          auto e_label_id = std::get<2>(edge_file.first);
          auto& file_names = edge_file.second;
          addEdges(src_label_id, dst_label_id, e_label_id, file_names);
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    LOG(INFO) << "Finished loading edges";
  }
}

void CSVFragmentLoader::LoadFragment(MutablePropertyFragment& fragment) {
  loadVertices();
  loadEdges();

  return basic_fragment_loader_.LoadFragment(fragment);
}

}  // namespace gs
