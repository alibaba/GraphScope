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

static void preprocess_line(char* line) {
  size_t len = strlen(line);
  while (len >= 0) {
    if (line[len] != '\0' && line[len] != '\n' && line[len] != '\r' &&
        line[len] != ' ' && line[len] != '\t') {
      break;
    } else {
      --len;
    }
  }
  line[len + 1] = '\0';
}

static std::vector<std::string> read_header(const std::string& file_name,
                                            char delimiter) {
  char line_buf[4096];
  FILE* fin = fopen(file_name.c_str(), "r");
  if (fgets(line_buf, 4096, fin) == NULL) {
    LOG(FATAL) << "Failed to read header from file: " << file_name;
  }
  preprocess_line(line_buf);
  const char* cur = line_buf;
  std::vector<std::string> res_vec;
  while (*cur != '\0') {
    const char* tmp = cur;
    while (*tmp != '\0' && *tmp != delimiter) {
      ++tmp;
    }

    std::string_view sv(cur, tmp - cur);
    res_vec.emplace_back(sv);
    cur = tmp + 1;
  }
  return res_vec;
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
  if (col_type == PropertyType::kInt64) {
    CHECK(type == arrow::int64())
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
    CHECK(type == arrow::int32())
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
  } else if (col_type == PropertyType::kDouble) {
    CHECK(type == arrow::float64())
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
  } else if (col_type == PropertyType::kString) {
    CHECK(type == arrow::large_utf8() || type == arrow::utf8())
        << "Inconsistent data type, expect string, but got "
        << type->ToString();
    if (type == arrow::large_utf8()) {
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

template <typename EDATA_T>
static void append_edges(
    std::shared_ptr<arrow::Int64Array> src_col,
    std::shared_ptr<arrow::Int64Array> dst_col,
    const LFIndexer<vid_t>& src_indexer, const LFIndexer<vid_t>& dst_indexer,
    std::vector<std::shared_ptr<arrow::Array>>& edata_cols,
    std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& parsed_edges,
    std::vector<int32_t>& ie_degree, std::vector<int32_t>& oe_degree) {
  CHECK(src_col->length() == dst_col->length());

  auto old_size = parsed_edges.size();
  parsed_edges.resize(old_size + src_col->length());
  VLOG(10) << "resize parsed_edges from" << old_size << " to "
           << parsed_edges.size();

  auto src_col_thread = std::thread([&]() {
    size_t cur_ind = old_size;
    for (auto i = 0; i < src_col->length(); ++i) {
      auto src_vid = src_indexer.get_index(src_col->Value(i));
      std::get<0>(parsed_edges[cur_ind++]) = src_vid;
      oe_degree[src_vid]++;
    }
  });
  auto dst_col_thread = std::thread([&]() {
    size_t cur_ind = old_size;
    for (auto i = 0; i < dst_col->length(); ++i) {
      auto dst_vid = dst_indexer.get_index(dst_col->Value(i));
      std::get<1>(parsed_edges[cur_ind++]) = dst_vid;
      ie_degree[dst_vid]++;
    }
  });
  src_col_thread.join();
  dst_col_thread.join();

  // if EDATA_T is grape::EmptyType, no need to read columns
  if constexpr (!std::is_same<EDATA_T, grape::EmptyType>::value) {
    CHECK(edata_cols.size() == 1);
    auto edata_col = edata_cols[0];
    CHECK(src_col->length() == edata_col->length());
    size_t cur_ind = old_size;
    auto type = edata_col->type();
    if (type != CppTypeToArrowType<EDATA_T>::TypeValue()) {
      LOG(FATAL) << "Inconsistent data type, expect "
                 << CppTypeToArrowType<EDATA_T>::TypeValue()->ToString()
                 << ", but got " << type->ToString();
    }

    using arrow_array_type =
        typename gs::CppTypeToArrowType<EDATA_T>::ArrayType;
    // cast chunk to EDATA_T array
    auto data = std::static_pointer_cast<arrow_array_type>(edata_col);
    for (auto j = 0; j < edata_col->length(); ++j) {
      if constexpr (std::is_same<arrow_array_type, arrow::StringArray>::value ||
                    std::is_same<arrow_array_type,
                                 arrow::LargeStringArray>::value) {
        std::get<2>(parsed_edges[cur_ind++]) = data->GetString(j);
      } else {
        std::get<2>(parsed_edges[cur_ind++]) = data->Value(j);
      }
    }
    VLOG(10) << "Finish inserting:  " << src_col->length() << " edges";
  }
}

template <typename EDATA_T>
static void append_edges(
    std::shared_ptr<arrow::ChunkedArray> src_col,
    std::shared_ptr<arrow::ChunkedArray> dst_col,
    const LFIndexer<vid_t>& src_indexer, const LFIndexer<vid_t>& dst_indexer,
    std::vector<std::shared_ptr<arrow::ChunkedArray>>& edata_cols,
    std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& parsed_edges,
    std::vector<int32_t>& ie_degree, std::vector<int32_t>& oe_degree) {
  CHECK(src_col->length() == dst_col->length());
  CHECK(src_col->type() == arrow::int64());
  CHECK(dst_col->type() == arrow::int64());

  auto old_size = parsed_edges.size();
  parsed_edges.resize(old_size + src_col->length());
  VLOG(10) << "resize parsed_edges from" << old_size << " to "
           << parsed_edges.size();

  auto src_col_thread = std::thread([&]() {
    size_t cur_ind = old_size;
    for (auto i = 0; i < src_col->num_chunks(); ++i) {
      auto chunk = src_col->chunk(i);
      CHECK(chunk->type() == arrow::int64());
      auto casted_chunk = std::static_pointer_cast<arrow::Int64Array>(chunk);
      for (auto j = 0; j < casted_chunk->length(); ++j) {
        auto src_vid = src_indexer.get_index(casted_chunk->Value(j));
        std::get<0>(parsed_edges[cur_ind++]) = src_vid;
        oe_degree[src_vid]++;
      }
    }
  });
  auto dst_col_thread = std::thread([&]() {
    size_t cur_ind = old_size;
    for (auto i = 0; i < dst_col->num_chunks(); ++i) {
      auto chunk = dst_col->chunk(i);
      CHECK(chunk->type() == arrow::int64());
      auto casted_chunk = std::static_pointer_cast<arrow::Int64Array>(chunk);
      for (auto j = 0; j < casted_chunk->length(); ++j) {
        auto dst_vid = dst_indexer.get_index(casted_chunk->Value(j));
        std::get<1>(parsed_edges[cur_ind++]) = dst_vid;
        ie_degree[dst_vid]++;
      }
    }
  });

  // if EDATA_T is grape::EmptyType, no need to read columns
  auto edata_col_thread = std::thread([&]() {
    if constexpr (!std::is_same<EDATA_T, grape::EmptyType>::value) {
      CHECK(edata_cols.size() == 1);
      auto edata_col = edata_cols[0];
      CHECK(src_col->length() == edata_col->length());
      // iterate and put data
      size_t cur_ind = old_size;
      auto type = edata_col->type();

      using arrow_array_type =
          typename gs::CppTypeToArrowType<EDATA_T>::ArrayType;
      if (type->Equals(arrow::timestamp(arrow::TimeUnit::MILLI))) {
        for (auto i = 0; i < edata_col->num_chunks(); ++i) {
          auto chunk = edata_col->chunk(i);
          auto casted_chunk = std::static_pointer_cast<arrow_array_type>(chunk);
          for (auto j = 0; j < casted_chunk->length(); ++j) {
            std::get<2>(parsed_edges[cur_ind++]) = casted_chunk->Value(j);
          }
        }
      } else if (type->Equals(arrow::large_utf8()) ||
                 type->Equals(arrow::utf8())) {
        for (auto i = 0; i < edata_col->num_chunks(); ++i) {
          auto chunk = edata_col->chunk(i);
          auto casted_chunk = std::static_pointer_cast<arrow_array_type>(chunk);
          for (auto j = 0; j < casted_chunk->length(); ++j) {
            std::get<2>(parsed_edges[cur_ind++]) = casted_chunk->GetView(j);
          }
        }
      } else {
        for (auto i = 0; i < edata_col->num_chunks(); ++i) {
          auto chunk = edata_col->chunk(i);
          auto casted_chunk = std::static_pointer_cast<arrow_array_type>(chunk);
          for (auto j = 0; j < casted_chunk->length(); ++j) {
            std::get<2>(parsed_edges[cur_ind++]) = casted_chunk->Value(j);
          }
        }
      }
    }
  });
  src_col_thread.join();
  dst_col_thread.join();
  edata_col_thread.join();
  VLOG(10) << "Finish inserting:  " << src_col->length() << " edges";
}

// Create VertexTableReader
std::shared_ptr<arrow::csv::TableReader>
CSVFragmentLoader::createVertexTableReader(label_t v_label,
                                           const std::string& v_file) {
  // Create options.
  arrow::csv::ConvertOptions convert_options;
  arrow::csv::ReadOptions read_options;
  arrow::csv::ParseOptions parse_options;
  fillVertexReaderMeta(read_options, parse_options, convert_options, v_file,
                       v_label);

  auto read_result = arrow::io::ReadableFile::Open(v_file);
  if (!read_result.ok()) {
    LOG(FATAL) << "Fail to open: " << v_file
               << " error: " << read_result.status().message();
  }
  std::shared_ptr<arrow::io::ReadableFile> file = read_result.ValueOrDie();
  auto res =
      arrow::csv::TableReader::Make(arrow::io::IOContext(), file, read_options,
                                    parse_options, convert_options);
  if (!res.ok()) {
    LOG(FATAL) << "Fail to create StreamingReader for file: " << v_file
               << " error: " << res.status().message();
  }
  return res.ValueOrDie();
}

std::shared_ptr<arrow::csv::StreamingReader>
CSVFragmentLoader::createVertexStreamReader(label_t v_label,
                                            const std::string& v_file) {
  arrow::csv::ConvertOptions convert_options;
  arrow::csv::ReadOptions read_options;
  arrow::csv::ParseOptions parse_options;
  fillVertexReaderMeta(read_options, parse_options, convert_options, v_file,
                       v_label);

  auto read_result = arrow::io::ReadableFile::Open(v_file);
  if (!read_result.ok()) {
    LOG(FATAL) << "Fail to open: " << v_file
               << " error: " << read_result.status().message();
  }
  std::shared_ptr<arrow::io::ReadableFile> file = read_result.ValueOrDie();
  auto res = arrow::csv::StreamingReader::Make(arrow::io::IOContext(), file,
                                               read_options, parse_options,
                                               convert_options);
  if (!res.ok()) {
    LOG(FATAL) << "Fail to create StreamingReader for file: " << v_file
               << " error: " << res.status().message();
  }
  return res.ValueOrDie();
}

std::shared_ptr<arrow::csv::StreamingReader>
CSVFragmentLoader::createEdgeStreamReader(label_t src_label_id,
                                          label_t dst_label_id,
                                          label_t label_id,
                                          const std::string& e_file) {
  arrow::csv::ConvertOptions convert_options;
  arrow::csv::ReadOptions read_options;
  arrow::csv::ParseOptions parse_options;

  fillEdgeReaderMeta(read_options, parse_options, convert_options, e_file,
                     src_label_id, dst_label_id, label_id);

  auto read_result = arrow::io::ReadableFile::Open(e_file);
  if (!read_result.ok()) {
    LOG(FATAL) << "Fail to open: " << e_file
               << " error: " << read_result.status().message();
  }
  std::shared_ptr<arrow::io::ReadableFile> file = read_result.ValueOrDie();
  auto res = arrow::csv::StreamingReader::Make(arrow::io::IOContext(), file,
                                               read_options, parse_options,
                                               convert_options);
  if (!res.ok()) {
    LOG(FATAL) << "Fail to create StreamingReader for file: " << e_file
               << " error: " << res.status().message();
  }
  return res.ValueOrDie();
}

std::shared_ptr<arrow::csv::TableReader>
CSVFragmentLoader::createEdgeTableReader(label_t src_label_id,
                                         label_t dst_label_id, label_t label_id,
                                         const std::string& e_file) {
  arrow::csv::ConvertOptions convert_options;
  arrow::csv::ReadOptions read_options;
  arrow::csv::ParseOptions parse_options;

  fillEdgeReaderMeta(read_options, parse_options, convert_options, e_file,
                     src_label_id, dst_label_id, label_id);

  auto read_result = arrow::io::ReadableFile::Open(e_file);
  if (!read_result.ok()) {
    LOG(FATAL) << "Fail to open: " << e_file
               << " error: " << read_result.status().message();
  }
  std::shared_ptr<arrow::io::ReadableFile> file = read_result.ValueOrDie();
  auto res =
      arrow::csv::TableReader::Make(arrow::io::IOContext(), file, read_options,
                                    parse_options, convert_options);
  if (!res.ok()) {
    LOG(FATAL) << "Fail to create TableReader for file: " << e_file
               << " error: " << res.status().message();
  }
  return res.ValueOrDie();
}

void CSVFragmentLoader::addVertexBatch(
    label_t v_label_id, IdIndexer<oid_t, vid_t>& indexer,
    std::shared_ptr<arrow::Array>& primary_key_col,
    const std::vector<std::shared_ptr<arrow::Array>>& property_cols) {
  size_t row_num = primary_key_col->length();
  CHECK_EQ(primary_key_col->type()->id(), arrow::Type::INT64);
  auto col_num = property_cols.size();
  for (size_t i = 0; i < col_num; ++i) {
    CHECK_EQ(property_cols[i]->length(), row_num);
  }
  auto casted_array =
      std::static_pointer_cast<arrow::Int64Array>(primary_key_col);
  std::vector<std::vector<Any>> prop_vec(property_cols.size());

  double t = -grape::GetCurrentTime();
  vid_t vid;
  std::vector<vid_t> vids;
  vids.reserve(row_num);
  for (auto i = 0; i < row_num; ++i) {
    if (!indexer.add(casted_array->Value(i), vid)) {
      LOG(FATAL) << "Duplicate vertex id: " << casted_array->Value(i) << " for "
                 << schema_.get_vertex_label_name(v_label_id);
    }
    vids.emplace_back(vid);
  }

  t += grape::GetCurrentTime();
  for (double tmp = convert_to_internal_vertex_time_;
       !convert_to_internal_vertex_time_.compare_exchange_weak(tmp, tmp + t);) {
  }

  t = -grape::GetCurrentTime();
  for (auto j = 0; j < property_cols.size(); ++j) {
    auto array = property_cols[j];
    auto chunked_array = std::make_shared<arrow::ChunkedArray>(array);
    set_vertex_properties(
        basic_fragment_loader_.GetVertexTable(v_label_id).column_ptrs()[j],
        chunked_array, vids);
  }

  t += grape::GetCurrentTime();
  for (double tmp = basic_frag_loader_vertex_time_;
       !basic_frag_loader_vertex_time_.compare_exchange_weak(tmp, tmp + t);) {}

  VLOG(10) << "Insert rows: " << row_num;
}

void CSVFragmentLoader::addVertexBatch(
    label_t v_label_id, IdIndexer<oid_t, vid_t>& indexer,
    std::shared_ptr<arrow::ChunkedArray>& primary_key_col,
    const std::vector<std::shared_ptr<arrow::ChunkedArray>>& property_cols) {
  size_t row_num = primary_key_col->length();
  std::vector<vid_t> vids;
  vids.reserve(row_num);
  CHECK_EQ(primary_key_col->type()->id(), arrow::Type::INT64);
  // check row num
  auto col_num = property_cols.size();
  for (size_t i = 0; i < col_num; ++i) {
    CHECK_EQ(property_cols[i]->length(), row_num);
  }
  std::vector<std::vector<Any>> prop_vec(property_cols.size());

  double t = -grape::GetCurrentTime();
  for (auto i = 0; i < primary_key_col->num_chunks(); ++i) {
    auto chunk = primary_key_col->chunk(i);
    auto casted_array = std::static_pointer_cast<arrow::Int64Array>(chunk);
    for (auto j = 0; j < casted_array->length(); ++j) {
      vid_t vid;
      if (!indexer.add(casted_array->Value(j), vid)) {
        LOG(FATAL) << "Duplicate vertex id: " << casted_array->Value(j)
                   << " for " << schema_.get_vertex_label_name(v_label_id);
      }
      vids.emplace_back(vid);
    }
  }

  t += grape::GetCurrentTime();
  for (double tmp = convert_to_internal_vertex_time_;
       !convert_to_internal_vertex_time_.compare_exchange_weak(tmp, tmp + t);) {
  }

  t = -grape::GetCurrentTime();
  for (auto i = 0; i < property_cols.size(); ++i) {
    auto array = property_cols[i];
    auto& table = basic_fragment_loader_.GetVertexTable(v_label_id);
    auto& col_ptrs = table.column_ptrs();
    set_vertex_properties(col_ptrs[i], array, vids);
  }
  t += grape::GetCurrentTime();
  for (double tmp = basic_frag_loader_vertex_time_;
       !basic_frag_loader_vertex_time_.compare_exchange_weak(tmp, tmp + t);) {}

  VLOG(10) << "Insert rows: " << row_num;
}

void CSVFragmentLoader::addVerticesImplWithTableReader(
    const std::string& v_file, label_t v_label_id,
    IdIndexer<oid_t, vid_t>& indexer) {
  auto vertex_column_mappings =
      loading_config_.GetVertexColumnMappings(v_label_id);
  auto primary_key = schema_.get_vertex_primary_key(v_label_id)[0];
  size_t primary_key_ind = std::get<2>(primary_key);
  auto reader = createVertexTableReader(v_label_id, v_file);
  std::shared_ptr<arrow::Table> table;
  double t = -grape::GetCurrentTime();
  auto result = reader->Read();
  t += grape::GetCurrentTime();
  for (double tmp = read_vertex_table_time_;
       !read_vertex_table_time_.compare_exchange_weak(tmp, tmp + t);) {}

  auto status = result.status();
  if (!status.ok()) {
    LOG(FATAL) << "Failed to read next batch from file " << v_file
               << status.message();
  }
  table = result.ValueOrDie();
  if (table == nullptr) {
    LOG(FATAL) << "Empty file: " << v_file;
  }
  auto header = table->schema()->field_names();
  auto schema_column_names = schema_.get_vertex_property_names(v_label_id);
  CHECK(schema_column_names.size() + 1 == header.size());
  VLOG(10) << "Find header of size: " << header.size();

  auto columns = table->columns();
  CHECK(primary_key_ind < columns.size());
  auto primary_key_column = columns[primary_key_ind];
  auto other_columns_array = columns;
  other_columns_array.erase(other_columns_array.begin() + primary_key_ind);
  VLOG(10) << "Reading record batch of size: " << table->num_rows();
  addVertexBatch(v_label_id, indexer, primary_key_column, other_columns_array);
}

void CSVFragmentLoader::addVerticesImplWithStreamReader(
    const std::string& v_file, label_t v_label_id,
    IdIndexer<oid_t, vid_t>& indexer) {
  auto vertex_column_mappings =
      loading_config_.GetVertexColumnMappings(v_label_id);
  auto primary_key = schema_.get_vertex_primary_key(v_label_id)[0];
  auto primary_key_name = std::get<1>(primary_key);
  size_t primary_key_ind = std::get<2>(primary_key);
  auto reader = createVertexStreamReader(v_label_id, v_file);
  std::shared_ptr<arrow::RecordBatch> record_batch;
  bool first_batch = true;
  while (true) {
    double t = -grape::GetCurrentTime();
    auto status = reader->ReadNext(&record_batch);
    t += grape::GetCurrentTime();
    for (double tmp = read_vertex_table_time_;
         !read_vertex_table_time_.compare_exchange_weak(tmp, tmp + t);) {}
    if (!status.ok()) {
      LOG(FATAL) << "Failed to read next batch from file " << v_file
                 << status.message();
    }
    if (record_batch == nullptr) {
      break;
    }
    if (first_batch) {
      // get header
      auto header = record_batch->schema()->field_names();
      auto schema_column_names = schema_.get_vertex_property_names(v_label_id);
      CHECK(schema_column_names.size() + 1 == header.size());
      VLOG(10) << "Find header of size: " << header.size();
      first_batch = false;
    }

    auto columns = record_batch->columns();
    CHECK(primary_key_ind < columns.size());
    auto primary_key_column = columns[primary_key_ind];
    auto other_columns_array = columns;
    other_columns_array.erase(other_columns_array.begin() + primary_key_ind);
    VLOG(10) << "Reading record batch of size: " << record_batch->num_rows();
    addVertexBatch(v_label_id, indexer, primary_key_column,
                   other_columns_array);
  }
}

void CSVFragmentLoader::addVerticesImpl(label_t v_label_id,
                                        const std::string& v_label_name,
                                        const std::vector<std::string> v_files,
                                        IdIndexer<oid_t, vid_t>& indexer) {
  VLOG(10) << "Parsing vertex file:" << v_files.size() << " for label "
           << v_label_name;

  for (auto& v_file : v_files) {
    if (loading_config_.GetIsBatchReader()) {
      addVerticesImplWithStreamReader(v_file, v_label_id, indexer);
    } else {
      addVerticesImplWithTableReader(v_file, v_label_id, indexer);
    }
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
  if (std::get<0>(primary_keys[0]) != PropertyType::kInt64) {
    LOG(FATAL) << "Only support int64_t primary key for vertex.";
  }

  std::string v_label_name = schema_.get_vertex_label_name(v_label_id);
  VLOG(10) << "Start init vertices for label " << v_label_name << " with "
           << v_files.size() << " files.";

  IdIndexer<oid_t, vid_t> indexer;

  addVerticesImpl(v_label_id, v_label_name, v_files, indexer);

  if (indexer.bucket_count() == 0) {
    indexer._rehash(schema_.get_max_vnum(v_label_name));
  }
  basic_fragment_loader_.FinishAddingVertex(v_label_id, indexer);

  VLOG(10) << "Finish init vertices for label " << v_label_name;
}

template <typename EDATA_T>
void CSVFragmentLoader::addEdgesImplWithTableReader(
    const std::string& filename, label_t src_label_id, label_t dst_label_id,
    label_t e_label_id, std::vector<int32_t>& ie_degree,
    std::vector<int32_t>& oe_degree,
    std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& parsed_edges) {
  const auto& src_indexer = basic_fragment_loader_.GetLFIndexer(src_label_id);
  const auto& dst_indexer = basic_fragment_loader_.GetLFIndexer(dst_label_id);
  auto reader =
      createEdgeTableReader(src_label_id, dst_label_id, e_label_id, filename);
  std::shared_ptr<arrow::Table> table;
  double t = -grape::GetCurrentTime();
  auto result = reader->Read();
  t += grape::GetCurrentTime();
  for (double tmp = read_edge_table_time_;
       !read_edge_table_time_.compare_exchange_weak(tmp, tmp + t);) {}

  auto status = result.status();
  if (!status.ok()) {
    LOG(FATAL) << "Failed to read Table from file " << filename
               << status.message();
  }
  table = result.ValueOrDie();
  if (table == nullptr) {
    LOG(FATAL) << "Empty file: " << filename;
  }
  auto header = table->schema()->field_names();
  auto schema_column_names =
      schema_.get_edge_property_names(src_label_id, dst_label_id, e_label_id);
  auto schema_column_types =
      schema_.get_edge_properties(src_label_id, dst_label_id, e_label_id);
  CHECK(schema_column_names.size() + 2 == header.size());
  CHECK(schema_column_types.size() + 2 == header.size());
  VLOG(10) << "Find header of size: " << header.size();

  auto columns = table->columns();
  CHECK(columns.size() >= 2);
  auto src_col = columns[0];
  auto dst_col = columns[1];
  CHECK(src_col->type() == arrow::int64())
      << "src_col type: " << src_col->type()->ToString();
  CHECK(dst_col->type() == arrow::int64())
      << "dst_col type: " << dst_col->type()->ToString();

  std::vector<std::shared_ptr<arrow::ChunkedArray>> property_cols;
  for (auto i = 2; i < columns.size(); ++i) {
    property_cols.emplace_back(columns[i]);
  }
  CHECK(property_cols.size() <= 1)
      << "Currently only support at most one property on edge";
  {
    CHECK(src_col->length() == dst_col->length());
    CHECK(src_col->type() == arrow::int64());
    CHECK(dst_col->type() == arrow::int64());
    t = -grape::GetCurrentTime();
    append_edges(src_col, dst_col, src_indexer, dst_indexer, property_cols,
                 parsed_edges, ie_degree, oe_degree);
    t += grape::GetCurrentTime();
    for (double tmp = convert_to_internal_edge_time_;
         !convert_to_internal_edge_time_.compare_exchange_weak(tmp, tmp + t);) {
    }
  }
}

template <typename EDATA_T>
void CSVFragmentLoader::addEdgesImplWithStreamReader(
    const std::string& filename, label_t src_label_id, label_t dst_label_id,
    label_t e_label_id, std::vector<int32_t>& ie_degree,
    std::vector<int32_t>& oe_degree,
    std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& parsed_edges) {
  const auto& src_indexer = basic_fragment_loader_.GetLFIndexer(src_label_id);
  const auto& dst_indexer = basic_fragment_loader_.GetLFIndexer(dst_label_id);
  auto reader =
      createEdgeStreamReader(src_label_id, dst_label_id, e_label_id, filename);
  std::shared_ptr<arrow::RecordBatch> record_batch;
  // read first batch
  bool first_batch = true;
  while (true) {
    double t = -grape::GetCurrentTime();
    auto status = reader->ReadNext(&record_batch);
    t += grape::GetCurrentTime();
    for (double tmp = read_edge_table_time_;
         !read_edge_table_time_.compare_exchange_weak(tmp, tmp + t);) {}
    if (!status.ok()) {
      LOG(FATAL) << "Failed to read next batch from file " << filename
                 << status.message();
    }
    if (record_batch == nullptr) {
      break;
    }
    if (first_batch) {
      auto header = record_batch->schema()->field_names();
      auto schema_column_names = schema_.get_edge_property_names(
          src_label_id, dst_label_id, e_label_id);
      auto schema_column_types =
          schema_.get_edge_properties(src_label_id, dst_label_id, e_label_id);
      CHECK(schema_column_names.size() + 2 == header.size())
          << "schema size: " << schema_column_names.size()
          << " header size: " << header.size();
      CHECK(schema_column_types.size() + 2 == header.size())
          << "schema size: " << schema_column_types.size()
          << " header size: " << header.size();
      VLOG(10) << "Find header of size: " << header.size();
      first_batch = false;
    }

    // copy the table to csr.
    auto columns = record_batch->columns();
    // We assume the src_col and dst_col will always be put at front.
    CHECK(columns.size() >= 2);
    auto src_col = columns[0];
    auto dst_col = columns[1];
    CHECK(src_col->type() == arrow::int64())
        << "src_col type: " << src_col->type()->ToString();
    CHECK(dst_col->type() == arrow::int64())
        << "dst_col type: " << dst_col->type()->ToString();

    std::vector<std::shared_ptr<arrow::Array>> property_cols;
    for (auto i = 2; i < columns.size(); ++i) {
      property_cols.emplace_back(columns[i]);
    }
    CHECK(property_cols.size() <= 1)
        << "Currently only support at most one property on edge";
    {
      // add edges to vector
      CHECK(src_col->length() == dst_col->length());
      CHECK(src_col->type() == arrow::int64());
      CHECK(dst_col->type() == arrow::int64());
      auto src_casted_array =
          std::static_pointer_cast<arrow::Int64Array>(src_col);
      auto dst_casted_array =
          std::static_pointer_cast<arrow::Int64Array>(dst_col);
      t = -grape::GetCurrentTime();
      append_edges(src_casted_array, dst_casted_array, src_indexer, dst_indexer,
                   property_cols, parsed_edges, ie_degree, oe_degree);
      t += grape::GetCurrentTime();
      for (double tmp = convert_to_internal_edge_time_;
           !convert_to_internal_edge_time_.compare_exchange_weak(tmp, tmp + t);
           tmp = convert_to_internal_edge_time_) {}
    }
  }
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
    VLOG(10) << "processing " << filename << " with src_col_id " << src_col_ind
             << " and dst_col_id " << dst_col_ind;
    if (loading_config_.GetIsBatchReader()) {
      VLOG(1) << "Using batch reader";
      addEdgesImplWithStreamReader(filename, src_label_id, dst_label_id,
                                   e_label_id, ie_degree, oe_degree,
                                   parsed_edges);
    } else {
      VLOG(1) << "Using table reader";
      addEdgesImplWithTableReader(filename, src_label_id, dst_label_id,
                                  e_label_id, ie_degree, oe_degree,
                                  parsed_edges);
    }
  }
  double t = -grape::GetCurrentTime();
  basic_fragment_loader_.PutEdges(src_label_id, dst_label_id, e_label_id,
                                  parsed_edges, ie_degree, oe_degree);
  t += grape::GetCurrentTime();
  // basic_frag_loader_edge_time_.fetch_add(t);
  for (double tmp = basic_frag_loader_edge_time_;
       !basic_frag_loader_edge_time_.compare_exchange_weak(tmp, tmp + t);) {}
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
  } else if (property_types[0] == PropertyType::kDate) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<Date>(src_label_i, dst_label_i,
                                                      edge_label_i);
    } else {
      addEdgesImpl<Date>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else if (property_types[0] == PropertyType::kInt32) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<int>(src_label_i, dst_label_i,
                                                     edge_label_i);
    } else {
      addEdgesImpl<int>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else if (property_types[0] == PropertyType::kInt64) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<int64_t>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesImpl<int64_t>(src_label_i, dst_label_i, edge_label_i, filenames);
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
      VLOG(10) << "vertex_label: " << schema_.get_edge_label_name(label_id)
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
