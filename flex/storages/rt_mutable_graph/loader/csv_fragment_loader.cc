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

CSVStreamRecordBatchSupplier::CSVStreamRecordBatchSupplier(
    label_t label_id, const std::string& file_path,
    arrow::csv::ConvertOptions convert_options,
    arrow::csv::ReadOptions read_options,
    arrow::csv::ParseOptions parse_options)
    : file_path_(file_path) {
  auto read_result = arrow::io::ReadableFile::Open(file_path);
  if (!read_result.ok()) {
    LOG(FATAL) << "Failed to open file: " << file_path
               << " error: " << read_result.status().message();
  }
  auto file = read_result.ValueOrDie();
  auto res = arrow::csv::StreamingReader::Make(arrow::io::default_io_context(),
                                               file, read_options,
                                               parse_options, convert_options);
  if (!res.ok()) {
    LOG(FATAL) << "Failed to create streaming reader for file: " << file_path
               << " error: " << res.status().message();
  }
  reader_ = res.ValueOrDie();
  VLOG(10) << "Finish init CSVRecordBatchSupplier for file: " << file_path;
}

std::shared_ptr<arrow::RecordBatch>
CSVStreamRecordBatchSupplier::GetNextBatch() {
  auto res = reader_->Next();
  if (res.ok()) {
    return res.ValueOrDie();
  } else {
    LOG(ERROR) << "Failed to read next batch from file: " << file_path_
               << " error: " << res.status().message();
    return nullptr;
  }
}

CSVTableRecordBatchSupplier::CSVTableRecordBatchSupplier(
    label_t label_id, const std::string& path,
    arrow::csv::ConvertOptions convert_options,
    arrow::csv::ReadOptions read_options,
    arrow::csv::ParseOptions parse_options)
    : file_path_(path) {
  auto read_result = arrow::io::ReadableFile::Open(path);
  if (!read_result.ok()) {
    LOG(FATAL) << "Failed to open file: " << path
               << " error: " << read_result.status().message();
  }
  std::shared_ptr<arrow::io::ReadableFile> file = read_result.ValueOrDie();
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
  table_ = result.ValueOrDie();
  reader_ = std::make_shared<arrow::TableBatchReader>(*table_);
}

std::shared_ptr<arrow::RecordBatch>
CSVTableRecordBatchSupplier::GetNextBatch() {
  std::shared_ptr<arrow::RecordBatch> batch;
  auto status = reader_->ReadNext(&batch);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to read batch from file: " << file_path_
               << " error: " << status.message();
  }
  return batch;
}

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
    for (size_t i = 0; i < all_column_names.size(); ++i) {
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
    for (size_t i = 0; i < all_column_names.size(); ++i) {
      all_column_names[i] = std::string("f") + std::to_string(i);
    }
  }
  read_options.column_names = all_column_names;
  VLOG(10) << "Got all column names: " << all_column_names.size()
           << gs::to_string(all_column_names);
}

std::shared_ptr<IFragmentLoader> CSVFragmentLoader::Make(
    const std::string& work_dir, const Schema& schema,
    const LoadingConfig& loading_config, int32_t thread_num) {
  return std::shared_ptr<IFragmentLoader>(
      new CSVFragmentLoader(work_dir, schema, loading_config, thread_num));
}

void CSVFragmentLoader::addVertices(label_t v_label_id,
                                    const std::vector<std::string>& v_files) {
  auto record_batch_supplier_creator =
      [this](label_t label_id, const std::string& v_file,
             const LoadingConfig& loading_config) {
        arrow::csv::ConvertOptions convert_options;
        arrow::csv::ReadOptions read_options;
        arrow::csv::ParseOptions parse_options;
        fillVertexReaderMeta(read_options, parse_options, convert_options,
                             v_file, label_id);
        if (loading_config.GetIsBatchReader()) {
          auto res = std::make_shared<CSVStreamRecordBatchSupplier>(
              label_id, v_file, convert_options, read_options, parse_options);
          return std::dynamic_pointer_cast<IRecordBatchSupplier>(res);
        } else {
          auto res = std::make_shared<CSVTableRecordBatchSupplier>(
              label_id, v_file, convert_options, read_options, parse_options);
          return std::dynamic_pointer_cast<IRecordBatchSupplier>(res);
        }
      };
  return AbstractArrowFragmentLoader::AddVerticesRecordBatch(
      v_label_id, v_files, record_batch_supplier_creator);
}

void CSVFragmentLoader::addEdges(label_t src_label_i, label_t dst_label_i,
                                 label_t edge_label_i,
                                 const std::vector<std::string>& filenames) {
  auto lambda = [this](label_t src_label_id, label_t dst_label_id,
                       label_t e_label_id, const std::string& filename,
                       const LoadingConfig& loading_config) {
    arrow::csv::ConvertOptions convert_options;
    arrow::csv::ReadOptions read_options;
    arrow::csv::ParseOptions parse_options;
    fillEdgeReaderMeta(read_options, parse_options, convert_options, filename,
                       src_label_id, dst_label_id, e_label_id);
    if (loading_config.GetIsBatchReader()) {
      auto res = std::make_shared<CSVStreamRecordBatchSupplier>(
          e_label_id, filename, convert_options, read_options, parse_options);
      return std::dynamic_pointer_cast<IRecordBatchSupplier>(res);
    } else {
      auto res = std::make_shared<CSVTableRecordBatchSupplier>(
          e_label_id, filename, convert_options, read_options, parse_options);
      return std::dynamic_pointer_cast<IRecordBatchSupplier>(res);
    }
  };
  AbstractArrowFragmentLoader::AddEdgesRecordBatch(
      src_label_i, dst_label_i, edge_label_i, filenames, lambda);
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

    for (size_t i = 0; i < read_options.column_names.size(); ++i) {
      included_col_names.emplace_back(read_options.column_names[i]);
      included_col_indices.emplace_back(i);
      // We assume the order of the columns in the file is the same as the
      // order of the properties in the schema, except for primary key.
      mapped_property_names.emplace_back(property_names[i]);
    }
  } else {
    for (size_t i = 0; i < cur_label_col_mapping.size(); ++i) {
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

    for (size_t i = 0; i < property_types.size(); ++i) {
      // for each schema' property name, get the index of the column in
      // vertex_column mapping, and bind the type with the column name
      auto property_type = property_types[i];
      auto property_name = property_names[i];
      size_t ind = mapped_property_names.size();
      for (size_t i = 0; i < mapped_property_names.size(); ++i) {
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
      for (size_t i = 0; i < mapped_property_names.size(); ++i) {
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
    auto src_col_ind = src_dst_cols.first[0].second;
    auto dst_col_ind = src_dst_cols.second[0].second;
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
    for (size_t i = 0; i < edge_prop_names.size(); ++i) {
      auto property_name = edge_prop_names[i];
      included_col_names.emplace_back(property_name);
      mapped_property_names.emplace_back(property_name);
    }
  } else {
    // add the property columns into the included columns
    for (size_t i = 0; i < cur_label_col_mapping.size(); ++i) {
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
    for (size_t i = 0; i < property_types.size(); ++i) {
      // for each schema' property name, get the index of the column in
      // vertex_column mapping, and bind the type with the column name
      auto property_type = property_types[i];
      auto property_name = property_names[i];
      size_t ind = mapped_property_names.size();
      for (size_t i = 0; i < mapped_property_names.size(); ++i) {
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
      auto src_col_ind = src_dst_cols.first[0].second;
      auto dst_col_ind = src_dst_cols.second[0].second;
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

void CSVFragmentLoader::LoadFragment() {
  loadVertices();
  loadEdges();

  basic_fragment_loader_.LoadFragment();
}

const bool CSVFragmentLoader::registered_ = LoaderFactory::Register(
    "file", "csv",
    static_cast<LoaderFactory::loader_initializer_t>(&CSVFragmentLoader::Make));

}  // namespace gs
