
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

#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_FRAGMENT_LOADER_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_FRAGMENT_LOADER_H_

#include "flex/storages/rt_mutable_graph/loader/abstract_arrow_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loader/basic_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loader/i_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loader/loader_factory.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include "arrow/util/value_parsing.h"

#include "grape/util.h"

namespace gs {

class CSVStreamRecordBatchSupplier : public IRecordBatchSupplier {
 public:
  CSVStreamRecordBatchSupplier(label_t label_id, const std::string& file_path,
                               arrow::csv::ConvertOptions convert_options,
                               arrow::csv::ReadOptions read_options,
                               arrow::csv::ParseOptions parse_options);

  std::shared_ptr<arrow::RecordBatch> GetNextBatch() override;

 private:
  std::string file_path_;
  std::shared_ptr<arrow::csv::StreamingReader> reader_;
};

class CSVTableRecordBatchSupplier : public IRecordBatchSupplier {
 public:
  CSVTableRecordBatchSupplier(label_t label_id, const std::string& file_path,
                              arrow::csv::ConvertOptions convert_options,
                              arrow::csv::ReadOptions read_options,
                              arrow::csv::ParseOptions parse_options);

  std::shared_ptr<arrow::RecordBatch> GetNextBatch() override;

 private:
  std::string file_path_;
  std::shared_ptr<arrow::Table> table_;
  std::shared_ptr<arrow::TableBatchReader> reader_;
};

// LoadFragment for csv files.
class CSVFragmentLoader : public AbstractArrowFragmentLoader {
 public:
  CSVFragmentLoader(const std::string& work_dir, const Schema& schema,
                    const LoadingConfig& loading_config, int32_t thread_num)
      : AbstractArrowFragmentLoader(work_dir, schema, loading_config,
                                    thread_num) {}

  static std::shared_ptr<IFragmentLoader> Make(
      const std::string& work_dir, const Schema& schema,
      const LoadingConfig& loading_config, int32_t thread_num);

  ~CSVFragmentLoader() {}

  void LoadFragment() override;

 private:
  void loadVertices();

  void loadEdges();

  void addVertices(label_t v_label_id, const std::vector<std::string>& v_files);

  void addEdges(label_t src_label_id, label_t dst_label_id, label_t e_label_id,
                const std::vector<std::string>& e_files);

  void fillEdgeReaderMeta(arrow::csv::ReadOptions& read_options,
                          arrow::csv::ParseOptions& parse_options,
                          arrow::csv::ConvertOptions& convert_options,
                          const std::string& e_file, label_t src_label_id,
                          label_t dst_label_id, label_t label_id) const;

  void fillVertexReaderMeta(arrow::csv::ReadOptions& read_options,
                            arrow::csv::ParseOptions& parse_options,
                            arrow::csv::ConvertOptions& convert_options,
                            const std::string& v_file, label_t v_label) const;

  static const bool registered_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_FRAGMENT_LOADER_H_