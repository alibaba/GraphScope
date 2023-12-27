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

#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include "flex/storages/rt_mutable_graph/loader/odps_client.h"

#include <glog/logging.h>

namespace gs {

void parseLocation(const std::string& odps_table_path,
                   TableIdentifier& table_identifier,
                   std::vector<std::string>& res_partitions,
                   std::vector<std::string>& selected_partitions) {
  LOG(INFO) << "Parse real path: " << odps_table_path;

  std::vector<std::string> splits;
  boost::split(splits, odps_table_path, boost::is_any_of("/"));
  // the first one is empty
  CHECK(splits.size() >= 2) << "Invalid odps table path: " << odps_table_path;
  table_identifier.project_ = splits[0];
  table_identifier.table_ = splits[1];

  if (splits.size() == 3) {
    boost::split(selected_partitions, splits[2], boost::is_any_of(","));
    std::vector<std::string> partitions;
    for (size_t i = 0; i < selected_partitions.size(); ++i) {
      partitions.emplace_back(selected_partitions[i].substr(
          0, selected_partitions[i].find_first_of("=")));
    }
    // dedup partitions
    std::sort(partitions.begin(), partitions.end());
    partitions.erase(std::unique(partitions.begin(), partitions.end()),
                     partitions.end());
    res_partitions = partitions;
  }
}

std::shared_ptr<arrow::Table> read_odps_table(const std::string& odps_table,
                                              int thread_num) {
  ODPSReadClient odps_read_client;
  odps_read_client.init();
  std::string session_id;
  int split_count;
  TableIdentifier table_identifier;
  std::vector<std::string> partition_cols;
  std::vector<std::string> selected_partitions;
  std::vector<std::string> selected_cols;  // empty means read all data.
  parseLocation(odps_table, table_identifier, partition_cols,
                selected_partitions);
  odps_read_client.CreateReadSession(&session_id, &split_count,
                                     table_identifier, selected_cols,
                                     partition_cols, selected_partitions);
  VLOG(1) << "Successfully got session_id: " << session_id
          << ", split count: " << split_count;
  auto table = odps_read_client.ReadTable(session_id, split_count,
                                          table_identifier, thread_num);
  return table;
}

void dump_to_csv(std::shared_ptr<arrow::Table> table,
                 const std::string& output_path) {
  // convert / in table_name to _
  // if csv_file exists, override
  if (std::filesystem::exists(output_path)) {
    LOG(FATAL) << "File " << output_path << " exists";
  }
  LOG(INFO) << "Dump table to csv: " << output_path << ", the table has "
            << table->num_rows() << " rows";
  // output table to csv file in csv format
  auto write_options = arrow::csv::WriteOptions::Defaults();
  auto outstream = arrow::io::FileOutputStream::Open(output_path);
  auto st =
      arrow::csv::WriteCSV(*table, write_options, outstream.ValueOrDie().get());
  if (!st.ok()) {
    LOG(FATAL) << "WriteCSV failed: " << st.ToString();
  }
  LOG(INFO) << "Dump table to csv done";
}

}  // namespace gs

int main(int argc, char** argv) {
  // Input args:
  // ODPS_TABLE_STRING: "odps://project_name/table_name"
  // OUTPUT_CSV_PATH;
  if (argc < 3 || argc > 4) {
    std::cout << "Usage: " << argv[0] << " <ODPS_TABLE_STRING>"
              << " <OUTPUT_CSV_PATH> [thread_num=1]" << std::endl;
    return 1;
  }

  // if OUTPUT_CSV_PATH exists, exit
  if (std::filesystem::exists(argv[2])) {
    LOG(ERROR) << "OUTPUT_CSV_PATH: " << argv[2] << " exists, exit";
    return 1;
  }
  int thread_num = 1;
  if (argc == 4) {
    thread_num = std::stoi(argv[3]);
  }
  LOG(INFO) << "ODPS_TABLE_STRING: " << argv[1];
  LOG(INFO) << "OUTPUT_CSV_PATH: " << argv[2];
  LOG(INFO) << "thread_num: " << thread_num;

  auto table = gs::read_odps_table(argv[1], thread_num);
  gs::dump_to_csv(table, argv[2]);
  return 0;
}