/**
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iostream>
#include "db/readonly_db.h"

void PrintVertexInfo(lgraph::Vertex* v) {
  std::string v_id = std::to_string(v->GetVertexId());

  std::string info = "[INFO] ";
  info += "<VertexID: " + v_id + "> ";
  info += "<LabelId: " + std::to_string(v->GetLabelId()) + "> \n";
  std::cout << info << std::endl;
}

int main(int argc, char *argv[]) {
  assert(argc == 2 || argc == 3);
  const char* store_path = argv[1];
  const char* log4rs_config_file = (argc == 3)? argv[2] : nullptr;
  lgraph::ReadonlyDB rg = lgraph::ReadonlyDB::Open(store_path, log4rs_config_file);
  lgraph::Snapshot ss = rg.GetSnapshot(std::numeric_limits<uint32_t>::max());
  auto iter = ss.ScanVertex().unwrap();
  assert(iter.Valid());
  unsigned v_cnt = 0;
  while (true) {
    auto v = iter.Next().unwrap();
    if (!v.Valid()) { break; }
    v_cnt++;
    PrintVertexInfo(&v);
  }
}
