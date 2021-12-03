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
#include <string>
#include "lgraph/db/readonly_db.h"

std::string GetPropValueAsStr(lgraph::db::Property *p, const lgraph::Schema &schema) {
  auto &prop_def = schema.GetPropDef(p->GetPropertyId());
  switch (prop_def.GetDataType()) {
    case lgraph::INT:
      return std::to_string(p->GetAsInt32().unwrap());
    case lgraph::LONG:
      return std::to_string(p->GetAsInt64().unwrap());
    case lgraph::FLOAT:
      return std::to_string(p->GetAsFloat().unwrap());
    case lgraph::DOUBLE:
      return std::to_string(p->GetAsDouble().unwrap());
    case lgraph::STRING: {
      auto slice = p->GetAsStr().unwrap();
      return {static_cast<const char *>(slice.data), slice.len};
    }
    default:
      return "";
  }
}

void PrintVertexInfo(lgraph::db::Vertex *v, const lgraph::Schema &schema) {
  std::string v_id = std::to_string(v->GetVertexId());
  std::string info = "[INFO] ";
  info += "<VertexID: " + v_id + "> ";
  info += "<Label: " + schema.GetTypeDef(v->GetLabelId()).GetLabelName() + ">";
  auto pi = v->GetPropertyIterator();
  assert(pi.Valid());
  while (true) {
    auto p = pi.Next().unwrap();
    if (!p.Valid()) { break; }
    info += "<" + schema.GetPropDef(p.GetPropertyId()).GetPropName() + ": " + GetPropValueAsStr(&p, schema) + "> ";
  }
  std::cout << info << std::endl;
}

int main(int argc, char *argv[]) {
  assert(argc == 3);
  const char *store_path = argv[1];
  const char *schema_path = argv[2];
  lgraph::Schema schema = lgraph::Schema::FromProtoFile(schema_path);
  lgraph::db::ReadonlyDB rg = lgraph::db::ReadonlyDB::Open(store_path);
  lgraph::db::Snapshot ss = rg.GetSnapshot(std::numeric_limits<uint32_t>::max());
  auto iter = ss.ScanVertex().unwrap();
  assert(iter.Valid());
  unsigned v_cnt = 0;
  while (true) {
    auto v = iter.Next().unwrap();
    if (!v.Valid()) { break; }
    v_cnt++;
    PrintVertexInfo(&v, schema);
  }
}
