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

#include <cassert>

#include "jna/test.h"

namespace DB_NAMESPACE {

void CheckProperty(Property* p) {
  assert(p->Valid());
  auto r = p->GetAsInt64();
  assert(r.isErr());
  std::cout << "(PropertyId = " << p->GetPropertyId() << ", PropertyValue = Null)" << std::endl;
  std::cout << "(Rust Print Error Msg...)" << std::endl;
  r.unwrapErr().Print();
}

void TestGetVertex() {
  std::cout << "[C++ End] <GetVertexTest>" << std::endl;

  std::cout << "---- Get Vertex" << std::endl;
  auto r1 = local_snapshot_->GetVertex(1001, none_label_id);
  assert(r1.isOk());
  Vertex v = r1.unwrap();
  assert(v.Valid());
  std::cout << "(VertexId = " << v.GetVertexId() << ", LabelId = " << v.GetLabelId() << ")" << std::endl;

  std::cout << "--- Get Property" << std::endl;
  auto p1 = v.GetPropertyBy(1002);
  CheckProperty(&p1);

  std::cout << "--- Get Vertex Property Iterator" << std::endl;
  auto pi = v.GetPropertyIterator();
  assert(pi.Valid());
  std::cout << "--- Call Iterator Next()" << std::endl;
  unsigned count = 0;
  while (true) {
    auto res_p = pi.Next();
    assert(res_p.isOk());
    auto p = res_p.unwrap();
    if (!p.Valid()) {
      break;
    }
    count++;
    CheckProperty(&p);
  }
  std::cout << "(Total Record number in iterator: " << count << ")" << std::endl;
}

void RunLocalTests() {
  assert(local_snapshot_.get() != nullptr);
  TestGetVertex();
}

}  // namespace DB_NAMESPACE