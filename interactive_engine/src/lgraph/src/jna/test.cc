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
#include "store_ffi/store_ffi.h"

namespace DB_NAMESPACE {

void PrintVertexInfo(Vertex* v) {
  std::string info;
  info += "<VertexID: " + std::to_string(v->GetVertexId()) + ">";
  info += "<LabelID: " + std::to_string(v->GetLabelId()) + ">";

  auto pi = v->GetPropertyIterator();
  assert(pi.Valid());
  while (true) {
    auto rp = pi.Next();
    assert(rp.isOk());
    auto p = rp.unwrap();
    if (!p.Valid()) {
      break;
    }
    info += "<PropertyID: " + std::to_string(p.GetPropertyId()) + ">";
  }
  std::cout << info << std::endl;
}

void TestScanVertex(Snapshot* ss) {
  std::cout << std::endl << "[lgraph] <ScanVertexTest Begin>" << std::endl;

  auto r = ss->ScanVertex();
  assert(r.isOk());
  auto vi = r.unwrap();
  assert(vi.Valid());

  unsigned v_cnt = 0;
  while(true) {
    auto rv = vi.Next();
    assert(rv.isOk());
    auto v = rv.unwrap();
    if (!v.Valid()) { break; }
    v_cnt++;
    PrintVertexInfo(&v);
  }
  std::cout << "-- Total Vertex Number: " << v_cnt << std::endl;

  std::cout << "[lgraph] <ScanVertexTest Finish>" << std::endl;
}

void PrintEdgeInfo(Edge* e) {
  std::string info;

  auto e_id = e->GetEdgeId();
  info += "<EdgeID: ("
      + std::to_string(e_id.edge_inner_id) + ", "
      + std::to_string(e_id.src_vertex_id) + ", "
      + std::to_string(e_id.dst_vertex_id) + ")>";
  auto e_rel = e->GetEdgeRelation();
  info += "<EdgeRelation: ("
      + std::to_string(e_rel.edge_label_id) + ", "
      + std::to_string(e_rel.src_vertex_label_id) + ", "
      + std::to_string(e_rel.dst_vertex_label_id) + ")>";

  auto pi = e->GetPropertyIterator();
  assert(pi.Valid());
  while (true) {
    auto rp = pi.Next();
    assert(rp.isOk());
    auto p = rp.unwrap();
    if (!p.Valid()) {
      break;
    }
    info += "<PropertyID: " + std::to_string(p.GetPropertyId()) + ">";
  }
  std::cout << info << std::endl;
}

void TestScanEdge(Snapshot* ss) {
  std::cout << std::endl << "[lgraph] <ScanEdgeTest Begin>" << std::endl;

  auto r = ss->ScanEdge();
  assert(r.isOk());
  auto ei = r.unwrap();
  assert(ei.Valid());

  unsigned e_cnt = 0;
  while(true) {
    auto re = ei.Next();
    assert(re.isOk());
    auto e = re.unwrap();
    if (!e.Valid()) { break; }
    e_cnt++;
    PrintEdgeInfo(&e);
  }
  std::cout << "-- Total Edge Number: " << e_cnt << std::endl;

  std::cout << "[lgraph] <ScanEdgeTest Finish>" << std::endl;
}

void runLocalTests() {
  assert(local_graph_handle_ != nullptr);
  Snapshot latest_ss(ffi::GetSnapshot(local_graph_handle_, std::numeric_limits<SnapshotId>::max()));

  TestScanVertex(&latest_ss);
  TestScanEdge(&latest_ss);
}

}  // namespace DB_NAMESPACE