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
#include <sstream>

#include "lgraph/jna/ffi_test.h"
#include "lgraph/db/store_ffi/store_ffi.h"

namespace LGRAPH_NAMESPACE {

/// Modern Graph Schema

const LabelId software_LabelId = 1;
const LabelId person_LabelId = 3;

const LabelId created_EdgeLabelId = 9;
const LabelId knows_EdgeLabelId = 12;

const PropertyId id_PropId = 1;
const PropertyId name_PropId = 2;
const PropertyId lang_PropId = 3;
const PropertyId age_PropId = 5;
const PropertyId weight_PropId = 10;

const unsigned total_vertex_count = 6;
const unsigned total_edge_count = 6;

std::string LabelName(LabelId label_id) {
  if (label_id == software_LabelId) {
    return std::string{"software"};
  } else if (label_id == person_LabelId) {
    return std::string{"person"};
  } else if (label_id == created_EdgeLabelId) {
    return std::string{"created"};
  } else if (label_id == knows_EdgeLabelId) {
    return std::string{"knows"};
  }
  return std::string{};
}

std::string PropName(PropertyId prop_id) {
  if (prop_id == id_PropId) {
    return std::string{"id"};
  } else if (prop_id == name_PropId) {
    return std::string{"name"};
  } else if (prop_id == lang_PropId) {
    return std::string{"lang"};
  } else if (prop_id == age_PropId) {
    return std::string{"age"};
  } else if (prop_id == weight_PropId) {
    return std::string{"weight"};
  }
  return std::string{};
}

std::string GetPropValueAsStr(db::Property* p) {
  if (p->GetPropertyId() == id_PropId) {
    return std::to_string(p->GetAsInt64().unwrap());
  } else if (p->GetPropertyId() == name_PropId || p->GetPropertyId() == lang_PropId) {
    auto str_slice = p->GetAsStr().unwrap();
    return std::string{static_cast<const char*>(str_slice.data), str_slice.len};
  } else if (p->GetPropertyId() == age_PropId) {
    return std::to_string(p->GetAsInt32().unwrap());
  } else if (p->GetPropertyId() == weight_PropId) {
    return std::to_string(p->GetAsDouble().unwrap());
  }
  return std::string{};
}

/// Test Functions

bool LogVertexInfo(db::Vertex* v, std::stringstream& logger) {
  std::string v_id = std::to_string(v->GetVertexId());

  std::string info = "[INFO] ";
  info += "<VertexID: " + v_id + "> ";
  info += "<Label: " + LabelName(v->GetLabelId()) + "> ";

  auto pi = v->GetPropertyIterator();
  if (!pi.Valid()) {
    logger << "[Error] Got invalid Property iterator handle of vertex<" << v_id << ">!\n";
    return false;
  }
  while (true) {
    auto rp = pi.Next();
    if (rp.isErr()) {
      logger << "[Error] PropertyIterator.Next(): " << rp.unwrapErr().GetInfo() << "\n";
      return false;
    }
    auto p = rp.unwrap();
    if (!p.Valid()) { break; }
    info += "<" + PropName(p.GetPropertyId()) + ": " + GetPropValueAsStr(&p) + "> ";
  }
  logger << info << "\n";
  return true;
}

bool LogEdgeInfo(db::Edge* e, std::stringstream& logger) {
  auto e_id = e->GetEdgeId();
  std::string e_id_str = "(" + std::to_string(e_id.edge_inner_id) + ", "
                         + std::to_string(e_id.src_vertex_id) + ", " + std::to_string(e_id.dst_vertex_id) + ")";
  auto e_rel = e->GetEdgeRelation();
  std::string e_rel_str = "(" + LabelName(e_rel.edge_label_id) + ", "
                          + LabelName(e_rel.src_vertex_label_id) + ", " + LabelName(e_rel.dst_vertex_label_id) + ")";

  std::string info = "[INFO] ";
  info += "<EdgeID: " + e_id_str + "> ";
  info += "<EdgeRelation: " + e_rel_str + "> ";

  auto pi = e->GetPropertyIterator();
  if (!pi.Valid()) {
    logger << "[Error] Got invalid Property iterator handle of edge<" << e_id_str << ">!\n";
    return false;
  }
  while (true) {
    auto rp = pi.Next();
    if (rp.isErr()) {
      logger << "[Error] PropertyIterator.Next(): " << rp.unwrapErr().GetInfo() << "\n";
      return false;
    }
    auto p = rp.unwrap();
    if (!p.Valid()) { break; }
    info += "<" + PropName(p.GetPropertyId()) + ": " + GetPropValueAsStr(&p) + "> ";
  }
  logger << info << "\n";
  return true;
}

bool TestScanVertex(db::Snapshot* ss, std::stringstream& logger) {
  auto r = ss->ScanVertex();
  if (r.isErr()) {
    logger << "[Error] ScanVertex: " << r.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto vi = r.unwrap();
  if (!vi.Valid()) {
    logger << "[Error] Got invalid vertex iterator handle!\n";
    return false;
  }
  unsigned v_cnt = 0;
  while (true) {
    auto rv = vi.Next();
    if (rv.isErr()) {
      logger << "[Error] VertexIterator.Next(): " << rv.unwrapErr().GetInfo() << "\n";
      return false;
    }
    auto v = rv.unwrap();
    if (!v.Valid()) { break; }
    v_cnt++;
    if (!LogVertexInfo(&v, logger)) {
      return false;
    }
  }
  if (v_cnt != total_vertex_count) {
    logger << "[Error] Incorrect vertex number! "
           << "Expect: " << total_vertex_count << ", Got: " << v_cnt << "!\n";
    return false;
  }
  logger << "[INFO] --- Total Vertex Number: " << v_cnt << "\n";
  return true;
}

bool TestScanEdge(db::Snapshot* ss, std::stringstream& logger) {
  auto r = ss->ScanEdge();
  if (r.isErr()) {
    logger << "[Error] ScanEdge: " << r.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto ei = r.unwrap();
  if (!ei.Valid()) {
    logger << "[Error] Got invalid edge iterator handle!\n";
    return false;
  }

  unsigned e_cnt = 0;
  while (true) {
    auto re = ei.Next();
    if (re.isErr()) {
      logger << "[Error] EdgeIterator.Next(): " << re.unwrapErr().GetInfo() << "\n";
      return false;
    }
    auto e = re.unwrap();
    if (!e.Valid()) { break; }
    e_cnt++;
    if (!LogEdgeInfo(&e, logger)) {
      return false;
    }
  }
  if (e_cnt != total_edge_count) {
    logger << "[Error] Incorrect edge number! "
           << "Expect: " << total_edge_count << ", Got: " << e_cnt << "!\n";
    return false;
  }
  logger << "[INFO] --- Total Edge Number: " << e_cnt << "\n";
  return true;
}

bool TestGetVertex(db::Snapshot* ss, std::stringstream& logger) {
  // Get vertex: <VertexID: 2233628339503041259> <Label: software> <id: 5> <lang: java> <name: ripple>
  VertexId query_vid = 2233628339503041259U;
  int64_t expect_id_prop = 5L;
  std::string expect_name_prop = "ripple";
  std::string expect_lang_prop = "java";

  auto r = ss->GetVertex(query_vid, software_LabelId);
  if (r.isErr()) {
    logger << "[Error] GetVertex: " << r.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto v = r.unwrap();
  if (!v.Valid()) {
    logger << "[Error] Got invalid vertex handle!\n";
    return false;
  }

  // Check 'id' property
  auto id_prop = v.GetPropertyBy(id_PropId);
  if (!id_prop.Valid()) {
    logger << "[Error] Got invalid 'id' property handle!\n";
    return false;
  }
  auto id_prop_r = id_prop.GetAsInt64();
  if (id_prop_r.isErr()) {
    logger << "[Error] Property.GetAsInt64(): " << id_prop_r.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto id_prop_unwrapped = id_prop_r.unwrap();
  if (id_prop_unwrapped != expect_id_prop) {
    logger << "[Error] 'id' property mismatched! "
           << "Expect: " << expect_id_prop << ", Got: " << id_prop_unwrapped << "!\n";
    return false;
  }
  logger << "[INFO] --- 'id' property checking passed!\n";

  // Check 'name' property
  auto name_prop = v.GetPropertyBy(name_PropId);
  if (!name_prop.Valid()) {
    logger << "[Error] Got invalid 'name' property handle!\n";
    return false;
  }
  auto name_prop_r = name_prop.GetAsStr();
  if (name_prop_r.isErr()) {
    logger << "[Error] Property.GetAsStr(): " << name_prop_r.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto name_prop_unwrapped = name_prop_r.unwrap();
  std::string name_prop_str{static_cast<const char*>(name_prop_unwrapped.data), name_prop_unwrapped.len};
  if (name_prop_str != expect_name_prop) {
    logger << "[Error] 'name' property mismatched! "
           << "Expect: " << expect_name_prop << ", Got: " << name_prop_str << "!\n";
    return false;
  }
  logger << "[INFO] --- 'name' property checking passed!\n";

  // Check 'lang' property
  auto lang_prop = v.GetPropertyBy(lang_PropId);
  if (!lang_prop.Valid()) {
    logger << "[Error] Got invalid 'lang' property handle!\n";
    return false;
  }
  auto lang_prop_r = lang_prop.GetAsStr();
  if (lang_prop_r.isErr()) {
    logger << "[Error] Property.GetAsStr(): " << lang_prop_r.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto lang_prop_unwrapped = lang_prop_r.unwrap();
  std::string lang_prop_str{static_cast<const char*>(lang_prop_unwrapped.data), lang_prop_unwrapped.len};
  if (lang_prop_str != expect_lang_prop) {
    logger << "[Error] 'lang' property mismatched! "
           << "Expect: " << expect_lang_prop << ", Got: " << lang_prop_str << "!\n";
    return false;
  }
  logger << "[INFO] --- 'lang' property checking passed!\n";

  return true;
}

bool TestGetEdge(db::Snapshot* ss, std::stringstream& logger) {
  // Get edge: <EdgeID: (0, 16401677891599130309, 10454779632061085998)>
  //           <EdgeRelation: (created, person, software)>
  //           <id: 12> <weight: 0.200000>
  EdgeId query_edge_id{0, 16401677891599130309U, 10454779632061085998U};
  EdgeRelation query_edge_rel{created_EdgeLabelId, person_LabelId, software_LabelId};
  int64_t expect_id_prop = 12L;
  double expect_weight_prop = 0.200000;

  auto r = ss->GetEdge(query_edge_id, query_edge_rel);
  if (r.isErr()) {
    logger << "[Error] GetEdge: " << r.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto e = r.unwrap();
  if (!e.Valid()) {
    logger << "[Error] Got invalid edge handle!\n";
    return false;
  }

  // Check 'id' property
  auto id_prop = e.GetPropertyBy(id_PropId);
  if (!id_prop.Valid()) {
    logger << "[Error] Got invalid 'id' property handle!\n";
    return false;
  }
  auto id_prop_r = id_prop.GetAsInt64();
  if (id_prop_r.isErr()) {
    logger << "[Error] Property.GetAsInt64(): " << id_prop_r.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto id_prop_unwrapped = id_prop_r.unwrap();
  if (id_prop_unwrapped != expect_id_prop) {
    logger << "[Error] 'id' property mismatched! "
           << "Expect: " << expect_id_prop << ", Got: " << id_prop_unwrapped << "!\n";
    return false;
  }
  logger << "[INFO] --- 'id' property checking passed!\n";

  // Check 'weight' property
  auto weight_prop = e.GetPropertyBy(weight_PropId);
  if (!weight_prop.Valid()) {
    logger << "[Error] Got invalid 'weight' property handle!\n";
    return false;
  }
  auto weight_prop_r = weight_prop.GetAsDouble();
  if (weight_prop_r.isErr()) {
    logger << "[Error] Property.GetAsDouble(): " << weight_prop_r.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto weight_prop_unwrapped = weight_prop_r.unwrap();
  if (weight_prop_unwrapped != expect_weight_prop) {
    logger << "[Error] 'weight' property mismatched! "
           << "Expect: " << expect_weight_prop << ", Got: " << weight_prop_unwrapped << "!\n";
    return false;
  }
  logger << "[INFO] --- 'weight' property checking passed!\n";

  return true;
}

bool TestGetOutEdges(db::Snapshot* ss, std::stringstream& logger) {
  // Query src vertex: <VertexID: 10714315738933730127> <Label: person> <age: 29> <name: marko> <id: 1>
  VertexId query_vid = 10714315738933730127U;
  unsigned expect_knows_num = 2;
  unsigned expect_created_num = 1;

  auto knows_r = ss->GetOutEdges(query_vid, knows_EdgeLabelId);
  if (knows_r.isErr()) {
    logger << "[Error] GetOutEdges: " << knows_r.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto knows_ei = knows_r.unwrap();
  if (!knows_ei.Valid()) {
    logger << "[Error] Got invalid edge iterator handle at GetOutEdges for (" << query_vid << ", knows)!\n";
    return false;
  }
  unsigned knows_cnt = 0;
  while (true) {
    auto re = knows_ei.Next();
    if (re.isErr()) {
      logger << "[Error] EdgeIterator.Next(): " << re.unwrapErr().GetInfo() << "\n";
      return false;
    }
    auto e = re.unwrap();
    if (!e.Valid()) { break; }
    knows_cnt++;
    if (!LogEdgeInfo(&e, logger)) {
      return false;
    }
  }
  if (knows_cnt != expect_knows_num) {
    logger << "[Error] Incorrect 'knows' out-neighbour number! "
           << "Expect: " << expect_knows_num << ", Got: " << knows_cnt << "!\n";
    return false;
  }
  logger << "[INFO] --- Get 'knows' out-edges passed! Total 'knows' out-neighbour number: " << knows_cnt << "\n";

  auto created_r = ss->GetOutEdges(query_vid, created_EdgeLabelId);
  if (created_r.isErr()) {
    logger << "[Error] GetOutEdges: " << created_r.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto created_ei = created_r.unwrap();
  if (!created_ei.Valid()) {
    logger << "[Error] Got invalid edge iterator handle at GetOutEdges for (" << query_vid << ", created)!\n";
    return false;
  }
  unsigned created_cnt = 0;
  while (true) {
    auto re = created_ei.Next();
    if (re.isErr()) {
      logger << "[Error] EdgeIterator.Next(): " << re.unwrapErr().GetInfo() << "\n";
      return false;
    }
    auto e = re.unwrap();
    if (!e.Valid()) { break; }
    created_cnt++;
    if (!LogEdgeInfo(&e, logger)) {
      return false;
    }
  }
  if (created_cnt != expect_created_num) {
    logger << "[Error] Incorrect 'created' out-neighbour number! "
           << "Expect: " << expect_created_num << ", Got: " << created_cnt << "!\n";
    return false;
  }
  logger << "[INFO] --- Get 'created' out-edges passed! Total 'created' out-neighbour number: " << created_cnt << "\n";

  return true;
}

bool TestGetInEdges(db::Snapshot* ss, std::stringstream& logger) {
  // Query src vertex: <VertexID: 10454779632061085998> <Label: software> <id: 3> <name: lop> <lang: java>
  VertexId query_vid = 10454779632061085998U;
  unsigned expect_nbr_num = 3;

  auto r = ss->GetInEdges(query_vid);
  if (r.isErr()) {
    logger << "[Error] GetInEdges: " << r.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto ei = r.unwrap();
  if (!ei.Valid()) {
    logger << "[Error] Got invalid edge iterator handle at GetInEdges for (" << query_vid << ", None)!\n";
    return false;
  }
  unsigned nbr_cnt = 0;
  while (true) {
    auto re = ei.Next();
    if (re.isErr()) {
      logger << "[Error] EdgeIterator.Next(): " << re.unwrapErr().GetInfo() << "\n";
      return false;
    }
    auto e = re.unwrap();
    if (!e.Valid()) { break; }
    nbr_cnt++;
    if (!LogEdgeInfo(&e, logger)) {
      return false;
    }
  }
  if (nbr_cnt != expect_nbr_num) {
    logger << "[Error] Incorrect in-neighbour number! "
           << "Expect: " << expect_nbr_num << ", Got: " << nbr_cnt << "!\n";
    return false;
  }
  logger << "[INFO] --- Get in-edges passed! Total in-neighbour number: " << nbr_cnt << "\n";

  return true;
}

bool TestGetOutDegree(db::Snapshot* ss, std::stringstream& logger) {
  // Query src vertex: <VertexID: 12334515728491031937> <Label: person> <name: josh> <id: 4> <age: 32>
  VertexId query_vid = 12334515728491031937U;
  EdgeRelation query_edge_rel{created_EdgeLabelId, person_LabelId, software_LabelId};
  size_t expect_created_degree = 2;

  auto r = ss->GetOutDegree(query_vid, query_edge_rel);
  if (r.isErr()) {
    logger << "[Error] GetOutDegree: " << r.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto out_degree = r.unwrap();
  if (out_degree != expect_created_degree) {
    logger << "[Error] Incorrect 'created' out degree! "
           << "Expect: " << expect_created_degree << ", Got: " << out_degree << "!\n";
    return false;
  }
  logger << "[INFO] --- Get 'created' out degree passed!\n";

  return true;
}

bool TestGetInDegree(db::Snapshot* ss, std::stringstream& logger) {
  // Query src vertex: <VertexID: 10454779632061085998> <Label: software> <id: 3> <name: lop> <lang: java>
  VertexId query_vid = 10454779632061085998U;
  EdgeRelation query_edge_rel{created_EdgeLabelId, person_LabelId, software_LabelId};
  size_t expect_created_degree = 3;

  auto r = ss->GetInDegree(query_vid, query_edge_rel);
  if (r.isErr()) {
    logger << "[Error] GetInDegree: " << r.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto in_degree = r.unwrap();
  if (in_degree != expect_created_degree) {
    logger << "[Error] Incorrect 'created' in degree! "
           << "Expect: " << expect_created_degree << ", Got: " << in_degree << "!\n";
    return false;
  }
  logger << "[INFO] --- Get 'created' in degree passed!\n";

  return true;
}

bool TestGetKthOutEdge(db::Snapshot* ss, std::stringstream& logger) {
  // Query src vertex: <VertexID: 10714315738933730127> <Label: person> <age: 29> <id: 1> <name: marko>
  VertexId query_vid = 10714315738933730127U;
  EdgeRelation query_edge_rel{knows_EdgeLabelId, person_LabelId, person_LabelId};
  SerialId k1 = 1;
  VertexId expect_k1_nbr_vid = 12334515728491031937U;
  SerialId k2 = 5;

  auto r1 = ss->GetKthOutEdge(query_vid, query_edge_rel, k1);
  if (r1.isErr()) {
    logger << "[Error] GetKthOutEdge: " << r1.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto e1 = r1.unwrap();
  if (!e1.Valid()) {
    logger << "[Error] Got invalid edge handle at GetKthOutEdge for (" << query_vid << ", knows, " << k1 << ")!\n";
    return false;
  }
  auto k1_nbr_vid = e1.GetEdgeId().dst_vertex_id;
  if (k1_nbr_vid != expect_k1_nbr_vid) {
    logger << "[Error] The k-th out-neighbour mismatched!(k = " << k1 << ") "
           << "Expect VertexId: " << expect_k1_nbr_vid << ", Got VertexId: " << k1_nbr_vid << "!\n";
    return false;
  }
  logger << "[INFO] Got k-th 'knows' out neighbour: " << k1_nbr_vid << "\n";
  logger << "[INFO] --- Case_1 Passed!\n";

  auto r2 = ss->GetKthOutEdge(query_vid, query_edge_rel, k2);
  if (r2.isErr()) {
    logger << "[Error] GetKthOutEdge: " << r2.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto e2 = r2.unwrap();
  if (e2.Valid()) {
    logger << "[Error] Expect null edge handle, but got an exact one!\n";
    return false;
  }
  logger << "[INFO] Got expected null edge handle.\n";
  logger << "[INFO] --- Case_2 Passed!\n";

  return true;
}

bool TestGetKthInEdge(db::Snapshot* ss, std::stringstream& logger) {
  // Query src vertex: <VertexID: 10454779632061085998> <Label: software> <id: 3> <name: lop> <lang: java>
  VertexId query_vid = 10454779632061085998U;
  EdgeRelation query_edge_rel{created_EdgeLabelId, person_LabelId, software_LabelId};
  SerialId k1 = 2;
  VertexId expect_k1_nbr_vid = 16401677891599130309U;
  SerialId k2 = 5;

  auto r1 = ss->GetKthInEdge(query_vid, query_edge_rel, k1);
  if (r1.isErr()) {
    logger << "[Error] GetKthInEdge: " << r1.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto e1 = r1.unwrap();
  if (!e1.Valid()) {
    logger << "[Error] Got invalid edge handle at GetKthInEdge for (" << query_vid << ", created, " << k1 << ")!\n";
    return false;
  }
  auto k1_nbr_vid = e1.GetEdgeId().src_vertex_id;
  if (k1_nbr_vid != expect_k1_nbr_vid) {
    logger << "[Error] The k-th in-neighbour mismatched!(k = " << k1 << ") "
           << "Expect VertexId: " << expect_k1_nbr_vid << ", Got VertexId: " << k1_nbr_vid << "!\n";
    return false;
  }
  logger << "[INFO] Got k-th 'created' in neighbour: " << k1_nbr_vid << "\n";
  logger << "[INFO] --- Case_1 Passed!\n";

  auto r2 = ss->GetKthInEdge(query_vid, query_edge_rel, k2);
  if (r2.isErr()) {
    logger << "[Error] GetKthInEdge: " << r2.unwrapErr().GetInfo() << "\n";
    return false;
  }
  auto e2 = r2.unwrap();
  if (e2.Valid()) {
    logger << "[Error] Expect null edge handle, but got an exact one!\n";
    return false;
  }
  logger << "[INFO] Got expected null edge handle.\n";
  logger << "[INFO] --- Case_2 Passed!\n";

  return true;
}

bool TestGetSnapshotId(db::Snapshot* ss, std::stringstream& logger) {
  SnapshotId expect_ss_id = std::numeric_limits<uint32_t>::max();
  auto ss_id = ss->GetSnapshotId();
  if (ss_id != expect_ss_id) {
    logger << "[Error] Snapshot id mismatched! "
           << "Expect: " << expect_ss_id << ", Got: " << ss_id << "!\n";
    return false;
  }
  logger << "[INFO] Got correct snapshot id: " << ss_id << "\n";

  return true;
}

const unsigned test_num = 11;

typedef bool (*TestFunc)(db::Snapshot* ss, std::stringstream& logger);

bool RunTest(unsigned id, const std::string& test_name, TestFunc f, db::Snapshot* ss, std::stringstream& logger) {
  logger << "[INFO] ----------------------------------------------\n";
  logger << "[INFO] --- " << test_name << " Test [" << id << "/" << test_num << "]\n";
  logger << "[INFO] ----------------------------------------------\n";
  bool result = f(ss, logger);
  logger << "[INFO] --- " << (result ? "PASSED!" : "FAILED!") << "\n";
  logger << "[INFO]\n";
  return result;
}

TestResult* runLocalTests() {
  assert(local_graph_handle_ != nullptr);

  std::stringstream logger;
  logger << "\n----------------------- Store FFI Tests -----------------------\n";

  unsigned success_num = 0;
  SnapshotId query_snapshot_id = std::numeric_limits<uint32_t>::max();
  db::Snapshot latest_ss(ffi::GetSnapshot(local_graph_handle_, query_snapshot_id));
  if (latest_ss.Valid()) {
    success_num += RunTest(1, "ScanVertex", TestScanVertex, &latest_ss, logger) ? 1 : 0;
    success_num += RunTest(2, "ScanEdge", TestScanEdge, &latest_ss, logger) ? 1 : 0;
    success_num += RunTest(3, "GetVertex", TestGetVertex, &latest_ss, logger) ? 1 : 0;
    success_num += RunTest(4, "GetEdge", TestGetEdge, &latest_ss, logger) ? 1 : 0;
    success_num += RunTest(5, "GetOutEdges", TestGetOutEdges, &latest_ss, logger) ? 1 : 0;
    success_num += RunTest(6, "GetInEdges", TestGetInEdges, &latest_ss, logger) ? 1 : 0;
    success_num += RunTest(7, "GetOutDegree", TestGetOutDegree, &latest_ss, logger) ? 1 : 0;
    success_num += RunTest(8, "GetInDegree", TestGetInDegree, &latest_ss, logger) ? 1 : 0;
    success_num += RunTest(9, "GetKthOutEdge", TestGetKthOutEdge, &latest_ss, logger) ? 1 : 0;
    success_num += RunTest(10, "GetKthInEdge", TestGetKthInEdge, &latest_ss, logger) ? 1 : 0;
    success_num += RunTest(11, "GetSnapshotId", TestGetSnapshotId, &latest_ss, logger) ? 1 : 0;
  } else {
    logger << "[Error] Got invalid snapshot handle with SnapshotId=" << query_snapshot_id << "!\n";
  }

  logger << "---------------------------------------------------------------\n";
  logger << "[SUMMARY] " << "Successful: " << success_num << "/" << test_num
         << ", Failed: " << (test_num - success_num) << "/" << test_num << ".\n";
  logger << "---------------------------------------------------------------\n";

  return new TestResult(success_num == test_num, logger);
}

bool getTestResultFlag(const TestResult* r) {
  return r->GetResult();
}

const char* getTestResultInfo(const TestResult* r) {
  return r->GetInfo();
}

void freeTestResult(TestResult* r) {
  delete r;
}

}
