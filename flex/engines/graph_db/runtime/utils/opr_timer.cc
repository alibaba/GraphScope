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

#include "flex/engines/graph_db/runtime/utils/opr_timer.h"
#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {

namespace runtime {

void OprTimer::output(const std::string& path) const {
#ifdef RT_PROFILE
  std::ofstream fout(path);
  fout << "Total time: " << total_time_ << std::endl;
  fout << "============= operators =============" << std::endl;
  double opr_total = 0;
  for (const auto& pair : opr_timers_) {
    opr_total += pair.second;
    fout << pair.first << ": " << pair.second << " ("
         << pair.second / total_time_ * 100.0 << "%)" << std::endl;
  }
  fout << "remaining: " << total_time_ - opr_total << " ("
       << (total_time_ - opr_total) / total_time_ * 100.0 << "%)" << std::endl;
  fout << "============= routines  =============" << std::endl;
  for (const auto& pair : routine_timers_) {
    fout << pair.first << ": " << pair.second << " ("
         << pair.second / total_time_ * 100.0 << "%)" << std::endl;
  }
  fout << "=====================================" << std::endl;
#endif
}

void OprTimer::clear() {
#ifdef RT_PROFILE
  opr_timers_.clear();
  routine_timers_.clear();
  total_time_ = 0;
#endif
}

OprTimer& OprTimer::operator+=(const OprTimer& other) {
#ifdef RT_PROFILE
  total_time_ += other.total_time_;
  for (const auto& pair : other.opr_timers_) {
    opr_timers_[pair.first] += pair.second;
  }
  for (const auto& pair : other.routine_timers_) {
    routine_timers_[pair.first] += pair.second;
  }
#endif
  return *this;
}
/**
static std::string get_opr_name(const physical::PhysicalOpr& opr) {
  switch (opr.opr().op_kind_case()) {
  case physical::PhysicalOpr_Operator::OpKindCase::kScan: {
    return "scan";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kEdge: {
    return "edge_expand";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kVertex: {
    return "get_v";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kOrderBy: {
    return "order_by";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kProject: {
    return "project";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kSink: {
    return "sink";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kDedup: {
    return "dedup";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kGroupBy: {
    return "group_by";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kSelect: {
    return "select";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kPath: {
    return "path";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kJoin: {
    return "join";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kRoot: {
    return "root";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kIntersect: {
    return "intersect";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kUnion: {
    return "union";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kUnfold: {
    return "unfold";
  }
  default:
    return "unknown - " +
           std::to_string(static_cast<int>(opr.opr().op_kind_case()));
  }
}*/

}  // namespace runtime

}  // namespace gs