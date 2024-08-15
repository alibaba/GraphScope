#ifndef RUNTIME_COMMON_UTILS_H_
#define RUNTIME_COMMON_UTILS_H_
#include <sstream>
#include <string>
#include <vector>

#include "flex/proto_generated_gie/algebra.pb.h"

#include "flex/engines/graph_db/runtime/common/types.h"
namespace gs {
namespace runtime {

Direction parse_direction(const physical::EdgeExpand_Direction& dir);

std::vector<label_t> parse_tables(const algebra::QueryParams& query_params);

std::vector<LabelTriplet> parse_label_triplets(
    const physical::PhysicalOpr_MetaData& meta);

struct ScanParams {
  int alias;
  std::vector<label_t> tables;

  ScanParams(int alias, const std::vector<label_t>& tables)
      : alias(alias), tables(tables) {}
  ScanParams() = default;

  std::string toString() const {
    std::stringstream ss;
    ss << "ScanParams(" << alias << ", ";
    ss << "{";
    for (size_t i = 0; i < tables.size(); ++i) {
      if (i + 1 == tables.size()) {
        ss << static_cast<int>(tables[i]);
      } else {
        ss << static_cast<int>(tables[i]) << ", ";
      }
    }
    ss << "})";
    return ss.str();
  }
};

struct GetVParams {
  VOpt opt;
  int tag;
  std::vector<label_t> tables;
  int alias;
  GetVParams(VOpt opt, int tag, const std::vector<label_t>& tables, int alias)
      : opt(opt), tag(tag), tables(tables), alias(alias) {}
  GetVParams() = default;
  std::string toString() const {
    std::stringstream ss;
    ss << "GetVParams(" << vopt_2_str(opt) << ", " << tag << ", ";
    ss << "{";
    for (size_t i = 0; i < tables.size(); ++i) {
      if (i + 1 == tables.size()) {
        ss << static_cast<int>(tables[i]);
      } else {
        ss << static_cast<int>(tables[i]) << ", ";
      }
    }
    ss << "}, " << alias << ")";
    return ss.str();
  }
};

}  // namespace runtime
}  // namespace gs

#endif
