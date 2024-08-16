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

  std::string to_string() const {
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
  std::string to_string() const {
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

struct EdgeExpandParams {
  int v_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  Direction dir;

  EdgeExpandParams(int v_tag, const std::vector<LabelTriplet>& labels,
                   int alias, Direction dir)
      : v_tag(v_tag), labels(labels), alias(alias), dir(dir) {}
  EdgeExpandParams() = default;

  std::string to_string() const {
    std::stringstream ss;
    ss << "EdgeExpandParams(" << v_tag << ", ";
    ss << "{";
    for (size_t i = 0; i < labels.size(); ++i) {
      if (i + 1 == labels.size()) {
        ss << "LabelTriplet" << labels[i].to_string();
      } else {
        ss << "LabelTriplet" << labels[i].to_string() << ", ";
      }
    }
    ss << "}, " << alias << ", " << dir_2_str(dir) << ")";
    return ss.str();
  }
};

struct PathExpandParams {
  int start_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  Direction dir;
  int hop_lower;
  int hop_upper;
  std::set<int> keep_cols;

  PathExpandParams(int start_tag, const std::vector<LabelTriplet>& labels,
                   int alias, Direction dir, int hop_lower, int hop_upper,
                   const std::set<int>& keep_cols = {})
      : start_tag(start_tag),
        labels(labels),
        alias(alias),
        dir(dir),
        hop_lower(hop_lower),
        hop_upper(hop_upper),
        keep_cols(keep_cols) {}
  PathExpandParams() = default;

  std::string to_string() const {
    std::string ss;
    ss += "PathExpandParams(";
    ss += std::to_string(start_tag) + ", ";
    ss += "{";
    for (size_t i = 0; i < labels.size(); ++i) {
      if (i + 1 == labels.size()) {
        ss += "LabelTriplet" + labels[i].to_string();
      } else {
        ss += "LabelTriplet" + labels[i].to_string() + ", ";
      }
    }
    ss += "}, ";
    ss += dir_2_str(dir) + ", ";
    ss += std::to_string(hop_lower) + ", ";
    ss += std::to_string(hop_upper);
    ss += ")";
    return ss;
  }
};

}  // namespace runtime
}  // namespace gs

#endif
