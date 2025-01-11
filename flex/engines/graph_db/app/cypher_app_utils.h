#ifndef ENGINES_GRAPH_DB_CYPHER_APP_UTILS_H_
#define ENGINES_GRAPH_DB_CYPHER_APP_UTILS_H_

#include <map>
#include <string>
#include <unordered_map>
#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {
bool generate_plan(
    const std::string& query, const std::string& statistics,
    const std::string& compiler_yaml,
    std::unordered_map<std::string, physical::PhysicalPlan>& plan_cache);
void parse_params(std::string_view sw,
                  std::map<std::string, std::string>& params);
}  // namespace gs

#endif  // ENGINES_GRAPH_DB_CYPHER_APP_UTILS_H_