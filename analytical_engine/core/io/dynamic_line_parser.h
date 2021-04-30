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

#ifndef ANALYTICAL_ENGINE_CORE_IO_DYNAMIC_LINE_PARSER_H_
#define ANALYTICAL_ENGINE_CORE_IO_DYNAMIC_LINE_PARSER_H_

#ifdef NETWORKX

#include <cctype>

#include <regex>
#include <string>
#include <utility>
#include <vector>

#include "folly/dynamic.h"
#include "folly/json.h"

#include "grape/config.h"
#include "grape/io/line_parser_base.h"
#include "grape/types.h"

namespace gs {
/**
 * @brief A parser can parse a line that represents an edge. A line may contain
 * the source, destination, and data of the edge.
 *
 */
class DynamicLineParser
    : public grape::LineParserBase<folly::dynamic, folly::dynamic,
                                   folly::dynamic> {
 public:
  using oid_t = folly::dynamic;
  using vdata_t = folly::dynamic;
  using edata_t = folly::dynamic;
  DynamicLineParser() = default;

  void LineParserForEFile(const std::string& line, oid_t& u, oid_t& v,
                          edata_t& e_data) override {
    auto edge = folly::parseJson(line);
    u = edge[0];
    v = edge[1];
    if (edge.size() == 3) {
      e_data = edge[2];
    } else if (edge.size() > 3) {
      throw std::runtime_error("not a valid edge: " + line);
    }
  }

  void LineParserForVFile(const std::string& line, oid_t& u,
                          oid_t& u_data) override {
    auto node = folly::parseJson(line);
    u = node[0];
    if (node.size() > 1) {
      u_data = node[1];
    }
  }
};
}  // namespace gs

#endif  // NETWORKX
#endif  // ANALYTICAL_ENGINE_CORE_IO_DYNAMIC_LINE_PARSER_H_
