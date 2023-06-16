/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#ifndef CODEGEN_SRC_PB_PARSER_IR_DATA_TYPE_PARSER_H_
#define CODEGEN_SRC_PB_PARSER_IR_DATA_TYPE_PARSER_H_

#include <boost/functional/hash.hpp>

#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/pb_parser/name_id_parser.h"
#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

namespace gs {

// There can be multiple labels.
// for each label, we have multiple properties.
// deuplicate the property name and types if two edge label are same, only
// differs on src-dst pair
static bool try_to_get_prop_names_and_types_from_ir_data_type(
    const common::IrDataType& ir_data_type,
    std::vector<std::vector<std::string>>& prop_names,
    std::vector<std::vector<std::string>>& prop_types) {
  switch (ir_data_type.type_case()) {
  case common::IrDataType::TypeCase::kDataType: {
    VLOG(10) << "Primitive type is not supported yet.";
    return false;
  }
  case common::IrDataType::TypeCase::kGraphType: {
    auto& graph_ele_type = ir_data_type.graph_type();
    auto ele = graph_ele_type.element_opt();
    if (ele == common::GraphDataType::GraphElementOpt::
                   GraphDataType_GraphElementOpt_VERTEX) {
      VLOG(10) << "Get property for vertex element: ";
    } else {
      VLOG(10) << "Get property for edge element: ";
    }
    auto& graph_data_type = graph_ele_type.graph_data_type();
    VLOG(10) << "Element label types size: " << graph_data_type.size();
    // these graph_data_type should be the same.
    if (graph_data_type.size() > 0) {
      auto& cur_ele_prop_types = graph_data_type[0].props();
      // one label can have multiple properties
      if (cur_ele_prop_types.size() > 0) {
        std::unordered_set<std::pair<std::string, std::string>,
                           boost::hash<std::pair<std::string, std::string>>>
            prop_set;
        for (auto j = 0; j < cur_ele_prop_types.size(); ++j) {
          std::string prop_name;
          CHECK(get_name_from_name_or_id(cur_ele_prop_types[j].prop_id(),
                                         prop_name));
          auto prop_type = data_type_2_string(
              common_data_type_pb_2_data_type(cur_ele_prop_types[j].type()));
          prop_set.insert(std::make_pair(prop_name, prop_type));
        }
        std::vector<std::string> cur_prop_names;
        std::vector<std::string> cur_prop_types;
        for (auto iter : prop_set) {
          cur_prop_names.emplace_back(iter.first);
          cur_prop_types.emplace_back(iter.second);
        }
        prop_names.emplace_back(cur_prop_names);
        prop_types.emplace_back(cur_prop_types);
      } else {
        // no property for this label
        VLOG(10) << "No property type found for GraphElementType";
      }

      VLOG(10) << "Finish parsing property names and types.";
      if (prop_names.size() > 0) {
        VLOG(10) << "Property names: " << gs::to_string(prop_names);
        VLOG(10) << "Property types: " << gs::to_string(prop_types);
        return true;
      } else {
        VLOG(10) << "No property names and types found in the graph element.";
        return false;
      }
    } else {
      return false;
    }
  }
  default: {
    VLOG(10) << "Unsupported data type: " << ir_data_type.DebugString();
    return false;
  }
  }
}
}  // namespace gs

#endif  // CODEGEN_SRC_PB_PARSER_IR_DATA_TYPE_PARSER_H_