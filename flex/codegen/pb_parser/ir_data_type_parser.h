#ifndef IR_DATA_TYPE_PARSER_H
#define IR_DATA_TYPE_PARSER_H

#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

#include "flex/codegen/graph_types.h"
#include "flex/codegen/pb_parser/name_id_parser.h"

#include <boost/functional/hash.hpp>
#include "flex/engines/hqps/engine/hqps_utils.h"

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
    LOG(INFO) << "Primitive type is not supported yet.";
    return false;
  }
  case common::IrDataType::TypeCase::kGraphType: {
    auto& graph_ele_type = ir_data_type.graph_type();
    auto ele = graph_ele_type.element_opt();
    if (ele == common::GraphDataType::GraphElementOpt::
                   GraphDataType_GraphElementOpt_VERTEX) {
      LOG(INFO) << "Get property for vertex element: ";
    } else {
      LOG(INFO) << "Get property for edge element: ";
    }
    auto& graph_data_type = graph_ele_type.graph_data_type();
    LOG(INFO) << "Element label types size: " << graph_data_type.size();
    // these graph_data_type should be the same.
    if (graph_data_type.size() > 0) {
      auto& cur_ele_prop_types = graph_data_type[0].props();
      // one label can have multiple properties
      if (cur_ele_prop_types.size() > 0) {
        // std::vector<std::string> cur_prop_names;
        // std::vector<std::string> cur_prop_types;
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
        LOG(INFO) << "No property type found for GraphElementType";
      }

      LOG(INFO) << "Finish parsing property names and types.";
      if (prop_names.size() > 0) {
        LOG(INFO) << "Property names: " << gs::to_string(prop_names);
        LOG(INFO) << "Property types: " << gs::to_string(prop_types);
        return true;
      } else {
        LOG(INFO) << "No property names and types found in the graph element.";
        return false;
      }
    } else {
      return false;
    }
  }
  default: {
    LOG(INFO) << "Unsupported data type: " << ir_data_type.DebugString();
    return false;
  }
  }
}
}  // namespace gs

#endif  // IR_DATA_TYPE_PARSER_H