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

#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_SELECTOR_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_SELECTOR_H_

#include <glog/logging.h>

#include <regex>
#include <string>
#include <utility>
#include <vector>

#include "boost/algorithm/string/case_conv.hpp"
#include "boost/foreach.hpp"
#include "boost/leaf/error.hpp"
#include "boost/leaf/result.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/property_tree/exceptions.hpp"
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"

#include "vineyard/graph/fragment/property_graph_types.h"

#include "core/error.h"

namespace bl = boost::leaf;

namespace gs {

inline std::string generate_selectors(
    const std::vector<std::pair<std::string, std::string>>& selector_list) {
  boost::property_tree::ptree tree;
  for (auto& pair : selector_list) {
    tree.put(pair.first, pair.second);
  }
  std::stringstream ss;
  boost::property_tree::json_parser::write_json(ss, tree, false);
  return ss.str();
}

enum class SelectorType {
  kVertexId,
  kVertexLabelId,
  kVertexData,
  kEdgeSrc,
  kEdgeDst,
  kEdgeData,
  kResult
};

/**
 * @brief This is the model class of non-labeled selector. The selector is used
 * to select the data in the context/fragment. A selector can pick up the vertex
 * ids or the data attached to the vertex or the data in the context.
 */
class Selector {
 protected:
  explicit Selector(std::string property_name)
      : type_(SelectorType::kResult),
        property_name_(std::move(property_name)) {}

  explicit Selector(SelectorType type) : type_(type) {}

 public:
  virtual ~Selector() = default;

  SelectorType type() const { return type_; }

  std::string property_name() const { return property_name_; }

  virtual std::string str() const {
    switch (type_) {
    case SelectorType::kVertexId:
      return "v.id";
    case SelectorType::kVertexLabelId:
      return "v.label_id";
    case SelectorType::kVertexData:
      return "v.data";
    case SelectorType::kEdgeSrc:
      return "e.src";
    case SelectorType::kEdgeDst:
      return "e.dst";
    case SelectorType::kEdgeData:
      return "e.data";
    case SelectorType::kResult: {
      if (property_name_.empty())
        return "r";
      return "r." + property_name_;
    }
    }
    return "";
  }

  /**
   * @brief parse a string selector to Selector object.
   *
   * @param selector, valid selector patterns:
   *  v.id
   *  v.data
   *  r
   *  r.prop_name
   * @return bl::result<Selector>
   */
  static bl::result<Selector> parse(std::string selector) {
    boost::algorithm::to_lower(selector);
    std::smatch sm;

    std::regex r_vid("v\\.id");
    std::regex r_vlabel_id("v\\.label_id");
    std::regex r_vdata("v\\.data");
    std::regex r_esrc("e\\.src");
    std::regex r_edst("e\\.dst");
    std::regex r_edata("e\\.data");
    std::regex r_result("r");
    std::regex r_result_prop("r\\.(.*?)");

    if (std::regex_match(selector, sm, r_vid)) {
      return Selector(SelectorType::kVertexId);
    } else if (std::regex_match(selector, sm, r_vlabel_id)) {
      return Selector(SelectorType::kVertexLabelId);
    } else if (std::regex_match(selector, sm, r_vdata)) {
      return Selector(SelectorType::kVertexData);
    } else if (std::regex_match(selector, sm, r_esrc)) {
      return Selector(SelectorType::kEdgeSrc);
    } else if (std::regex_match(selector, sm, r_edst)) {
      return Selector(SelectorType::kEdgeDst);
    } else if (std::regex_match(selector, sm, r_edata)) {
      return Selector(SelectorType::kEdgeData);
    } else if (std::regex_match(selector, sm, r_result)) {
      return Selector(SelectorType::kResult);
    } else if (std::regex_match(selector, sm, r_result_prop)) {
      std::string prop_name = sm[1];
      if (prop_name.empty()) {
        RETURN_GS_ERROR(
            vineyard::ErrorCode::kInvalidValueError,
            "Property name not found, the selector is: " + selector);
      }
      return Selector(prop_name);
    }
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                    "Invalid syntax, the selector is: " + selector);
  }

  /**
   * @brief parse selectors from a json string.
   *
   * @param s_selectors JSON {"col_name": "selector", ...}
   * @return bl::result<std::vector<std::pair<std::string, Selector>>>
   */
  static bl::result<std::vector<std::pair<std::string, Selector>>>
  ParseSelectors(const std::string& s_selectors) {
    std::stringstream ss(s_selectors);
    boost::property_tree::ptree pt;
    std::vector<std::pair<std::string, Selector>> selectors;

    try {
      boost::property_tree::read_json(ss, pt);
      BOOST_FOREACH  // NOLINT(whitespace/parens)
          (boost::property_tree::ptree::value_type & v, pt) {
        CHECK(v.second.empty());
        std::string col_name = v.first;
        std::string s_selector = v.second.data();

        BOOST_LEAF_AUTO(selector, Selector::parse(s_selector));
        selectors.emplace_back(col_name, selector);
      }
    } catch (boost::property_tree::ptree_error& e) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "Failed to parse json: " + s_selectors);
    }

    return selectors;
  }

 private:
  SelectorType type_;
  std::string property_name_;
};

/**
 * @brief LabeledSelector is used to select the data in the labeled
 * context/fragment. A selector can pick up the vertex ids or the data attached
 * to the vertex or the data in the context.
 */
class LabeledSelector : public Selector {
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;
  using prop_id_t = vineyard::property_graph_types::PROP_ID_TYPE;

  LabeledSelector(SelectorType type, label_id_t label_id)
      : Selector(type), label_id_(label_id), property_id_(0) {}

  LabeledSelector(SelectorType type, label_id_t label_id, prop_id_t prop_id)
      : Selector(type), label_id_(label_id), property_id_(prop_id) {}

  LabeledSelector(label_id_t label_id, std::string prop_name)
      : Selector(std::move(prop_name)), label_id_(label_id), property_id_(0) {}

 public:
  label_id_t label_id() const { return label_id_; }

  prop_id_t property_id() const { return property_id_; }

  std::string str() const override {
    switch (type()) {
    case SelectorType::kVertexId:
      return "v:label" + std::to_string(label_id_) + ".id";
    case SelectorType::kVertexData:
      return "v:label" + std::to_string(label_id_) + ".property" +
             std::to_string(property_id_);
    case SelectorType::kEdgeSrc:
      return "e:label" + std::to_string(label_id_) + ".src";
    case SelectorType::kEdgeDst:
      return "e:label" + std::to_string(label_id_) + ".dst";
    case SelectorType::kEdgeData:
      return "e:label" + std::to_string(label_id_) + ".property" +
             std::to_string(property_id_);
    case SelectorType::kResult: {
      std::string ret = "r:label" + std::to_string(label_id_);
      if (!property_name().empty()) {
        ret = ret + "." + property_name();
      }
      return ret;
    }
    default:
      break;
    }
    return "";
  }

  /**
   *
   * @param selector
   *  v:label{x}.id
   *  v:label{x}.property{y}
   *  e:label{x}.src
   *  e:label{x}.dst
   *  e:label{x}.property{y}
   *  r:label{x}[.prop_name]
   *
   *  \a x and \a y represent the index of the label and property responsively.
   * @return
   */
  static bl::result<LabeledSelector> parse(std::string selector) {
    boost::algorithm::to_lower(selector);
    std::smatch sm;

    std::regex r_vid("v:label(\\d+)\\.id");
    std::regex r_vdata("v:label(\\d+)\\.property(\\d+)");
    std::regex r_esrc_id("e:label(\\d+)\\.src");
    std::regex r_edst_id("e:label(\\d+)\\.dst");
    std::regex r_edata("e:label(\\d+)\\.property(\\d+)");
    std::regex r_result("r:label(\\d+)");
    std::regex r_result_prop("r:label(\\d+)\\.(.*?)");
    if (std::regex_match(selector, sm, r_vid)) {
      auto label_id = boost::lexical_cast<label_id_t>(sm[1]);

      return LabeledSelector(SelectorType::kVertexId, label_id);
    } else if (std::regex_match(selector, sm, r_vdata)) {
      auto label_id = boost::lexical_cast<label_id_t>(sm[1]);
      auto prop_id = boost::lexical_cast<prop_id_t>(sm[2]);

      return LabeledSelector(SelectorType::kVertexData, label_id, prop_id);
    } else if (std::regex_match(selector, sm, r_esrc_id)) {
      auto label_id = boost::lexical_cast<label_id_t>(sm[1]);

      return LabeledSelector(SelectorType::kEdgeSrc, label_id);
    } else if (std::regex_match(selector, sm, r_edst_id)) {
      auto label_id = boost::lexical_cast<label_id_t>(sm[1]);

      return LabeledSelector(SelectorType::kEdgeDst, label_id);
    } else if (std::regex_match(selector, sm, r_edata)) {
      auto label_id = boost::lexical_cast<label_id_t>(sm[1]);
      auto prop_id = boost::lexical_cast<prop_id_t>(sm[2]);

      return LabeledSelector(SelectorType::kEdgeData, label_id, prop_id);
    } else if (std::regex_match(selector, sm, r_result)) {
      auto label_id = boost::lexical_cast<label_id_t>(sm[1]);

      return LabeledSelector(SelectorType::kResult, label_id);
    } else if (std::regex_match(selector, sm, r_result_prop)) {
      auto label_id = boost::lexical_cast<label_id_t>(sm[1]);
      std::string prop_name = sm[2];

      if (prop_name.empty()) {
        RETURN_GS_ERROR(
            vineyard::ErrorCode::kInvalidValueError,
            "Property name not found, the selector is: " + selector);
      }
      return LabeledSelector(label_id, prop_name);
    }
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                    "Invalid syntax, the selector is: " + selector);
  }

  /**
   *
   * @param selectors selectors represented by JSON string e.g. {"col_name":
   * "selector", ...}
   * @return
   */
  static bl::result<std::vector<std::pair<std::string, LabeledSelector>>>
  ParseSelectors(const std::string& s_selectors) {
    std::stringstream ss(s_selectors);
    boost::property_tree::ptree pt;
    std::vector<std::pair<std::string, LabeledSelector>> selectors;

    try {
      boost::property_tree::read_json(ss, pt);
      BOOST_FOREACH  // NOLINT(whitespace/parens)
          (boost::property_tree::ptree::value_type & v, pt) {
        CHECK(v.second.empty());
        std::string col_name = v.first;
        std::string s_selector = v.second.data();

        BOOST_LEAF_AUTO(selector, LabeledSelector::parse(s_selector));
        selectors.emplace_back(col_name, selector);
      }
    } catch (boost::property_tree::ptree_error& e) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "Failed to parse json: " + s_selectors);
    }

    return selectors;
  }

  static bl::result<label_id_t> GetVertexLabelId(
      const std::vector<std::pair<std::string, LabeledSelector>>& selectors) {
    label_id_t label_id = -1;

    for (auto& pair : selectors) {
      auto& selector = pair.second;

      if (selector.type() == SelectorType::kVertexId ||
          selector.type() == SelectorType::kVertexData ||
          selector.type() == SelectorType::kResult) {
        if (label_id == -1) {
          label_id = selector.label_id();
        } else if (selector.label_id() != label_id) {
          RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                          "Vertex label is not same");
        }
      }
    }
    if (label_id == -1) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError,
                      "Can not found vertex label from selectors");
    }
    return label_id;
  }

 private:
  label_id_t label_id_;
  prop_id_t property_id_;
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_SELECTOR_H_
