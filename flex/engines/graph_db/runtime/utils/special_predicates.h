
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

#ifndef RUNTIME_UTILS_SPECIAL_PREDICATES_H_
#define RUNTIME_UTILS_SPECIAL_PREDICATES_H_

#include "flex/engines/graph_db/runtime/common/graph_interface.h"
#include "flex/engines/graph_db/runtime/common/rt_any.h"
#include "flex/proto_generated_gie/expr.pb.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

namespace runtime {

inline bool is_label_within_predicate(const common::Expression& expr,
                                      std::set<label_t>& label_set) {
  if (expr.operators_size() == 3) {
    auto& var_op = expr.operators(0);

    if (var_op.has_var() && var_op.var().has_property() &&
        var_op.var().property().has_label()) {
      auto& within_op = expr.operators(1);
      if (within_op.item_case() == common::ExprOpr::kLogical &&
          within_op.logical() == common::Logical::WITHIN) {
        auto& labels_op = expr.operators(2);
        if (labels_op.has_const_() && labels_op.const_().has_i64_array()) {
          auto& array = labels_op.const_().i64_array();
          size_t num = array.item_size();
          for (size_t k = 0; k < num; ++k) {
            label_set.insert(static_cast<label_t>(array.item(k)));
          }
          return true;
        }
      }
    }
  }
  return false;
}

inline bool is_pk_oid_exact_check(
    const gs::Schema& schema, label_t label, const common::Expression& expr,
    std::function<Any(const std::map<std::string, std::string>&)>& value) {
  if (expr.operators_size() != 3) {
    return false;
  }
  if (!(expr.operators(0).has_var() && expr.operators(0).var().has_property() &&
        expr.operators(0).var().property().has_key())) {
    auto& key = expr.operators(7).var().property().key();
    if (!(key.item_case() == common::NameOrId::ItemCase::kName &&
          key.name() == schema.get_vertex_primary_key_name(label))) {
      return false;
    }
    return false;
  }
  if (!(expr.operators(1).item_case() == common::ExprOpr::kLogical &&
        expr.operators(1).logical() == common::Logical::EQ)) {
    return false;
  }

  if (expr.operators(2).has_param()) {
    auto& p = expr.operators(2).param();
    auto name = p.name();
    // todo: check data type
    auto type_ = parse_from_ir_data_type(p.data_type());
    if (type_ != RTAnyType::kI64Value && type_ != RTAnyType::kI32Value) {
      return false;
    }
    value = [name](const std::map<std::string, std::string>& params) {
      return Any(static_cast<int64_t>(std::stoll(params.at(name))));
    };
    return true;
  } else if (expr.operators(2).has_const_()) {
    auto& c = expr.operators(2).const_();
    if (c.item_case() == common::Value::kI64) {
      value = [c](const std::map<std::string, std::string>&) {
        return Any(c.i64());
      };

    } else if (c.item_case() == common::Value::kI32) {
      value = [c](const std::map<std::string, std::string>&) {
        return Any(static_cast<int64_t>(c.i32()));
      };
    } else {
      return false;
    }
    return true;
  } else {
    return false;
  }
  return false;
}

inline bool is_pk_exact_check(const gs::Schema& schema,
                              const common::Expression& expr, label_t& label,
                              std::string& pk) {
  if (expr.operators_size() != 11) {
    return false;
  }
  if (!(expr.operators(0).item_case() == common::ExprOpr::kBrace &&
        expr.operators(0).brace() ==
            common::ExprOpr_Brace::ExprOpr_Brace_LEFT_BRACE)) {
    return false;
  }
  if (!(expr.operators(1).has_var() && expr.operators(1).var().has_property() &&
        expr.operators(1).var().property().has_label())) {
    return false;
  }
  if (!(expr.operators(2).item_case() == common::ExprOpr::kLogical &&
        expr.operators(2).logical() == common::Logical::WITHIN)) {
    return false;
  }
  if (expr.operators(3).has_const_() &&
      expr.operators(3).const_().has_i64_array()) {
    auto& array = expr.operators(3).const_().i64_array();
    if (array.item_size() != 1) {
      return false;
    }
    label = static_cast<label_t>(array.item(0));
  } else {
    return false;
  }
  if (!(expr.operators(4).item_case() == common::ExprOpr::kBrace &&
        expr.operators(4).brace() ==
            common::ExprOpr_Brace::ExprOpr_Brace_RIGHT_BRACE)) {
    return false;
  }
  if (!(expr.operators(5).item_case() == common::ExprOpr::kLogical &&
        expr.operators(5).logical() == common::Logical::AND)) {
    return false;
  }
  if (!(expr.operators(6).item_case() == common::ExprOpr::kBrace &&
        expr.operators(6).brace() ==
            common::ExprOpr_Brace::ExprOpr_Brace_LEFT_BRACE)) {
    return false;
  }
  if (!(expr.operators(7).has_var() && expr.operators(7).var().has_property() &&
        expr.operators(7).var().property().has_key())) {
    auto& key = expr.operators(7).var().property().key();
    if (!(key.item_case() == common::NameOrId::ItemCase::kName &&
          key.name() == schema.get_vertex_primary_key_name(label))) {
      return false;
    }
  }
  if (!(expr.operators(8).item_case() == common::ExprOpr::kLogical &&
        expr.operators(8).logical() == common::Logical::EQ)) {
    return false;
  }

  if (expr.operators(9).has_param()) {
    auto& p = expr.operators(9).param();
    pk = p.name();
    if (!(p.has_data_type() && p.data_type().type_case() ==
                                   common::IrDataType::TypeCase::kDataType)) {
      return false;
    }
  } else {
    return false;
  }
  if (!(expr.operators(10).item_case() == common::ExprOpr::kBrace &&
        expr.operators(10).brace() ==
            common::ExprOpr_Brace::ExprOpr_Brace_RIGHT_BRACE)) {
    return false;
  }
  return true;
}

enum class SPPredicateType {
  kPropertyGT,
  kPropertyLT,
  kPropertyLE,
  kPropertyGE,
  kPropertyEQ,
  kPropertyNE,
  kPropertyBetween,
  kWithIn,
  kUnknown
};

inline SPPredicateType parse_sp_pred(const common::Expression& expr) {
  if (expr.operators_size() != 3) {
    return SPPredicateType::kUnknown;
  }

  if (!(expr.operators(0).has_var() &&
        expr.operators(0).var().has_property())) {
    return SPPredicateType::kUnknown;
  }
  if (!(expr.operators(1).item_case() == common::ExprOpr::ItemCase::kLogical)) {
    return SPPredicateType::kUnknown;
  }
  if (!expr.operators(2).has_param() && !expr.operators(2).has_const_()) {
    return SPPredicateType::kUnknown;
  }
  switch (expr.operators(1).logical()) {
  case common::Logical::GT:
    return SPPredicateType::kPropertyGT;
  case common::Logical::LT:
    return SPPredicateType::kPropertyLT;
  case common::Logical::LE:
    return SPPredicateType::kPropertyLE;
  case common::Logical::GE:
    return SPPredicateType::kPropertyGE;
  case common::Logical::EQ:
    return SPPredicateType::kPropertyEQ;
  case common::Logical::NE:
    return SPPredicateType::kPropertyNE;
  case common::Logical::WITHIN:
    return SPPredicateType::kWithIn;
  default:
    return SPPredicateType::kUnknown;
  }
}

class SPVertexPredicate {
 public:
  virtual ~SPVertexPredicate() {}
  virtual SPPredicateType type() const = 0;
  virtual RTAnyType data_type() const = 0;
};

template <typename T>
class VertexPropertyLTPredicateBeta : public SPVertexPredicate {
 public:
  VertexPropertyLTPredicateBeta(const GraphReadInterface& graph,
                                const std::string& property_name,
                                const std::string& target_str) {
    label_t label_num = graph.schema().vertex_label_num();
    for (label_t i = 0; i < label_num; ++i) {
      columns_.emplace_back(graph.GetVertexColumn<T>(i, property_name));
    }
    target_str_ = target_str;
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  VertexPropertyLTPredicateBeta(VertexPropertyLTPredicateBeta&& other) {
    columns_ = std::move(other.columns_);
    target_str_ = std::move(other.target_str_);
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  ~VertexPropertyLTPredicateBeta() = default;

  inline SPPredicateType type() const override {
    return SPPredicateType::kPropertyLT;
  }

  inline RTAnyType data_type() const override {
    return TypedConverter<T>::type();
  }

  inline bool operator()(label_t label, vid_t v) const {
    return columns_[label].get_view(v) < target_;
  }

 private:
  std::vector<GraphReadInterface::vertex_column_t<T>> columns_;
  T target_;
  std::string target_str_;
};

template <typename T>
class VertexPropertyLEPredicateBeta : public SPVertexPredicate {
 public:
  VertexPropertyLEPredicateBeta(const GraphReadInterface& graph,
                                const std::string& property_name,
                                const std::string& target_str) {
    label_t label_num = graph.schema().vertex_label_num();
    for (label_t i = 0; i < label_num; ++i) {
      columns_.emplace_back(graph.GetVertexColumn<T>(i, property_name));
    }
    target_str_ = target_str;
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  VertexPropertyLEPredicateBeta(VertexPropertyLEPredicateBeta&& other) {
    columns_ = std::move(other.columns_);
    target_str_ = std::move(other.target_str_);
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  ~VertexPropertyLEPredicateBeta() = default;

  inline SPPredicateType type() const override {
    return SPPredicateType::kPropertyLE;
  }

  inline RTAnyType data_type() const override {
    return TypedConverter<T>::type();
  }

  inline bool operator()(label_t label, vid_t v) const {
    return !(target_ < columns_[label].get_view(v));
  }

 private:
  std::vector<GraphReadInterface::vertex_column_t<T>> columns_;
  std::string target_str_;
  T target_;
};

template <typename T>
class VertexPropertyGEPredicateBeta : public SPVertexPredicate {
 public:
  VertexPropertyGEPredicateBeta(const GraphReadInterface& graph,
                                const std::string& property_name,
                                const std::string& target_str) {
    label_t label_num = graph.schema().vertex_label_num();
    for (label_t i = 0; i < label_num; ++i) {
      columns_.emplace_back(graph.GetVertexColumn<T>(i, property_name));
    }
    target_str_ = target_str;
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  VertexPropertyGEPredicateBeta(VertexPropertyGEPredicateBeta&& other) {
    columns_ = std::move(other.columns_);
    target_str_ = std::move(other.target_str_);
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  ~VertexPropertyGEPredicateBeta() = default;

  inline SPPredicateType type() const override {
    return SPPredicateType::kPropertyGE;
  }

  inline RTAnyType data_type() const override {
    return TypedConverter<T>::type();
  }

  inline bool operator()(label_t label, vid_t v) const {
    return !(columns_[label].get_view(v) < target_);
  }

 private:
  std::vector<GraphReadInterface::vertex_column_t<T>> columns_;
  T target_;
  std::string target_str_;
};

template <typename T>
class VertexPropertyGTPredicateBeta : public SPVertexPredicate {
 public:
  VertexPropertyGTPredicateBeta(const GraphReadInterface& graph,
                                const std::string& property_name,
                                const std::string& target_str) {
    label_t label_num = graph.schema().vertex_label_num();
    for (label_t i = 0; i < label_num; ++i) {
      columns_.emplace_back(graph.GetVertexColumn<T>(i, property_name));
    }
    target_str_ = target_str;
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  VertexPropertyGTPredicateBeta(VertexPropertyGTPredicateBeta&& other) {
    columns_ = std::move(other.columns_);
    target_str_ = std::move(other.target_str_);
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  ~VertexPropertyGTPredicateBeta() = default;

  inline SPPredicateType type() const override {
    return SPPredicateType::kPropertyGT;
  }

  inline RTAnyType data_type() const override {
    return TypedConverter<T>::type();
  }

  inline bool operator()(label_t label, vid_t v) const {
    return target_ < columns_[label].get_view(v);
  }

 private:
  std::vector<GraphReadInterface::vertex_column_t<T>> columns_;
  std::string target_str_;
  T target_;
};

template <typename T>
class VertexPropertyEQPredicateBeta : public SPVertexPredicate {
 public:
  VertexPropertyEQPredicateBeta(const GraphReadInterface& graph,
                                const std::string& property_name,
                                const std::string& target_str) {
    label_t label_num = graph.schema().vertex_label_num();
    for (label_t i = 0; i < label_num; ++i) {
      columns_.emplace_back(graph.GetVertexColumn<T>(i, property_name));
    }
    target_str_ = target_str;
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  VertexPropertyEQPredicateBeta(VertexPropertyEQPredicateBeta&& other) {
    columns_ = std::move(other.columns_);
    target_str_ = std::move(other.target_str_);
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  ~VertexPropertyEQPredicateBeta() = default;

  inline SPPredicateType type() const override {
    return SPPredicateType::kPropertyEQ;
  }

  inline RTAnyType data_type() const override {
    return TypedConverter<T>::type();
  }

  inline bool operator()(label_t label, vid_t v) const {
    return target_ == columns_[label].get_view(v);
  }

 private:
  std::vector<GraphReadInterface::vertex_column_t<T>> columns_;
  T target_;
  // for string_view
  std::string target_str_;
};

template <typename T>
class VertexPropertyNEPredicateBeta : public SPVertexPredicate {
 public:
  VertexPropertyNEPredicateBeta(const GraphReadInterface& graph,
                                const std::string& property_name,
                                const std::string& target_str) {
    label_t label_num = graph.schema().vertex_label_num();
    for (label_t i = 0; i < label_num; ++i) {
      columns_.emplace_back(graph.GetVertexColumn<T>(i, property_name));
    }
    target_str_ = target_str;
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  VertexPropertyNEPredicateBeta(VertexPropertyNEPredicateBeta&& other) {
    columns_ = std::move(other.columns_);
    target_str_ = std::move(other.target_str_);
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  ~VertexPropertyNEPredicateBeta() = default;

  inline SPPredicateType type() const override {
    return SPPredicateType::kPropertyNE;
  }

  inline RTAnyType data_type() const override {
    return TypedConverter<T>::type();
  }

  inline bool operator()(label_t label, vid_t v) const {
    return !(target_ == columns_[label].get_view(v));
  }

 private:
  std::vector<GraphReadInterface::vertex_column_t<T>> columns_;
  T target_;
  std::string target_str_;
};

template <typename T>
class VertexPropertyBetweenPredicateBeta : public SPVertexPredicate {
 public:
  VertexPropertyBetweenPredicateBeta(const GraphReadInterface& graph,
                                     const std::string& property_name,
                                     const std::string& from_str,
                                     const std::string& to_str) {
    label_t label_num = graph.schema().vertex_label_num();
    for (label_t i = 0; i < label_num; ++i) {
      columns_.emplace_back(graph.GetVertexColumn<T>(i, property_name));
    }
    from_str_ = from_str;
    to_str_ = to_str;
    from_ = TypedConverter<T>::typed_from_string(from_str_);
    to_ = TypedConverter<T>::typed_from_string(to_str_);
  }

  VertexPropertyBetweenPredicateBeta(
      VertexPropertyBetweenPredicateBeta&& other) {
    columns_ = std::move(other.columns_);
    from_str_ = std::move(other.from_str_);
    to_str_ = std::move(other.to_str_);
    from_ = TypedConverter<T>::typed_from_string(from_str_);
    to_ = TypedConverter<T>::typed_from_string(to_str_);
  }

  ~VertexPropertyBetweenPredicateBeta() = default;

  inline SPPredicateType type() const override {
    return SPPredicateType::kPropertyBetween;
  }

  inline RTAnyType data_type() const override {
    return TypedConverter<T>::type();
  }

  inline bool operator()(label_t label, vid_t v) const {
    auto val = columns_[label].get_view(v);
    return ((val < to_) && !(val < from_));
  }

 private:
  std::vector<GraphReadInterface::vertex_column_t<T>> columns_;
  T from_;
  T to_;
  std::string from_str_;
  std::string to_str_;
};

template <typename T>
inline std::unique_ptr<SPVertexPredicate> _make_vertex_predicate(
    const SPPredicateType& ptype, const GraphReadInterface& graph,
    const std::string& property_name, const std::string& target_str) {
  if (ptype == SPPredicateType::kPropertyLT) {
    return std::make_unique<VertexPropertyLTPredicateBeta<T>>(
        graph, property_name, target_str);

  } else if (ptype == SPPredicateType::kPropertyEQ) {
    return std::make_unique<VertexPropertyEQPredicateBeta<T>>(
        graph, property_name, target_str);

  } else if (ptype == SPPredicateType::kPropertyGT) {
    return std::make_unique<VertexPropertyGTPredicateBeta<T>>(
        graph, property_name, target_str);

  } else if (ptype == SPPredicateType::kPropertyLE) {
    return std::make_unique<VertexPropertyLEPredicateBeta<T>>(
        graph, property_name, target_str);

  } else if (ptype == SPPredicateType::kPropertyGE) {
    return std::make_unique<VertexPropertyGEPredicateBeta<T>>(
        graph, property_name, target_str);

  } else if (ptype == SPPredicateType::kPropertyNE) {
    return std::make_unique<VertexPropertyNEPredicateBeta<T>>(
        graph, property_name, target_str);
  } else {
    return nullptr;
  }
}

inline std::optional<std::function<std::unique_ptr<SPVertexPredicate>(
    const GraphReadInterface&, const std::map<std::string, std::string>&)>>
parse_special_vertex_predicate(const common::Expression& expr) {
  if (expr.operators_size() == 3) {
    const common::ExprOpr& op0 = expr.operators(0);
    if (!op0.has_var()) {
      return std::nullopt;
    }
    if (!op0.var().has_property()) {
      return std::nullopt;
    }
    if (!op0.var().property().has_key()) {
      return std::nullopt;
    }
    if (!(op0.var().property().key().item_case() ==
          common::NameOrId::ItemCase::kName)) {
      return std::nullopt;
    }

    std::string property_name = op0.var().property().key().name();

    const common::ExprOpr& op1 = expr.operators(1);
    if (!(op1.item_case() == common::ExprOpr::kLogical)) {
      return std::nullopt;
    }

    SPPredicateType ptype;
    if (op1.logical() == common::Logical::LT) {
      ptype = SPPredicateType::kPropertyLT;
    } else if (op1.logical() == common::Logical::GT) {
      ptype = SPPredicateType::kPropertyGT;
    } else if (op1.logical() == common::Logical::EQ) {
      ptype = SPPredicateType::kPropertyEQ;
    } else if (op1.logical() == common::Logical::LE) {
      ptype = SPPredicateType::kPropertyLE;
    } else if (op1.logical() == common::Logical::GE) {
      ptype = SPPredicateType::kPropertyGE;
    } else if (op1.logical() == common::Logical::NE) {
      ptype = SPPredicateType::kPropertyNE;
    } else {
      return std::nullopt;
    }

    const common::ExprOpr& op2 = expr.operators(2);
    if (!op2.has_param()) {
      return std::nullopt;
    }
    if (!op2.param().has_data_type()) {
      return std::nullopt;
    }
    if (!(op2.param().data_type().type_case() ==
          common::IrDataType::TypeCase::kDataType)) {
      return std::nullopt;
    }
    auto name = op2.param().name();
    auto type = parse_from_ir_data_type(op2.param().data_type());
    if (type == RTAnyType::kI64Value) {
      return [ptype, property_name, name](
                 const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params)
                 -> std::unique_ptr<SPVertexPredicate> {
        return _make_vertex_predicate<int64_t>(ptype, graph, property_name,
                                               params.at(name));
      };

    } else if (type == RTAnyType::kStringValue) {
      return [ptype, property_name, name](
                 const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params)
                 -> std::unique_ptr<SPVertexPredicate> {
        return _make_vertex_predicate<std::string_view>(
            ptype, graph, property_name, params.at(name));
      };

    } else if (type == RTAnyType::kTimestamp) {
      return [ptype, property_name, name](
                 const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params)
                 -> std::unique_ptr<SPVertexPredicate> {
        return _make_vertex_predicate<Date>(ptype, graph, property_name,
                                            params.at(name));
      };

    } else if (type == RTAnyType::kI32Value) {
      return [ptype, property_name, name](
                 const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params)
                 -> std::unique_ptr<SPVertexPredicate> {
        return _make_vertex_predicate<int32_t>(ptype, graph, property_name,
                                               params.at(name));
      };

    } else if (type == RTAnyType::kF64Value) {
      return [ptype, property_name, name](
                 const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params)
                 -> std::unique_ptr<SPVertexPredicate> {
        return _make_vertex_predicate<double>(ptype, graph, property_name,
                                              params.at(name));
      };
    }
  } else if (expr.operators_size() == 7) {
    // between
    const common::ExprOpr& op0 = expr.operators(0);
    if (!op0.has_var()) {
      return std::nullopt;
    }
    if (!op0.var().has_property()) {
      return std::nullopt;
    }
    if (!op0.var().property().has_key()) {
      return std::nullopt;
    }
    if (!(op0.var().property().key().item_case() ==
          common::NameOrId::ItemCase::kName)) {
      return std::nullopt;
    }
    std::string property_name = op0.var().property().key().name();

    const common::ExprOpr& op1 = expr.operators(1);
    if (!(op1.item_case() == common::ExprOpr::kLogical)) {
      return std::nullopt;
    }
    if (op1.logical() != common::Logical::GE) {
      return std::nullopt;
    }

    const common::ExprOpr& op2 = expr.operators(2);
    if (!op2.has_param()) {
      return std::nullopt;
    }
    if (!op2.param().has_data_type()) {
      return std::nullopt;
    }
    if (!(op2.param().data_type().type_case() ==
          common::IrDataType::TypeCase::kDataType)) {
      return std::nullopt;
    }
    std::string from_str = op2.param().name();

    const common::ExprOpr& op3 = expr.operators(3);
    if (!(op3.item_case() == common::ExprOpr::kLogical)) {
      return std::nullopt;
    }
    if (op3.logical() != common::Logical::AND) {
      return std::nullopt;
    }

    const common::ExprOpr& op4 = expr.operators(4);
    if (!op4.has_var()) {
      return std::nullopt;
    }
    if (!op4.var().has_property()) {
      return std::nullopt;
    }
    if (!op4.var().property().has_key()) {
      return std::nullopt;
    }
    if (!(op4.var().property().key().item_case() ==
          common::NameOrId::ItemCase::kName)) {
      return std::nullopt;
    }
    if (property_name != op4.var().property().key().name()) {
      return std::nullopt;
    }

    const common::ExprOpr& op5 = expr.operators(5);
    if (!(op5.item_case() == common::ExprOpr::kLogical)) {
      return std::nullopt;
    }
    if (op5.logical() != common::Logical::LT) {
      return std::nullopt;
    }

    const common::ExprOpr& op6 = expr.operators(6);
    if (!op6.has_param()) {
      return std::nullopt;
    }
    if (!op6.param().has_data_type()) {
      return std::nullopt;
    }
    if (!(op6.param().data_type().type_case() ==
          common::IrDataType::TypeCase::kDataType)) {
      return std::nullopt;
    }
    std::string to_str = op6.param().name();

    auto type = parse_from_ir_data_type(op2.param().data_type());
    auto type1 = parse_from_ir_data_type(op6.param().data_type());

    if (type != type1) {
      return std::nullopt;
    }

    if (type == RTAnyType::kI64Value) {
      return [property_name, from_str, to_str](
                 const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params)
                 -> std::unique_ptr<SPVertexPredicate> {
        return std::make_unique<VertexPropertyBetweenPredicateBeta<int64_t>>(
            graph, property_name, params.at(from_str), params.at(to_str));
      };

    } else if (type == RTAnyType::kTimestamp) {
      return [property_name, from_str, to_str](
                 const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params)
                 -> std::unique_ptr<SPVertexPredicate> {
        return std::make_unique<VertexPropertyBetweenPredicateBeta<Date>>(
            graph, property_name, params.at(from_str), params.at(to_str));
      };

    } else if (type == RTAnyType::kI32Value) {
      return [property_name, from_str, to_str](
                 const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params)
                 -> std::unique_ptr<SPVertexPredicate> {
        return std::make_unique<VertexPropertyBetweenPredicateBeta<int32_t>>(
            graph, property_name, params.at(from_str), params.at(to_str));
      };
    } else if (type == RTAnyType::kF64Value) {
      return [property_name, from_str, to_str](
                 const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params)
                 -> std::unique_ptr<SPVertexPredicate> {
        return std::make_unique<VertexPropertyBetweenPredicateBeta<double>>(
            graph, property_name, params.at(from_str), params.at(to_str));
      };
    } else if (type == RTAnyType::kStringValue) {
      return [property_name, from_str, to_str](
                 const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params)
                 -> std::unique_ptr<SPVertexPredicate> {
        return std::make_unique<
            VertexPropertyBetweenPredicateBeta<std::string_view>>(
            graph, property_name, params.at(from_str), params.at(to_str));
      };
    } else {
      return std::nullopt;
    }
  }

  return std::nullopt;
}

class SPEdgePredicate {
 public:
  virtual ~SPEdgePredicate() {}
  virtual SPPredicateType type() const = 0;
  virtual RTAnyType data_type() const = 0;
};

template <typename T>
class EdgePropertyLTPredicate : public SPEdgePredicate {
 public:
  EdgePropertyLTPredicate(const std::string& target_str) {
    target_str_ = target_str;
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  ~EdgePropertyLTPredicate() = default;

  SPPredicateType type() const override { return SPPredicateType::kPropertyLT; }

  inline RTAnyType data_type() const override {
    return TypedConverter<T>::type();
  }

  inline bool operator()(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr,
                         label_t edge_label, Direction dir,
                         const T& edata) const {
    return edata < target_;
  }

  inline bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, Direction dir, size_t idx) const {
    return AnyConverter<T>::from_any(edata) < target_;
  }

 private:
  T target_;
  std::string target_str_;
};

template <typename T>
class EdgePropertyGTPredicate : public SPEdgePredicate {
 public:
  EdgePropertyGTPredicate(const std::string& target_str) {
    target_str_ = target_str;
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  ~EdgePropertyGTPredicate() = default;

  inline SPPredicateType type() const override {
    return SPPredicateType::kPropertyGT;
  }

  inline RTAnyType data_type() const override {
    return TypedConverter<T>::type();
  }

  inline bool operator()(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr,
                         label_t edge_label, Direction dir,
                         const T& edata) const {
    return target_ < edata;
  }

  inline bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, Direction dir, size_t idx) const {
    return target_ < AnyConverter<T>::from_any(edata);
  }

 private:
  T target_;
  std::string target_str_;
};

template <typename T>
class EdgePropertyEQPredicate : public SPEdgePredicate {
 public:
  EdgePropertyEQPredicate(const std::string& target_str) {
    target_str_ = target_str;
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  ~EdgePropertyEQPredicate() = default;

  inline SPPredicateType type() const override {
    return SPPredicateType::kPropertyEQ;
  }

  inline RTAnyType data_type() const override {
    return TypedConverter<T>::type();
  }

  inline bool operator()(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr,
                         label_t edge_label, Direction dir,
                         const T& edata) const {
    return target_ == edata;
  }

  inline bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, Direction dir, size_t idx) const {
    return target_ == AnyConverter<T>::from_any(edata);
  }

 private:
  T target_;
  std::string target_str_;
};

template <typename T>
class EdgePropertyGEPredicate : public SPEdgePredicate {
 public:
  EdgePropertyGEPredicate(const std::string& target_str) {
    target_str_ = target_str;
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  ~EdgePropertyGEPredicate() = default;

  inline SPPredicateType type() const override {
    return SPPredicateType::kPropertyGE;
  }

  inline RTAnyType data_type() const override {
    return TypedConverter<T>::type();
  }

  inline bool operator()(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr,
                         label_t edge_label, Direction dir,
                         const T& edata) const {
    return !(edata < target_);
  }

  inline bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, Direction dir, size_t idx) const {
    return !(AnyConverter<T>::from_any(edata) < target_);
  }

 private:
  T target_;
  std::string target_str_;
};

template <typename T>
class EdgePropertyLEPredicate : public SPEdgePredicate {
 public:
  EdgePropertyLEPredicate(const std::string& target_str) {
    target_str_ = target_str;

    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  ~EdgePropertyLEPredicate() = default;

  inline SPPredicateType type() const override {
    return SPPredicateType::kPropertyLE;
  }

  inline RTAnyType data_type() const override {
    return TypedConverter<T>::type();
  }

  inline bool operator()(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr,
                         label_t edge_label, Direction dir,
                         const T& edata) const {
    return !(target_ < edata);
  }

  inline bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, Direction dir, size_t idx) const {
    return !(target_ < AnyConverter<T>::from_any(edata));
  }

 private:
  T target_;
  std::string target_str_;
};

template <typename T>
class EdgePropertyNEPredicate : public SPEdgePredicate {
 public:
  EdgePropertyNEPredicate(const std::string& target_str) {
    target_str_ = target_str;
    target_ = TypedConverter<T>::typed_from_string(target_str_);
  }

  ~EdgePropertyNEPredicate() = default;

  inline SPPredicateType type() const override {
    return SPPredicateType::kPropertyNE;
  }

  inline RTAnyType data_type() const override {
    return TypedConverter<T>::type();
  }

  inline bool operator()(label_t v_label, vid_t v, label_t nbr_label, vid_t nbr,
                         label_t edge_label, Direction dir,
                         const T& edata) const {
    return !(target_ == edata);
  }

  inline bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, Direction dir, size_t idx) const {
    return !(target_ == AnyConverter<T>::from_any(edata));
  }

 private:
  T target_;
  std::string target_str_;
};

template <typename T>
inline std::unique_ptr<SPEdgePredicate> _make_edge_predicate(
    const SPPredicateType& ptype, const std::string& target_str) {
  if (ptype == SPPredicateType::kPropertyLT) {
    return std::make_unique<EdgePropertyLTPredicate<T>>(target_str);
  } else if (ptype == SPPredicateType::kPropertyGT) {
    return std::make_unique<EdgePropertyGTPredicate<T>>(target_str);
  } else if (ptype == SPPredicateType::kPropertyEQ) {
    return std::make_unique<EdgePropertyEQPredicate<T>>(target_str);
  } else if (ptype == SPPredicateType::kPropertyLE) {
    return std::make_unique<EdgePropertyLEPredicate<T>>(target_str);
  } else if (ptype == SPPredicateType::kPropertyGE) {
    return std::make_unique<EdgePropertyGEPredicate<T>>(target_str);
  } else if (ptype == SPPredicateType::kPropertyNE) {
    return std::make_unique<EdgePropertyNEPredicate<T>>(target_str);
  } else {
    return nullptr;
  }
}

inline std::optional<std::function<std::unique_ptr<SPEdgePredicate>(
    const GraphReadInterface&,
    const std::map<std::string, std::string>& params)>>
parse_special_edge_predicate(const common::Expression& expr) {
  if (expr.operators_size() == 3) {
    const common::ExprOpr& op0 = expr.operators(0);
    if (!op0.has_var()) {
      return std::nullopt;
    }
    if (!op0.var().has_property()) {
      return std::nullopt;
    }
    if (!op0.var().property().has_key()) {
      return std::nullopt;
    }
    if (!(op0.var().property().key().item_case() ==
          common::NameOrId::ItemCase::kName)) {
      return std::nullopt;
    }
    // std::string property_name = op0.var().property().key().name();

    const common::ExprOpr& op1 = expr.operators(1);
    if (!(op1.item_case() == common::ExprOpr::kLogical)) {
      return std::nullopt;
    }
    SPPredicateType ptype;
    if (op1.logical() == common::Logical::LT) {
      ptype = SPPredicateType::kPropertyLT;
    } else if (op1.logical() == common::Logical::GT) {
      ptype = SPPredicateType::kPropertyGT;
    } else if (op1.logical() == common::Logical::GE) {
      ptype = SPPredicateType::kPropertyGE;
    } else if (op1.logical() == common::Logical::LE) {
      ptype = SPPredicateType::kPropertyLE;
    } else if (op1.logical() == common::Logical::EQ) {
      ptype = SPPredicateType::kPropertyEQ;
    } else if (op1.logical() == common::Logical::NE) {
      ptype = SPPredicateType::kPropertyNE;
    } else {
      return std::nullopt;
    }
    const common::ExprOpr& op2 = expr.operators(2);
    if (!op2.has_param()) {
      return std::nullopt;
    }
    if (!op2.param().has_data_type()) {
      return std::nullopt;
    }
    if (!(op2.param().data_type().type_case() ==
          common::IrDataType::TypeCase::kDataType)) {
      return std::nullopt;
    }
    const std::string& name = op2.param().name();
    auto type = parse_from_ir_data_type(op2.param().data_type());
    if (type == RTAnyType::kI64Value) {
      return [ptype, name](const GraphReadInterface& graph,
                           const std::map<std::string, std::string>& params) {
        return _make_edge_predicate<int64_t>(ptype, params.at(name));
      };
    } else if (type == RTAnyType::kF64Value) {
      return [ptype, name](const GraphReadInterface& graph,
                           const std::map<std::string, std::string>& params) {
        return _make_edge_predicate<double>(ptype, params.at(name));
      };
    } else if (type == RTAnyType::kI32Value) {
      return [ptype, name](const GraphReadInterface& graph,
                           const std::map<std::string, std::string>& params) {
        return _make_edge_predicate<int32_t>(ptype, params.at(name));
      };
    } else if (type == RTAnyType::kTimestamp) {
      return [ptype, name](const GraphReadInterface& graph,
                           const std::map<std::string, std::string>& params) {
        return _make_edge_predicate<Date>(ptype, params.at(name));
      };
    } else if (type == RTAnyType::kStringValue) {
      return [ptype, name](const GraphReadInterface& graph,
                           const std::map<std::string, std::string>& params) {
        return _make_edge_predicate<std::string_view>(ptype, params.at(name));
      };
    } else if (type == RTAnyType::kDate32) {
      return [ptype, name](const GraphReadInterface& graph,
                           const std::map<std::string, std::string>& params) {
        return _make_edge_predicate<Day>(ptype, params.at(name));
      };
    } else {
      return std::nullopt;
    }
  }
  return std::nullopt;
}

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_UTILS_OPERATORS_SPECIAL_PREDICATES_H_