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

#ifndef RUNTIME_CODEGEN_EXPR_STATIC_EXPR_H_
#define RUNTIME_CODEGEN_EXPR_STATIC_EXPR_H_

namespace gs {

namespace runtime {

template <typename T>
class is_optional_test {
 private:
  template <typename U>
  static auto test(int) -> decltype(std::declval<U>().is_optional(),
                                    std::true_type());

  template <typename U>
  static std::false_type test(...);

 public:
  static constexpr bool value = decltype(test<T>(0))::value;
};

template <typename EXPR, typename OP>
struct UnaryOpExpr {
  UnaryOpExpr(const EXPR& expr, OP&& op) : expr_(expr), op_(op) {}

  using elem_t = typename OP::elem;

  elem_t typed_eval_vertex(label_t label, vid_t v, size_t path_idx) const {
    return op_(expr_.typed_eval_vertex(label, v, path_idx));
  }

  elem_t typed_eval_path(size_t path_idx) const {
    return op_(expr_.typed_eval_path(path_idx));
  }

  elem_t typed_eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, size_t path_idx) const {
    return op_(expr_.typed_eval_edge(label, src, dst, edata, path_idx));
  }
};

template <typename LHS, typename RHS, typename OP>
struct BinaryOpExpr {
  BinaryOpExpr(const LHS& lhs, const RHS& rhs, OP&& op)
      : lhs_(lhs), rhs_(rhs), op_(op) {}

  using lhs_elem_t = typename LHS::elem_t;
  using rhs_elem_t = typename RHS::elem_t;

  using elem_t = typename OP::elem_t;

  elem_t typed_eval_vertex(label_t label, vid_t v, size_t path_idx) const {
    return op_(lhs_.typed_eval_vertex(label, v, path_idx),
               rhs_.typed_eval_vertex(label, v, path_idx));
  }

  elem_t typed_eval_path(size_t path_idx) const {
    return op_(lhs_.typed_eval_path(path_idx), rhs_.typed_eval_path(path_idx));
  }

  elem_t typed_eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, size_t path_idx) const {
    return op_(lhs_.typed_eval_edge(label, src, dst, edata, path_idx),
               rhs_.typed_eval_edge(label, src, dst, edata, path_idx));
  }

  RTAny eval_path(size_t path_idx) const {
    return TypedConverter<elem_t>::from_typed(typed_eval_path(path_idx));
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t path_idx) const {
    return TypedConverter<elem_t>::from_typed(
        typed_eval_vertex(label, v, path_idx));
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, size_t path_idx) const {
    return TypedConverter<elem_t>::from_typed(
        typed_eval_edge(label, src, dst, edata, path_idx));
  }

  const LHS& lhs_;
  const RHS& rhs_;
  OP op_;
};

template <typename WHEN, typename THEN, typename ELSE>
struct CaseOpExpr {
  CaseOpExpr(const WHEN& w, const THEN& t, const ELSE& e)
      : when_stmt_(w), then_stmt_(t), else_stmt_(e) {}

  using elem_t = typename THEN::elem_t;

  elem_t typed_eval_path(size_t path_idx) const {
    // LOG(INFO) << "path_idx = " << path_idx;
    if (when_stmt_.typed_eval_path(path_idx)) {
      // LOG(INFO) << "hit then";
      return then_stmt_.typed_eval_path(path_idx);
    } else {
      // LOG(INFO) << "hit else";
      return else_stmt_.typed_eval_path(path_idx);
    }
  }

  elem_t typed_eval_vertex(label_t label, vid_t v, size_t path_idx) const {
    if (when_stmt_.typed_eval_vertex(label, v, path_idx)) {
      return then_stmt_.typed_eval_vertex(label, v, path_idx);
    } else {
      return else_stmt_.typed_eval_vertex(label, v, path_idx);
    }
  }

  elem_t typed_eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, size_t path_idx) const {
    if (when_stmt_.typed_eval_edge(label, src, dst, edata, path_idx)) {
      return then_stmt_.typed_eval_edge(label, src, dst, edata, path_idx);
    } else {
      return else_stmt_.typed_eval_edge(label, src, dst, edata, path_idx);
    }
  }

  RTAny eval_path(size_t path_idx) const {
    return TypedConverter<elem_t>::from_typed(typed_eval_path(path_idx));
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t path_idx) const {
    return TypedConverter<elem_t>::from_typed(
        typed_eval_vertex(label, v, path_idx));
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, size_t path_idx) const {
    return TypedConverter<elem_t>::from_typed(
        typed_eval_edge(label, src, dst, edata, path_idx));
  }
  const WHEN& when_stmt_;
  const THEN& then_stmt_;
  const ELSE& else_stmt_;
};

template <typename... EXPRS>
struct TupleExpr {
  TupleExpr(const EXPRS&... exprs) : exprs_(exprs...) {}

  using elem_t = std::tuple<typename EXPRS::elem_t...>;

  elem_t typed_eval_vertex(label_t label, vid_t v, size_t path_idx) const {
    return std::apply(
        [label, v, path_idx](const auto&... exprs) {
          return std::make_tuple(
              exprs.typed_eval_vertex(label, v, path_idx)...);
        },
        exprs_);
  }

  elem_t typed_eval_path(size_t path_idx) const {
    return std::apply(
        [path_idx](const auto&... exprs) {
          return std::make_tuple(exprs.typed_eval_path(path_idx)...);
        },
        exprs_);
  }

  elem_t typed_eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, size_t path_idx) const {
    return std::apply(
        [label, src, dst, &edata, path_idx](const auto&... exprs) {
          return std::make_tuple(
              exprs.typed_eval_edge(label, src, dst, edata, path_idx)...);
        },
        exprs_);
  }

  template <size_t I>
  RTAny eval_path(size_t path_idx) const {
    return TypedConverter<elem_t<I>>::from_typed(typed_eval_path<I>(path_idx));
  }

  template <size_t I>
  RTAny eval_vertex(label_t label, vid_t v, size_t path_idx) const {
    return TypedConverter<elem_t<I>>::from_typed(
        typed_eval_vertex<I>(label, v, path_idx));
  }

  template <size_t I>
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, size_t path_idx) const {
    return TypedConverter<elem_t<I>>::from_typed(
        typed_eval_edge<I>(label, src, dst, edata, path_idx));
  }

  std::tuple<EXPRS...> exprs_;
};

template <typename EXPR>
class WithInExpr {
 public:
  WithInExpr(const EXPR& expr, const std::vector<typename EXPR::elem_t>& list)
      : expr_(expr), list_(list) {}

  using elem_t = bool;

  elem_t typed_eval_vertex(label_t label, vid_t v, size_t path_idx) const {
    auto val = expr_.typed_eval_vertex(label, v, path_idx);
    return std::find(list_.begin(), list_.end(), val) != list_.end();
  }

  elem_t typed_eval_path(size_t path_idx) const {
    auto val = expr_.typed_eval_path(path_idx);
    return std::find(list_.begin(), list_.end(), val) != list_.end();
  }

  elem_t typed_eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, size_t path_idx) const {
    auto val = expr_.typed_eval_edge(label, src, dst, edata, path_idx);
    return std::find(list_.begin(), list_.end(), val) != list_.end();
  }

 private:
  const EXPR& expr_;
  std::vector<typename EXPR::elem_t> list_;
};

template <typename T>
struct LTOp {
  using elem_t = bool;
  bool operator()(const T& lhs, const T& rhs) const { return lhs < rhs; }
};

template <typename T>
struct GTOp {
  using elem_t = bool;
  bool operator()(const T& lhs, const T& rhs) const { return rhs < lhs; }
};

template <typename T>
struct GEOp {
  using elem_t = bool;
  bool operator()(const T& lhs, const T& rhs) const { return !(lhs < rhs); }
};

template <typename T>
struct EQOp {
  using elem_t = bool;
  bool operator()(const T& lhs, const T& rhs) const { return lhs == rhs; }
};

template <typename T>
struct AddOp {
  using elem_t = T;
  T operator()(const T& lhs, const T& rhs) const { return lhs + rhs; }
};

struct AndOp {
  using elem_t = bool;
  bool operator()(bool lhs, bool rhs) const { return lhs && rhs; }
};

struct NotOp {
  using elem_t = bool;
  bool operator()(bool lhs) const { return !lhs; }
};

struct RegexMatchOp {
  using elem_t = bool;
  bool operator()(const std::string& lhs, const std::string& rhs) const {
    return std::regex_match(lhs, std::regex(rhs));
  }
};

template <typename EXPR>
struct VertexPredicate {
  VertexPredicate(const EXPR& expr) : expr_(expr) {}

  bool operator()(label_t label, vid_t v, size_t path_idx) const {
    return expr_.typed_eval_vertex(label, v, path_idx);
  }

  const EXPR& expr_;
};

template <typename EXPR>
struct EdgePredicate {
  EdgePredicate(const EXPR& expr) : expr_(expr) {}

  bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, Direction dir, size_t path_idx) const {
    return expr_.typed_eval_edge(label, src, dst, edata, path_idx);
  }

  const EXPR& expr_;
};

template <typename EXPR>
struct PathPredicate {
  PathPredicate(const EXPR& expr) : expr_(expr) {}

  bool operator()(size_t path_idx) const {
    return expr_.typed_eval_path(path_idx);
  }

  bool is_optional() const {
    return is_optional_test<EXPR>::value && expr_.is_optional();
  }

  bool operator()(size_t path_idx, int) const {
    // typed_eval_path(idx, 0) return std::optional<bool>
    auto val = expr_.typed_eval_path(path_idx, 0);
    if (val.has_value()) {
      return val.value();
    }
    return false;
  }

  const EXPR& expr_;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_CODEGEN_EXPR_STATIC_EXPR_H_