#ifndef EXPRESSION_H
#define EXPRESSION_H

#include <tuple>
#include <vector>

namespace gs {

/// @brief Indicate which property of which tag is needed.
/// @tparam T
/// @tparam tag_id
template <typename T, int tag_id = -1>
struct PropertySelector {
  using prop_t = T;
  std::string name;
  PropertySelector(std::string n) : name(n) {}
};

/// @brief Indicate we select the tag's itself.
/// @tparam tag_id
template <int tag_id>
struct ColumnSelector {};

/// @brief Apply the property got from selectors.
/// @tparam RET_T
/// @tparam ...ARGS_T
template <typename RET_T, typename... ARGS_T>
struct Expression {
  // can not have a virtual variadic function
  // virtual RET_T evaluate(const ARGS_T&... args) = 0;
};

// Expression only return bool
template <typename... T>
using Predicate = Expression<bool, T...>;

}  // namespace gs

#endif  // EXPRESSION_H