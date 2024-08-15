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

#ifndef RUNTIME_CODEGEN_EXPR_REDUCERS_H_
#define RUNTIME_CODEGEN_EXPR_REDUCERS_H_

namespace gs {

namespace runtime {

template <typename EXPR_T>
struct SumReducer {
  using elem_t = typename EXPR_T::elem_t;
  SumReducer(const EXPR_T& expr) : expr_(expr) {}

  elem_t reduce(const std::vector<size_t>& offsets) const {
    elem_t ret{};
    for (auto idx : offsets) {
      ret += expr_.typed_eval_path(idx);
    }
    return ret;
  }

 private:
  const EXPR_T expr_;
};

template <typename EXPR_T>
struct ToSetReducer {
  using elem_t = std::set<typename EXPR_T::elem_t>;
  ToSetReducer(const EXPR_T& expr) : expr_(expr) {}

  elem_t reduce(const std::vector<size_t>& offsets) const {
    elem_t ret{};
    for (auto idx : offsets) {
      ret.emplace(expr_.typed_eval_path(idx));
    }
    return ret;
  }

 private:
  const EXPR_T expr_;
};

template <typename EXPR_T>
struct ToStringSetReducer {
  using elem_t = std::set<std::string>;
  ToStringSetReducer(const EXPR_T& expr) : expr_(expr) {}

  elem_t reduce(const std::vector<size_t>& offsets) const {
    elem_t ret{};
    for (auto idx : offsets) {
      ret.emplace(expr_.typed_eval_path(idx));
    }
    return ret;
  }

 private:
  const EXPR_T expr_;
};

template <typename EXPR_T>
struct ToVertexSetReducer {
  using elem_t = std::vector<vid_t>;
  ToVertexSetReducer(const EXPR_T& expr) : expr_(expr) {}

  elem_t reduce(const std::vector<size_t>& offsets) const {
    elem_t ret{};
    for (auto idx : offsets) {
      ret.push_back(expr_.typed_eval_path(idx).second);
    }
    std::sort(ret.begin(), ret.end());
    return ret;
  }

 private:
  const EXPR_T expr_;
};

template <typename EXPR_T>
struct DistinctCountReducer {
  using elem_t = int64_t;
  DistinctCountReducer(const EXPR_T& expr) : expr_(expr) {}

  int64_t reduce(const std::vector<size_t>& offsets) const {
    std::set<typename EXPR_T::elem_t> table;
    for (auto idx : offsets) {
      table.insert(expr_.typed_eval_path(idx));
    }
    return table.size();
  }

 private:
  const EXPR_T expr_;
};

struct CountReducer {
  using elem_t = int64_t;
  CountReducer() {}

  int64_t reduce(const std::vector<size_t>& offsets) const {
    return offsets.size();
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_CODEGEN_EXPR_REDUCERS_H_