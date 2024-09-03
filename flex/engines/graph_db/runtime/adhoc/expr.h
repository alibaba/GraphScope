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
#ifndef RUNTIME_ADHOC_RUNTIME_EXPR_H_
#define RUNTIME_ADHOC_RUNTIME_EXPR_H_

#include "flex/engines/graph_db/database/read_transaction.h"

#include "flex/engines/graph_db/runtime/adhoc/expr_impl.h"
#include "flex/engines/graph_db/runtime/common/rt_any.h"

namespace gs {

namespace runtime {

class Expr {
 public:
  Expr(const ReadTransaction& txn, const Context& ctx,
       const std::map<std::string, std::string>& params,
       const common::Expression& expr, VarType var_type);

  RTAny eval_path(size_t idx) const;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const;
  RTAny eval_path(size_t idx, int) const;
  RTAny eval_vertex(label_t label, vid_t v, size_t idx, int) const;
  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx, int) const;

  RTAnyType type() const;

  std::shared_ptr<IContextColumnBuilder> builder() const {
    return expr_->builder();
  }

  bool is_optional() const { return expr_->is_optional(); }

 private:
  std::unique_ptr<ExprBase> expr_;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_RUNTIME_EXPR_H_