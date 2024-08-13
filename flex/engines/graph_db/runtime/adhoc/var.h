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

#ifndef RUNTIME_ADHOC_VAR_H_
#define RUNTIME_ADHOC_VAR_H_

#include "flex/engines/graph_db/database/read_transaction.h"

#include "flex/engines/graph_db/runtime/common/accessors.h"
#include "flex/engines/graph_db/runtime/common/context.h"

#include "flex/proto_generated_gie/expr.pb.h"

namespace gs {

namespace runtime {

enum class VarType {
  kVertexVar,
  kEdgeVar,
  kPathVar,
};

class VarGetterBase {
 public:
  virtual ~VarGetterBase() = default;
  virtual RTAny eval_path(size_t idx) const = 0;
  virtual RTAny eval_vertex(label_t label, vid_t v, size_t idx) const = 0;
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const Any& data, size_t idx) const = 0;
  virtual std::string name() const = 0;
};

class Var {
 public:
  Var(const ReadTransaction& txn, const Context& ctx,
      const common::Variable& pb, VarType var_type);
  ~Var();

  RTAny get(size_t path_idx) const;
  RTAny get_vertex(label_t label, vid_t v, size_t idx) const;
  RTAny get_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                 const Any& data, size_t idx) const;

  RTAny get(size_t path_idx, int) const;
  RTAny get_vertex(label_t label, vid_t v, size_t idx, int) const;
  RTAny get_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                 const Any& data, size_t idx, int) const;
  RTAnyType type() const;
  bool is_optional() const { return getter_->is_optional(); }
  std::shared_ptr<IContextColumnBuilder> builder() const;

 private:
  std::shared_ptr<IAccessor> getter_;
  RTAnyType type_;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_VAR_H_