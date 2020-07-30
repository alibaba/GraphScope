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

#ifndef ANALYTICAL_ENGINE_CORE_APP_PREGEL_CYTHON_VERTEX_PROGRAM_H_
#define ANALYTICAL_ENGINE_CORE_APP_PREGEL_CYTHON_VERTEX_PROGRAM_H_

#include <dlfcn.h>

#include <memory>
#include <string>

#include "core/app/pregel/aggregators/aggregator.h"
#include "core/app/pregel/export.h"
#include "core/app/pregel/i_vertex_program.h"
#include "core/app/pregel/pregel_property_vertex.h"

namespace gs {

/**
 * CythonPregelProgram drives cython functions to implement cython pregel
 * program.
 * @tparam VD_TYPE
 * @tparam MD_TYPE
 */
template <typename VD_TYPE, typename MD_TYPE>
class CythonPregelProgram
    : public IPregelProgram<pregel::Vertex<VD_TYPE, MD_TYPE>,
                            pregel::Context<VD_TYPE, MD_TYPE>> {
  typedef void (*InitFuncT)(pregel::Vertex<VD_TYPE, MD_TYPE>&,
                            pregel::Context<VD_TYPE, MD_TYPE>&);
  typedef void (*ComputeFuncT)(pregel::MessageIterator<MD_TYPE>,
                               pregel::Vertex<VD_TYPE, MD_TYPE>&,
                               pregel::Context<VD_TYPE, MD_TYPE>&);

 public:
  CythonPregelProgram() : init_func_(NULL), compute_func_(NULL) {}

  void SetInitFunction(InitFuncT init_func) { init_func_ = init_func; }

  void SetComputeFunction(ComputeFuncT compute_func) {
    compute_func_ = compute_func;
  }

  inline void Init(pregel::Vertex<VD_TYPE, MD_TYPE>& v,
                   pregel::Context<VD_TYPE, MD_TYPE>& context) {
    init_func_(v, context);
  }

  inline void Compute(pregel::MessageIterator<MD_TYPE> messages,
                      pregel::Vertex<VD_TYPE, MD_TYPE>& vertex,
                      pregel::Context<VD_TYPE, MD_TYPE>& context) {
    compute_func_(messages, vertex, context);
  }

 private:
  InitFuncT init_func_;
  ComputeFuncT compute_func_;
};

/**
 * @brief CythonCombinator invokes the combinator implemented with cython
 * @tparam MD_TYPE
 */
template <typename MD_TYPE>
class CythonCombinator : public ICombinator<MD_TYPE> {
  typedef MD_TYPE (*CombineFuncT)(grape::IteratorPair<MD_TYPE*> messages);

 public:
  CythonCombinator() : combine_func_(NULL) {}

  void SetCombineFunction(CombineFuncT combine_func) {
    combine_func_ = combine_func;
  }

  inline MD_TYPE CombineMessages(pregel::MessageIterator<MD_TYPE> messages) {
    return combine_func_(messages);
  }

 private:
  CombineFuncT combine_func_;
};

/**
 * @brief CythonCombinator invokes the aggregator implemented with cython
 * @tparam MD_TYPE
 */
template <typename AGGR_TYPE>
class CythonAggregator : public Aggregator<AGGR_TYPE> {
  typedef AGGR_TYPE (*AggregateFuncT)(Aggregator<AGGR_TYPE>&, AGGR_TYPE);
  typedef void (*ResetFuncT)(Aggregator<AGGR_TYPE>&);
  typedef void (*InitFuncT)(Aggregator<AGGR_TYPE>&);

 public:
  CythonAggregator()
      : aggregate_func_(NULL), reset_func_(NULL), init_func_(NULL) {}

  CythonAggregator(AggregateFuncT afunc, ResetFuncT rfunc, InitFuncT ifunc)
      : aggregate_func_(afunc), reset_func_(rfunc), init_func_(ifunc) {}

  void SetAggregateFunc(AggregateFuncT func) { aggregate_func_ = func; }

  void SetResetFunc(ResetFuncT func) { reset_func_ = func; }

  void SetInitFunc(InitFuncT func) { init_func_ = func; }

  Aggregator<AGGR_TYPE>& AsAggregator() {
    return *dynamic_cast<Aggregator<AGGR_TYPE>*>(this);
  }

  void Aggregate(AGGR_TYPE value) { aggregate_func_(AsAggregator(), value); }

  void Init() { init_func_(AsAggregator()); }

  void Reset() { reset_func_(AsAggregator()); }

  std::shared_ptr<IAggregator> clone() override {
    return std::shared_ptr<IAggregator>(new CythonAggregator<AGGR_TYPE>(
        this->aggregate_func_, this->reset_func_, this->init_func_));
  }

 private:
  AggregateFuncT aggregate_func_;
  ResetFuncT reset_func_;
  InitFuncT init_func_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_APP_PREGEL_CYTHON_VERTEX_PROGRAM_H_
