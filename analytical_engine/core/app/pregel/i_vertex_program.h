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

#ifndef ANALYTICAL_ENGINE_CORE_APP_PREGEL_I_VERTEX_PROGRAM_H_
#define ANALYTICAL_ENGINE_CORE_APP_PREGEL_I_VERTEX_PROGRAM_H_

#include <memory>
#include <string>
#include <vector>

#include "grape/grape.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
#include "grape/utils/iterator_pair.h"

namespace gs {

template <typename MD_T>
using MessageIterator = grape::IteratorPair<MD_T*>;

template <typename FRAG_T, typename VD_T, typename MD_T>
class PregelPropertyVertex;

template <typename FRAG_T, typename VD_T, typename MD_T>
class PregelPropertyComputeContext;

/**
 * @brief The pregel programming interface
 * @tparam PREGEL_VERTEX_T
 * @tparam PREGEL_COMPUTE_CONTEXT_T
 */
template <typename PREGEL_VERTEX_T, typename PREGEL_COMPUTE_CONTEXT_T>
class IPregelProgram {
 public:
  using vd_t = typename PREGEL_VERTEX_T::vd_t;
  using md_t = typename PREGEL_VERTEX_T::md_t;

  using pregel_vertex_t = PREGEL_VERTEX_T;
  using compute_context_t = PREGEL_COMPUTE_CONTEXT_T;

  virtual void Init(pregel_vertex_t& v, compute_context_t& context) = 0;
  virtual void Compute(MessageIterator<md_t> messages, pregel_vertex_t& v,
                       compute_context_t& context) = 0;
};

/**
 * @brief The combinator collects local messages among all the workers and
 * generates an aggregated value.
 * @tparam MD_T
 */
template <typename MD_T>
class ICombinator {
 public:
  virtual MD_T CombineMessages(MessageIterator<MD_T> messages) = 0;
};

/**
 * @brief Aggregator interface for pregel program
 */
class IAggregator {
 public:
  virtual ~IAggregator() = default;

  virtual void Init() = 0;

  virtual void Reset() = 0;

  virtual void Serialize(grape::InArchive& arc) = 0;

  virtual void DeserializeAndAggregate(grape::OutArchive& arc) = 0;

  virtual void DeserializeAndAggregate(std::vector<grape::InArchive>& arcs) = 0;

  virtual void StartNewRound() = 0;

  virtual std::shared_ptr<IAggregator> clone() = 0;

  virtual std::string ToString() { return ""; }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_APP_PREGEL_I_VERTEX_PROGRAM_H_
