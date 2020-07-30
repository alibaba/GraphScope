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
#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_PROJECTED_FRAGMENT_BASE_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_PROJECTED_FRAGMENT_BASE_H_
#include "vineyard/client/ds/i_object.h"

namespace gs {
/**
 * @brief This is the base class of ArrowProjectedFragment
 */
class ArrowProjectedFragmentBase : public vineyard::Object {
 public:
  virtual ~ArrowProjectedFragmentBase() = default;
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_PROJECTED_FRAGMENT_BASE_H_
