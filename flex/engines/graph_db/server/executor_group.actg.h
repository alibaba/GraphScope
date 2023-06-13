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

#ifndef SERVER_EXECUTOR_GROUP_ACTG_H_
#define SERVER_EXECUTOR_GROUP_ACTG_H_

#include <hiactor/core/actor-template.hh>

namespace server {

class ANNOTATION(actor:group) executor_group : public hiactor::schedulable_actor_group {
public:
  executor_group(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr)
      : hiactor::schedulable_actor_group(exec_ctx, addr) {}

  bool compare(const actor_base* a, const actor_base* b) const override {
    /// Larger actor id will have higher priority
    return a->actor_id() < b->actor_id();
  }
};

}  // namespace server

#endif  // SERVER_EXECUTOR_GROUP_ACTG_H_
