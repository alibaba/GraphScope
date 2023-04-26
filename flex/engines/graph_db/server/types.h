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

#ifndef SERVER_TYPES_ACT_H_
#define SERVER_TYPES_ACT_H_

#include <hiactor/net/serializable_queue.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>

#include <string>

namespace server {

using timestamp_t = uint32_t;

template <typename BufType,
          typename = std::enable_if_t<
              std::is_nothrow_move_constructible<BufType>::value &&
                  std::is_nothrow_move_assignable<BufType>::value &&
                  std::is_nothrow_destructible<BufType>::value,
              void>>
struct payload {
  explicit payload(BufType&& content) : content(std::move(content)) {}
  ~payload() = default;

  payload(const payload&) = delete;
  payload& operator=(const payload&) = delete;
  payload(payload&&) = default;
  payload& operator=(payload&&) = default;

  void dump_to(hiactor::serializable_queue& qu) {}

  static payload load_from(hiactor::serializable_queue& qu) {
    return payload{BufType{}};
  }

  BufType content;
};

using query_param = payload<seastar::sstring>;
using query_result = payload<seastar::sstring>;

}  // namespace server

#endif  // SERVER_TYPES_ACT_H_
