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

#ifndef ENGINES_HTTP_SERVER_TYPES_H_
#define ENGINES_HTTP_SERVER_TYPES_H_

#include <boost/property_tree/ptree.hpp>
#include <hiactor/net/serializable_queue.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include "flex/utils/service_utils.h"

#include <string>

namespace server {

using timestamp_t = uint32_t;
using boost_ptree = boost::property_tree::ptree;

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
using admin_query_result = payload<gs::Result<seastar::sstring>>;
// url_path, query_param
using graph_management_param =
    payload<std::pair<seastar::sstring, seastar::sstring>>;
using graph_management_query_param =
    payload<std::unordered_map<seastar::sstring, seastar::sstring>>;
using procedure_query_param =
    payload<std::pair<seastar::sstring, seastar::sstring>>;
using create_procedure_query_param =
    payload<std::pair<seastar::sstring, seastar::sstring>>;
using update_procedure_query_param =
    payload<std::tuple<seastar::sstring, seastar::sstring, seastar::sstring>>;

}  // namespace server

#endif  // ENGINES_HTTP_SERVER_TYPES_H_
