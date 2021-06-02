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

#ifndef ANALYTICAL_ENGINE_CORE_UTILS_APP_UTILS_H_
#define ANALYTICAL_ENGINE_CORE_UTILS_APP_UTILS_H_

#include <dlfcn.h>

#include <string>
#include <type_traits>

namespace gs {
template <typename T, typename F>
auto static_if(std::true_type, T t, F f) {
  return t;
}

template <typename T, typename F>
auto static_if(std::false_type, T t, F f) {
  return f;
}

template <bool B, typename T, typename F>
auto static_if(T t, F f) {
  return static_if(std::integral_constant<bool, B>{}, t, f);
}

template <bool B, typename T>
auto static_if(T t) {
  return static_if(std::integral_constant<bool, B>{}, t, [](auto&&...) {});
}
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_UTILS_APP_UTILS_H_
