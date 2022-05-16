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

#ifndef ANALYTICAL_ENGINE_CORE_UTILS_LIB_UTILS_H_
#define ANALYTICAL_ENGINE_CORE_UTILS_LIB_UTILS_H_

#include <dlfcn.h>

#include <ostream>
#include <string>
#include <utility>

#include "boost/leaf/result.hpp"

#include "core/error.h"

namespace bl = boost::leaf;

namespace gs {
inline bl::result<void*> open_lib(const char* path) {
  void* handle = dlopen(path, RTLD_LAZY);
  auto* p_error_msg = dlerror();
  if (p_error_msg) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kIOError,
                    "Failed to open library: " + std::string(path) +
                        ". Reason: " + std::string(p_error_msg));
  }
  return handle;
}

inline bl::result<void*> get_func_ptr(const std::string& lib_path, void* handle,
                                      const char* symbol) {
  auto* p_func = dlsym(handle, symbol);
  auto* p_error_msg = dlerror();
  if (p_error_msg) {
    std::stringstream ss;

    ss << "Failed to get symbol " << symbol << " from " << lib_path
       << ". Reason: " << std::string(p_error_msg);

    RETURN_GS_ERROR(vineyard::ErrorCode::kIOError, ss.str());
  }
  return p_func;
}
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_UTILS_LIB_UTILS_H_
