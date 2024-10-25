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

#ifndef RUNTIME_COMMON_LEAF_UTILS_H_
#define RUNTIME_COMMON_LEAF_UTILS_H_

#include <boost/leaf.hpp>
#include "flex/utils/result.h"

namespace bl = boost::leaf;

// Concatenate the current function name and line number to form the error
// message
#define PREPEND_LINE_INFO(msg)                             \
  std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
      " func: " + std::string(__FUNCTION__) + ", " + msg

#define RETURN_UNSUPPORTED_ERROR(msg)           \
  return ::boost::leaf::new_error(::gs::Status( \
      ::gs::StatusCode::UNSUPPORTED_OPERATION, PREPEND_LINE_INFO(msg)))

#define RETURN_BAD_REQUEST_ERROR(msg) \
  return ::boost::leaf::new_error(    \
      ::gs::Status(::gs::StatusCode::BAD_REQUEST, PREPEND_LINE_INFO(msg)))

#define RETURN_NOT_IMPLEMENTED_ERROR(msg) \
  return ::boost::leaf::new_error(        \
      ::gs::Status(::gs::StatusCode::UNIMPLEMENTED, PREPEND_LINE_INFO(msg)))

#define RETURN_CALL_PROCEDURE_ERROR(msg) \
  return ::boost::leaf::new_error(       \
      ::gs::Status(::gs::StatusCode::QUERY_FAILED, PREPEND_LINE_INFO(msg)))

#endif  // RUNTIME_COMMON_LEAF_UTILS_H_
