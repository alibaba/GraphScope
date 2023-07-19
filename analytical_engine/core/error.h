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

#ifndef ANALYTICAL_ENGINE_CORE_ERROR_H_
#define ANALYTICAL_ENGINE_CORE_ERROR_H_

#if !(defined(__GLIBCXX__) || defined(__GLIBCPP__))
#include <cxxabi.h>
#endif

#include <string>

#include "vineyard/graph/utils/error.h"  // IWYU pragma: export

#include "proto/error_codes.pb.h"  // IWYU pragma: export

namespace gs {

inline rpc::Code ErrorCodeToProto(vineyard::ErrorCode ec) {
  switch (ec) {
  case vineyard::ErrorCode::kOk:
    return rpc::Code::OK;
  case vineyard::ErrorCode::kVineyardError:
    return rpc::Code::VINEYARD_ERROR;
  case vineyard::ErrorCode::kNetworkError:
    return rpc::Code::NETWORK_ERROR;
  case vineyard::ErrorCode::kUnimplementedMethod:
    return rpc::Code::UNIMPLEMENTED_ERROR;
  default:
    return rpc::Code::ANALYTICAL_ENGINE_INTERNAL_ERROR;
  }
}

#ifndef __FRAME_MAKE_GS_ERROR
#define __FRAME_MAKE_GS_ERROR(var, code, msg)                               \
  do {                                                                      \
    std::stringstream TOKENPASTE2(_ss, __LINE__);                           \
    vineyard::backtrace_info::backtrace(TOKENPASTE2(_ss, __LINE__), true);  \
    LOG(ERROR) << "graphscope error in frame: code = "                      \
               << static_cast<int>(code) << " at "                          \
               << (std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
                   ": " + std::string(__FUNCTION__))                        \
               << " -> " << (msg)                                           \
               << ", backtrace: " << TOKENPASTE2(_ss, __LINE__).str();      \
    var = ::boost::leaf::new_error(vineyard::GSError(                       \
        (code),                                                             \
        std::string(__FILE__) + ":" + std::to_string(__LINE__) + ": " +     \
            std::string(__FUNCTION__) + " -> " + (msg),                     \
        TOKENPASTE2(_ss, __LINE__).str()));                                 \
  } while (0)
#endif

#ifndef __FRAME_LOG_GS_ERROR
#define __FRAME_LOG_GS_ERROR(code, msg)                                     \
  do {                                                                      \
    std::stringstream TOKENPASTE2(_ss, __LINE__);                           \
    vineyard::backtrace_info::backtrace(TOKENPASTE2(_ss, __LINE__), true);  \
    LOG(ERROR) << "graphscope error in frame: code = "                      \
               << static_cast<int>(code) << " at "                          \
               << (std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
                   ": " + std::string(__FUNCTION__))                        \
               << " -> " << (msg)                                           \
               << ", backtrace: " << TOKENPASTE2(_ss, __LINE__).str();      \
  } while (0)
#endif

#ifndef __FRAME_CURRENT_EXCEPTION_TYPENAME
#if defined(__GLIBCXX__) || defined(__GLIBCPP__)
#define __FRAME_CURRENT_EXCEPTION_TYPENAME(var)                     \
  do {                                                              \
    std::exception_ptr __p = std::current_exception();              \
    var = __p ? __p.__cxa_exception_type()->name() : "unknow type"; \
  } while (0)
#else
#define __FRAME_CURRENT_EXCEPTION_TYPENAME(var)                               \
  do {                                                                        \
    int __status = 0;                                                         \
    var = abi::__cxa_demangle(abi::__cxa_current_exception_type()->name(), 0, \
                              0, &__status);                                  \
  } while (0)
#endif
#endif

#ifndef __FRAME_CATCH_AND_ASSIGN_GS_ERROR
#if defined(NDEBUG)
#define __FRAME_CATCH_AND_ASSIGN_GS_ERROR(var, expr)                           \
  do {                                                                         \
    try {                                                                      \
      var = expr;                                                              \
    } catch (std::exception & ex) {                                            \
      __FRAME_MAKE_GS_ERROR(var, vineyard::ErrorCode::kIllegalStateError,      \
                            ex.what());                                        \
    } catch (std::string & ex) {                                               \
      __FRAME_MAKE_GS_ERROR(var, vineyard::ErrorCode::kIllegalStateError, ex); \
    } catch (...) {                                                            \
      std::string __exception_type;                                            \
      __FRAME_CURRENT_EXCEPTION_TYPENAME(__exception_type);                    \
      __FRAME_MAKE_GS_ERROR(                                                   \
          var, vineyard::ErrorCode::kIllegalStateError,                        \
          std::string("Unknown error occurred: ") + __exception_type);         \
    }                                                                          \
  } while (0)
#else
#define __FRAME_CATCH_AND_ASSIGN_GS_ERROR(var, expr) \
  do {                                               \
    var = expr;                                      \
  } while (0)
#endif
#endif

#ifndef __FRAME_CATCH_AND_LOG_GS_ERROR
#if defined(NDEBUG)
#define __FRAME_CATCH_AND_LOG_GS_ERROR(var, expr)                        \
  do {                                                                   \
    try {                                                                \
      var = expr;                                                        \
    } catch (std::exception & ex) {                                      \
      __FRAME_LOG_GS_ERROR(vineyard::ErrorCode::kIllegalStateError,      \
                           ex.what());                                   \
    } catch (std::string & ex) {                                         \
      __FRAME_LOG_GS_ERROR(vineyard::ErrorCode::kIllegalStateError, ex); \
    } catch (...) {                                                      \
      std::string __exception_type;                                      \
      __FRAME_CURRENT_EXCEPTION_TYPENAME(__exception_type);              \
      __FRAME_LOG_GS_ERROR(                                              \
          vineyard::ErrorCode::kIllegalStateError,                       \
          std::string("Unknown error occurred: ") + __exception_type);   \
    }                                                                    \
  } while (0)
#else
#define __FRAME_CATCH_AND_LOG_GS_ERROR(var, expr) \
  do {                                            \
    var = expr;                                   \
  } while (0)
#endif
#endif

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_ERROR_H_
