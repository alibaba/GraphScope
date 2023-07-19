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

#ifndef ANALYTICAL_ENGINE_CORE_APP_APP_INVOKER_H_
#define ANALYTICAL_ENGINE_CORE_APP_APP_INVOKER_H_

#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>

#include "glog/logging.h"
#include "grape/util.h"

#ifdef NETWORKX
#include "core/object/dynamic.h"
#endif
#include "core/config.h"
#include "core/error.h"
#include "proto/data_types.pb.h"
#include "proto/types.pb.h"

namespace bl = boost::leaf;

namespace gs {

/**
 * @brief ArgsUnpacker is an util to unpack the Any type of proto to C++
 * datatype.
 *
 * @tparam T The destination type
 */
template <typename T>
struct ArgsUnpacker {};

/**
 * @brief A specialized ArgsUnpacker with bool type
 */
template <>
struct ArgsUnpacker<bool> {
  using ProtoType = rpc::BoolValue;

  static bool unpack(const google::protobuf::Any& arg) {
    ProtoType proto_arg;
    arg.UnpackTo(&proto_arg);
    return proto_arg.value();
  }
};

/**
 * @brief A specialized ArgsUnpacker with int64_t type
 */
template <>
struct ArgsUnpacker<int64_t> {
  using ProtoType = rpc::Int64Value;

  static int64_t unpack(const google::protobuf::Any& arg) {
    ProtoType proto_arg;
    arg.UnpackTo(&proto_arg);
    return proto_arg.value();
  }
};

/**
 * @brief A specialized ArgsUnpacker with int type
 */
template <>
struct ArgsUnpacker<int> {
  using ProtoType = rpc::Int64Value;

  static int unpack(const google::protobuf::Any& arg) {
    ProtoType proto_arg;
    arg.UnpackTo(&proto_arg);
    return static_cast<int>(proto_arg.value());
  }
};

/**
 * @brief A specialized ArgsUnpacker with double type
 */
template <>
struct ArgsUnpacker<double> {
  using ProtoType = rpc::DoubleValue;

  static double unpack(const google::protobuf::Any& arg) {
    ProtoType proto_arg;
    arg.UnpackTo(&proto_arg);
    return proto_arg.value();
  }
};

/**
 * @brief A specialized ArgsUnpacker with std::string type
 */
template <>
struct ArgsUnpacker<std::string> {
  using ProtoType = rpc::StringValue;

  static std::string unpack(const google::protobuf::Any& arg) {
    ProtoType proto_arg;
    arg.UnpackTo(&proto_arg);
    return proto_arg.value();
  }
};

#ifdef NETWORKX
/**
 * @brief A specialized ArgsUnpacker with dynamic::Value type
 */
template <>
struct ArgsUnpacker<dynamic::Value> {
  static dynamic::Value unpack(const google::protobuf::Any& arg) {
    if (arg.Is<rpc::Int64Value>()) {
      rpc::Int64Value proto_arg;
      arg.UnpackTo(&proto_arg);
      return dynamic::Value(proto_arg.value());
    } else if (arg.Is<rpc::StringValue>()) {
      rpc::StringValue proto_arg;
      arg.UnpackTo(&proto_arg);
      return dynamic::Value(proto_arg.value());
    } else {
      throw std::runtime_error("Not support oid type.");
    }
  }
};
#endif

template <class FunctionType>
struct ArgsNum;

template <class ResultType, class ContextType, class FirstArgType,
          class... ArgsType>
struct ArgsNum<ResultType (ContextType::*)(FirstArgType, ArgsType...)> {
  static constexpr std::size_t value =
      ArgsNum<ResultType (ContextType::*)(ArgsType...)>::value + 1;
};

template <class ResultType, class ContextType>
struct ArgsNum<ResultType (ContextType::*)()> {
  static constexpr std::size_t value = 0;
};

template <std::size_t index, class FunctionType>
struct ArgTypeAt;

template <class ResultType, class ContextType, class FirstArgType,
          class... ArgsType>
struct ArgTypeAt<0, ResultType (ContextType::*)(FirstArgType, ArgsType...)> {
  using type = FirstArgType;
};

template <std::size_t index, class ResultType, class ContextType,
          class FirstArgType, class... ArgsType>
struct ArgTypeAt<index,
                 ResultType (ContextType::*)(FirstArgType, ArgsType...)> {
  using type =
      typename ArgTypeAt<index - 1,
                         ResultType (ContextType::*)(ArgsType...)>::type;
};

/**
 * @brief AppInvoker is a utility to construct QueryArgs and issue a query.
 * QueryArgs is constructed by inferring the variadic of "Init" method in the
 * app context class.
 *
 * @tparam APP_T The App class
 */
template <typename APP_T>
class AppInvoker {
  using worker_t = typename APP_T::worker_t;
  using context_t = typename APP_T::context_t;
  using context_init_func_t = decltype(&context_t::Init);

  template <std::size_t... I>
  static void query_impl(std::shared_ptr<worker_t> worker,
                         const rpc::QueryArgs& query_args,
                         std::index_sequence<I...>) {
    double start_time = grape::GetCurrentTime();
    worker->Query(
        ArgsUnpacker<typename std::remove_const<typename std::remove_reference<
            typename ArgTypeAt<I + 1, context_init_func_t>::type>::type>::
                         type>::unpack(query_args.args(I))...);
    double end_time = grape::GetCurrentTime();
    LOG(INFO) << "Query time: " << end_time - start_time << " seconds";
  }

 public:
  static bl::result<std::nullptr_t> Query(std::shared_ptr<worker_t> worker,
                                          const rpc::QueryArgs& query_args) {
    constexpr std::size_t args_num = ArgsNum<context_init_func_t>::value - 1;
    // There maybe default argument
    CHECK_OR_RAISE(args_num >= query_args.args_size());
    query_impl(worker, query_args, std::make_index_sequence<args_num>());
    return nullptr;
  }
};

template <typename APP_T>
class FlashAppInvoker {
  using worker_t = typename APP_T::worker_t;
  using app_run_func_t = decltype(&APP_T::Run);

  template <std::size_t... I>
  static void query_impl(std::shared_ptr<worker_t> worker,
                         const rpc::QueryArgs& query_args,
                         std::index_sequence<I...>) {
    double start_time = grape::GetCurrentTime();
    worker->Query(
        ArgsUnpacker<typename std::remove_const<typename std::remove_reference<
            typename ArgTypeAt<I + 2, app_run_func_t>::type>::type>::type>::
            unpack(query_args.args(I))...);
    double end_time = grape::GetCurrentTime();
    LOG(INFO) << "Query time: " << end_time - start_time << " seconds";
  }

 public:
  static bl::result<std::nullptr_t> Query(std::shared_ptr<worker_t> worker,
                                          const rpc::QueryArgs& query_args) {
    constexpr std::size_t args_num = ArgsNum<app_run_func_t>::value - 2;
    // There maybe default argument
    CHECK_OR_RAISE(args_num >= query_args.args_size());
    query_impl(worker, query_args, std::make_index_sequence<args_num>());
    return nullptr;
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_APP_APP_INVOKER_H_
