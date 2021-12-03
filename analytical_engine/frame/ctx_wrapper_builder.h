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

#ifndef ANALYTICAL_ENGINE_FRAME_CTX_WRAPPER_BUILDER_H_
#define ANALYTICAL_ENGINE_FRAME_CTX_WRAPPER_BUILDER_H_

#include <memory>
#include <string>

#include "grape/app/context_base.h"
#include "grape/app/vertex_data_context.h"
#include "grape/app/void_context.h"

#include "core/context/i_context.h"
#ifdef ENABLE_JAVA_SDK
#include "core/context/java_pie_projected_context.h"
#include "core/context/java_pie_property_context.h"
#endif
#include "core/context/labeled_vertex_property_context.h"
#include "core/context/tensor_context.h"
#include "core/context/vertex_data_context.h"
#include "core/context/vertex_property_context.h"

#if !defined(_GRAPH_TYPE)
#error _GRAPH_TYPE is not defined
#endif
namespace gs {
template <template <typename...> class C, typename... Ts>
std::true_type is_base_of_template_impl(const C<Ts...>*);

template <template <typename...> class C>
std::false_type is_base_of_template_impl(...);

template <typename T, template <typename...> class C>
using is_base_of_template =
    decltype(is_base_of_template_impl<C>(std::declval<T*>()));

/**
 * @brief This is a utility to build kinds of ContextWrapper instances.
 * @tparam CTX_T
 */
template <typename CTX_T, typename = void>
struct CtxWrapperBuilder {};

/**
 * @brief A specialized CtxWrapperBuilder for grape::VoidContext
 * @tparam CTX_T
 */
template <typename CTX_T>
struct CtxWrapperBuilder<CTX_T, typename std::enable_if<is_base_of_template<
                                    CTX_T, grape::VoidContext>::value>::type> {
  static std::shared_ptr<gs::IContextWrapper> build(
      const std::string& id, std::shared_ptr<IFragmentWrapper> frag_wrapper,
      std::shared_ptr<CTX_T> ctx) {
    return nullptr;
  }
};

/**
 * @brief A specialized CtxWrapperBuilder for grape::VertexDataContext
 * @tparam CTX_T
 */
template <typename CTX_T>
struct CtxWrapperBuilder<CTX_T,
                         typename std::enable_if<is_base_of_template<
                             CTX_T, grape::VertexDataContext>::value>::type> {
  static std::shared_ptr<gs::IContextWrapper> build(
      const std::string& id, std::shared_ptr<IFragmentWrapper> frag_wrapper,
      std::shared_ptr<CTX_T> ctx) {
    using data_t = typename CTX_T::data_t;
    return std::make_shared<gs::VertexDataContextWrapper<_GRAPH_TYPE, data_t>>(
        id, frag_wrapper, ctx);
  }
};

/**
 * @brief A specialized CtxWrapperBuilder for VertexPropertyContext
 * @tparam CTX_T
 */
template <typename CTX_T>
struct CtxWrapperBuilder<CTX_T,
                         typename std::enable_if<is_base_of_template<
                             CTX_T, VertexPropertyContext>::value>::type> {
  static std::shared_ptr<gs::IContextWrapper> build(
      const std::string& id, std::shared_ptr<IFragmentWrapper> frag_wrapper,
      std::shared_ptr<CTX_T> ctx) {
    return std::make_shared<gs::VertexPropertyContextWrapper<_GRAPH_TYPE>>(
        id, frag_wrapper, ctx);
  }
};

/**
 * @brief A specialized CtxWrapperBuilder for LabeledVertexDataContext
 * @tparam CTX_T
 */
template <typename CTX_T>
struct CtxWrapperBuilder<CTX_T,
                         typename std::enable_if<is_base_of_template<
                             CTX_T, LabeledVertexDataContext>::value>::type> {
  static std::shared_ptr<gs::IContextWrapper> build(
      const std::string& id, std::shared_ptr<IFragmentWrapper> frag_wrapper,
      std::shared_ptr<CTX_T> ctx) {
    using data_t = typename CTX_T::data_t;

    return std::make_shared<
        gs::LabeledVertexDataContextWrapper<_GRAPH_TYPE, data_t>>(
        id, frag_wrapper, ctx);
  }
};

/**
 * @brief A specialized CtxWrapperBuilder for LabeledVertexPropertyContext
 * @tparam CTX_T
 */
template <typename CTX_T>
struct CtxWrapperBuilder<
    CTX_T, typename std::enable_if<is_base_of_template<
               CTX_T, LabeledVertexPropertyContext>::value>::type> {
  static std::shared_ptr<gs::IContextWrapper> build(
      const std::string& id, std::shared_ptr<IFragmentWrapper> frag_wrapper,
      std::shared_ptr<CTX_T> ctx) {
    return std::make_shared<
        gs::LabeledVertexPropertyContextWrapper<_GRAPH_TYPE>>(id, frag_wrapper,
                                                              ctx);
  }
};

/**
 * @brief A specialized CtxWrapperBuilder for TensorContext
 * @tparam CTX_T
 */
template <typename CTX_T>
struct CtxWrapperBuilder<CTX_T, typename std::enable_if<is_base_of_template<
                                    CTX_T, TensorContext>::value>::type> {
  static std::shared_ptr<gs::IContextWrapper> build(
      const std::string& id, std::shared_ptr<IFragmentWrapper> frag_wrapper,
      std::shared_ptr<CTX_T> ctx) {
    using data_t = typename CTX_T::data_t;

    return std::make_shared<gs::TensorContextWrapper<_GRAPH_TYPE, data_t>>(
        id, frag_wrapper, ctx);
  }
};

#ifdef ENABLE_JAVA_SDK
/**
 * @brief A specialized CtxWrapperBuilder for JavaPropertyPIEctx
 * @tparam CTX_T
 */
template <typename CTX_T>
struct CtxWrapperBuilder<CTX_T,
                         typename std::enable_if<is_base_of_template<
                             CTX_T, JavaPIEPropertyContext>::value>::type> {
  static std::shared_ptr<gs::IContextWrapper> build(
      const std::string& id, std::shared_ptr<IFragmentWrapper> frag_wrapper,
      std::shared_ptr<CTX_T> ctx) {
    return ctx->CreateInnerCtxWrapper(id, frag_wrapper);
  }
};

/**
 * @brief A specialized CtxWrapperBuilder for JavaProjectedPIEctx
 * @tparam CTX_T
 */
template <typename CTX_T>
struct CtxWrapperBuilder<CTX_T,
                         typename std::enable_if<is_base_of_template<
                             CTX_T, JavaPIEProjectedContext>::value>::type> {
  static std::shared_ptr<gs::IContextWrapper> build(
      const std::string& id, std::shared_ptr<IFragmentWrapper> frag_wrapper,
      std::shared_ptr<CTX_T> ctx) {
    return ctx->CreateInnerCtxWrapper(id, frag_wrapper);
  }
};
#endif
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_FRAME_CTX_WRAPPER_BUILDER_H_
