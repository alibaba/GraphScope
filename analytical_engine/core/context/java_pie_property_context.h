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

#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_PIE_PROPERTY_CONTEXT_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_PIE_PROPERTY_CONTEXT_H_

#ifdef ENABLE_JAVA_SDK

#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"

#include "grape/grape.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/fragment_traits.h"

#include "core/config.h"
#include "core/context/java_context_base.h"
#include "core/error.h"
#include "core/java/javasdk.h"
#include "core/object/i_fragment_wrapper.h"
#include "core/parallel/parallel_property_message_manager.h"
#include "core/parallel/property_message_manager.h"

namespace bl = boost::leaf;

namespace gs {

static constexpr const char* _java_property_message_manager_name =
    "gs::PropertyMessageManager";
static constexpr const char* _java_parallel_property_message_manager_name =
    "gs::ParallelPropertyMessageManager";
/**
 * @brief Context for the java pie app, used by java sdk.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class JavaPIEPropertyContext : public JavaContextBase<FRAG_T> {
 public:
  explicit JavaPIEPropertyContext(const FRAG_T& fragment)
      : JavaContextBase<FRAG_T>(fragment) {}

  virtual ~JavaPIEPropertyContext() {}

  void init(jlong messages_addr, const char* java_message_manager_name,
            const std::string& params, const std::string& lib_path) {
    JavaContextBase<FRAG_T>::init(messages_addr, java_message_manager_name,
                                  params, lib_path);
  }

  void Output(std::ostream& os) override {
    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();

      jclass context_class = env->GetObjectClass(this->context_object());
      CHECK_NOTNULL(context_class);

      const char* descriptor =
          "(Lcom/alibaba/graphscope/fragment/ArrowFragment;)V";
      jmethodID output_methodID =
          env->GetMethodID(context_class, "Output", descriptor);
      if (output_methodID) {
        VLOG(1) << "Found output method in java context.";
        env->CallVoidMethod(this->context_object(), output_methodID,
                            this->fragment_object());
      } else {
        VLOG(1) << "Output method not found, skip.";
      }
    } else {
      LOG(ERROR) << "JNI env not available.";
    }
  }

  std::shared_ptr<gs::IContextWrapper> CreateInnerCtxWrapper(
      const std::string& id, std::shared_ptr<IFragmentWrapper> frag_wrapper) {
    std::string java_ctx_type_name = getJavaCtxTypeName(this->context_object());
    VLOG(1) << "Java ctx type name" << java_ctx_type_name;
    if (java_ctx_type_name == "LabeledVertexDataContext") {
      // Get the DATA_T;
      std::string data_type =
          getLabeledVertexDataContextDataType(this->context_object());
      if (data_type == "double") {
        using inner_ctx_type = LabeledVertexDataContext<FRAG_T, double>;
        using inner_ctx_wrapper_type =
            LabeledVertexDataContextWrapper<FRAG_T, double>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(this->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        return std::make_shared<inner_ctx_wrapper_type>(id, frag_wrapper,
                                                        inner_ctx_impl_shared);
      } else if (data_type == "uint32_t") {
        using inner_ctx_type = LabeledVertexDataContext<FRAG_T, uint32_t>;
        using inner_ctx_wrapper_type =
            LabeledVertexDataContextWrapper<FRAG_T, uint32_t>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(this->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        return std::make_shared<inner_ctx_wrapper_type>(id, frag_wrapper,
                                                        inner_ctx_impl_shared);
      } else if (data_type == "uint64_t") {
        using inner_ctx_type = LabeledVertexDataContext<FRAG_T, uint64_t>;
        using inner_ctx_wrapper_type =
            LabeledVertexDataContextWrapper<FRAG_T, uint64_t>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(this->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        return std::make_shared<inner_ctx_wrapper_type>(id, frag_wrapper,
                                                        inner_ctx_impl_shared);
      } else {
        LOG(ERROR) << "Unregonizable data type: " << data_type;
      }
    } else if (java_ctx_type_name == "LabeledVertexPropertyContext") {
      using inner_ctx_type = LabeledVertexPropertyContext<FRAG_T>;
      using inner_ctx_wrapper_type =
          LabeledVertexPropertyContextWrapper<FRAG_T>;
      auto inner_ctx_impl =
          reinterpret_cast<inner_ctx_type*>(this->inner_context_addr());
      std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
      return std::make_shared<inner_ctx_wrapper_type>(id, frag_wrapper,
                                                      inner_ctx_impl_shared);

    } else {
      LOG(ERROR) << "Unsupported context type: " << java_ctx_type_name;
    }
    return std::shared_ptr<gs::IContextWrapper>(nullptr);
  }

 protected:
  virtual const char* getPropertyCtxObjBaseClazNameDesc() = 0;

 private:
  std::string getJavaCtxTypeName(const jobject& ctx_object) {
    JNIEnvMark m;
    if (m.env()) {
      jclass context_utils_class = LoadClassWithClassLoader(
          m.env(), this->url_class_loader_object(), CONTEXT_UTILS_CLASS);
      CHECK_NOTNULL(context_utils_class);
      jmethodID ctx_base_class_name_get_method = m.env()->GetStaticMethodID(
          context_utils_class, "getCtxObjBaseClzName",
          getPropertyCtxObjBaseClazNameDesc());

      CHECK_NOTNULL(ctx_base_class_name_get_method);
      jstring ctx_base_clz_name = (jstring) m.env()->CallStaticObjectMethod(
          context_utils_class, ctx_base_class_name_get_method, ctx_object);
      CHECK_NOTNULL(ctx_base_clz_name);
      return JString2String(m.env(), ctx_base_clz_name);
    }
    LOG(ERROR) << "Java env not available";
    return NULL;
  }
  std::string getLabeledVertexDataContextDataType(const jobject& ctx_object) {
    JNIEnvMark m;
    if (m.env()) {
      jclass app_context_getter_class = LoadClassWithClassLoader(
          m.env(), this->url_class_loader_object(), APP_CONTEXT_GETTER_CLASS);
      CHECK_NOTNULL(app_context_getter_class);
      jmethodID getter_method = m.env()->GetStaticMethodID(
          app_context_getter_class, "getLabeledVertexDataContextDataType",
          "(Lcom/alibaba/graphscope/context/LabeledVertexDataContext;)"
          "Ljava/lang/String;");
      CHECK_NOTNULL(getter_method);
      // Pass app class's class object
      jstring context_class_jstring = (jstring) m.env()->CallStaticObjectMethod(
          app_context_getter_class, getter_method, ctx_object);
      CHECK_NOTNULL(context_class_jstring);
      return JString2String(m.env(), context_class_jstring);
    }
    LOG(ERROR) << "Java env not available";
    return NULL;
  }
};

/**
 * @brief Context for the java pie default property app, used by java sdk.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class JavaPIEPropertyDefaultContext : public JavaPIEPropertyContext<FRAG_T> {
 public:
  explicit JavaPIEPropertyDefaultContext(const FRAG_T& fragment)
      : JavaPIEPropertyContext<FRAG_T>(fragment) {}
  virtual ~JavaPIEPropertyDefaultContext() {}

  void Init(PropertyMessageManager& messages, const std::string& params,
            const std::string& lib_path) {
    VLOG(1) << "lib path: " << lib_path;
    JavaPIEPropertyContext<FRAG_T>::init(reinterpret_cast<jlong>(&messages),
                                         _java_property_message_manager_name,
                                         params, lib_path);
  }

 protected:
  const char* evalDescriptor() override {
    return "(Lcom/alibaba/graphscope/fragment/ArrowFragment;"
           "Lcom/alibaba/graphscope/parallel/PropertyMessageManager;"
           "Lcom/alibaba/fastjson/JSONObject;)V";
  }
  const char* getPropertyCtxObjBaseClazNameDesc() override {
    return "(Lcom/alibaba/graphscope/context/ContextBase;)"
           "Ljava/lang/String;";
  }
};

/**
 * @brief Context for the java pie parallel property app, used by java sdk.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class JavaPIEPropertyParallelContext : public JavaPIEPropertyContext<FRAG_T> {
 public:
  explicit JavaPIEPropertyParallelContext(const FRAG_T& fragment)
      : JavaPIEPropertyContext<FRAG_T>(fragment) {}
  virtual ~JavaPIEPropertyParallelContext() {}

  void Init(ParallelPropertyMessageManager& messages, const std::string& params,
            const std::string& lib_path) {
    VLOG(1) << "lib path: " << lib_path;
    JavaPIEPropertyContext<FRAG_T>::init(
        reinterpret_cast<jlong>(&messages),
        _java_parallel_property_message_manager_name, params, lib_path);
  }

 protected:
  const char* evalDescriptor() override {
    return "(Lcom/alibaba/graphscope/fragment/ArrowFragment;"
           "Lcom/alibaba/graphscope/parallel/ParallelPropertyMessageManager;"
           "Lcom/alibaba/fastjson/JSONObject;)V";
  }
  const char* getPropertyCtxObjBaseClazNameDesc() override {
    return "(Lcom/alibaba/graphscope/context/ContextBase;)"
           "Ljava/lang/String;";
  }
};

// This Wrapper works as a proxy, forward requests like toNdArray, to the c++
// context held by java object.
template <typename FRAG_T>
class JavaPIEPropertyContextWrapper : public IJavaPIEPropertyContextWrapper {
  using fragment_t = FRAG_T;
  using label_id_t = typename fragment_t::label_id_t;
  using prop_id_t = typename fragment_t::prop_id_t;
  using oid_t = typename fragment_t::oid_t;
  using context_t = JavaPIEPropertyContext<fragment_t>;
  static_assert(vineyard::is_property_fragment<FRAG_T>::value,
                "JavaPIEPropertyContextWrapper is only available for "
                "property graph");

 public:
  JavaPIEPropertyContextWrapper(const std::string& id,
                                std::shared_ptr<IFragmentWrapper> frag_wrapper,
                                std::shared_ptr<context_t> context) {}

  std::string context_type() override {
    return std::string(CONTEXT_TYPE_JAVA_PIE_PROPERTY);
  }

  std::shared_ptr<IFragmentWrapper> fragment_wrapper() override {
    return std::shared_ptr<IFragmentWrapper>(nullptr);
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const LabeledSelector& selector,
      const std::pair<std::string, std::string>& range) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                    "No implementation needed for Java context wrapper");
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, LabeledSelector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                    "No implementation needed for Java context wrapper");
  }

  bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const LabeledSelector& selector,
      const std::pair<std::string, std::string>& range) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                    "No implementation needed for Java context wrapper");
  }

  bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const std::vector<std::pair<std::string, LabeledSelector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                    "No implementation needed for Java context wrapper");
  }

  bl::result<std::map<
      label_id_t,
      std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>>
  ToArrowArrays(const grape::CommSpec& comm_spec,
                const std::vector<std::pair<std::string, LabeledSelector>>&
                    selectors) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                    "No implementation needed for Java context wrapper");
  }
};
}  // namespace gs

#endif
#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_PIE_PROPERTY_CONTEXT_H_
