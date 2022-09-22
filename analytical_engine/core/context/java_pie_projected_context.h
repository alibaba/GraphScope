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

#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_PIE_PROJECTED_CONTEXT_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_PIE_PROJECTED_CONTEXT_H_

#ifdef ENABLE_JAVA_SDK

#include <jni.h>
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
#include "grape/parallel/default_message_manager.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/fragment_traits.h"

#include "core/config.h"
#include "core/context/java_context_base.h"
#include "core/error.h"
#include "core/java/javasdk.h"
#include "core/object/i_fragment_wrapper.h"

namespace bl = boost::leaf;

namespace gs {

static constexpr const char* _java_projected_message_manager_name =
    "grape::DefaultMessageManager";
static constexpr const char* _java_projected_parallel_message_manager_name =
    "grape::ParallelMessageManager";

/**
 * @brief Context for the java pie app, used by java sdk.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class JavaPIEProjectedContext : public JavaContextBase<FRAG_T> {
 public:
  explicit JavaPIEProjectedContext(const FRAG_T& fragment)
      : JavaContextBase<FRAG_T>(fragment) {}

  virtual ~JavaPIEProjectedContext() {}

  void init(jlong messages_addr, const char* java_message_manager_name,
            const std::string& params, const std::string& lib_path,
            int local_num = 1) {
    JavaContextBase<FRAG_T>::init(messages_addr, java_message_manager_name,
                                  params, lib_path, local_num);
  }

  void Output(std::ostream& os) override {
    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();

      jclass context_class = env->GetObjectClass(this->context_object());
      CHECK_NOTNULL(context_class);

      const char* descriptor = "(Lcom/alibaba/graphscope/fragment/IFragment;)V";
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
    if (java_ctx_type_name == "VertexDataContext") {
      std::string data_type =
          getVertexDataContextDataType(this->context_object());
      if (data_type == "double") {
        using inner_ctx_type = grape::VertexDataContext<FRAG_T, double>;
        using inner_ctx_wrapper_type = VertexDataContextWrapper<FRAG_T, double>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(this->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        return std::make_shared<inner_ctx_wrapper_type>(id, frag_wrapper,
                                                        inner_ctx_impl_shared);
      } else if (data_type == "uint32_t") {
        using inner_ctx_type = grape::VertexDataContext<FRAG_T, uint32_t>;
        using inner_ctx_wrapper_type =
            VertexDataContextWrapper<FRAG_T, uint32_t>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(this->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        return std::make_shared<inner_ctx_wrapper_type>(id, frag_wrapper,
                                                        inner_ctx_impl_shared);
      } else if (data_type == "int32_t") {
        using inner_ctx_type = grape::VertexDataContext<FRAG_T, int32_t>;
        using inner_ctx_wrapper_type =
            VertexDataContextWrapper<FRAG_T, int32_t>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(this->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        return std::make_shared<inner_ctx_wrapper_type>(id, frag_wrapper,
                                                        inner_ctx_impl_shared);
      } else if (data_type == "uint64_t") {
        using inner_ctx_type = grape::VertexDataContext<FRAG_T, uint64_t>;
        using inner_ctx_wrapper_type =
            VertexDataContextWrapper<FRAG_T, uint64_t>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(this->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        return std::make_shared<inner_ctx_wrapper_type>(id, frag_wrapper,
                                                        inner_ctx_impl_shared);
      } else if (data_type == "int64_t") {
        using inner_ctx_type = grape::VertexDataContext<FRAG_T, int64_t>;
        using inner_ctx_wrapper_type =
            VertexDataContextWrapper<FRAG_T, int64_t>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(this->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        return std::make_shared<inner_ctx_wrapper_type>(id, frag_wrapper,
                                                        inner_ctx_impl_shared);
      } else {
        LOG(ERROR) << "Unregonizable data type: " << data_type;
      }
    } else if (java_ctx_type_name == "VertexPropertyContext") {
      using inner_ctx_type = VertexPropertyContext<FRAG_T>;
      using inner_ctx_wrapper_type = VertexPropertyContextWrapper<FRAG_T>;
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
  virtual const char* getSimpleCtxObjBaseClzNameDesc() = 0;

 private:
  std::string getJavaCtxTypeName(const jobject& ctx_object) {
    JNIEnvMark m;
    if (m.env()) {
      // jclass context_utils_class = m.env()->FindClass(CONTEXT_UTILS_CLASS);
      jclass context_utils_class = LoadClassWithClassLoader(
          m.env(), this->url_class_loader_object(), CONTEXT_UTILS_CLASS);
      CHECK_NOTNULL(context_utils_class);
      jmethodID ctx_base_class_name_get_method = m.env()->GetStaticMethodID(
          context_utils_class, "getCtxObjBaseClzName",
          getSimpleCtxObjBaseClzNameDesc());

      CHECK_NOTNULL(ctx_base_class_name_get_method);
      jstring ctx_base_clz_name = (jstring) m.env()->CallStaticObjectMethod(
          context_utils_class, ctx_base_class_name_get_method, ctx_object);
      CHECK_NOTNULL(ctx_base_clz_name);
      return JString2String(m.env(), ctx_base_clz_name);
    }
    LOG(ERROR) << "Java env not available";
    return NULL;
  }
  std::string getVertexDataContextDataType(const jobject& ctx_object) {
    JNIEnvMark m;
    if (m.env()) {
      jclass app_context_getter_class = LoadClassWithClassLoader(
          m.env(), this->url_class_loader_object(), APP_CONTEXT_GETTER_CLASS);
      CHECK_NOTNULL(app_context_getter_class);
      jmethodID getter_method = m.env()->GetStaticMethodID(
          app_context_getter_class, "getVertexDataContextDataType",
          "(Lcom/alibaba/graphscope/context/VertexDataContext;)"
          "Ljava/lang/String;");
      CHECK_NOTNULL(getter_method);
      // Pass app class's class object
      jstring context_class_jstring = (jstring) m.env()->CallStaticObjectMethod(
          app_context_getter_class, getter_method, ctx_object);
      if (m.env()->ExceptionCheck()) {
        LOG(ERROR) << "Exception in get vertex data type";
        m.env()->ExceptionDescribe();
        m.env()->ExceptionClear();
      }
      CHECK_NOTNULL(context_class_jstring);
      return JString2String(m.env(), context_class_jstring);
    }
    LOG(ERROR) << "Java env not available";
    return NULL;
  }
};

template <typename FRAG_T>
class JavaPIEProjectedDefaultContext : public JavaPIEProjectedContext<FRAG_T> {
 public:
  explicit JavaPIEProjectedDefaultContext(const FRAG_T& fragment)
      : JavaPIEProjectedContext<FRAG_T>(fragment) {}
  virtual ~JavaPIEProjectedDefaultContext() {}

  void Init(grape::DefaultMessageManager& messages, const std::string& params,
            const std::string& lib_path) {
    JavaPIEProjectedContext<FRAG_T>::init(reinterpret_cast<jlong>(&messages),
                                          _java_projected_message_manager_name,
                                          params, lib_path);
  }

 protected:
  const char* evalDescriptor() override {
    return "(Lcom/alibaba/graphscope/fragment/IFragment;"
           "Lcom/alibaba/graphscope/parallel/DefaultMessageManager;"
           "Lcom/alibaba/fastjson/JSONObject;)V";
  }
  const char* getSimpleCtxObjBaseClzNameDesc() override {
    return "(Lcom/alibaba/graphscope/context/ContextBase;)"
           "Ljava/lang/String;";
  }
};

template <typename FRAG_T>
class JavaPIEProjectedParallelContext : public JavaPIEProjectedContext<FRAG_T> {
 public:
  explicit JavaPIEProjectedParallelContext(const FRAG_T& fragment)
      : JavaPIEProjectedContext<FRAG_T>(fragment) {}
  virtual ~JavaPIEProjectedParallelContext() {}

  void Init(grape::ParallelMessageManager& messages, const std::string& params,
            const std::string& lib_path) {
    JavaPIEProjectedContext<FRAG_T>::init(
        reinterpret_cast<jlong>(&messages),
        _java_projected_parallel_message_manager_name, params, lib_path);
  }

 protected:
  const char* evalDescriptor() override {
    return "(Lcom/alibaba/graphscope/fragment/IFragment;"
           "Lcom/alibaba/graphscope/parallel/ParallelMessageManager;"
           "Lcom/alibaba/fastjson/JSONObject;)V";
  }
  const char* getSimpleCtxObjBaseClzNameDesc() override {
    return "(Lcom/alibaba/graphscope/context/ContextBase;)"
           "Ljava/lang/String;";
  }
};

// shall not be invoked
template <typename FRAG_T>
class JavaPIEProjectedContextWrapper : public IJavaPIEProjectedContextWrapper {
  using fragment_t = FRAG_T;
  using label_id_t = typename fragment_t::label_id_t;
  using prop_id_t = typename fragment_t::prop_id_t;
  using oid_t = typename fragment_t::oid_t;
  using context_t = JavaPIEProjectedContext<fragment_t>;

 public:
  JavaPIEProjectedContextWrapper(const std::string& id,
                                 std::shared_ptr<IFragmentWrapper> frag_wrapper,
                                 std::shared_ptr<context_t> context) {}

  std::string context_type() override {
    return std::string(CONTEXT_TYPE_JAVA_PIE_PROJECTED);
  }

  std::shared_ptr<IFragmentWrapper> fragment_wrapper() override {
    return std::shared_ptr<IFragmentWrapper>(nullptr);
  }
  // Considering labeledSelector vs selector
  bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const Selector& selector,
      const std::pair<std::string, std::string>& range) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                    "No implementation needed for Java context wrapper");
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                    "No implementation needed for Java context wrapper");
  }

  bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const Selector& selector,
      const std::pair<std::string, std::string>& range) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                    "No implementation needed for Java context wrapper");
  }

  bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                    "No implementation needed for Java context wrapper");
  }

  bl::result<std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>
  ToArrowArrays(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors) override {
    RETURN_GS_ERROR(vineyard::ErrorCode::kUnimplementedMethod,
                    "No implementation needed for Java context wrapper");
  }
};

}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_PIE_PROJECTED_CONTEXT_H_
