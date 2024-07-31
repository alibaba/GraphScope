/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_PROJECTED_PARALLEL_APP_H_
#define ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_PROJECTED_PARALLEL_APP_H_

#ifdef ENABLE_JAVA_SDK

#include <utility>

#include "grape/communication/communicator.h"
#include "grape/grape.h"
#include "grape/types.h"

#include "core/context/java_pie_projected_context.h"
#include "core/error.h"
#include "core/java/utils.h"

namespace gs {

/**
 * @brief This is a driver app for Java app. The driven java app should be
 * inherited from ProjectedDefaultAppBase.
 *
 * @tparam FRAG_T Should be gs::ArrowProjectedFragment<...>
 */
template <typename FRAG_T,
          grape::MessageStrategy Strategy =
              grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex>
class JavaPIEProjectedParallelApp
    : public grape::ParallelAppBase<FRAG_T,
                                    JavaPIEProjectedParallelContext<FRAG_T>>,
      public grape::Communicator {
 public:
  using app_type = JavaPIEProjectedParallelApp<FRAG_T, Strategy>;
  INSTALL_JAVA_PARALLEL_WORKER(app_type,
                               JavaPIEProjectedParallelContext<FRAG_T>, FRAG_T);
  static constexpr grape::MessageStrategy message_strategy = Strategy;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();

      jobject app_object = ctx.app_object();
      auto communicator = static_cast<grape::Communicator*>(this);
      InitJavaCommunicator(env, ctx.url_class_loader_object(), app_object,
                           reinterpret_cast<jlong>(communicator));

      jclass app_class = env->GetObjectClass(app_object);

      const char* descriptor =
          "(Lcom/alibaba/graphscope/fragment/IFragment;"
          "Lcom/alibaba/graphscope/context/ParallelContextBase;"
          "Lcom/alibaba/graphscope/parallel/ParallelMessageManager;)V";
      jmethodID pEval_methodID =
          env->GetMethodID(app_class, "PEval", descriptor);
      CHECK_NOTNULL(pEval_methodID);

      jobject frag_object = ctx.fragment_object();
      jobject context_object = ctx.context_object();
      jobject mm_object = ctx.message_manager_object();

      env->CallVoidMethod(app_object, pEval_methodID, frag_object,
                          context_object, mm_object);
    } else {
      LOG(ERROR) << "JNI env not available.";
    }
  }

  /**
   * @brief Incremental evaluation.
   *
   * @param frag
   * @param ctx
   */
  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();

      jobject app_object = ctx.app_object();

      jclass app_class = env->GetObjectClass(app_object);
      CHECK_NOTNULL(app_class);

      const char* descriptor =
          "(Lcom/alibaba/graphscope/fragment/IFragment;"
          "Lcom/alibaba/graphscope/context/ParallelContextBase;"
          "Lcom/alibaba/graphscope/parallel/ParallelMessageManager;)V";
      jmethodID incEval_methodID =
          env->GetMethodID(app_class, "IncEval", descriptor);
      CHECK_NOTNULL(incEval_methodID);

      jobject frag_object = ctx.fragment_object();
      jobject context_object = ctx.context_object();
      jobject mm_object = ctx.message_manager_object();

      env->CallVoidMethod(app_object, incEval_methodID, frag_object,
                          context_object, mm_object);
    } else {
      LOG(ERROR) << "JNI env not available.";
    }
  }
};

template <typename FRAG_T>
using JavaPIEProjectedParallelAppOE = JavaPIEProjectedParallelApp<
    FRAG_T, grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex>;

template <typename FRAG_T>
using JavaPIEProjectedParallelAppIE = JavaPIEProjectedParallelApp<
    FRAG_T, grape::MessageStrategy::kAlongIncomingEdgeToOuterVertex>;

template <typename FRAG_T>
using JavaPIEProjectedParallelAppE = JavaPIEProjectedParallelApp<
    FRAG_T, grape::MessageStrategy::kAlongEdgeToOuterVertex>;

template <typename FRAG_T>
using JavaPIEProjectedParallelAppSync =
    JavaPIEProjectedParallelApp<FRAG_T,
                                grape::MessageStrategy::kSyncOnOuterVertex>;

}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_PROJECTED_PARALLEL_APP_H_
