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

#ifndef ANALYTICAL_ENGINE_APPS_PYTHON_PIE_CYTHON_PIE_PROGRAM_H_
#define ANALYTICAL_ENGINE_APPS_PYTHON_PIE_CYTHON_PIE_PROGRAM_H_

#include "apps/python_pie/export.h"
#include "apps/python_pie/wrapper.h"

namespace gs {

template <typename VD_TYPE, typename MD_TYPE>
class CythonPIEProgram {
 public:
  using vd_t = VD_TYPE;
  using md_t = MD_TYPE;
  using wrapper_fragment_t = python_grape::Fragment;
  using wrapper_context_t = python_grape::Context<vd_t, md_t>;
  typedef void (*InitFuncT)(wrapper_fragment_t&, wrapper_context_t&);
  typedef void (*PEvalFuncT)(wrapper_fragment_t&, wrapper_context_t&);
  typedef void (*IncEvalFuncT)(wrapper_fragment_t&, wrapper_context_t&);

  CythonPIEProgram()
      : init_func_(nullptr), peval_func_(nullptr), inceval_func_(nullptr) {}

  void SetInitFunction(InitFuncT init_func) { init_func_ = init_func; }

  void SetPEvalFunction(PEvalFuncT peval_func) { peval_func_ = peval_func; }

  void SetIncEvalFunction(IncEvalFuncT inceval_func) {
    inceval_func_ = inceval_func;
  }

  inline void Init(wrapper_fragment_t& frag, wrapper_context_t& context) {
    init_func_(frag, context);
  }

  inline void PEval(wrapper_fragment_t& frag, wrapper_context_t& context) {
    peval_func_(frag, context);
  }

  inline void IncEval(wrapper_fragment_t& frag, wrapper_context_t& context) {
    inceval_func_(frag, context);
  }

 private:
  InitFuncT init_func_;
  PEvalFuncT peval_func_;
  IncEvalFuncT inceval_func_;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PYTHON_PIE_CYTHON_PIE_PROGRAM_H_
