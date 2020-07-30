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

#ifndef ANALYTICAL_ENGINE_APPS_PYTHON_PIE_PYTHON_PIE_APP_H_
#define ANALYTICAL_ENGINE_APPS_PYTHON_PIE_PYTHON_PIE_APP_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "core/app/property_auto_app_base.h"

#include "apps/python_pie/python_pie_context.h"
#include "apps/python_pie/wrapper.h"

namespace gs {

template <typename FRAG_T, typename PIE_PROGRAM_T>
class PythonPIEApp
    : public PropertyAutoAppBase<
          FRAG_T, PIEContext<FRAG_T, PythonPIEComputeContext<
                                         FRAG_T, typename PIE_PROGRAM_T::vd_t,
                                         typename PIE_PROGRAM_T::md_t>>> {
  using vd_t = typename PIE_PROGRAM_T::vd_t;
  using md_t = typename PIE_PROGRAM_T::md_t;

  using wrapper_fragment_t = PythonPIEFragment<FRAG_T>;
  using wrapper_context_t = PythonPIEComputeContext<FRAG_T, vd_t, md_t>;

  using app_t = PythonPIEApp<FRAG_T, PIE_PROGRAM_T>;
  using pie_context_t = PIEContext<FRAG_T, wrapper_context_t>;

  INSTALL_AUTO_PROPERTY_WORKER(app_t, pie_context_t, FRAG_T)

 public:
  explicit PythonPIEApp(const PIE_PROGRAM_T& program) : program_(program) {}
  ~PythonPIEApp() {}

  virtual void PEval(const fragment_t& frag, pie_context_t& context) {
    fragment_.set_fragment(&frag);
    // call python function
    program_.Init(fragment_, context.compute_context_);

    context.compute_context_.inc_superstep();

    // call python function
    program_.PEval(fragment_, context.compute_context_);
  }

  virtual void IncEval(const fragment_t& graph, pie_context_t& context) {
    context.compute_context_.inc_superstep();

    // call python function
    program_.IncEval(fragment_, context.compute_context_);
  }

 private:
  PIE_PROGRAM_T program_;

  // python wrapper
  wrapper_fragment_t fragment_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PYTHON_PIE_PYTHON_PIE_APP_H_
