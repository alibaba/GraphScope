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

#ifndef EXAMPLES_ANALYTICAL_APPS_KCLIQUE_KCLIQUE_CONTEXT_H_
#define EXAMPLES_ANALYTICAL_APPS_KCLIQUE_KCLIQUE_CONTEXT_H_

namespace grape {

template <typename FRAG_T>
class KCliqueContext : public VoidContext<FRAG_T> {
 public:
  explicit KCliqueContext(const FRAG_T& frag) : VoidContext<FRAG_T>(frag) {}

  void Init(ParallelMessageManagerOpt& messages, int k_) { k = k_; }

  void Output(std::ostream& os) override {
    os << clique_num << std::endl;
    LOG(INFO) << "[frag-" << this->fragment().fid()
              << "] clique_num = " << clique_num;
  }

  int k;
  size_t clique_num;
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_KCLIQUE_KCLIQUE_CONTEXT_H_
