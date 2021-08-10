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

Author: Ning Xin
*/

#ifndef ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_UTILS_H_
#define ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_UTILS_H_

#include <unordered_map>
#include <vector>

namespace gs {
enum class DegreeType { IN, OUT, INANDOUT };

template <typename degree_t>
double Variance(std::vector<double>& vec,
                std::unordered_map<int, degree_t>& map) {
  double sum1 = 0.0, sum2 = 0.0;
  int n = vec.size();
  for (int i = 0; i < n; i++) {
    sum1 += map[i] * map[i] * vec[i];
    sum2 += map[i] * vec[i];
  }
  return sqrt(sum1 - sum2 * sum2);
}

template <typename degree_t>
double ProcessMatrix(std::vector<std::vector<double>>& degree_mixing_matrix,
                     std::unordered_map<int, degree_t>& map) {
  int n = degree_mixing_matrix.size();
  std::vector<double> a;
  // sum of column
  for (auto& row : degree_mixing_matrix) {
    a.emplace_back(accumulate(row.begin(), row.end(), 0.0));
  }
  std::vector<double> b;
  // sum of row
  for (int i = 0; i < n; i++) {
    double sum = 0.0;
    for (int j = 0; j < n; j++) {
      sum += degree_mixing_matrix[j][i];
    }
    b.emplace_back(sum);
  }
  double sum = 0.0;
  for (int i = 0; i < n; i++) {
    for (int j = 0; j < n; j++) {
      sum += map[i] * map[j] * (degree_mixing_matrix[i][j] - a[i] * b[j]);
    }
  }
  double vara = Variance(a, map);
  double varb = Variance(b, map);
  return sum / (vara * varb);
}

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_UTILS_H_
