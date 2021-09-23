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

/**
 * @brief Compute the variance of matrix vec.
 *
 * @param vec
 * @param map
 *
 * @tparam T data type(e.g., int, float, double)
 */
template <typename T>
double Variance(std::vector<T>& vec, std::unordered_map<int, T>& map) {
  T sum1 = 0, sum2 = 0;
  int n = vec.size();
  for (int i = 0; i < n; i++) {
    sum1 += map[i] * map[i] * vec[i];
    sum2 += map[i] * vec[i];
  }
  return sqrt(static_cast<double>(sum1 - sum2 * sum2));
}

/**
 * @brief Process matrix for degree assortativity and numeric assortativity app.
 *
 * @param degree_mixing_matrix n x n matrix
 * @param map
 *
 * @tparam T data type(e.g., int, float, double)
 */
template <typename T>
double ProcessMatrix(std::vector<std::vector<T>>& degree_mixing_matrix,
                     std::unordered_map<int, T>& map) {
  int n = degree_mixing_matrix.size();
  // sum of column
  std::vector<T> a;
  // sum of row
  std::vector<T> b;
  for (int i = 0; i < n; i++) {
    T sum_row = 0, sum_column = 0;
    for (int j = 0; j < n; j++) {
      sum_row += degree_mixing_matrix[j][i];
      sum_column += degree_mixing_matrix[i][j];
    }
    a.emplace_back(sum_column);
    b.emplace_back(sum_row);
  }
  T sum = 0;
  for (int i = 0; i < n; i++) {
    for (int j = 0; j < n; j++) {
      sum += map[i] * map[j] * (degree_mixing_matrix[i][j] - a[i] * b[j]);
    }
  }
  double vara = Variance(a, map);
  double varb = Variance(b, map);
  return static_cast<double>(sum) / (vara * varb);
}

/**
 * @brief deterimine if type T can convert to type U in compile-time.
 *
 * @tparam T
 * @tparam U
 *
 */
template <typename T, typename U>
class Conversion {
 private:
  static char Test(U);
  static int Test(...);
  static T MakeT();

 public:
  enum { exists = sizeof(Test(MakeT())) == sizeof(char) };
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_UTILS_H_
