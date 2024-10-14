/** Copyright 2022 Alibaba Group Holding Limited.

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

/**
 * @file flash_utils.h
 *
 * Some values, types and util functions used in the Flash programming model.
 */

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_FLASH_UTILS_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_FLASH_UTILS_H_

#include <algorithm>
#include <vector>

namespace gs {

#define EPS 1e-10
#define EU -1
#define ED -2
#define ER -3
#define THRESHOLD (VSize(All) / 50)

#define VSet VertexSubset<fragment_t, value_t>
#define All fw->all_
#define GetMax(A, B) fw->Max(A, B)
#define GetMin(A, B) fw->Min(A, B)
#define GetSum(A, B) fw->Sum(A, B)

#define GetV(id) fw->Get(id)
#define OutDeg(id) getOutDegree<fragment_t, vid_t>(graph, fw->Key2Lid(id))
#define InDeg(id) getInDegree<fragment_t, vid_t>(graph, fw->Key2Lid(id))
#define Deg(id) (OutDeg(id) + InDeg(id))
#define VDATA(id) getVData<fragment_t, vid_t>(graph, fw->Key2Lid(id))

#define for_in(...)                                     \
  {                                                     \
    vertex_t u;                                         \
    u.SetValue(fw->Key2Lid(id));                        \
    auto es = graph.GetIncomingAdjList(u);              \
    for (auto& e : es) {                                \
      vid_t nb_id = graph.Vertex2Gid(e.get_neighbor()); \
      edata_t weight = e.get_data();                    \
      nb_id = fw->Gid2Key(nb_id);                       \
      value_t nb = *(fw->Get(nb_id));                   \
      __VA_ARGS__                                       \
    }                                                   \
  }
#define for_out(...)                                    \
  {                                                     \
    vertex_t u;                                         \
    u.SetValue(fw->Key2Lid(id));                        \
    auto es = graph.GetOutgoingAdjList(u);              \
    for (auto& e : es) {                                \
      vid_t nb_id = graph.Vertex2Gid(e.get_neighbor()); \
      edata_t weight = e.get_data();                    \
      nb_id = fw->Gid2Key(nb_id);                       \
      value_t nb = *(fw->Get(nb_id));                   \
      __VA_ARGS__                                       \
    }                                                   \
  }
#define for_nb(...) \
  { for_in(__VA_ARGS__) for_out(__VA_ARGS__) }
#define for_i(...)                \
  for (int i = 0; i < len; ++i) { \
    __VA_ARGS__;                  \
  }
#define for_j(...)                \
  for (int j = 0; j < len; ++j) { \
    __VA_ARGS__;                  \
  }
#define for_k(...)                \
  for (int k = 0; k < len; ++k) { \
    __VA_ARGS__;                  \
  }

#define ToDense(U) toDenseFunction(U, fw)
#define Oid2FlashId(source) oid2FlashIdFunction(graph, fw, source)
#define VSize(U) vSizeFunction(U, fw)
#define VertexMap(U, f, ...) vertexMapFunction(graph, fw, U, f, ##__VA_ARGS__)
#define VertexMapSeq(U, f, ...) \
  vertexMapSeqFunction(graph, fw, U, f, ##__VA_ARGS__)
#define EdgeMap(U, H, F, M, C, ...) \
  edgeMapFunction(graph, fw, U, H, F, M, C, ##__VA_ARGS__)
#define EdgeMapDense(U, H, F, M, C, ...) \
  edgeMapDenseFunction(graph, fw, U, H, F, M, C, ##__VA_ARGS__)
#define EdgeMapSparse(U, H, F, M, C, ...) \
  edgeMapSparseFunction(graph, fw, U, H, F, M, C, ##__VA_ARGS__)

#define DefineFV(F) auto F = [&](const vid_t id, const value_t& v) -> bool
#define DefineMapV(F) auto F = [&](const vid_t id, value_t& v)
#define DefineFE(F)                                                \
  auto F = [&](const vid_t sid, const vid_t did, const value_t& s, \
               const value_t& d, const edata_t& weight) -> bool
#define DefineMapE(F)                                                          \
  auto F = [&](const vid_t sid, const vid_t did, const value_t& s, value_t& d, \
               const edata_t& weight)
#define CTrueV cTrueV<vid_t, value_t>
#define CTrueE cTrueE<vid_t, value_t, edata_t>

#define EjoinV(E, V) E, V, true
#define VjoinP(property)     \
  std::vector<vid_t> res;    \
  res.clear();               \
  res.push_back(v.property); \
  return res;
#define DefineOutEdges(F) \
  auto F = [&](const vid_t vid, const value_t& v) -> std::vector<vid_t>
#define DefineInEdges(F) \
  auto F = [&](const vid_t vid, const value_t& v) -> std::vector<vid_t>
#define use_edge(F) F(vid, v)

#define Block(F) blockFunction(graph, fw, FUNC_BLOCK(F))
#define FUNC_BLOCK(F) [&]() { F; }
#define Traverse(...)                       \
  {                                         \
    for (int id = 0; id < n_vertex; id++) { \
      value_t v = *(fw->Get(id));           \
      __VA_ARGS__                           \
    }                                       \
  }
#define TraverseLocal(...)        \
  {                               \
    for (auto& id : All.s) {      \
      value_t v = *(fw->Get(id)); \
      __VA_ARGS__                 \
    }                             \
  }
#define Print(...)       \
  if (fw->GetPid() == 0) \
  printf(__VA_ARGS__)

template <class T>
void reduce_vec(std::vector<T>& src, std::vector<T>& rst,
                void (*f)(void* la, void* lb, int* len, MPI_Datatype* dtype),
                bool bcast) {
  int id;
  MPI_Comm_rank(MPI_COMM_WORLD, &id);
  bool is_master = (id == 0);
  MPI_Datatype type;
  MPI_Type_contiguous(sizeof(src[0]) * src.size() + sizeof(int), MPI_CHAR,
                      &type);
  MPI_Type_commit(&type);
  MPI_Op op;
  MPI_Op_create(f, 1, &op);

  char* tmp_in = new char[sizeof(src[0]) * src.size() + sizeof(int)];
  int len = static_cast<int>(src.size());
  memcpy(tmp_in, &len, sizeof(int));
  memcpy(tmp_in + sizeof(int), src.data(), sizeof(src[0]) * src.size());

  char* tmp_out =
      is_master ? new char[sizeof(src[0]) * src.size() + sizeof(int)] : NULL;

  MPI_Reduce(tmp_in, tmp_out, 1, type, op, 0, MPI_COMM_WORLD);
  MPI_Op_free(&op);

  delete[] tmp_in;
  if (is_master) {
    rst.resize(len);
    memcpy(rst.data(), tmp_out + sizeof(int), sizeof(src[0]) * src.size());
    delete[] tmp_out;
  }

  if (bcast) {
    if (!is_master)
      rst.resize(src.size());
    MPI_Bcast(rst.data(), sizeof(rst[0]) * rst.size(), MPI_CHAR, 0,
              MPI_COMM_WORLD);
  }
}

#define ReduceVec3(src, rst, F)                                   \
  reduce_vec(                                                     \
      src, rst,                                                   \
      [](void* la, void* lb, int* lens, MPI_Datatype* dtype) {    \
        using T = decltype(src.data());                           \
        int len;                                                  \
        memcpy(&len, la, sizeof(int));                            \
        memcpy(lb, &len, sizeof(int));                            \
        T src = (T)((reinterpret_cast<char*>(la)) + sizeof(int)); \
        T rst = (T)((reinterpret_cast<char*>(lb)) + sizeof(int)); \
        F;                                                        \
      },                                                          \
      true);

#define ReduceVec4(src, rst, F, bcast)                            \
  reduce_vec(                                                     \
      src, rst,                                                   \
      [](void* la, void* lb, int* lens, MPI_Datatype* dtype) {    \
        using T = decltype(src.data());                           \
        int len;                                                  \
        memcpy(&len, la, sizeof(int));                            \
        memcpy(lb, &len, sizeof(int));                            \
        T src = (T)((reinterpret_cast<char*>(la)) + sizeof(int)); \
        T rst = (T)((reinterpret_cast<char*>(lb)) + sizeof(int)); \
        F;                                                        \
      },                                                          \
      bcast);

#define GetReduceVec(_1, _2, _3, _4, NAME, ...) NAME
#define ReduceVec(...) \
  GetReduceVec(__VA_ARGS__, ReduceVec4, ReduceVec3, _2, _1, ...)(__VA_ARGS__)
#define Reduce ReduceVec

template <class T>
int set_intersect(const std::vector<T>& x, const std::vector<T>& y,
                  std::vector<T>& v) {
  auto it = set_intersection(x.begin(), x.end(), y.begin(), y.end(), v.begin());
  return it - v.begin();
}

template <class T1, class T2>
void add(std::vector<T1>& x, std::vector<T1>& y, T2 c) {
  for (size_t i = 0; i < x.size(); ++i)
    x[i] += y[i] * c;
}

template <class T>
void add(std::vector<T>& x, std::vector<T>& y) {
  for (size_t i = 0; i < x.size(); ++i)
    x[i] += y[i];
}

template <class T>
T prod(std::vector<T>& x, std::vector<T>& y) {
  T s = 0;
  for (size_t i = 0; i < x.size(); ++i) {
    s += x[i] * y[i];
  }
  return s;
}

template <class T1, class T2>
void mult(std::vector<T1>& v, T2 c) {
  for (size_t i = 0; i < v.size(); ++i)
    v[i] *= c;
}

template <class T>
bool find(const std::vector<T>& vec, const T& val) {
  return find(vec.begin(), vec.end(), val) != vec.end();
}

template <class T>
int locate(const std::vector<T>& vec, const T& val) {
  return find(vec.begin(), vec.end(), val) - vec.begin();
}

class union_find : public std::vector<int> {
 public:
  explicit union_find(int n) {
    resize(n);
    for (int i = 0; i < n; ++i)
      (*this)[i] = i;
  }

 public:
  union_find() {}
};

int get_f(int* f, int v) {
  if (f[v] != v)
    f[v] = get_f(f, f[v]);
  return f[v];
}

void union_f(int* f, int a, int b) {
  int fa = get_f(f, a);
  int fb = get_f(f, b);
  f[fa] = fb;
}

int get_f(std::vector<int>& f, int v) {
  if (f[v] != v)
    f[v] = get_f(f, f[v]);
  return f[v];
}

void union_f(std::vector<int>& f, int a, int b) {
  int fa = get_f(f, a);
  int fb = get_f(f, b);
  f[fa] = fb;
}

template <typename E>
void kruskal(std::vector<E>& edges, E* mst, int n) {
  union_find f(n);
  memset(mst, 0, sizeof(E) * (n - 1));
  sort(edges.begin(), edges.end());
  for (int i = 0, p = 0; i < static_cast<int>(edges.size()) && p < n - 1; ++i) {
    int a = get_f(f, edges[i].second.first),
        b = get_f(f, edges[i].second.second);
    if (a != b) {
      union_f(f, a, b);
      mst[p++] = edges[i];
    }
  }
}

template <typename vid_t, typename value_t>
inline bool cTrueV(const vid_t id, const value_t& v) {
  return true;
}

template <typename vid_t, typename value_t, typename edata_t>
inline bool cTrueE(const vid_t sid, const vid_t did, const value_t& s,
                   const value_t& d, const edata_t& weight) {
  return true;
}

template <typename fragment_t, typename vid_t>
inline int getOutDegree(const fragment_t& graph, vid_t lid) {
  grape::Vertex<vid_t> v;
  v.SetValue(lid);
  return graph.GetLocalOutDegree(v);
}

template <typename fragment_t, typename vid_t>
inline int getInDegree(const fragment_t& graph, vid_t lid) {
  grape::Vertex<vid_t> v;
  v.SetValue(lid);
  return graph.GetLocalInDegree(v);
}

template <typename fragment_t, typename vid_t>
inline const typename fragment_t::vdata_t& getVData(const fragment_t& graph,
                                                    vid_t lid) {
  grape::Vertex<vid_t> v;
  v.SetValue(lid);
  return graph.GetData(v);
}

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_FLASH_UTILS_H_
