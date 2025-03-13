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

#ifndef EXAMPLES_ANALYTICAL_APPS_LCC_LCC_OPT_H_
#define EXAMPLES_ANALYTICAL_APPS_LCC_LCC_OPT_H_

#include <grape/grape.h>

#include <vector>

#include "grape/utils/varint.h"
#include "lcc/lcc_opt_context.h"

#ifdef USE_BMISS_STTNI_INTERSECT
#include <x86intrin.h>

static const uint8_t bmiss_sttni_bc_array[32] = {
    0,   1,   4,   5,   8,   9,   12,  13,  255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 0,   1,   4,   5,   8,   9,   12,  13,
};
static const __m128i* BMISS_BC_ORD =
    reinterpret_cast<const __m128i*>(bmiss_sttni_bc_array);

static const __m128i all_one_si128 =
    _mm_set_epi32(0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff);
#endif
#ifdef USE_SIMD_SORT
#include "x86-simd-sort/avx512-32bit-qsort.hpp"
#endif

namespace grape {

/**
 * @brief An implementation of LCC (Local CLustering Coefficient), the version
 * in LDBC, which only works on undirected graphs.
 *
 * This version of LCC inherits ParallelAppBase. Messages can be sent in
 * parallel to the evaluation. This strategy improve performance by overlapping
 * the communication time and the evaluation time.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T, typename COUNT_T = uint32_t, typename Enable = void>
class LCCOpt : public ParallelAppBase<FRAG_T, LCCOptContext<FRAG_T, COUNT_T>,
                                      ParallelMessageManagerOpt>,
               public ParallelEngine {
  using VecOutType = DeltaVarintEncoder<typename FRAG_T::vid_t>;
  using VecInType = DeltaVarintDecoder<typename FRAG_T::vid_t>;

 public:
  using fragment_t = FRAG_T;
  using context_t = LCCOptContext<FRAG_T, COUNT_T>;
  using message_manager_t = ParallelMessageManagerOpt;
  using worker_t = ParallelWorkerOpt<LCCOpt<FRAG_T, COUNT_T>>;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using count_t = COUNT_T;
  using tricnt_list_t = typename fragment_t::template vertex_array_t<count_t>;

  virtual ~LCCOpt() {}

  static std::shared_ptr<worker_t> CreateWorker(
      std::shared_ptr<LCCOpt<FRAG_T, COUNT_T>> app,
      std::shared_ptr<FRAG_T> frag) {
    return std::shared_ptr<worker_t>(new worker_t(app, frag));
  }

  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kOnlyOut;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();

    messages.InitChannels(thread_num());

    ctx.stage = 0;

    // Each vertex scatter its own out degree.
    ForEach(inner_vertices, [&messages, &frag, &ctx](int tid, vertex_t v) {
      ctx.global_degree[v] = frag.GetLocalOutDegree(v);
      messages.SendMsgThroughOEdges<fragment_t, int>(frag, v,
                                                     ctx.global_degree[v], tid);
    });

    // Just in case we are running on single process and no messages will
    // be send. ForceContinue() ensure the computation
    messages.ForceContinue();
  }

  count_t intersect_with_bs(const lcc_opt_impl::ref_vector<vertex_t>& small,
                            const lcc_opt_impl::ref_vector<vertex_t>& large,
                            tricnt_list_t& result) {
    count_t ret = 0;
    auto from = large.begin();
    auto to = large.end();
    for (auto v : small) {
      from = std::lower_bound(from, to, v);
      if (from == to) {
        return ret;
      }
      if (*from == v) {
        ++ret;
        ++from;
        atomic_add(result[v], static_cast<count_t>(1));
      }
    }
    return ret;
  }

  count_t intersect(const lcc_opt_impl::ref_vector<vertex_t>& lhs,
                    const lcc_opt_impl::ref_vector<vertex_t>& rhs,
                    tricnt_list_t& result) {
    if (lhs.empty() || rhs.empty()) {
      return 0;
    }
    vid_t v_size = lhs.size();
    vid_t u_size = rhs.size();
    if (static_cast<double>(v_size + u_size) <
        std::min<double>(v_size, u_size) *
            ilogb(std::max<double>(v_size, u_size))) {
      count_t count = 0;
      vid_t i = 0, j = 0;
      while (i < v_size && j < u_size) {
        if (lhs[i] == rhs[j]) {
          atomic_add(result[lhs[i]], static_cast<count_t>(1));
          ++count;
          ++i;
          ++j;
        } else if (lhs[i] < rhs[j]) {
          ++i;
        } else {
          ++j;
        }
      }
      return count;
    } else {
      if (v_size > u_size) {
        return intersect_with_bs(rhs, lhs, result);
      } else {
        return intersect_with_bs(lhs, rhs, result);
      }
    }
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    using vid_t = typename context_t::vid_t;

    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    if (ctx.stage == 0) {
      ctx.stage = 1;
      messages.ParallelProcess<fragment_t, int>(
          thread_num(), frag,
          [&ctx](int tid, vertex_t u, int msg) { ctx.global_degree[u] = msg; });
      std::vector<size_t> max_degrees(thread_num(), 0);
      ctx.memory_pools.resize(thread_num());
      ForEach(inner_vertices, [&frag, &ctx, &messages, &max_degrees](
                                  int tid, vertex_t v) {
        vid_t v_gid_hash = IdHasher<vid_t>::hash(frag.GetInnerVertexGid(v));
        auto& pool = ctx.memory_pools[tid];
        auto& nbr_vec = ctx.complete_neighbor[v];
        int degree = ctx.global_degree[v];
        auto es = frag.GetOutgoingAdjList(v);
        static thread_local VecOutType msg_vec;
        msg_vec.clear();
        pool.reserve(es.Size());
        for (auto& e : es) {
          auto u = e.get_neighbor();
          if (ctx.global_degree[u] > degree) {
            pool.push_back(u);
            msg_vec.push_back(frag.Vertex2Gid(u));
          } else if (ctx.global_degree[u] == degree) {
            vid_t u_gid = frag.Vertex2Gid(u);
            if (v_gid_hash > IdHasher<vid_t>::hash(u_gid)) {
              pool.push_back(u);
              msg_vec.push_back(u_gid);
            }
          }
        }
        nbr_vec = pool.finish();
        if (nbr_vec.empty()) {
          return;
        }
        std::sort(nbr_vec.begin(), nbr_vec.end());
        if (nbr_vec.size() > max_degrees[tid]) {
          max_degrees[tid] = nbr_vec.size();
        }
        messages.SendMsgThroughOEdges<fragment_t, VecOutType>(frag, v, msg_vec,
                                                              tid);
      });
      size_t max_degree = 0;
      for (auto x : max_degrees) {
        max_degree = std::max(x, max_degree);
      }
      ctx.degree_x = max_degree * 4 / 10;
      messages.ForceContinue();
    } else if (ctx.stage == 1) {
      ctx.stage = 2;
      messages.ParallelProcess<fragment_t, VecInType>(
          thread_num(), frag,
          [&frag, &ctx](int tid, vertex_t u, VecInType& msg) {
            auto& pool = ctx.memory_pools[tid];
            auto& nbr_vec = ctx.complete_neighbor[u];
            vid_t gid;
            pool.reserve(ctx.global_degree[u]);
            while (msg.pop(gid)) {
              vertex_t v;
              if (frag.Gid2Vertex(gid, v)) {
                pool.push_back(v);
              }
            }
            nbr_vec = pool.finish();
            std::sort(nbr_vec.begin(), nbr_vec.end());
          });
      std::vector<DenseVertexSet<typename FRAG_T::vertices_t>> vertexsets(
          thread_num());
      for (auto& vs : vertexsets) {
        vs.Init(frag.Vertices());
      }
      ForEach(inner_vertices, [this, &ctx, &vertexsets](int tid, vertex_t v) {
        auto& v0_nbr_vec = ctx.complete_neighbor[v];
        if (v0_nbr_vec.size() <= 1) {
          return;
        } else if (v0_nbr_vec.size() <= ctx.degree_x) {
          count_t v_count = 0;
          for (auto u : v0_nbr_vec) {
            auto& v1_nbr_vec = ctx.complete_neighbor[u];
            count_t u_count = intersect(v0_nbr_vec, v1_nbr_vec, ctx.tricnt);
            atomic_add(ctx.tricnt[u], u_count);
            v_count += u_count;
          }
          atomic_add(ctx.tricnt[v], v_count);
        } else {
          auto& v0_nbr_set = vertexsets[tid];
          for (auto u : v0_nbr_vec) {
            v0_nbr_set.Insert(u);
          }
          count_t v_count = 0;
          for (auto u : v0_nbr_vec) {
            count_t u_count = 0;
            auto& v1_nbr_vec = ctx.complete_neighbor[u];
            for (auto w : v1_nbr_vec) {
              if (v0_nbr_set.Exist(w)) {
                ++u_count;
                atomic_add(ctx.tricnt[w], static_cast<count_t>(1));
              }
            }
            v_count += u_count;
            atomic_add(ctx.tricnt[u], u_count);
          }
          atomic_add(ctx.tricnt[v], v_count);
          for (auto u : v0_nbr_vec) {
            v0_nbr_set.Erase(u);
          }
        }
      });
      ForEach(outer_vertices, [&messages, &frag, &ctx](int tid, vertex_t v) {
        if (ctx.tricnt[v] != 0) {
          messages.SyncStateOnOuterVertex<fragment_t, count_t>(
              frag, v, ctx.tricnt[v], tid);
        }
      });
      messages.ForceContinue();
    } else if (ctx.stage == 2) {
      ctx.stage = 3;
      messages.ParallelProcess<fragment_t, count_t>(
          thread_num(), frag, [&ctx](int tid, vertex_t u, count_t deg) {
            atomic_add(ctx.tricnt[u], deg);
          });

      // output result to context data
      auto& global_degree = ctx.global_degree;
      auto& tricnt = ctx.tricnt;
      auto& ctx_data = ctx.data();

      ForEach(inner_vertices, [&](int tid, vertex_t v) {
        if (global_degree[v] == 0 || global_degree[v] == 1) {
          ctx_data[v] = 0;
        } else {
          double re = 2.0 * (static_cast<int64_t>(tricnt[v])) /
                      (static_cast<int64_t>(global_degree[v]) *
                       (static_cast<int64_t>(global_degree[v]) - 1));
          ctx_data[v] = re;
        }
      });
    }
  }

  void EstimateMessageSize(const fragment_t& frag, size_t& send_size,
                           size_t& recv_size) {
    size_t avg_degree =
        frag.GetOutgoingEdgeNum() / frag.GetInnerVerticesNum() + 1;
    send_size = (avg_degree + 1) * sizeof(vid_t) * frag.OEDestsSize();
    recv_size = (avg_degree + 1) * frag.GetOuterVerticesNum() * sizeof(vid_t);
  }
};

#ifdef USE_BMISS_STTNI_INTERSECT

template <typename FRAG_T, typename COUNT_T>
class LCCOpt<FRAG_T, COUNT_T,
             typename std::enable_if<
                 std::is_same<typename FRAG_T::vid_t, uint32_t>::value>::type>
    : public ParallelAppBase<FRAG_T, LCCOptContext<FRAG_T, COUNT_T>,
                             ParallelMessageManagerOpt>,
      public ParallelEngine {
  using VecOutType = DeltaVarintEncoder<typename FRAG_T::vid_t>;
  using VecInType = DeltaVarintDecoder<typename FRAG_T::vid_t>;

 public:
  using fragment_t = FRAG_T;
  using context_t = LCCOptContext<FRAG_T, COUNT_T>;
  using message_manager_t = ParallelMessageManagerOpt;
  using worker_t = ParallelWorkerOpt<LCCOpt<FRAG_T, COUNT_T>>;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using count_t = COUNT_T;
  using tricnt_list_t = typename fragment_t::template vertex_array_t<count_t>;

  virtual ~LCCOpt() {}

  static std::shared_ptr<worker_t> CreateWorker(
      std::shared_ptr<LCCOpt<FRAG_T, COUNT_T>> app,
      std::shared_ptr<FRAG_T> frag) {
    return std::shared_ptr<worker_t>(new worker_t(app, frag));
  }

  static constexpr MessageStrategy message_strategy =
      MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr LoadStrategy load_strategy = LoadStrategy::kOnlyOut;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();

    messages.InitChannels(thread_num());

    ctx.stage = 0;

    // Each vertex scatter its own out degree.
    ForEach(inner_vertices, [&messages, &frag, &ctx](int tid, vertex_t v) {
      ctx.global_degree[v] = frag.GetLocalOutDegree(v);
      messages.SendMsgThroughOEdges<fragment_t, int>(frag, v,
                                                     ctx.global_degree[v], tid);
    });

    // Just in case we are running on single process and no messages will
    // be send. ForceContinue() ensure the computation
    messages.ForceContinue();
  }

  count_t intersect(lcc_opt_impl::ref_vector<vertex_t>& lhs,
                    lcc_opt_impl::ref_vector<vertex_t>& rhs,
                    tricnt_list_t& result) {
    vertex_t* list_a = lhs.data();
    int* set_a = reinterpret_cast<int*>(list_a);
    int size_a = lhs.size();
    vertex_t* list_b = rhs.data();
    int* set_b = reinterpret_cast<int*>(list_b);
    int size_b = rhs.size();

    if (size_a == 0 || size_b == 0) {
      return 0;
    }

    int min_size = size_a < size_b ? size_a : size_b;
    int max_size = size_a > size_b ? size_a : size_b;

    int size_c = 0;

    if (min_size * 32 < max_size) {
      if (size_a > size_b) {
        std::swap(list_a, list_b);
        std::swap(set_a, set_b);
        std::swap(size_a, size_b);
      }
      int i = 0, j = 0;
      int qs_b = size_b - (size_b & 3);
      for (i = 0; i < size_a; ++i) {
        int r = 1;
        while (j + (r << 2) < qs_b && set_a[i] > set_b[j + (r << 2) + 3]) {
          r <<= 1;
        }
        int upper = (j + (r << 2) < qs_b) ? (r) : ((qs_b - j - 4) >> 2);
        if (set_b[j + (upper << 2) + 3] < set_a[i]) {
          break;
        }
        int lower = (r >> 1);
        while (lower < upper) {
          int mid = (lower + upper) >> 1;
          if (set_b[j + (mid << 2) + 3] < set_a[i]) {
            lower = mid + 1;
          } else {
            upper = mid;
          }
        }
        j += (lower << 2);

        __m128i v_a = _mm_set_epi32(set_a[i], set_a[i], set_a[i], set_a[i]);
        __m128i v_b = _mm_lddqu_si128(reinterpret_cast<__m128i*>(set_b + j));
        __m128i cmp_mask = _mm_cmpeq_epi32(v_a, v_b);
        int mask = _mm_movemask_ps((__m128) cmp_mask);
        if (mask != 0) {
          atomic_add(result[list_a[i]], static_cast<count_t>(1));
          ++size_c;
        }
      }
      while (i < size_a && j < size_b) {
        if (set_a[i] == set_b[j]) {
          atomic_add(result[list_a[i]], static_cast<count_t>(1));
          ++size_c;
          ++i;
          ++j;
        } else if (set_a[i] < set_b[j]) {
          ++i;
        } else {
          ++j;
        }
      }
    } else {
      int i = 0, j = 0;
      int qs_a = size_a - (size_a & 7);
      int qs_b = size_b - (size_b & 7);

      while (i < qs_a && j < qs_b) {
        __m128i v_a0 = _mm_load_si128(reinterpret_cast<__m128i*>(set_a + i));
        __m128i v_a1 =
            _mm_load_si128(reinterpret_cast<__m128i*>(set_a + i + 4));
        __m128i v_b0 = _mm_load_si128(reinterpret_cast<__m128i*>(set_b + j));
        __m128i v_b1 =
            _mm_load_si128(reinterpret_cast<__m128i*>(set_b + j + 4));

        // byte-wise check by STTNI:
        __m128i byte_group_a0 = _mm_shuffle_epi8(v_a0, BMISS_BC_ORD[0]);
        __m128i byte_group_a1 = _mm_shuffle_epi8(v_a1, BMISS_BC_ORD[1]);
        __m128i byte_group_a = _mm_or_si128(byte_group_a0, byte_group_a1);
        __m128i byte_group_b0 = _mm_shuffle_epi8(v_b0, BMISS_BC_ORD[0]);
        __m128i byte_group_b1 = _mm_shuffle_epi8(v_b1, BMISS_BC_ORD[1]);
        __m128i byte_group_b = _mm_or_si128(byte_group_b0, byte_group_b1);

        __m128i bc_mask = _mm_cmpestrm(
            byte_group_b, 8, byte_group_a, 8,
            _SIDD_UWORD_OPS | _SIDD_CMP_EQUAL_ANY | _SIDD_BIT_MASK);
        int r = _mm_extract_epi32(bc_mask, 0);

        // word-wise check:
        while (r) {
          int p = _mm_popcnt_u32((~r) & (r - 1));
          r &= (r - 1);
          __m128i wc_a = _mm_set_epi32(set_a[i + p], set_a[i + p], set_a[i + p],
                                       set_a[i + p]);
          if (!_mm_test_all_zeros(_mm_cmpeq_epi32(wc_a, v_b0), all_one_si128) ||
              !_mm_test_all_zeros(_mm_cmpeq_epi32(wc_a, v_b1), all_one_si128)) {
            atomic_add(result[list_a[i + p]], static_cast<count_t>(1));
            ++size_c;
          }
        }

        if (set_a[i + 7] == set_b[j + 7]) {
          i += 8;
          j += 8;
        } else if (set_a[i + 7] < set_b[j + 7]) {
          i += 8;
        } else {
          j += 8;
        }
      }

      while (i < size_a && j < size_b) {
        if (set_a[i] == set_b[j]) {
          atomic_add(result[list_a[i]], static_cast<count_t>(1));
          ++size_c;
          i++;
          j++;
        } else if (set_a[i] < set_b[j]) {
          i++;
        } else {
          j++;
        }
      }
    }

    return size_c;
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    auto outer_vertices = frag.OuterVertices();

    if (ctx.stage == 0) {
      ctx.stage = 1;
      messages.ParallelProcess<fragment_t, int>(
          thread_num(), frag,
          [&ctx](int tid, vertex_t u, int msg) { ctx.global_degree[u] = msg; });
      ctx.memory_pools.resize(thread_num());
      ForEach(inner_vertices, [&frag, &ctx, &messages](int tid, vertex_t v) {
        vid_t v_gid_hash = IdHasher<vid_t>::hash(frag.GetInnerVertexGid(v));
        auto& pool = ctx.memory_pools[tid];
        auto& nbr_vec = ctx.complete_neighbor[v];
        int degree = ctx.global_degree[v];
        auto es = frag.GetOutgoingAdjList(v);
        static thread_local VecOutType msg_vec;
        msg_vec.clear();
        pool.reserve(es.Size());
        for (auto& e : es) {
          auto u = e.get_neighbor();
          if (ctx.global_degree[u] > degree) {
            pool.push_back(u);
            msg_vec.push_back(frag.Vertex2Gid(u));
          } else if (ctx.global_degree[u] == degree) {
            vid_t u_gid = frag.Vertex2Gid(u);
            if (v_gid_hash > IdHasher<vid_t>::hash(u_gid)) {
              pool.push_back(u);
              msg_vec.push_back(u_gid);
            }
          }
        }
        nbr_vec = pool.finish();
        if (nbr_vec.empty()) {
          return;
        }
        int* nbr_ptr = reinterpret_cast<int*>(nbr_vec.data());
#ifdef USE_SIMD_SORT
        avx512_qsort(nbr_ptr, nbr_vec.size());
#else
        std::sort(nbr_ptr, nbr_ptr + nbr_vec.size());
#endif
        messages.SendMsgThroughOEdges<fragment_t, VecOutType>(frag, v, msg_vec,
                                                              tid);
      });
      messages.ForceContinue();
    } else if (ctx.stage == 1) {
      ctx.stage = 2;
      messages.ParallelProcess<fragment_t, VecInType>(
          thread_num(), frag,
          [&frag, &ctx](int tid, vertex_t u, VecInType& msg) {
            auto& pool = ctx.memory_pools[tid];
            auto& nbr_vec = ctx.complete_neighbor[u];
            vid_t gid;
            pool.reserve(ctx.global_degree[u]);
            while (msg.pop(gid)) {
              vertex_t v;
              if (frag.Gid2Vertex(gid, v)) {
                pool.push_back(v);
              }
            }
            nbr_vec = pool.finish();
            int* nbr_ptr = reinterpret_cast<int*>(nbr_vec.data());
#ifdef USE_SIMD_SORT
            avx512_qsort(nbr_ptr, nbr_vec.size());
#else
            std::sort(nbr_ptr, nbr_ptr + nbr_vec.size());
#endif
          });

      ForEach(inner_vertices, [this, &ctx](int tid, vertex_t v) {
        auto& v0_nbr_vec = ctx.complete_neighbor[v];
        if (v0_nbr_vec.size() <= 1) {
          return;
        } else {
          count_t v_count = 0;
          for (auto u : v0_nbr_vec) {
            auto& v1_nbr_vec = ctx.complete_neighbor[u];
            count_t u_count = intersect(v0_nbr_vec, v1_nbr_vec, ctx.tricnt);
            atomic_add(ctx.tricnt[u], u_count);
            v_count += u_count;
          }
          atomic_add(ctx.tricnt[v], v_count);
        }
      });
      ForEach(outer_vertices, [&messages, &frag, &ctx](int tid, vertex_t v) {
        if (ctx.tricnt[v] != 0) {
          messages.SyncStateOnOuterVertex<fragment_t, count_t>(
              frag, v, ctx.tricnt[v], tid);
        }
      });
      messages.ForceContinue();
    } else if (ctx.stage == 2) {
      ctx.stage = 3;
      messages.ParallelProcess<fragment_t, count_t>(
          thread_num(), frag, [&ctx](int tid, vertex_t u, count_t deg) {
            atomic_add(ctx.tricnt[u], deg);
          });

      // output result to context data
      auto& global_degree = ctx.global_degree;
      auto& tricnt = ctx.tricnt;
      auto& ctx_data = ctx.data();

      ForEach(inner_vertices, [&](int tid, vertex_t v) {
        if (global_degree[v] == 0 || global_degree[v] == 1) {
          ctx_data[v] = 0;
        } else {
          double re = 2.0 * (static_cast<int64_t>(tricnt[v])) /
                      (static_cast<int64_t>(global_degree[v]) *
                       (static_cast<int64_t>(global_degree[v]) - 1));
          ctx_data[v] = re;
        }
      });
    }
  }

  void EstimateMessageSize(const fragment_t& frag, size_t& send_size,
                           size_t& recv_size) {
    size_t avg_degree =
        frag.GetOutgoingEdgeNum() / frag.GetInnerVerticesNum() + 1;
    send_size = (avg_degree + 1) * sizeof(vid_t) * frag.OEDestsSize();
    recv_size = (avg_degree + 1) * frag.GetOuterVerticesNum() * sizeof(vid_t);
  }
};

#endif

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_LCC_LCC_OPT_H_
