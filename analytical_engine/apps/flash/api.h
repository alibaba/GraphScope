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
 * @file api.h
 *
 * The APIs for the Flash programming model.
 */

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_API_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_API_H_

#include <memory>

#include "flash/flash_utils.h"
#include "flash/flash_ware.h"

namespace gs {

#define UseIncomingEdgesToUpdate                \
  auto es = graph.GetIncomingAdjList(u);        \
  for (auto& e : es) {                          \
    nb_id = graph.Vertex2Gid(e.get_neighbor()); \
    nb_id = fw->Gid2Key(nb_id);                 \
    if (flag || U.IsIn(nb_id)) {                \
      nb = *(fw->Get(nb_id));                   \
      if (f(nb_id, vid, nb, v, e.get_data())) { \
        m(nb_id, vid, nb, v, e.get_data());     \
        is_update = true;                       \
        if (!c(vid, v))                         \
          break;                                \
      }                                         \
    }                                           \
  }

#define UseOutgoingEdgesToUpdate                \
  auto es = graph.GetOutgoingAdjList(u);        \
  for (auto& e : es) {                          \
    nb_id = graph.Vertex2Gid(e.get_neighbor()); \
    nb_id = fw->Gid2Key(nb_id);                 \
    if (flag || U.IsIn(nb_id)) {                \
      nb = *(fw->Get(nb_id));                   \
      if (f(nb_id, vid, nb, v, e.get_data())) { \
        m(nb_id, vid, nb, v, e.get_data());     \
        is_update = true;                       \
        if (!c(vid, v))                         \
          break;                                \
      }                                         \
    }                                           \
  }

template <typename fragment_t, typename fw_t, class value_t>
void toDenseFunction(VSet& U, const std::shared_ptr<fw_t> fw) {
  if (U.is_dense)
    return;
  U.is_dense = true;
  if (U.d.get_size() == 0)
    U.d.init(fw->GetSize());
  else
    U.d.parallel_clear(fw->GetThreadPool());
  fw->ForEach(U.s.begin(), U.s.end(),
              [&U](int tid, int key) { U.d.set_bit(key); });
  fw->SyncBitset(U.d);
}

template <typename fragment_t, typename fw_t>
typename fragment_t::vid_t oid2FlashIdFunction(
    const fragment_t& graph, const std::shared_ptr<fw_t> fw,
    const typename fragment_t::oid_t& source) {
  typename fragment_t::vid_t gid;
  graph.GetVertexMap()->GetGid(source, gid);
  return fw->Gid2Key(gid);
}

template <typename fragment_t, typename fw_t, typename value_t>
int vSizeFunction(VSet& U, const std::shared_ptr<fw_t> fw) {
  int cnt_local = U.size(), cnt;
  fw->Sum(cnt_local, cnt);
  return cnt;
}

template <typename fragment_t, typename fw_t, typename value_t, class F>
VSet vertexMapFunction(const fragment_t& graph, const std::shared_ptr<fw_t> fw,
                       VSet& U, F& f) {
  VSet res;
  for (auto& key : U.s) {
    if (f(key, *(fw->Get(key))))
      res.s.push_back(key);
  }
  return res;
}

template <typename fragment_t, typename fw_t, typename value_t, class F,
          class M>
VSet vertexMapFunction(const fragment_t& graph, const std::shared_ptr<fw_t> fw,
                       VSet& U, F& f, M& m, bool b = true) {
  fw->ForEach(U.s.begin(), U.s.end(),
              [&fw, &U, &f, &m, &b](int tid, typename fragment_t::vid_t key) {
                value_t v = *(fw->Get(key));
                if (!f(key, v))
                  return;
                m(key, v);
                fw->PutNextPull(key, v, b, tid);
              });
  VSet res;
  fw->Barrier();
  fw->GetActiveVerticesAndSetStates(res.s);
  return res;
}

template <typename fragment_t, typename fw_t, typename value_t, class F,
          class M>
VSet vertexMapSeqFunction(const fragment_t& graph,
                          const std::shared_ptr<fw_t> fw, VSet& U, F& f, M& m,
                          bool b = true) {
  for (auto& key : U.s) {
    value_t v = *(fw->Get(key));
    if (f(key, v)) {
      m(key, v);
      fw->PutNextPull(key, v, b);
    }
  }
  VSet res;
  fw->Barrier();
  fw->GetActiveVerticesAndSetStates(res.s);
  return res;
}

template <typename fragment_t, typename fw_t, typename value_t, class F,
          class M, class C>
VSet edgeMapDenseFunction(const fragment_t& graph,
                          const std::shared_ptr<fw_t> fw, VSet& U, int h,
                          VSet& T, bool is_join, F& f, M& m, C& c,
                          bool b = true) {
  if ((&T) == (&All))
    return edgeMapDenseFunction(graph, fw, U, h, f, m, c, b);
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  bool flag = ((&U) == (&All));
  if (!flag)
    ToDense(U);

  fw->ForEach(T.s.begin(), T.s.end(),
              [&flag, &graph, &fw, &U, &h, &f, &m, &c, &b](int tid, vid_t vid) {
                vertex_t u;
                u.SetValue(fw->Key2Lid(vid));
                value_t v = *(fw->Get(vid));
                bool is_update = false;
                if (!c(vid, v))
                  return;
                vid_t nb_id;
                value_t nb;
                if (h == EU || h == ED) {
                  UseIncomingEdgesToUpdate
                }
                if (h == EU || h == ER) {
                  UseOutgoingEdgesToUpdate
                }
                if (is_update)
                  fw->PutNextPull(vid, v, b, tid);
              });

  VSet res;
  fw->Barrier();
  res.is_dense = true;
  fw->GetActiveVerticesAndSetStates(res.s, res.d);
  return res;
}

template <typename fragment_t, typename fw_t, typename value_t, class F,
          class M, class C>
VSet edgeMapDenseFunction(const fragment_t& graph,
                          const std::shared_ptr<fw_t> fw, VSet& U, int h, F& f,
                          M& m, C& c, bool b = true) {
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  bool flag = ((&U) == (&All));
  if (!flag)
    ToDense(U);

  fw->ForEach(graph.InnerVertices(), [&flag, &graph, &fw, &U, &h, &f, &m, &c,
                                      &b](int tid, vertex_t u) {
    vid_t vid = fw->Lid2Key(u.GetValue());
    value_t v = *(fw->Get(vid));
    bool is_update = false;
    if (!c(vid, v))
      return;
    vid_t nb_id;
    value_t nb;
    if (h == EU || h == ED) {
      UseIncomingEdgesToUpdate
    }
    if (h == EU || h == ER) {
      UseOutgoingEdgesToUpdate
    }
    if (is_update)
      fw->PutNextPull(vid, v, b, tid);
  });

  VSet res;
  fw->Barrier();
  res.is_dense = true;
  fw->GetActiveVerticesAndSetStates(res.s, res.d);
  return res;
}

template <typename fragment_t, typename fw_t, typename value_t, class F,
          class M, class C, class H>
VSet edgeMapDenseFunction(const fragment_t& graph,
                          const std::shared_ptr<fw_t> fw, VSet& U, H& h,
                          VSet& T, bool is_join, F& f, M& m, C& c,
                          bool b = true) {
  using vid_t = typename fragment_t::vid_t;
  using edata_t = typename fragment_t::edata_t;
  bool flag = ((&U) == (&All));
  if (!flag)
    ToDense(U);

  fw->ForEach(T.s.begin(), T.s.end(),
              [&fw, &flag, &U, &h, &f, &m, &c, &b](int tid, vid_t vid) {
                value_t v = *(fw->Get(vid));
                bool is_update = false;
                if (!c(vid, v))
                  return;
                value_t nb;
                auto es = h(vid, v);
                for (auto& nb_id : es) {
                  if (flag || U.IsIn(nb_id)) {
                    nb = *(fw->Get(nb_id));
                    if (f(nb_id, vid, nb, v, edata_t())) {
                      m(nb_id, vid, nb, v, edata_t());
                      is_update = true;
                      if (!c(vid, v))
                        break;
                    }
                  }
                }
                if (is_update)
                  fw->PutNextPull(vid, v, b, tid);
              });

  VSet res;
  fw->Barrier();
  res.is_dense = true;
  fw->GetActiveVerticesAndSetStates(res.s, res.d);
  return res;
}

template <typename fragment_t, typename fw_t, typename value_t, class F,
          class M, class C, class H>
VSet edgeMapDenseFunction(const fragment_t& graph,
                          const std::shared_ptr<fw_t> fw, VSet& U, H& h, F& f,
                          M& m, C& c, bool b = true) {
  return edgeMapDenseFunction(graph, fw, U, h, All, f, m, c, b);
}

template <typename fragment_t, typename fw_t, typename value_t, class F,
          class M, class C>
VSet doEdgeMapSparse(const fragment_t& graph, const std::shared_ptr<fw_t> fw,
                     VSet& U, int h, F& f, M& m, C& c) {
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;

  for (auto& vid : U.s) {
    vertex_t u;
    u.SetValue(fw->Key2Lid(vid));
    value_t v = *(fw->Get(vid));
    vid_t nb_id;
    value_t nb;
    if (h == EU || h == ED) {
      auto es = graph.GetOutgoingAdjList(u);
      for (auto& e : es) {
        nb_id = graph.Vertex2Gid(e.get_neighbor());
        nb_id = fw->Gid2Key(nb_id);
        nb = *(fw->Get(nb_id));
        if (c(nb_id, nb) && f(vid, nb_id, v, nb, e.get_data())) {
          m(vid, nb_id, v, nb, e.get_data());
          fw->PutNext(nb_id, nb);
        }
      }
    }
    if (h == EU || h == ER) {
      auto es = graph.GetIncomingAdjList(u);
      for (auto& e : es) {
        nb_id = graph.Vertex2Gid(e.get_neighbor());
        nb_id = fw->Gid2Key(nb_id);
        nb = *(fw->Get(nb_id));
        if (c(nb_id, nb) && f(vid, nb_id, v, nb, e.get_data())) {
          m(vid, nb_id, v, nb, e.get_data());
          fw->PutNext(nb_id, nb);
        }
      }
    }
  }

  VSet res;
  fw->Barrier(true);
  fw->GetActiveVerticesAndSetStates(res.s);
  return res;
}

template <typename fragment_t, typename fw_t, typename value_t, class F,
          class M, class C, class H>
VSet doEdgeMapSparse(const fragment_t& graph, const std::shared_ptr<fw_t> fw,
                     VSet& U, H& h, F& f, M& m, C& c) {
  using edata_t = typename fragment_t::edata_t;

  for (auto& vid : U.s) {
    value_t v = *(fw->Get(vid));
    auto es = h(vid, v);
    value_t nb;
    for (auto& nb_id : es) {
      nb = *(fw->Get(nb_id));
      if (c(nb_id, nb) && f(vid, nb_id, v, nb, edata_t())) {
        m(vid, nb_id, v, nb, edata_t());
        fw->PutNext(nb_id, nb);
      }
    }
  }

  VSet res;
  fw->Barrier(true);
  fw->GetActiveVerticesAndSetStates(res.s);
  return res;
}

template <typename fragment_t, typename fw_t, typename value_t, class F,
          class M, class C, class H>
inline VSet edgeMapSparseFunction(const fragment_t& graph,
                                  const std::shared_ptr<fw_t> fw, VSet& U, H h,
                                  F& f, M& m, C& c) {
  fw->ResetAggFunc();
  return doEdgeMapSparse(graph, fw, U, h, f, m, c);
}

template <typename fragment_t, typename fw_t, typename value_t, class F,
          class M, class C, class R, class H>
inline VSet edgeMapSparseFunction(const fragment_t& graph,
                                  const std::shared_ptr<fw_t> fw, VSet& U, H h,
                                  F& f, M& m, C& c, const R& r) {
  fw->SetAggFunc(r);
  return doEdgeMapSparse(graph, fw, U, h, f, m, c);
  fw->ResetAggFunc();
}

template <typename fragment_t, typename fw_t, typename value_t, class F,
          class M, class C, class H>
inline VSet edgeMapFunction(const fragment_t& graph,
                            const std::shared_ptr<fw_t> fw, VSet& U, H h, F& f,
                            M& m, C& c) {
  int len = VSize(U);
  if (len > THRESHOLD)
    return edgeMapDenseFunction(graph, fw, U, h, f, m, c);
  else
    return edgeMapSparseFunction(graph, fw, U, h, f, m, c);
}

template <typename fragment_t, typename fw_t, typename value_t, class F,
          class M, class C, class R, class H>
inline VSet edgeMapFunction(const fragment_t& graph,
                            const std::shared_ptr<fw_t> fw, VSet& U, H h, F& f,
                            M& m, C& c, const R& r) {
  int len = VSize(U);
  if (len > THRESHOLD)
    return edgeMapDenseFunction(graph, fw, U, h, f, m, c);
  else
    return edgeMapSparseFunction(graph, fw, U, h, f, m, c, r);
}

template <typename fragment_t, typename fw_t, typename value_t, class F,
          class M, class C, class H>
inline VSet edgeMapFunction(const fragment_t& graph,
                            const std::shared_ptr<fw_t> fw, VSet& U, H h,
                            VSet& T, bool is_join, F& f, M& m, C& c,
                            bool b = true) {
  return edgeMapDenseFunction(graph, fw, U, h, T, is_join, f, m, c, b);
}

template <typename fragment_t, typename fw_t>
inline void blockFunction(const fragment_t& graph,
                          const std::shared_ptr<fw_t> fw,
                          std::function<void()> f) {
  f();
}

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_API_H_
