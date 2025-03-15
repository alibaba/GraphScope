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

#ifndef EXAMPLES_ANALYTICAL_APPS_KCLIQUE_KCLIQUE_UTILS_H_
#define EXAMPLES_ANALYTICAL_APPS_KCLIQUE_KCLIQUE_UTILS_H_

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

// #include "thirdparty/parallel_hashmap/phmap.h"

namespace grape {

template <typename VID_T>
struct GidComparer {
  GidComparer(const GidComparer& rhs) : mask_(rhs.mask_) {}
  explicit GidComparer(fid_t fnum) {
    fid_t maxfid = fnum - 1;
    int fid_offset;
    if (maxfid == 0) {
      fid_offset = (sizeof(VID_T) * 8) - 1;
    } else {
      int i = 0;
      while (maxfid) {
        maxfid >>= 1;
        ++i;
      }
      fid_offset = (sizeof(VID_T) * 8) - i;
    }
    mask_ = ((VID_T) 1 << fid_offset) - (VID_T) 1;
  }

  bool operator()(VID_T a, VID_T b) const {
    VID_T xora = a & mask_;
    VID_T xorb = b & mask_;

    if (xora < xorb) {
      return true;
    } else if (xora > xorb) {
      return false;
    } else {
      return a < b;
    }
  }

 private:
  VID_T mask_;
};

template <typename VID_T>
struct KCliqueMsg {
  int prefix_size;
  int size;
  VID_T* data;
};

template <typename VID_T>
InArchive& operator<<(InArchive& arc, const KCliqueMsg<VID_T>& msg) {
  arc << msg.prefix_size;
  arc << msg.size;
  arc.AddBytes(msg.data, msg.size * sizeof(VID_T));
  return arc;
}

template <typename VID_T>
OutArchive& operator>>(OutArchive& arc, KCliqueMsg<VID_T>& msg) {
  arc >> msg.prefix_size;
  arc >> msg.size;
  msg.data = reinterpret_cast<VID_T*>(arc.GetBytes(msg.size * sizeof(VID_T)));
  return arc;
}

template <typename FRAG_T>
struct KCliqueUtils {
  using fragment_t = FRAG_T;
  using vertex_t = typename fragment_t::vertex_t;
  // using vertex_set_t = phmap::flat_hash_set<vertex_t>;
  using vertex_set_t = std::set<vertex_t>;
  using vid_t = typename fragment_t::vid_t;
  using msg_t = KCliqueMsg<vid_t>;

  static inline size_t UniFragCliqueNumRecursive(
      const fragment_t& frag, vertex_t vi, std::vector<uint8_t>& table, int k,
      std::vector<std::vector<vertex_t>>& levels) {
    size_t ret = 0;
    auto& level = levels[0];
    level.clear();
    auto es = frag.GetOutgoingAdjList(vi);
    auto es_ptr = es.end_pointer() - 1;
    auto es_end = es.begin_pointer() - 1;
    while (es_ptr != es_end) {
      vertex_t vj = es_ptr->get_neighbor();
      if (vj.GetValue() > vi.GetValue()) {
        table[vj.GetValue()] = 1;
        level.push_back(vj);
      } else {
        break;
      }
      --es_ptr;
    }
    for (auto vj : level) {
      ret += uniFragCliqueNumRecursiveImpl(frag, vj, table, k, 2, levels);
    }
    for (auto vj : level) {
      table[vj.GetValue()] = 0;
    }

    return ret;
  }

  static inline size_t UniFragCliqueNumIterative(const fragment_t& frag,
                                                 vertex_t vi, int k) {
    size_t local_clique_num = 0;

    std::queue<std::pair<int, vertex_set_t>> q;
    auto es = frag.GetOutgoingAdjList(vi);
    vertex_set_t neighbors;
    {
      auto ptr = es.end_pointer() - 1;
      auto end = es.begin_pointer() - 1;
      while (ptr != end) {
        if (ptr->get_neighbor().GetValue() > vi.GetValue()) {
          neighbors.insert(ptr->get_neighbor());
        } else {
          break;
        }
        --ptr;
      }
    }
    if (neighbors.size() >= static_cast<size_t>(k - 1)) {
      q.emplace(1, std::move(neighbors));
    }
    while (!q.empty()) {
      auto pair = std::move(q.front());
      q.pop();
      auto prefix_size = pair.first;
      auto& candidate = pair.second;

      if (static_cast<size_t>(prefix_size) == static_cast<size_t>(k - 2)) {
        for (auto& u : candidate) {
          auto es = frag.GetOutgoingAdjList(u);
          {
            auto ptr = es.end_pointer() - 1;
            auto end = es.begin_pointer() - 1;
            while (ptr != end) {
              auto nbr = ptr->get_neighbor();
              if (nbr.GetValue() > u.GetValue()) {
                if (candidate.count(nbr)) {
                  ++local_clique_num;
                }
              } else {
                break;
              }
              --ptr;
            }
          }
        }
      } else {
        for (auto& u : candidate) {
          auto es = frag.GetOutgoingAdjList(u);
          vertex_set_t new_candidate;
          {
            auto ptr = es.end_pointer() - 1;
            auto end = es.begin_pointer() - 1;
            while (ptr != end) {
              auto nbr = ptr->get_neighbor();
              if (nbr.GetValue() > u.GetValue()) {
                if (candidate.count(nbr)) {
                  new_candidate.insert(nbr);
                }
              } else {
                break;
              }
              --ptr;
            }
          }
          if (new_candidate.size() + prefix_size + 1 >=
              static_cast<size_t>(k)) {
            q.emplace(prefix_size + 1, std::move(new_candidate));
          }
        }
      }
    }

    return local_clique_num;
  }

  template <typename MESSAGE_BUFFER_T>
  static inline size_t MultiFragCliqueNumRecursive(
      const fragment_t& frag, vertex_t vi, std::vector<uint8_t>& table, int k,
      std::vector<std::vector<vid_t>>& levels, MESSAGE_BUFFER_T& channel,
      const GidComparer<vid_t>& cmp) {
    size_t ret = 0;
    msg_t msg;

    auto& level = levels[0];
    level.clear();
    vid_t vi_gid = frag.GetInnerVertexGid(vi);

    auto es = frag.GetOutgoingAdjList(vi);
    for (auto& e : es) {
      vertex_t vj = e.get_neighbor();
      vid_t vj_gid = frag.Vertex2Gid(vj);
      if (cmp(vi_gid, vj_gid)) {
        table[vj.GetValue()] = 1;
        level.push_back(vj_gid);
      }
    }

    if (level.size() >= static_cast<size_t>(k - 1)) {
      std::sort(level.begin(), level.end(), cmp);
      for (size_t j = 0; j < level.size(); ++j) {
        vid_t vj_gid = level[j];
        vertex_t vj;
        CHECK(frag.Gid2Vertex(vj_gid, vj));
        table[vj.GetValue()] = 0;
        if (frag.IsInnerVertex(vj)) {
          ret += multiFragCliqueNumRecursiveImpl(frag, vj, vj_gid, table, k,
                                                 levels, channel, cmp, 2);
        } else {
          msg.prefix_size = 2;
          msg.size = level.size() - j - 1;
          if (msg.size > (k - msg.prefix_size - 1)) {
            msg.data = level.data() + j + 1;
            channel.SyncStateOnOuterVertex(frag, vj, msg);
          }
        }
      }
    } else {
      for (auto gid : level) {
        vertex_t v;
        CHECK(frag.Gid2Vertex(gid, v));
        table[v.GetValue()] = 0;
      }
    }

    return ret;
  }

  template <typename MESSAGE_BUFFER_T>
  static inline size_t MultiFragCliqueNumRecursiveStep(
      const fragment_t& frag, vertex_t vi, std::vector<uint8_t>& table, int k,
      const msg_t& msg_in, std::vector<std::vector<vid_t>>& levels,
      MESSAGE_BUFFER_T& channel, const GidComparer<vid_t>& cmp) {
    size_t ret = 0;
    auto es = frag.GetOutgoingAdjList(vi);
    int i = msg_in.prefix_size;
    if (i == k - 1) {
      for (auto& e : es) {
        vertex_t vj = e.get_neighbor();
        vid_t vj_gid = frag.Vertex2Gid(vj);
        if (std::binary_search(msg_in.data, msg_in.data + msg_in.size, vj_gid,
                               cmp)) {
          ++ret;
        }
      }
    } else {
      msg_t msg_out;
      auto& level = levels[i - 1];
      level.clear();
      for (auto& e : es) {
        vertex_t vj = e.get_neighbor();
        vid_t vj_gid = frag.Vertex2Gid(vj);
        if (std::binary_search(msg_in.data, msg_in.data + msg_in.size, vj_gid,
                               cmp)) {
          table[vj.GetValue()] = i;
          level.push_back(vj_gid);
        }
      }
      if (level.size() >= static_cast<size_t>(k - i)) {
        std::sort(level.begin(), level.end(), cmp);
        for (size_t j = 0; j < level.size(); ++j) {
          vid_t vj_gid = level[j];
          vertex_t vj;
          CHECK(frag.Gid2Vertex(vj_gid, vj));
          table[vj.GetValue()] = 0;
          if (frag.IsInnerVertex(vj)) {
            ret += multiFragCliqueNumRecursiveImpl(frag, vj, vj_gid, table, k,
                                                   levels, channel, cmp, i + 1);
          } else {
            msg_out.prefix_size = i + 1;
            msg_out.size = level.size() - j - 1;
            if (msg_out.size > (k - msg_out.prefix_size - 1)) {
              msg_out.data = level.data() + j + 1;
              channel.SyncStateOnOuterVertex(frag, vj, msg_out);
            }
          }
        }
      } else {
        for (auto gid : level) {
          vertex_t v;
          CHECK(frag.Gid2Vertex(gid, v));
          table[v.GetValue()] = 0;
        }
      }
    }
    return ret;
  }

  template <typename MESSAGE_BUFFER_T>
  static inline size_t MultiFragCliqueNumIterative(
      const fragment_t& frag, vertex_t v, int k, MESSAGE_BUFFER_T& channel,
      const GidComparer<vid_t>& cmp) {
    std::queue<std::pair<int, std::vector<vid_t>>> q;
    auto es = frag.GetOutgoingAdjList(v);
    std::vector<vid_t> neighbors;
    vid_t v_gid = frag.GetInnerVertexGid(v);
    for (auto& e : es) {
      vid_t nbr_gid = frag.Vertex2Gid(e.get_neighbor());
      if (cmp(v_gid, nbr_gid)) {
        neighbors.push_back(nbr_gid);
      }
    }
    if (neighbors.size() >= static_cast<size_t>(k - 1)) {
      std::sort(neighbors.begin(), neighbors.end(), cmp);
      q.emplace(1, std::move(neighbors));
    }
    return multiFragCliqueNumIterativeImpl(frag, k, channel, cmp, q);
  }

  template <typename MESSAGE_BUFFER_T>
  static inline size_t MultiFragCliqueNumIterativeStep(
      const fragment_t& frag, vertex_t v, int k, const msg_t& msg_in,
      MESSAGE_BUFFER_T& channel, const GidComparer<vid_t>& cmp) {
    vid_t v_gid = frag.GetInnerVertexGid(v);
    auto es = frag.GetOutgoingAdjList(v);
    if (msg_in.prefix_size == k - 1) {
      size_t ret = 0;
      for (auto& e : es) {
        vid_t nbr_gid = frag.Vertex2Gid(e.get_neighbor());
        if (cmp(v_gid, nbr_gid)) {
          if (std::binary_search(msg_in.data, msg_in.data + msg_in.size,
                                 nbr_gid, cmp)) {
            ++ret;
          }
        }
      }
      return ret;
    } else {
      std::queue<std::pair<int, std::vector<vid_t>>> q;
      {
        std::vector<vid_t> neighbors;
        for (auto& e : es) {
          vid_t nbr_gid = frag.Vertex2Gid(e.get_neighbor());
          if (cmp(v_gid, nbr_gid)) {
            if (std::binary_search(msg_in.data, msg_in.data + msg_in.size,
                                   nbr_gid, cmp)) {
              neighbors.push_back(nbr_gid);
            }
          }
        }
        if (neighbors.size() + msg_in.prefix_size >= static_cast<size_t>(k)) {
          std::sort(neighbors.begin(), neighbors.end(), cmp);
          q.emplace(msg_in.prefix_size, std::move(neighbors));
        }
      }
      return multiFragCliqueNumIterativeImpl(frag, k, channel, cmp, q);
    }
  }

 private:
  static inline size_t uniFragCliqueNumRecursiveImpl(
      const fragment_t& frag, vertex_t vi, std::vector<uint8_t>& table, int k,
      int i, std::vector<std::vector<vertex_t>>& levels) {
    size_t ret = 0;
    auto es = frag.GetOutgoingAdjList(vi);
    auto es_ptr = es.end_pointer() - 1;
    auto es_end = es.begin_pointer() - 1;
    if (i == k - 1) {
      while (es_ptr != es_end) {
        vertex_t vj = es_ptr->get_neighbor();
        if (vj.GetValue() > vi.GetValue()) {
          if (table[vj.GetValue()] == (i - 1)) {
            ++ret;
          }
        } else {
          break;
        }
        --es_ptr;
      }
    } else {
      auto& level = levels[i - 1];
      level.clear();
      while (es_ptr != es_end) {
        vertex_t vj = es_ptr->get_neighbor();
        if (vj.GetValue() > vi.GetValue()) {
          if (table[vj.GetValue()] == (i - 1)) {
            level.push_back(vj);
          }
        } else {
          break;
        }
        --es_ptr;
      }
      for (auto vj : level) {
        table[vj.GetValue()] = i;
        ret += uniFragCliqueNumRecursiveImpl(frag, vj, table, k, i + 1, levels);
      }
      for (auto vj : level) {
        table[vj.GetValue()] = i - 1;
      }
    }
    return ret;
  }

  template <typename MESSAGE_BUFFER_T>
  static inline size_t multiFragCliqueNumRecursiveImpl(
      const fragment_t& frag, vertex_t vi, vid_t vi_gid,
      std::vector<uint8_t>& table, int k,
      std::vector<std::vector<vid_t>>& levels, MESSAGE_BUFFER_T& channel,
      const GidComparer<vid_t>& cmp, int i) {
    size_t ret = 0;
    auto es = frag.GetOutgoingAdjList(vi);

    if (i == k - 1) {
      for (auto& e : es) {
        vertex_t vj = e.get_neighbor();
        vid_t vj_gid = frag.Vertex2Gid(vj);
        if (cmp(vi_gid, vj_gid)) {
          if (table[vj.GetValue()] == (i - 1)) {
            ++ret;
          }
        }
      }
    } else {
      msg_t msg;
      auto& level = levels[i - 1];
      level.clear();

      for (auto& e : es) {
        vertex_t vj = e.get_neighbor();
        vid_t vj_gid = frag.Vertex2Gid(vj);
        if (cmp(vi_gid, vj_gid)) {
          if (table[vj.GetValue()] == (i - 1)) {
            table[vj.GetValue()] = i;
            level.push_back(vj_gid);
          }
        }
      }

      if (level.size() >= static_cast<size_t>(k - i)) {
        std::sort(level.begin(), level.end(), cmp);
        for (size_t j = 0; j < level.size(); ++j) {
          vid_t vj_gid = level[j];
          vertex_t vj;
          CHECK(frag.Gid2Vertex(vj_gid, vj));
          table[vj.GetValue()] = i - 1;
          if (frag.IsInnerVertex(vj)) {
            ret += multiFragCliqueNumRecursiveImpl(frag, vj, vj_gid, table, k,
                                                   levels, channel, cmp, i + 1);
          } else {
            msg.prefix_size = i + 1;
            msg.size = level.size() - j - 1;
            if (msg.size > (k - msg.prefix_size - 1)) {
              msg.data = level.data() + j + 1;
              channel.SyncStateOnOuterVertex(frag, vj, msg);
            }
          }
        }
      } else {
        for (auto gid : level) {
          vertex_t v;
          CHECK(frag.Gid2Vertex(gid, v));
          table[v.GetValue()] = i - 1;
        }
      }
    }
    return ret;
  }

  template <typename MESSAGE_BUFFER_T>
  static inline size_t multiFragCliqueNumIterativeImpl(
      const fragment_t& frag, int k, MESSAGE_BUFFER_T& channel,
      const GidComparer<vid_t>& cmp,
      std::queue<std::pair<int, std::vector<vid_t>>>& q) {
    msg_t msg_out;
    size_t ret = 0;
    while (!q.empty()) {
      auto pair = std::move(q.front());
      q.pop();
      auto prefix_size = pair.first;
      auto& candidate = pair.second;

      if (static_cast<size_t>(prefix_size) == static_cast<size_t>(k - 2)) {
        vid_t* ptr = candidate.data();
        vid_t* end = ptr + candidate.size() - 1;
        while (ptr != end) {
          vid_t u = *ptr;
          vertex_t u_vertex;
          CHECK(frag.Gid2Vertex(u, u_vertex));
          if (frag.IsInnerVertex(u_vertex)) {
            auto es = frag.GetOutgoingAdjList(u_vertex);
            for (auto& e : es) {
              vid_t nbr_gid = frag.Vertex2Gid(e.get_neighbor());
              if (cmp(u, nbr_gid)) {
                if (std::binary_search(ptr + 1, end + 1, nbr_gid, cmp)) {
                  ++ret;
                }
              }
            }
          } else {
            msg_out.prefix_size = prefix_size + 1;
            msg_out.size = end - ptr;
            if (msg_out.size > k - msg_out.prefix_size - 1) {
              msg_out.data = ptr + 1;
              channel.SyncStateOnOuterVertex(frag, u_vertex, msg_out);
            }
          }
          ++ptr;
        }
      } else {
        vid_t* ptr = candidate.data();
        vid_t* end = ptr + candidate.size() - 1;
        while (ptr != end) {
          vid_t u = *ptr;
          vertex_t u_vertex;
          CHECK(frag.Gid2Vertex(u, u_vertex));
          if (frag.IsInnerVertex(u_vertex)) {
            auto es = frag.GetOutgoingAdjList(u_vertex);
            std::vector<vid_t> new_candidate;
            for (auto& e : es) {
              vid_t nbr_gid = frag.Vertex2Gid(e.get_neighbor());
              if (cmp(u, nbr_gid)) {
                if (std::binary_search(ptr + 1, end + 1, nbr_gid, cmp)) {
                  new_candidate.push_back(nbr_gid);
                }
              }
            }
            if (new_candidate.size() + prefix_size + 1 >=
                static_cast<size_t>(k)) {
              std::sort(new_candidate.begin(), new_candidate.end(), cmp);
              q.emplace(prefix_size + 1, std::move(new_candidate));
            }
          } else {
            msg_out.prefix_size = prefix_size + 1;
            msg_out.size = end - ptr;
            if (msg_out.size > k - msg_out.prefix_size - 1) {
              msg_out.data = ptr + 1;
              channel.SyncStateOnOuterVertex(frag, u_vertex, msg_out);
            }
          }
          ++ptr;
        }
      }
    }
    return ret;
  }
};

}  // namespace grape

#endif  // EXAMPLES_ANALYTICAL_APPS_KCLIQUE_KCLIQUE_UTILS_H_
