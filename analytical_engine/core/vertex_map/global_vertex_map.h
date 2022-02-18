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

#ifndef ANALYTICAL_ENGINE_CORE_VERTEX_MAP_GLOBAL_VERTEX_MAP_H_
#define ANALYTICAL_ENGINE_CORE_VERTEX_MAP_GLOBAL_VERTEX_MAP_H_

#include <memory>
#include <string>
#include <vector>

#include "grape/fragment/partitioner.h"
#include "grape/vertex_map/global_vertex_map.h"
#include "vineyard/graph/utils/string_collection.h"

namespace grape {

template <typename VID_T, typename PARTITIONER_T>
class GlobalVertexMapBuilder<std::string, VID_T, PARTITIONER_T> {
 private:
  GlobalVertexMapBuilder(fid_t fid, HashMap<RefString, VID_T>& hmap,
                         StringCollection& list,
                         const PARTITIONER_T& partitioner,
                         const IdParser<VID_T>& id_parser)
      : fid_(fid),
        map_(hmap),
        list_(list),
        partitioner_(partitioner),
        id_parser_(id_parser) {}

 public:
  ~GlobalVertexMapBuilder() {}

  void add_vertex(const std::string& id) {
    assert(partitioner_.GetPartitionId(id) == fid_);
    RefString ref_oid(id);
    if (map_.find(ref_oid) == map_.end()) {
      RefString key = list_.PutString(ref_oid);
      map_.emplace(key, static_cast<VID_T>(map_.size()));
    }
  }

  bool add_vertex(const std::string& id, VID_T& gid) {
    assert(partitioner_.GetPartitionId(id) == fid_);
    RefString ref_oid(id);
    auto iter = map_.find(ref_oid);
    if (iter == map_.end()) {
      RefString key = list_.PutString(ref_oid);
      gid = static_cast<VID_T>(map_.size());
      map_.emplace(key, gid);
      gid = id_parser_.generate_global_id(fid_, gid);
      return true;
    } else {
      gid = id_parser_.generate_global_id(fid_, iter->second);
      return false;
    }
  }

  void finish(GlobalVertexMap<std::string, VID_T, PARTITIONER_T>& vertex_map) {
    const CommSpec& comm_spec = vertex_map.GetCommSpec();
    int worker_id = comm_spec.worker_id();
    int worker_num = comm_spec.worker_num();
    fid_t fnum = comm_spec.fnum();
    {
      std::thread recv_thread([&]() {
        int src_worker_id = (worker_id + 1) % worker_num;
        while (src_worker_id != worker_id) {
          for (fid_t fid = 0; fid < fnum; ++fid) {
            if (comm_spec.FragToWorker(fid) != src_worker_id) {
              continue;
            }
            vertex_map.string_collections_[fid].RecvFrom(src_worker_id,
                                                         comm_spec.comm());
          }
          src_worker_id = (src_worker_id + 1) % worker_num;
        }
      });
      std::thread send_thread([&]() {
        int dst_worker_id = (worker_id + worker_num - 1) % worker_num;
        while (dst_worker_id != worker_id) {
          for (fid_t fid = 0; fid < fnum; ++fid) {
            if (comm_spec.FragToWorker(fid) != worker_id) {
              continue;
            }
            vertex_map.string_collections_[fid].SendTo(dst_worker_id,
                                                       comm_spec.comm());
          }
          dst_worker_id = (dst_worker_id + worker_num - 1) % worker_num;
        }
      });
      send_thread.join();
      recv_thread.join();
    }
    {
      int thread_num =
          (std::thread::hardware_concurrency() + comm_spec.local_num() - 1) /
          comm_spec.local_num();
      std::atomic<fid_t> current_fid(0);
      fid_t fnum = comm_spec.fnum();
      std::vector<std::thread> work_threads(thread_num);
      for (int tid = 0; tid < thread_num; ++tid) {
        work_threads[tid] = std::thread([&] {
          fid_t got;
          RefString rs;
          while (true) {
            got = current_fid.fetch_add(1, std::memory_order_relaxed);
            if (got >= fnum) {
              break;
            }
            if (comm_spec.FragToWorker(got) == worker_id) {
              continue;
            }
            auto& rm = vertex_map.o2l_[got];
            VID_T vnum =
                static_cast<VID_T>(vertex_map.string_collections_[got].Count());
            rm.reserve(vnum);
            for (VID_T lid = 0; lid < vnum; ++lid) {
              vertex_map.string_collections_[got].Get(lid, rs);
              rm.emplace(rs, lid);
            }
          }
        });
      }
      for (auto& thrd : work_threads) {
        thrd.join();
      }
    }
  }

 private:
  template <class _OID_T, class _VID_T, typename _PARTITIONER_T>
  friend class GlobalVertexMap;

  fid_t fid_;
  HashMap<RefString, VID_T>& map_;
  StringCollection& list_;
  const PARTITIONER_T& partitioner_;
  const IdParser<VID_T>& id_parser_;
};

/**
 * @brief A specialized GlobalVertexMap for string oid.
 *
 * * @tparam VID_T VID type
 */
template <typename VID_T, typename PARTITIONER_T>
class GlobalVertexMap<std::string, VID_T, PARTITIONER_T>
    : public VertexMapBase<std::string, VID_T, PARTITIONER_T> {
  using Base = VertexMapBase<std::string, VID_T, PARTITIONER_T>;

 public:
  explicit GlobalVertexMap(const CommSpec& comm_spec) : Base(comm_spec) {}
  ~GlobalVertexMap() = default;
  void Init() {
    o2l_.resize(comm_spec_.fnum());
    string_collections_.resize(comm_spec_.fnum());
  }

  size_t GetTotalVertexSize() {
    size_t size = 0;
    for (const auto& v : o2l_) {
      size += v.size();
    }
    return size;
  }

  size_t GetInnerVertexSize(fid_t fid) {
    return string_collections_[fid].Count();
  }

  void AddVertex(const std::string& oid) {
    fid_t fid = partitioner_.GetPartitionId(oid);
    RefString ref_oid(oid);
    auto& rm = o2l_[fid];
    if (rm.find(ref_oid) == rm.end()) {
      RefString key = string_collections_[fid].PutString(ref_oid);
      rm.emplace(key, static_cast<VID_T>(rm.size()));
    }
  }

  using Base::Lid2Gid;
  bool AddVertex(const std::string& oid, VID_T& gid) {
    fid_t fid = partitioner_.GetPartitionId(oid);
    RefString ref_oid(oid);
    auto& rm = o2l_[fid];
    auto iter = rm.find(ref_oid);
    if (iter == rm.end()) {
      RefString key = string_collections_[fid].PutString(ref_oid);
      gid = static_cast<VID_T>(rm.size());
      rm.emplace(key, gid);
      gid = Base::Lid2Gid(fid, gid);
      return true;
    } else {
      gid = Base::Lid2Gid(fid, iter->second);
      return false;
    }
  }

  using Base::GetFidFromGid;
  using Base::GetLidFromGid;
  bool GetOid(const VID_T& gid, std::string& oid) {
    fid_t fid = GetFidFromGid(gid);
    VID_T lid = GetLidFromGid(gid);
    return GetOid(fid, lid, oid);
  }

  bool GetOid(fid_t fid, const VID_T& lid, std::string& oid) {
    auto& sc = string_collections_[fid];
    if (lid >= sc.Count()) {
      return false;
    }
    sc.Get(lid, oid);
    return true;
  }

  bool GetGid(fid_t fid, const std::string& oid, VID_T& gid) {
    RefString ref_oid(oid);
    auto& rm = o2l_[fid];
    auto iter = rm.find(ref_oid);
    if (iter == rm.end()) {
      return false;
    } else {
      gid = Base::Lid2Gid(fid, iter->second);
      return true;
    }
  }

  bool GetGid(const std::string& oid, VID_T& gid) {
    for (fid_t i = 0; i < Base::GetFragmentNum(); ++i) {
      if (GetGid(i, oid, gid)) {
        return true;
      }
    }
    return false;
  }

  template <typename IOADAPTOR_T>
  void Serialize(const std::string& prefix) {
    char fbuf[1024];
    snprintf(fbuf, sizeof(fbuf), "%s/%s", prefix.c_str(),
             kSerializationVertexMapFilename);

    auto io_adaptor =
        std::unique_ptr<IOADAPTOR_T>(new IOADAPTOR_T(std::string(fbuf)));
    io_adaptor->Open("wb");

    Base::serialize(io_adaptor);
    for (auto& sc : string_collections_) {
      sc.Write(io_adaptor);
    }

    CHECK(io_adaptor->Close());
  }

  template <typename IOADAPTOR_T>
  void Deserialize(const std::string& prefix) {
    char fbuf[1024];
    snprintf(fbuf, sizeof(fbuf), "%s/%s", prefix.c_str(),
             kSerializationVertexMapFilename);

    auto io_adaptor =
        std::unique_ptr<IOADAPTOR_T>(new IOADAPTOR_T(std::string(fbuf)));
    io_adaptor->Open();

    Base::deserialize(io_adaptor);

    string_collections_.resize(Base::GetFragmentNum());
    for (auto& sc : string_collections_) {
      sc.Read(io_adaptor);
    }

    o2l_.clear();
    o2l_.resize(Base::GetFragmentNum());
    {
      int thread_num = (std::thread::hardware_concurrency() +
                        Base::GetCommSpec().local_num() - 1) /
                       Base::GetCommSpec().local_num();
      std::vector<std::thread> construct_threads(thread_num);
      std::atomic<fid_t> current_fid(0);
      fid_t fnum = Base::GetFragmentNum();
      for (int i = 0; i < thread_num; ++i) {
        construct_threads[i] = std::thread([&]() {
          fid_t got;
          RefString rs;
          while (true) {
            got = current_fid.fetch_add(1, std::memory_order_relaxed);
            if (got >= fnum) {
              break;
            }
            auto& rm = o2l_[got];
            size_t vnum = string_collections_[got].Count();
            rm.reserve(vnum);
            for (size_t lid = 0; lid < vnum; ++lid) {
              string_collections_[got].Get(lid, rs);
              rm.emplace(rs, static_cast<VID_T>(lid));
            }
          }
        });
      }

      for (auto& thrd : construct_threads) {
        thrd.join();
      }
    }

    CHECK(io_adaptor->Close());
  }

  void UpdateToBalance(std::vector<VID_T>& vnum_list,
                       std::vector<std::vector<VID_T>>& gid_maps) {
    LOG(FATAL) << "Not implemented.";
  }

 private:
  std::vector<StringCollection> string_collections_;
  std::vector<HashMap<RefString, VID_T>> o2l_;
  using Base::comm_spec_;
  using Base::id_parser_;
  using Base::partitioner_;
};

}  // namespace grape

#endif  // ANALYTICAL_ENGINE_CORE_VERTEX_MAP_GLOBAL_VERTEX_MAP_H_
