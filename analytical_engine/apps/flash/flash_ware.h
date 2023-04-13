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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_FLASH_WARE_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_FLASH_WARE_H_

#include <memory>
#include <utility>
#include <vector>

#include "grape/communication/communicator.h"
#include "grape/config.h"
#include "grape/parallel/parallel_engine.h"
#include "grape/parallel/parallel_message_manager.h"
#include "grape/worker/comm_spec.h"

#include "flash/flash_bitset.h"
#include "flash/vertex_subset.h"

namespace gs {

/**
 * @brief The middle-ware of Flash.
 *
 * @tparam FRAG_T
 * @tparam VALUE_T
 */
template <typename FRAG_T, class VALUE_T>
class FlashWare : public grape::Communicator, public grape::ParallelEngine {
 public:
  using fragment_t = FRAG_T;
  using value_t = VALUE_T;
  using vid_t = typename fragment_t::vid_t;
  using edata_t = typename fragment_t::edata_t;
  using vertex_map_t = typename fragment_t::vertex_map_t;
  using vset_t = VertexSubset<fragment_t, value_t>;

  FlashWare() = default;
  ~FlashWare() {
    delete[] states_;
    delete[] next_states_;
    delete[] vnum_;
    delete[] agg_vnum_;
    delete[] key2pid_;
    masters_.clear();
    mirrors_.clear();
  }

 public:
  void InitFlashWare(const grape::CommSpec& comm_spec, const bool& sync_all,
                     std::shared_ptr<fragment_t> graph);
  void Start();
  void Terminate();

  void GetActiveVertices(std::vector<vid_t>& result);
  void GetActiveVerticesAndSetStates(std::vector<vid_t>& result);
  void GetActiveVerticesAndSetStates(std::vector<vid_t>& result,
                                     FlashBitset& d);
  void SyncBitset(FlashBitset& tmp, FlashBitset& d);
  void SyncBitset(FlashBitset& b);

  inline value_t* Get(const vid_t& key);
  inline void PutNext(const vid_t& key, const value_t& value);
  inline void PutNextLocal(const vid_t& key, const value_t& value,
                           const bool& b, const int& tid = 0);
  inline void PutNextPull(const vid_t& key, const value_t& value, const bool& b,
                          const int& tid = 0);
  void Barrier(bool flag = false);

 public:
  inline void SetAggFunc(
      const std::function<void(const vid_t, const vid_t, const value_t&,
                               value_t&, const edata_t&)>& f_agg) {
    f_agg_ = f_agg;
  }
  inline void ResetAggFunc() { f_agg_ = nullptr; }

 public:
  inline fid_t GetPid() { return pid_; }
  inline vid_t GetSize() { return n_; }
  inline std::vector<vid_t>* GetMasters() { return &masters_; }
  inline std::vector<vid_t>* GetMirrors() { return &mirrors_; }
  inline int GetMasterPid(const vid_t& key) { return key2pid_[key]; }
  inline vid_t Gid2Key(const vid_t& gid) {
    return Lid2Key(vmap_->GetLidFromGid(gid), vmap_->GetFidFromGid(gid));
  }
  inline vid_t Key2Lid(const vid_t& key, const fid_t& pid) {
    return vmap_->Offset2Lid(key - agg_vnum_[pid]);
  }
  inline vid_t Key2Lid(const vid_t& key) {
    return vmap_->Offset2Lid(key - agg_vnum_[pid_]);
  }
  inline vid_t Lid2Key(const vid_t& lid, const fid_t& pid) {
    return vmap_->GetOffsetFromLid(lid) + agg_vnum_[pid];
  }
  inline vid_t Lid2Key(const vid_t& lid) {
    return vmap_->GetOffsetFromLid(lid) + agg_vnum_[pid_];
  }
  inline vid_t Key2Offset(const vid_t& key, const fid_t& pid) {
    return key - agg_vnum_[pid];
  }
  inline vid_t Key2Offset(const vid_t& key) { return key - agg_vnum_[pid_]; }
  inline vid_t Offset2Key(const vid_t& lid, const fid_t& pid) {
    return lid + agg_vnum_[pid];
  }
  inline vid_t Offset2Key(const vid_t& lid) { return lid + agg_vnum_[pid_]; }

  inline bool IsMaster(const vid_t& key) { return GetMasterPid(key) == pid_; }
  inline bool IsActive(const vid_t& key) { return is_active_.get_bit(key); }
  inline void SetActive(const vid_t& key) { is_active_.set_bit(key); }
  inline void ResetActive(const vid_t& key) { is_active_.reset_bit(key); }
  inline void SetStates(const vid_t& key) { states_[key] = next_states_[key]; }

 private:
  inline void sendNext(const fid_t& pid, const vid_t& key, const int& tid);
  inline void sendCurrent(const fid_t& pid, const vid_t& key, const int& tid);
  inline void synchronizeNext(const int& tid, const vid_t& key);
  inline void synchronizeCurrent(const int& tid, const vid_t& key);
  inline void updateAllMirrors();
  inline void processMasterMessage(const vid_t& key, const value_t& value);
  inline void processMirrorMessage(const vid_t& key, const value_t& value);
  inline void processAllMessages(const bool& is_master,
                                 const bool& is_parallel);

 public:
  vset_t all_;

 private:
  vid_t n_;
  vid_t n_loc_;
  fid_t pid_;
  int n_procs_;
  int n_threads_;
  std::vector<vid_t> masters_;
  std::vector<vid_t> mirrors_;
  grape::CommSpec comm_spec_;
  grape::ParallelMessageManager messages_;
  bool sync_all_;
  FlashBitset nb_ids_;

  value_t* states_;
  value_t* next_states_;
  FlashBitset is_active_;
  int step_;

  std::shared_ptr<vertex_map_t> vmap_;
  size_t* vnum_;
  size_t* agg_vnum_;
  fid_t* key2pid_;

  std::function<void(const vid_t, const vid_t, const value_t&, value_t&,
                     const edata_t&)>
      f_agg_;
};

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::InitFlashWare(
    const grape::CommSpec& comm_spec, const bool& sync_all,
    std::shared_ptr<fragment_t> graph) {
  comm_spec_ = comm_spec;
  MPI_Barrier(comm_spec_.comm());
  InitParallelEngine();
  InitCommunicator(comm_spec_.comm());
  messages_.Init(comm_spec_.comm());
  messages_.InitChannels(thread_num());

  n_procs_ = comm_spec_.fnum();
  pid_ = comm_spec_.fid();
  n_threads_ = thread_num();

  vmap_ = graph->GetVertexMap();
  n_ = graph->GetTotalVerticesNum();
  n_loc_ = vmap_->GetInnerVertexSize(pid_);

  vnum_ = new size_t[n_procs_];
  agg_vnum_ = new size_t[n_procs_];
  key2pid_ = new fid_t[n_];
  for (fid_t fid = 0; fid < n_procs_; fid++) {
    vnum_[fid] = vmap_->GetInnerVertexSize(fid);
    if (fid == 0)
      agg_vnum_[fid] = 0;
    else
      agg_vnum_[fid] = agg_vnum_[fid - 1] + vnum_[fid - 1];
    for (vid_t key = agg_vnum_[fid]; key < agg_vnum_[fid] + vnum_[fid]; key++)
      key2pid_[key] = fid;
  }

  states_ = new value_t[n_];
  next_states_ = new value_t[n_];
  is_active_.init(n_);

  masters_.clear();
  mirrors_.clear();
  for (vid_t i = 0; i < n_; i++)
    if (IsMaster(i))
      masters_.push_back(i);
    else
      mirrors_.push_back(i);
  all_.Clear();
  for (auto it = masters_.begin(); it != masters_.end(); it++) {
    all_.AddV(*it);
  }

  sync_all_ = sync_all;
  if (!sync_all_) {
    nb_ids_.init((n_loc_ + 1) * n_procs_);
    ForEach(graph->InnerVertices(),
            [this, &graph](int tid, typename fragment_t::vertex_t v) {
              auto dsts = graph->IOEDests(v);
              vid_t lid = v.GetValue();
              vid_t offset = vmap_->GetOffsetFromLid(lid);
              const fid_t* ptr = dsts.begin;
              while (ptr != dsts.end) {
                fid_t fid = *(ptr++);
                nb_ids_.set_bit(offset * n_procs_ + fid);
              }
            });
  }

  f_agg_ = nullptr;
  step_ = 0;

  LOG(INFO) << "init flashware: " << n_procs_ << ' ' << pid_ << ' '
            << n_threads_ << ':' << n_loc_ << std::endl;
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::Start() {
  MPI_Barrier(comm_spec_.comm());
  messages_.Start();
  messages_.StartARound();
  step_++;
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::Terminate() {
  messages_.FinishARound();
  MPI_Barrier(comm_spec_.comm());
  messages_.Finalize();
  LOG(INFO) << "flashware terminate" << std::endl;
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::GetActiveVertices(
    std::vector<vid_t>& result) {
  result.clear();
  for (auto& u : masters_) {
    if (IsActive(u)) {
      result.push_back(u);
      ResetActive(u);
    }
  }
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::GetActiveVerticesAndSetStates(
    std::vector<vid_t>& result) {
  result.clear();
  for (auto& u : masters_) {
    if (IsActive(u)) {
      SetStates(u);
      result.push_back(u);
      ResetActive(u);
    }
  }
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::GetActiveVerticesAndSetStates(
    std::vector<vid_t>& result, FlashBitset& d) {
  SyncBitset(is_active_, d);
  GetActiveVerticesAndSetStates(result);
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::SyncBitset(FlashBitset& tmp,
                                                FlashBitset& res) {
  if (res.get_size() != tmp.get_size())
    res.init(tmp.get_size());
  MPI_Allreduce(tmp.get_data(), res.get_data(), res.get_size_in_words(),
                MPI_UINT64_T, MPI_BOR, comm_spec_.comm());
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::SyncBitset(FlashBitset& b) {
  MPI_Allreduce(MPI_IN_PLACE, b.get_data(), b.get_size_in_words(), MPI_UINT64_T,
                MPI_BOR, comm_spec_.comm());
}

template <typename fragment_t, class value_t>
inline value_t* FlashWare<fragment_t, value_t>::Get(const vid_t& key) {
  return &states_[key];
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::PutNextLocal(const vid_t& key,
                                                  const value_t& value,
                                                  const bool& b,
                                                  const int& tid) {
  states_[key] = value;
  SetActive(key);
  if (b)
    synchronizeCurrent(tid, key);
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::PutNextPull(const vid_t& key,
                                                 const value_t& value,
                                                 const bool& b,
                                                 const int& tid) {
  next_states_[key] = value;
  SetActive(key);
  if (b)
    synchronizeNext(tid, key);
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::PutNext(const vid_t& key,
                                             const value_t& value) {
  if (!IsActive(key)) {
    SetActive(key);
    next_states_[key] = states_[key];
  }
  if (f_agg_ != nullptr)
    f_agg_(key, key, value, next_states_[key], edata_t());
  else
    next_states_[key] = value;
}

template <typename fragment_t, class value_t>
void FlashWare<fragment_t, value_t>::Barrier(bool flag) {
  if (flag)
    updateAllMirrors();
  messages_.FinishARound();
  MPI_Barrier(comm_spec_.comm());
  messages_.StartARound();

  processAllMessages(false, true);
  step_++;
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::sendNext(const fid_t& pid,
                                                     const vid_t& key,
                                                     const int& tid) {
  messages_.SendToFragment<std::pair<vid_t, value_t>>(
      pid, std::make_pair(key, next_states_[key]), tid);
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::sendCurrent(const fid_t& pid,
                                                        const vid_t& key,
                                                        const int& tid) {
  messages_.SendToFragment<std::pair<vid_t, value_t>>(
      pid, std::make_pair(key, states_[key]), tid);
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::synchronizeCurrent(
    const int& tid, const vid_t& key) {
  vid_t x = Key2Offset(key) * n_procs_;
  for (fid_t i = 0; i < n_procs_; i++)
    if (i != pid_ && (sync_all_ || (nb_ids_.get_bit(x + i))))
      sendCurrent(i, key, tid);
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::synchronizeNext(const int& tid,
                                                            const vid_t& key) {
  vid_t x = Key2Offset(key) * n_procs_;
  for (fid_t i = 0; i < n_procs_; i++)
    if (i != pid_ && (sync_all_ || (nb_ids_.get_bit(x + i))))
      sendNext(i, key, tid);
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::updateAllMirrors() {
  ForEach(mirrors_.begin(), mirrors_.end(), [this](int tid, vid_t key) {
    if (IsActive(key)) {
      sendNext(GetMasterPid(key), key, tid);
      ResetActive(key);
    }
  });

  messages_.FinishARound();
  MPI_Barrier(comm_spec_.comm());
  messages_.StartARound();
  processAllMessages(true, f_agg_ == nullptr);

  ForEach(masters_.begin(), masters_.end(), [this](int tid, vid_t key) {
    if (IsActive(key))
      synchronizeNext(tid, key);
  });
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::processMasterMessage(
    const vid_t& key, const value_t& value) {
  if (!IsActive(key)) {
    SetActive(key);
    next_states_[key] = states_[key];
  }
  if (f_agg_ == nullptr)
    next_states_[key] = value;
  else
    f_agg_(key, key, value, next_states_[key], edata_t());
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::processMirrorMessage(
    const vid_t& key, const value_t& value) {
  states_[key] = value;
}

template <typename fragment_t, class value_t>
inline void FlashWare<fragment_t, value_t>::processAllMessages(
    const bool& is_master, const bool& is_parallel) {
  int n_threads = is_parallel ? n_threads_ : 1;
  if (is_master) {
    messages_.ParallelProcess<std::pair<vid_t, value_t>>(
        n_threads, [this](int tid, const std::pair<vid_t, value_t>& msg) {
          this->processMasterMessage(msg.first, msg.second);
        });
  } else {
    messages_.ParallelProcess<std::pair<vid_t, value_t>>(
        n_threads, [this](int tid, const std::pair<vid_t, value_t>& msg) {
          this->processMirrorMessage(msg.first, msg.second);
        });
  }
}

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_FLASH_WARE_H_
