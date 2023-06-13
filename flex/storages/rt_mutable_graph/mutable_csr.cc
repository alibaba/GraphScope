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

#include "flex/storages/rt_mutable_graph/mutable_csr.h"

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace gs {

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const MutableNbr<std::string>& value) {
  in_archive << value.neighbor << value.timestamp.load() << value.data;
  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              MutableNbr<std::string>& value) {
  timestamp_t ts;
  out_archive >> value.neighbor >> ts >> value.data;
  value.timestamp.store(ts);
  return out_archive;
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::Serialize(const std::string& path) {
  std::vector<int> size_list;
  for (vid_t i = 0; i < capacity_; ++i) {
    size_list.push_back(adj_lists_[i].size());
  }
  {
    size_t size_list_size = size_list.size();
    std::string degree_file_path = path + ".degree";
    FILE* fout = fopen(degree_file_path.c_str(), "wb");
    CHECK_EQ(fwrite(&size_list_size, sizeof(size_t), 1, fout), 1);
    CHECK_EQ(fwrite(size_list.data(), sizeof(int), size_list_size, fout),
             size_list_size);
    fflush(fout);
    fclose(fout);
  }

  init_nbr_list_.dump_to_file(path + ".nbr_list", init_nbr_list_.size());
}

template <typename EDATA_T>
void MutableCsr<EDATA_T>::Deserialize(const std::string& path) {
  size_t size_list_size;
  std::vector<int> size_list;
  {
    std::string degree_file_path = path + ".degree";
    FILE* fin = fopen(degree_file_path.c_str(), "r");
    CHECK_EQ(fread(&size_list_size, sizeof(size_t), 1, fin), 1);
    size_list.resize(size_list_size);
    CHECK_EQ(fread(size_list.data(), sizeof(int), size_list_size, fin),
             size_list_size);
    fclose(fin);
  }

  init_nbr_list_.open_for_read(path + ".nbr_list");
  capacity_ = size_list_size;
  adj_lists_ = static_cast<adjlist_t*>(malloc(sizeof(adjlist_t) * capacity_));
  locks_ = new grape::SpinLock[capacity_];
  nbr_t* ptr = init_nbr_list_.data();
  for (vid_t i = 0; i < capacity_; ++i) {
    size_t cur_cap = size_list[i] + (size_list[i] + 4) / 5;
    adj_lists_[i].init(ptr, cur_cap, size_list[i]);
    ptr += cur_cap;
  }
}

void MutableCsr<std::string>::Serialize(const std::string& path) {
  std::vector<int> size_list;
  for (vid_t i = 0; i < capacity_; ++i) {
    size_list.push_back(adj_lists_[i].size());
  }
  {
    size_t size_list_size = size_list.size();
    std::string degree_file_path = path + ".degree";
    FILE* fout = fopen(degree_file_path.c_str(), "wb");
    CHECK_EQ(fwrite(&size_list_size, sizeof(size_t), 1, fout), 1);
    CHECK_EQ(fwrite(size_list.data(), sizeof(int), size_list_size, fout),
             size_list_size);
    fflush(fout);
    fclose(fout);
  }

  grape::InArchive arc;
  arc << nbr_list_;
  {
    size_t arc_size = arc.GetSize();
    std::string nbr_list_file_path = path + ".nbr_list";
    FILE* fout = fopen(nbr_list_file_path.c_str(), "wb");
    CHECK_EQ(fwrite(&arc_size, sizeof(size_t), 1, fout), 1);
    CHECK_EQ(fwrite(arc.GetBuffer(), 1, arc_size, fout), arc_size);
    fflush(fout);
    fclose(fout);
  }
}

void MutableCsr<std::string>::Deserialize(const std::string& path) {
  size_t size_list_size;
  std::vector<int> size_list;
  {
    std::string degree_file_path = path + ".degree";
    FILE* fin = fopen(degree_file_path.c_str(), "r");
    CHECK_EQ(fread(&size_list_size, sizeof(size_t), 1, fin), 1);
    size_list.resize(size_list_size);
    CHECK_EQ(fread(size_list.data(), sizeof(int), size_list_size, fin),
             size_list_size);
    fclose(fin);
  }

  grape::OutArchive arc;
  {
    std::string nbr_list_file_path = path + ".nbr_list";
    FILE* fin = fopen(nbr_list_file_path.c_str(), "r");
    size_t arc_size;
    CHECK_EQ(fread(&arc_size, sizeof(size_t), 1, fin), 1);
    arc.Allocate(arc_size);
    CHECK_EQ(fread(arc.GetBuffer(), 1, arc_size, fin), arc_size);
    fclose(fin);
  }
  arc >> nbr_list_;

  capacity_ = size_list_size;
  adj_lists_ = static_cast<adjlist_t*>(malloc(sizeof(adjlist_t) * capacity_));
  locks_ = new grape::SpinLock[capacity_];
  nbr_t* ptr = nbr_list_.data();
  for (vid_t i = 0; i < capacity_; ++i) {
    size_t cur_cap = size_list[i] + (size_list[i] + 4) / 5;
    adj_lists_[i].init(ptr, cur_cap, size_list[i]);
    ptr += cur_cap;
  }
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::Serialize(const std::string& path) {
  nbr_list_.dump_to_file(path, nbr_list_.size());
}

template <typename EDATA_T>
void SingleMutableCsr<EDATA_T>::Deserialize(const std::string& path) {
  nbr_list_.open_for_read(path);
}

template class SingleMutableCsr<grape::EmptyType>;
template class MutableCsr<grape::EmptyType>;

template class SingleMutableCsr<int>;
template class MutableCsr<int>;

template class SingleMutableCsr<Date>;
template class MutableCsr<Date>;

template class SingleMutableCsr<std::string>;
template class MutableCsr<std::string>;

template class SingleMutableCsr<int64_t>;
template class MutableCsr<int64_t>;

}  // namespace gs
