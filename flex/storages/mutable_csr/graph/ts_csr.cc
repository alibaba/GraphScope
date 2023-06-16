#include "flex/storages/mutable_csr/graph/ts_csr.h"

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace gs {

grape::InArchive& operator<<(grape::InArchive& in_archive, const TSNbr<std::string>& value) {
  in_archive << value.neighbor << value.timestamp << value.data;
  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive, TSNbr<std::string>& value) {
  out_archive >> value.neighbor >> value.timestamp >> value.data;
  return out_archive;
}

template <typename EDATA_T>
void TSCsr<EDATA_T>::Serialize(const std::string& path) {
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
void TSCsr<EDATA_T>::Deserialize(const std::string& path) {
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

void TSCsr<std::string>::Serialize(const std::string& path) {
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

void TSCsr<std::string>::Deserialize(const std::string& path) {
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
  arc >>nbr_list_;

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
void SingleTSCsr<EDATA_T>::Serialize(const std::string& path) {
  nbr_list_.dump_to_file(path, nbr_list_.size());
}

template <typename EDATA_T>
void SingleTSCsr<EDATA_T>::Deserialize(const std::string& path) {
  nbr_list_.open_for_read(path);
}


template class SingleTSCsr<grape::EmptyType>;
template class TSCsr<grape::EmptyType>;

template class SingleTSCsr<int>;
template class TSCsr<int>;

template class SingleTSCsr<Date>;
template class TSCsr<Date>;

template class SingleTSCsr<std::string>;
// template class TSCsr<std::string>;

template class SingleTSCsr<int64_t>;
template class TSCsr<int64_t>;

}  // namespace gs
