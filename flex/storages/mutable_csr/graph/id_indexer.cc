#include "flex/storages/mutable_csr/graph/id_indexer.h"
#include "flex/storages/mutable_csr/types.h"
#include "grape/io/local_io_adaptor.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace gs {

template <typename INDEX_T>
void LFIndexer<INDEX_T>::Serialize(const std::string& prefix) {
  {
    grape::InArchive arc;
    arc << keys_.size() << indices_.size();
    arc << hash_policy_.get_mod_function_index() << num_elements_.load()
        << num_slots_minus_one_ << indices_size_;
    std::string meta_file_path = prefix + ".meta";
    FILE* fout = fopen(meta_file_path.c_str(), "wb");
    fwrite(arc.GetBuffer(), sizeof(char), arc.GetSize(), fout);
    fflush(fout);
    fclose(fout);
  }

  if (keys_.size() > 0) {
    keys_.dump_to_file(prefix + ".keys");
  }
  if (indices_.size() > 0) {
    indices_.dump_to_file(prefix + ".indices");
  }
}

template <typename INDEX_T>
void LFIndexer<INDEX_T>::Deserialize(const std::string& prefix) {
  size_t keys_size, indices_size;
  size_t mod_function_index;
  size_t num_elements;
  {
    std::string meta_file_path = prefix + ".meta";
    size_t meta_file_size = std::filesystem::file_size(meta_file_path);
    std::vector<char> buf(meta_file_size);
    FILE* fin = fopen(meta_file_path.c_str(), "r");
    CHECK_EQ(fread(buf.data(), sizeof(char), meta_file_size, fin),
             meta_file_size);
    grape::OutArchive arc;
    arc.SetSlice(buf.data(), meta_file_size);

    arc >> keys_size >> indices_size;
    arc >> mod_function_index >> num_elements >> num_slots_minus_one_ >>
        indices_size_;
  }
  keys_.open_for_read(prefix + ".keys");
  CHECK_EQ(keys_.size(), keys_size);
  indices_.open_for_read(prefix + ".indices");
  CHECK_EQ(indices_.size(), indices_size);
  hash_policy_.set_mod_function_by_index(mod_function_index);
  num_elements_.store(num_elements);
}

template class LFIndexer<vid_t>;

template <typename KEY_T, typename INDEX_T>
void IdIndexer<KEY_T, INDEX_T>::Serialize(
    std::unique_ptr<grape::LocalIOAdaptor>& writer) {
  id_indexer_impl::KeyBuffer<KEY_T>::serialize(writer, keys_);
  grape::InArchive arc;
  arc << hash_policy_.get_mod_function_index() << max_lookups_ << num_elements_
      << num_slots_minus_one_ << indices_.size() << distances_.size();
  CHECK(writer->WriteArchive(arc));
  arc.Clear();

  if (indices_.size() > 0) {
    CHECK(writer->Write(indices_.data(), indices_.size() * sizeof(INDEX_T)));
  }
  if (distances_.size() > 0) {
    CHECK(writer->Write(distances_.data(), distances_.size() * sizeof(int8_t)));
  }
}

template <typename KEY_T, typename INDEX_T>
void IdIndexer<KEY_T, INDEX_T>::Deserialize(
    std::unique_ptr<grape::LocalIOAdaptor>& reader) {
  id_indexer_impl::KeyBuffer<KEY_T>::deserialize(reader, keys_);
  grape::OutArchive arc;
  CHECK(reader->ReadArchive(arc));
  size_t mod_function_index;
  size_t indices_size, distances_size;
  arc >> mod_function_index >> max_lookups_ >> num_elements_ >>
      num_slots_minus_one_ >> indices_size >> distances_size;
  arc.Clear();

  hash_policy_.set_mod_function_by_index(mod_function_index);
  indices_.resize(indices_size);
  distances_.resize(distances_size);
  if (indices_size > 0) {
    CHECK(reader->Read(indices_.data(), indices_.size() * sizeof(INDEX_T)));
  }
  if (distances_size > 0) {
    CHECK(reader->Read(distances_.data(), distances_.size() * sizeof(int8_t)));
  }
  LOG(INFO) << "indices: " << indices_.size()
            << ", distances: " << distances_.size()
            << ", keys: " << keys_.size();
}

template class IdIndexer<oid_t, vid_t>;
template class IdIndexer<std::string, vid_t>;
template class IdIndexer<std::string, int>;
template class IdIndexer<std::string, uint8_t>;

}  // namespace gs
