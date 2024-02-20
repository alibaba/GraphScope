/** Copyright 2019-2021 Giulio Ermanno Pibiri
 *
 * The following sets forth attribution notices for third party software.
 *
 * C++ Essentials:
 * The software includes components licensed by Giulio Ermanno Pibiri and
 * Roberto Trani, available at https://github.com/jermp/essentials
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <dirent.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <locale>
#include <numeric>
#include <random>
#include <type_traits>
#include <vector>

#ifdef __GNUG__
#include <cxxabi.h>  // for name demangling
#endif

namespace essentials {

inline void logger(std::string const& msg) {
  time_t t = std::time(nullptr);
  std::locale loc;
  const std::time_put<char>& tp = std::use_facet<std::time_put<char>>(loc);
  const char* fmt = "%F %T";
  tp.put(std::cout, std::cout, ' ', std::localtime(&t), fmt, fmt + strlen(fmt));
  std::cout << ": " << msg << std::endl;
}

static const uint64_t GB = 1000 * 1000 * 1000;
static const uint64_t GiB = uint64_t(1) << 30;
static const uint64_t MB = 1000 * 1000;
static const uint64_t MiB = uint64_t(1) << 20;
static const uint64_t KB = 1000;
static const uint64_t KiB = uint64_t(1) << 10;

inline double convert(size_t bytes, uint64_t unit) {
  return static_cast<double>(bytes) / unit;
}

template <typename T>
size_t vec_bytes(T const& vec) {
  return vec.size() * sizeof(vec.front()) + sizeof(typename T::size_type);
}

template <typename T>
size_t pod_bytes(T const& pod) {
  static_assert(std::is_pod<T>::value);
  return sizeof(pod);
}

inline size_t file_size(char const* filename) {
  std::ifstream is(filename, std::ios::binary | std::ios::ate);
  if (!is.good()) {
    throw std::runtime_error(
        "Error in opening binary "
        "file.");
  }
  size_t bytes = (size_t) is.tellg();
  is.close();
  return bytes;
}

template <typename WordType = uint64_t>
uint64_t words_for(uint64_t bits) {
  uint64_t word_bits = sizeof(WordType) * 8;
  return (bits + word_bits - 1) / word_bits;
}

template <typename T>
inline void do_not_optimize_away(T&& value) {
  asm volatile("" : "+r"(value));
}

inline uint64_t maxrss_in_bytes() {
  struct rusage ru;
  if (getrusage(RUSAGE_SELF, &ru) == 0) {
    // NOTE: ru_maxrss is in kilobytes on Linux, but not on Apple...
#ifdef __APPLE__
    return ru.ru_maxrss;
#endif
    return ru.ru_maxrss * 1000;
  }
  return 0;
}

template <typename T>
void load_pod(std::istream& is, T& val) {
  static_assert(std::is_pod<T>::value);
  is.read(reinterpret_cast<char*>(&val), sizeof(T));
}

template <typename T, typename Allocator>
void load_vec(std::istream& is, std::vector<T, Allocator>& vec) {
  size_t n;
  load_pod(is, n);
  vec.resize(n);
  is.read(reinterpret_cast<char*>(vec.data()),
          static_cast<std::streamsize>(sizeof(T) * n));
}

template <typename T>
void save_pod(std::ostream& os, T const& val) {
  static_assert(std::is_pod<T>::value);
  os.write(reinterpret_cast<char const*>(&val), sizeof(T));
}

template <typename T, typename Allocator>
void save_vec(std::ostream& os, std::vector<T, Allocator> const& vec) {
  static_assert(std::is_pod<T>::value);
  size_t n = vec.size();
  save_pod(os, n);
  os.write(reinterpret_cast<char const*>(vec.data()),
           static_cast<std::streamsize>(sizeof(T) * n));
}

struct json_lines {
  struct property {
    property(std::string n, std::string v) : name(n), value(v) {}

    std::string name;
    std::string value;
  };

  void new_line() { m_properties.push_back(std::vector<property>()); }

  template <typename T>
  void add(std::string name, T value) {
    if (!m_properties.size()) {
      new_line();
    }
    if constexpr (std::is_same<T, char const*>::value) {
      m_properties.back().emplace_back(name, value);
    } else {
      m_properties.back().emplace_back(name, std::to_string(value));
    }
  }

  void save_to_file(char const* filename) const {
    std::ofstream out(filename);
    print_to(out);
    out.close();
  }

  void print_line() const { print_line_to(m_properties.back(), std::cerr); }

  void print() const { print_to(std::cerr); }

 private:
  std::vector<std::vector<property>> m_properties;

  template <typename T>
  void print_line_to(std::vector<property> const& properties, T& device) const {
    device << "{";
    for (uint64_t i = 0; i != properties.size(); ++i) {
      auto const& p = properties[i];
      device << "\"" << p.name << "\": \"" << p.value << "\"";
      if (i != properties.size() - 1) {
        device << ", ";
      }
    }
    device << "}\n";
  }

  template <typename T>
  void print_to(T& device) const {
    for (auto const& properties : m_properties) {
      print_line_to(properties, device);
    }
  }
};

template <typename ClockType, typename DurationType>
struct timer {
  void start() { m_start = ClockType::now(); }

  void stop() {
    m_stop = ClockType::now();
    auto elapsed = std::chrono::duration_cast<DurationType>(m_stop - m_start);
    m_timings.push_back(elapsed.count());
  }

  size_t runs() const { return m_timings.size(); }

  void reset() { m_timings.clear(); }

  double min() const {
    return *std::min_element(m_timings.begin(), m_timings.end());
  }

  double max() const {
    return *std::max_element(m_timings.begin(), m_timings.end());
  }

  void discard_first() {
    if (runs()) {
      m_timings.erase(m_timings.begin());
    }
  }

  void discard_min() {
    if (runs() > 1) {
      m_timings.erase(std::min_element(m_timings.begin(), m_timings.end()));
    }
  }

  void discard_max() {
    if (runs() > 1) {
      m_timings.erase(std::max_element(m_timings.begin(), m_timings.end()));
    }
  }

  double elapsed() {
    return std::accumulate(m_timings.begin(), m_timings.end(), 0.0);
  }

  double average() { return elapsed() / runs(); }

 private:
  typename ClockType::time_point m_start;
  typename ClockType::time_point m_stop;
  std::vector<double> m_timings;
};

typedef std::chrono::high_resolution_clock clock_type;
typedef std::chrono::microseconds duration_type;
typedef timer<clock_type, duration_type> timer_type;

inline unsigned get_random_seed() {
  return std::chrono::system_clock::now().time_since_epoch().count();
}

template <typename IntType>
struct uniform_int_rng {
  uniform_int_rng(IntType from, IntType to, unsigned seed = 13)
      : m_rng(seed), m_distr(from, to) {}

  IntType gen() { return m_distr(m_rng); }

 private:
  std::mt19937_64 m_rng;
  std::uniform_int_distribution<IntType> m_distr;
};

struct loader {
  loader(char const* filename)
      : m_num_bytes_pods(0),
        m_num_bytes_vecs_of_pods(0),
        m_is(filename, std::ios::binary) {
    if (!m_is.good()) {
      throw std::runtime_error(
          "Error in opening binary "
          "file.");
    }
  }

  ~loader() { m_is.close(); }

  template <typename T>
  void visit(T& val) {
    if constexpr (std::is_pod<T>::value) {
      load_pod(m_is, val);
      m_num_bytes_pods += pod_bytes(val);
    } else {
      val.visit(*this);
    }
  }

  template <typename T, typename Allocator>
  void visit(std::vector<T, Allocator>& vec) {
    size_t n;
    visit(n);
    vec.resize(n);
    if constexpr (std::is_pod<T>::value) {
      m_is.read(reinterpret_cast<char*>(vec.data()),
                static_cast<std::streamsize>(sizeof(T) * n));
      m_num_bytes_vecs_of_pods += n * sizeof(T);
    } else {
      for (auto& v : vec)
        visit(v);
    }
  }

  size_t bytes() { return m_is.tellg(); }

  size_t bytes_pods() { return m_num_bytes_pods; }

  size_t bytes_vecs_of_pods() { return m_num_bytes_vecs_of_pods; }

 private:
  size_t m_num_bytes_pods;
  size_t m_num_bytes_vecs_of_pods;
  std::ifstream m_is;
};

struct saver {
  saver(char const* filename) : m_os(filename, std::ios::binary) {
    if (!m_os.good()) {
      throw std::runtime_error(
          "Error in opening binary "
          "file.");
    }
  }

  ~saver() { m_os.close(); }

  template <typename T>
  void visit(T& val) {
    if constexpr (std::is_pod<T>::value) {
      save_pod(m_os, val);
    } else {
      val.visit(*this);
    }
  }

  template <typename T, typename Allocator>
  void visit(std::vector<T, Allocator>& vec) {
    if constexpr (std::is_pod<T>::value) {
      save_vec(m_os, vec);
    } else {
      size_t n = vec.size();
      visit(n);
      for (auto& v : vec)
        visit(v);
    }
  }

  size_t bytes() { return m_os.tellp(); }

 private:
  std::ofstream m_os;
};

inline std::string demangle(char const* mangled_name) {
  size_t len = 0;
  int status = 0;
  std::unique_ptr<char, decltype(&std::free)> ptr(
      __cxxabiv1::__cxa_demangle(mangled_name, nullptr, &len, &status),
      &std::free);
  return ptr.get();
}

struct sizer {
  sizer(std::string const& root_name = "")
      : m_root(0, 0, root_name), m_current(&m_root) {}

  struct node {
    node(size_t b, size_t d, std::string const& n = "")
        : bytes(b), depth(d), name(n) {}

    size_t bytes;
    size_t depth;
    std::string name;
    std::vector<node> children;
  };

  template <typename T>
  void visit(T& val) {
    if constexpr (std::is_pod<T>::value) {
      node n(pod_bytes(val), m_current->depth + 1, demangle(typeid(T).name()));
      m_current->children.push_back(n);
      m_current->bytes += n.bytes;
    } else {
      val.visit(*this);
    }
  }

  template <typename T, typename Allocator>
  void visit(std::vector<T, Allocator>& vec) {
    if constexpr (std::is_pod<T>::value) {
      node n(vec_bytes(vec), m_current->depth + 1,
             demangle(typeid(std::vector<T>).name()));
      m_current->children.push_back(n);
      m_current->bytes += n.bytes;
    } else {
      size_t n = vec.size();
      m_current->bytes += pod_bytes(n);
      node* parent = m_current;
      for (auto& v : vec) {
        node n(0, parent->depth + 1, demangle(typeid(T).name()));
        parent->children.push_back(n);
        m_current = &parent->children.back();
        visit(v);
        parent->bytes += m_current->bytes;
      }
      m_current = parent;
    }
  }

  template <typename Device>
  void print(node const& n, size_t total_bytes, Device& device) const {
    auto indent = std::string(n.depth * 4, ' ');
    device << indent << "'" << n.name << "' - bytes = " << n.bytes << " ("
           << n.bytes * 100.0 / total_bytes << "%)" << std::endl;
    for (auto const& child : n.children) {
      device << indent;
      print(child, total_bytes, device);
    }
  }

  template <typename Device>
  void print(Device& device) const {
    print(m_root, bytes(), device);
  }

  size_t bytes() const { return m_root.bytes; }

 private:
  node m_root;
  node* m_current;
};

template <typename T>
struct allocator : std::allocator<T> {
  typedef T value_type;

  allocator() : m_addr(nullptr) {}

  allocator(T* addr) : m_addr(addr) {}

  T* allocate(size_t n) {
    if (m_addr == nullptr)
      return std::allocator<T>::allocate(n);
    return m_addr;
  }

  void deallocate(T* p, size_t n) {
    if (m_addr == nullptr)
      return std::allocator<T>::deallocate(p, n);
  }

 private:
  T* m_addr;
};

struct contiguous_memory_allocator {
  contiguous_memory_allocator() : m_begin(nullptr), m_end(nullptr), m_size(0) {}

  struct visitor {
    visitor(uint8_t* begin, size_t size, char const* filename)
        : m_begin(begin),
          m_end(begin),
          m_size(size),
          m_is(filename, std::ios::binary) {
      if (!m_is.good()) {
        throw std::runtime_error(
            "Error in opening binary "
            "file.");
      }
    }

    ~visitor() { m_is.close(); }

    template <typename T>
    void visit(T& val) {
      if constexpr (std::is_pod<T>::value) {
        load_pod(m_is, val);
      } else {
        val.visit(*this);
      }
    }

    template <typename T, typename Allocator>
    void visit(std::vector<T, Allocator>& vec) {
      if constexpr (std::is_pod<T>::value) {
        vec = std::vector<T, Allocator>(make_allocator<T>());
        load_vec(m_is, vec);
        consume(vec.size() * sizeof(T));
      } else {
        size_t n;
        visit(n);
        vec.resize(n);
        for (auto& v : vec)
          visit(v);
      }
    }

    uint8_t* end() { return m_end; }

    size_t size() const { return m_size; }

    size_t allocated() const {
      assert(m_end >= m_begin);
      return m_end - m_begin;
    }

    template <typename T>
    allocator<T> make_allocator() {
      return allocator<T>(reinterpret_cast<T*>(m_end));
    }

    void consume(size_t num_bytes) {
      if (m_end == nullptr)
        return;
      if (allocated() + num_bytes > size()) {
        throw std::runtime_error("allocation failed");
      }
      m_end += num_bytes;
    }

   private:
    uint8_t* m_begin;
    uint8_t* m_end;
    size_t m_size;
    std::ifstream m_is;
  };

  template <typename T>
  size_t allocate(T& data_structure, char const* filename) {
    loader l(filename);
    l.visit(data_structure);
    m_size = l.bytes_vecs_of_pods();
    m_begin = reinterpret_cast<uint8_t*>(malloc(m_size));
    if (m_begin == nullptr)
      throw std::runtime_error("malloc failed");
    visitor v(m_begin, m_size, filename);
    v.visit(data_structure);
    m_end = v.end();
    return l.bytes();
  }

  ~contiguous_memory_allocator() { free(m_begin); }

  uint8_t* begin() { return m_begin; }

  uint8_t* end() { return m_end; }

  size_t size() const { return m_size; }

 private:
  uint8_t* m_begin;
  uint8_t* m_end;
  size_t m_size;
};

template <typename T, typename Visitor>
size_t visit(T& data_structure, char const* filename) {
  Visitor visitor(filename);
  visitor.visit(data_structure);
  return visitor.bytes();
}

template <typename T>
size_t load(T& data_structure, char const* filename) {
  return visit<T, loader>(data_structure, filename);
}

template <typename T>
size_t load_with_custom_memory_allocation(T& data_structure,
                                          char const* filename) {
  return data_structure.get_allocator().allocate(data_structure, filename);
}

template <typename T>
size_t save(T& data_structure, char const* filename) {
  return visit<T, saver>(data_structure, filename);
}

template <typename T, typename Device>
size_t print_size(T& data_structure, Device& device) {
  sizer visitor(demangle(typeid(T).name()));
  visitor.visit(data_structure);
  visitor.print(device);
  return visitor.bytes();
}

#if defined(__CYGWIN__) || defined(_WIN32) || defined(_WIN64)
#else
struct directory {
  struct file_name {
    std::string name;
    std::string fullpath;
    std::string extension;
  };

  ~directory() {
    for (int i = 0; i != items(); ++i) {
      free(m_items_names[i]);
    }
    free(m_items_names);
  }

  directory(std::string const& name) : m_name(name) {
    m_n = scandir(m_name.c_str(), &m_items_names, NULL, alphasort);
    if (m_n < 0) {
      throw std::runtime_error("error during scandir");
    }
  }

  std::string const& name() const { return m_name; }

  int items() const { return m_n; }

  struct iterator {
    iterator(directory const* d, int i) : m_d(d), m_i(i) {}

    file_name operator*() {
      file_name fn;
      fn.name = m_d->m_items_names[m_i]->d_name;
      fn.fullpath = m_d->name() + "/" + fn.name;
      size_t p = fn.name.find_last_of(".");
      fn.extension = fn.name.substr(p + 1);
      return fn;
    }

    void operator++() { ++m_i; }

    bool operator==(iterator const& rhs) const { return m_i == rhs.m_i; }

    bool operator!=(iterator const& rhs) const { return !(*this == rhs); }

   private:
    directory const* m_d;
    int m_i;
  };

  iterator begin() { return iterator(this, 0); }

  iterator end() { return iterator(this, items()); }

 private:
  std::string m_name;
  struct dirent** m_items_names;
  int m_n;
};
#endif

inline bool create_directory(std::string const& name) {
  if (mkdir(name.c_str(), 0777) != 0) {
    if (errno == EEXIST) {
      std::cerr << "directory already exists" << std::endl;
    }
    return false;
  }
  return true;
}

inline bool remove_directory(std::string const& name) {
  return rmdir(name.c_str()) == 0;
}

}  // namespace essentials