#ifndef APSARA_COMMON_STRING_PIECE_H
#define APSARA_COMMON_STRING_PIECE_H

#include <stddef.h>
#include <algorithm>
#include <stdexcept>

namespace apsara {
namespace common {

namespace detail {

template <typename CharT>
class StringPieceDetail {
 public:
  // for standard STL container
  typedef size_t size_type;
  typedef CharT value_type;
  typedef const value_type* pointer;
  typedef const value_type& reference;
  typedef const value_type& const_reference;
  typedef ptrdiff_t difference_type;
  typedef const value_type* const_iterator;
  typedef std::reverse_iterator<const_iterator> const_reverse_iterator;

  static const size_type npos = -1;

  typedef std::basic_string<CharT> string_type;
  typedef typename string_type::traits_type traits_type;

 public:
  StringPieceDetail(const value_type* str = NULL)
      : mStr(str), mLength((str == NULL) ? 0 : traits_type::length(str)) {}
  StringPieceDetail(const string_type& str)
      : mStr(str.data()), mLength(str.size()) {}
  StringPieceDetail(const value_type* str, size_type len)
      : mStr(str), mLength(len) {}
  StringPieceDetail(const typename string_type::const_iterator& begin,
                    const typename string_type::const_iterator& end)
      : mStr((end > begin) ? &(*begin) : NULL),
        mLength((end > begin) ? (size_type) (end - begin) : 0) {}

  bool operator==(const StringPieceDetail& s) const {
    return this == &s || this->compare(s) == 0;
  }

  // Iterators
  const_iterator begin() const { return data(); }
  const_iterator end() const { return data() + size(); }
  const_reverse_iterator rbegin() const {
    return const_reverse_iterator(data() + size());
  }
  const_reverse_iterator rend() const { return const_reverse_iterator(data()); }

  // Capacity
  size_type size() const { return mLength; }
  size_type length() const { return size(); }
  size_type max_size() const { return size(); }
  size_type capacity() const { return size(); }
  void clear() {
    mStr = NULL;
    mLength = 0;
  }
  bool empty() const { return size() == 0; }

  // Element access
  value_type operator[](size_type pos) const { return data()[pos]; }
  value_type at(size_type pos) const {
    if (pos >= size()) {
      throw std::out_of_range("pos is out of range");
    }
    return data()[pos];
  }

  // Modifiers
  void swap(StringPieceDetail& sp) {
    std::swap(sp.mStr, mStr);
    std::swap(sp.mLength, mLength);
  }

  // String Operations
  const value_type* c_str() const { return mStr; }
  // data() may return a pointer to a buffer with embedded NULs, and the
  // returned buffer may or may not be null terminated.  Therefore it is
  // typically a mistake to pass data() to a routine that expects a NUL
  // terminated string.
  const value_type* data() const { return mStr; }

  // find
  size_type find(const StringPieceDetail& sp, size_type pos = 0) const {
    return find(sp.data(), pos, sp.size());
  }
  size_type find(const value_type* s, size_type pos = 0) const {
    StringPieceDetail sp(s);
    return find(sp, pos);
  }
  size_type find(const value_type* s, size_type pos, size_type n) const {
    size_type ret = npos;
    const size_type size = this->size();
    if (pos <= size && n <= size - pos) {
      const value_type* data = this->data();
      const value_type* p = std::search(data + pos, data + size, s, s + n);
      if (p != data + size || n == 0) {
        ret = p - data;
      }
    }
    return ret;
  }
  size_type find(const value_type c, size_type pos = 0) const {
    size_type ret = npos;
    const size_type size = this->size();
    if (pos < size) {
      const value_type* data = this->data();
      const size_type n = size - pos;
      const value_type* p = traits_type::find(data + pos, n, c);
      if (p != 0) {
        ret = p - data;
      }
    }
    return ret;
  }
  // rfind
  size_type rfind(const StringPieceDetail& sp, size_type pos = npos) const {
    return rfind(sp.data(), pos, sp.size());
  }
  size_type rfind(const value_type* s, size_type pos = npos) const {
    StringPieceDetail sp(s);
    return rfind(sp, pos);
  }
  size_type rfind(const value_type* s, size_type pos, size_type n) const {
    const size_type size = this->size();
    if (n <= size) {
      pos = std::min(size_type(size - n), pos);
      const value_type* data = this->data();
      do {
        if (traits_type::compare(data + pos, s, n) == 0) {
          return pos;
        }
      } while (pos-- > 0);
    }
    return npos;
  }
  size_type rfind(const value_type c, size_type pos = npos) const {
    size_type size = this->size();
    if (size) {
      if (--size > pos) {
        size = pos;
      }
      for (++size; size-- > 0;) {
        if (traits_type::eq(data()[size], c)) {
          return size;
        }
      }
    }
    return npos;
  }
  // find_first_of
  size_type find_first_of(const StringPieceDetail& s, size_type pos = 0) const {
    return find_first_of(s.data(), pos, s.size());
  }
  size_type find_first_of(const value_type* s, size_type pos = 0) const {
    StringPieceDetail sp(s);
    return find_first_of(sp, pos);
  }
  size_type find_first_of(const value_type* s, size_type pos,
                          size_type n) const {
    for (; n && pos < size(); ++pos) {
      const value_type* p = traits_type::find(s, n, data()[pos]);
      if (p) {
        return pos;
      }
    }
    return npos;
  }
  size_type find_first_of(value_type c, size_type pos = 0) const {
    return find(c, pos);
  }
  // find_last_of
  size_type find_last_of(const StringPieceDetail& s,
                         size_type pos = npos) const {
    return find_last_of(s.data(), pos, s.size());
  }
  size_type find_last_of(const value_type* s, size_type pos = npos) const {
    StringPieceDetail sp(s);
    return find_last_of(sp, pos);
  }
  size_type find_last_of(const value_type* s, size_type pos,
                         size_type n) const {
    size_type index = size();
    if (index && n) {
      if (--index > pos) {
        index = pos;
      }
      do {
        if (traits_type::find(s, n, data()[index])) {
          return index;
        }
      } while (index-- != 0);
    }
    return npos;
  }
  size_type find_last_of(value_type c, size_type pos = npos) const {
    return rfind(c, pos);
  }
  // find_first_not_of
  size_type find_first_not_of(const StringPieceDetail& s,
                              size_type pos = 0) const {
    return find_first_not_of(s.data(), pos, s.size());
  }
  size_type find_first_not_of(const value_type* s, size_type pos = 0) const {
    StringPieceDetail sp(s);
    return find_first_not_of(sp, pos);
  }
  size_type find_first_not_of(const value_type* s, size_type pos,
                              size_type n) const {
    for (; pos < size(); ++pos) {
      if (!traits_type::find(s, n, data()[pos])) {
        return pos;
      }
    }
    return npos;
  }
  size_type find_first_not_of(value_type c, size_type pos = 0) const {
    StringPieceDetail sp(&c, 1);
    return find_first_not_of(sp, pos);
  }
  // find_last_not_of
  size_type find_last_not_of(const StringPieceDetail& s,
                             size_type pos = npos) const {
    return find_last_not_of(s.data(), pos, s.size());
  }
  size_type find_last_not_of(const value_type* s, size_type pos = npos) const {
    StringPieceDetail sp(s);
    return find_last_not_of(s, pos);
  }
  size_type find_last_not_of(const value_type* s, size_type pos,
                             size_type n) const {
    size_type index = size();
    if (index) {
      if (--index > pos) {
        index = pos;
      }
      do {
        if (!traits_type::find(s, n, data()[index])) {
          return index;
        }
      } while (index--);
    }
    return npos;
  }
  size_type find_last_not_of(value_type c, size_type pos = npos) const {
    StringPieceDetail sp(&c, 1);
    return find_last_not_of(sp, pos);
  }

  void set(const value_type* data, size_type len) {
    mStr = data;
    mLength = len;
  }
  void set(const value_type* str) {
    mStr = str;
    mLength = str ? traits_type::length(str) : 0;
  }
  void set(const string_type& str) {
    mStr = str.data();
    mLength = str.size();
  }

  void remove_prefix(size_type n) {
    if (mLength < n) {
      throw std::out_of_range("invalid parameter");
    }
    mStr += n;
    mLength -= n;
  }
  void remove_suffix(size_type n) {
    if (mLength < n) {
      throw std::out_of_range("invalid parameter");
    }
    mLength -= n;
  }

  StringPieceDetail substr(size_t pos = 0, size_t len = npos) const {
    if (pos > size()) {
      throw std::out_of_range("pos is out of range");
    }
    const value_type* p = data() + pos;
    if (len == npos) {
      len = size() - pos;
    } else {
      len = std::min(size() - pos, len);
    }
    return StringPieceDetail(p, len);
  }
  int compare(const StringPieceDetail& s) const {
    const size_type this_size = size();
    const size_type other_size = s.size();
    const size_type len = std::min(this_size, other_size);

    int r = traits_type::compare(data(), s.data(), len);
    if (r == 0) {
      r = this_size - other_size;
    }
    return r;
  }

  string_type as_string() const {
    return empty() ? string_type() : string_type(data(), size());
  }

 private:
  const value_type* mStr;
  size_type mLength;
};

}  // namespace detail

typedef detail::StringPieceDetail<char> StringPiece;
// typedef detail::StringPieceDetail<wchar_t> StringPiece16;

}  // namespace common
}  // namespace apsara

#endif  // APSARA_COMMON_STRING_PIECE_H
