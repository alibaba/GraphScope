#pragma once

#include <immintrin.h>

namespace pthash::util {

template <typename T>
inline void prefetch(T const* ptr) {
    _mm_prefetch(reinterpret_cast<const char*>(ptr), _MM_HINT_T0);
}

inline uint8_t msb(uint64_t x) {
    assert(x);
    unsigned long ret = -1U;
    if (x) { ret = (unsigned long)(63 - __builtin_clzll(x)); }
    return (uint8_t)ret;
}

inline bool bsr64(unsigned long* const index, const uint64_t mask) {
    if (mask) {
        *index = (unsigned long)(63 - __builtin_clzll(mask));
        return true;
    } else {
        return false;
    }
}

inline uint8_t msb(uint64_t x, unsigned long& ret) {
    return bsr64(&ret, x);
}

inline uint8_t lsb(uint64_t x, unsigned long& ret) {
    if (x) {
        ret = (unsigned long)__builtin_ctzll(x);
        return true;
    }
    return false;
}

inline uint8_t lsb(uint64_t x) {
    assert(x);
    unsigned long ret = -1U;
    lsb(x, ret);
    return (uint8_t)ret;
}

inline uint64_t popcount(uint64_t x) {
    return static_cast<uint64_t>(_mm_popcnt_u64(x));
}

inline uint64_t select64_pdep_tzcnt(uint64_t x, const uint64_t k) {
    uint64_t i = 1ULL << k;
    asm("pdep %[x], %[mask], %[x]" : [x] "+r"(x) : [mask] "r"(i));
    asm("tzcnt %[bit], %[index]" : [index] "=r"(i) : [bit] "g"(x) : "cc");
    return i;
}

inline uint64_t select_in_word(const uint64_t x, const uint64_t k) {
    assert(k < popcount(x));
    return select64_pdep_tzcnt(x, k);
}

}  // namespace pthash::util