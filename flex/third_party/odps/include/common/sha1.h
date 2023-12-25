/*
 * This implementation is consistent with the Secure Hash Signature
 * Standard (SHS) (FIPS PUB 180-2). This Standard specifies four secure
 * hash algorithms - SHA-1, SHA-256, SHA-384, and SHA-512 - for computing
 * a condensed representation of electronic data (message). When a message
 * of any length < 2^64 bits (for SHA-1 and SHA-256) or < 2^128 bits (for
 * SHA-384 and SHA-512) is input to an algorithm, the result is an output
 * called a message digest.
 *
 * Currently we only support SHA-1. Other members in SHA family will be
 * supported in future. Any problem please contact jun.li@alibaba-inc.com
 */

#ifndef APSARA_SECURITY_SHA1_H
#define APSARA_SECURITY_SHA1_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

namespace apsara {
namespace security {
#define SHA1_INPUT_WORDS 16
#define SHA1_DIGEST_WORDS 5
#define SHA1_INPUT_BYTES (SHA1_INPUT_WORDS * sizeof(uint32_t))
#define SHA1_DIGEST_BYTES (SHA1_DIGEST_WORDS * sizeof(uint32_t))

#define BIT_COUNT_WORDS 2
#define BIT_COUNT_BYTES (BIT_COUNT_WORDS * sizeof(uint32_t))

class SHA1 {
 public:
  SHA1() : bits(0) { memcpy(H, IV, sizeof(H)); }
  SHA1(const SHA1& s) {
    bits = s.bits;
    memcpy(H, s.H, sizeof(H));
    memcpy(M, s.M, sizeof(M));
  }
  void init() {
    bits = 0;
    memcpy(H, IV, sizeof(H));
  }
  void add(const uint8_t* data, size_t len);
  uint8_t* result();

 private:
  uint64_t bits;
  uint32_t H[SHA1_DIGEST_WORDS];
  uint8_t M[SHA1_INPUT_BYTES];

  /*
   * Define constant of the initial vector
   */
  uint32_t IV[SHA1_DIGEST_WORDS] = {0x67452301, 0xEFCDAB89, 0x98BADCFE,
                                    0x10325476, 0xC3D2E1F0};
  void transform();
};

}  // end of namespace security
}  // end of namespace apsara

#ifdef __linux__
#include <asm/byteorder.h>
#else
uint32_t __cpu_to_be32(uint32_t x) { return x; }
#endif

namespace apsara {
namespace security {

/*
 * define the rotate left (circular left shift) operation
 */
#define rotl(v, b) (((v) << (b)) | ((v) >> (32 - (b))))

/*
 * Define the basic SHA-1 functions F1 ~ F4. Note that the exclusive-OR
 * operation (^) in F1 and F3 may be replaced by a bitwise OR operation
 * (|), which produce identical results.
 *
 * F1 is used in ROUND  0~19, F2 is used in ROUND 20~39
 * F3 is used in ROUND 40~59, F4 is used in ROUND 60~79
 */
#define F1(B, C, D) (((B) & (C)) ^ (~(B) & (D)))
#define F2(B, C, D) ((B) ^ (C) ^ (D))
#define F3(B, C, D) (((B) & (C)) ^ ((B) & (D)) ^ ((C) & (D)))
#define F4(B, C, D) ((B) ^ (C) ^ (D))

/*
 * Use different K in different ROUND
 */
#define K00_19 0x5A827999
#define K20_39 0x6ED9EBA1
#define K40_59 0x8F1BBCDC
#define K60_79 0xCA62C1D6

/*
 * Another implementation of the ROUND transformation:
 * (here the T is a temp variable)
 * For t=0 to 79:
 * {
 *     T=rotl(A,5)+Func(B,C,D)+K+W[t]+E;
 *     E=D; D=C; C=rotl(B,30); B=A; A=T;
 * }
 */
#define ROUND(t, A, B, C, D, E, Func, K)      \
  E += rotl(A, 5) + Func(B, C, D) + W[t] + K; \
  B = rotl(B, 30);

#define ROUND5(t, Func, K)              \
  ROUND(t, A, B, C, D, E, Func, K);     \
  ROUND(t + 1, E, A, B, C, D, Func, K); \
  ROUND(t + 2, D, E, A, B, C, Func, K); \
  ROUND(t + 3, C, D, E, A, B, Func, K); \
  ROUND(t + 4, B, C, D, E, A, Func, K)

#define ROUND20(t, Func, K) \
  ROUND5(t, Func, K);       \
  ROUND5(t + 5, Func, K);   \
  ROUND5(t + 10, Func, K);  \
  ROUND5(t + 15, Func, K)

/*
 * the message must be the big-endian32 (or left-most word)
 * before calling the transform() function.
 */
const static uint32_t iii = 1;
const static bool littleEndian = *(uint8_t*) &iii != 0;
inline void make_big_endian32(uint32_t* data, unsigned n) {
  if (littleEndian)
    for (; n > 0; ++data, --n)
      *data = __cpu_to_be32(*data);
}

inline size_t min(size_t a, size_t b) { return a < b ? a : b; }

inline void SHA1::transform() {
  uint32_t W[80];
  memcpy(W, M, SHA1_INPUT_BYTES);
  memset((uint8_t*) W + SHA1_INPUT_BYTES, 0, sizeof(W) - SHA1_INPUT_BYTES);
  for (unsigned t = 16; t < 80; t++) {
    W[t] = rotl(W[t - 16] ^ W[t - 14] ^ W[t - 8] ^ W[t - 3], 1);
  }

  uint32_t A = H[0];
  uint32_t B = H[1];
  uint32_t C = H[2];
  uint32_t D = H[3];
  uint32_t E = H[4];

  ROUND20(0, F1, K00_19);
  ROUND20(20, F2, K20_39);
  ROUND20(40, F3, K40_59);
  ROUND20(60, F4, K60_79);

  H[0] += A;
  H[1] += B;
  H[2] += C;
  H[3] += D;
  H[4] += E;
}

inline void SHA1::add(const uint8_t* data, size_t data_len) {
  unsigned mlen = (unsigned) ((bits >> 3) % SHA1_INPUT_BYTES);
  bits += (uint64_t) data_len << 3;
  unsigned use = (unsigned) min((size_t)(SHA1_INPUT_BYTES - mlen), data_len);
  memcpy(M + mlen, data, use);
  mlen += use;

  while (mlen == SHA1_INPUT_BYTES) {
    data_len -= use;
    data += use;
    make_big_endian32((uint32_t*) M, SHA1_INPUT_WORDS);
    transform();
    use = (unsigned) min((size_t) SHA1_INPUT_BYTES, data_len);
    memcpy(M, data, use);
    mlen = use;
  }
}

inline uint8_t* SHA1::result() {
  unsigned mlen = (unsigned) ((bits >> 3) % SHA1_INPUT_BYTES),
           padding = SHA1_INPUT_BYTES - mlen;
  M[mlen++] = 0x80;
  if (padding > BIT_COUNT_BYTES) {
    memset(M + mlen, 0x00, padding - BIT_COUNT_BYTES);
    make_big_endian32((uint32_t*) M, SHA1_INPUT_WORDS - BIT_COUNT_WORDS);
  } else {
    memset(M + mlen, 0x00, SHA1_INPUT_BYTES - mlen);
    make_big_endian32((uint32_t*) M, SHA1_INPUT_WORDS);
    transform();
    memset(M, 0x00, SHA1_INPUT_BYTES - BIT_COUNT_BYTES);
  }

  uint64_t temp = littleEndian ? bits << 32 | bits >> 32 : bits;
  memcpy(M + SHA1_INPUT_BYTES - BIT_COUNT_BYTES, &temp, BIT_COUNT_BYTES);
  transform();
  make_big_endian32(H, SHA1_DIGEST_WORDS);
  return (uint8_t*) H;
}

}  // end of namespace security
}  // end of namespace apsara

#endif
