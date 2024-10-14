/** @file md5.h
 * Define the interface of md5
 * Md5 function can calculate MD5 for a byte stream,
 * the big endian case needs test
 */

#ifndef APSARA_ERRORDETECTION_MD5_H
#define APSARA_ERRORDETECTION_MD5_H

#include <inttypes.h>
#include <stdint.h>
#include <cstring>
#include <fstream>
#include <string>

/* Type define */
typedef unsigned char byte;
typedef unsigned int uint32;

/// define namespace
namespace apsara {
namespace ErrorDetection {

/** Calculate Ms5 for a byte stream,
 * result is stored in md5[16]
 *
 * @param poolIn Input data
 * @param inputBytesNum Length of input data
 * @param md5[16] A 128-bit pool for storing md5
 */
void DoMd5(const uint8_t* poolIn, const uint64_t inputBytesNum,
           uint8_t md5[16]);

/** check correctness of a Md5-calculated byte stream
 *
 * @param poolIn Input data
 * @param inputBytesNum Length of input data
 * @param md5[16] A 128-bit md5 value for checking
 *
 * @return true if no error detected, false if error detected
 */
bool CheckMd5(const uint8_t* poolIn, const uint64_t inputBytesNum,
              const uint8_t md5[16]);

/* MD5 declaration. */
class MD5 {
 public:
  MD5();
  MD5(const void* input, size_t length);
  MD5(const std::string& str);
  MD5(std::ifstream& in);
  void update(const void* input, size_t length);
  void update(const std::string& str);
  void update(std::ifstream& in);
  const byte* digest();
  std::string toString();
  void reset();

 private:
  void update(const byte* input, size_t length);
  void final();
  void transform(const byte block[64]);
  void encode(const uint32* input, byte* output, size_t length);
  void decode(const byte* input, uint32* output, size_t length);
  std::string bytesToHexString(const byte* input, size_t length);

  /* class uncopyable */
  MD5(const MD5&);
  MD5& operator=(const MD5&);

 private:
  uint32 _state[4]; /* state (ABCD) */
  uint32 _count[2]; /* number of bits, modulo 2^64 (low-order word first) */
  byte _buffer[64]; /* input buffer */
  byte _digest[16]; /* message digest */
  bool _finished;   /* calculate finished ? */

  byte PADDING[64] = {0x80};
  char HEX[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                  '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  enum { BUFFER_SIZE = 1024 };
};

/** @brief Md5Stream can receive byte stream as input,
 *  use Put() to put sequential streams,
 *  then call Get() to get md5 for all the input streams
 */
class Md5Stream {
 public:
  /** @brief Constructor
   */
  Md5Stream();

  /** @brief Give an input pool into this class
   *
   *  @param poolIn The input pool
   *  @param inputBytesNum The input bytes number in poolIn
   */
  void Put(const uint8_t* poolIn, const uint64_t inputBytesNum);

  /** @brief Fill the given hash array with md5 value
   *
   *  @param hash The array to get md5 value
   */
  void Get(uint8_t hash[16]);

 private:
  /** @brief Initailize class members
   */
  void Initailize();

  bool mLittleEndian;     /// true if little endian, false if big endian
  uint8_t mBuf[64];       /// hold remained input stream
  uint64_t mBufPosition;  /// indicate the position of stream end in buf
  uint32_t mH[4];         /// hold inter result
  uint64_t mTotalBytes;   /// total bytes
};                        /// class Md5Stream

/* Constants for MD5Transform routine. */
#define S11 7
#define S12 12
#define S13 17
#define S14 22
#define S21 5
#define S22 9
#define S23 14
#define S24 20
#define S31 4
#define S32 11
#define S33 16
#define S34 23
#define S41 6
#define S42 10
#define S43 15
#define S44 21

/* F, G, H and I are basic MD5 functions.
 */
#define F(x, y, z) (((x) & (y)) | ((~x) & (z)))
#define G(x, y, z) (((x) & (z)) | ((y) & (~z)))
#define H(x, y, z) ((x) ^ (y) ^ (z))
#define I(x, y, z) ((y) ^ ((x) | (~z)))

/* ROTATE_LEFT rotates x left n bits.
 */
#define ROTATE_LEFT(x, n) (((x) << (n)) | ((x) >> (32 - (n))))

/* FF, GG, HH, and II transformations for rounds 1, 2, 3, and 4.
Rotation is separate from addition to prevent recomputation.
*/
#define FF(a, b, c, d, x, s, ac)        \
  {                                     \
    (a) += F((b), (c), (d)) + (x) + ac; \
    (a) = ROTATE_LEFT((a), (s));        \
    (a) += (b);                         \
  }
#define GG(a, b, c, d, x, s, ac)        \
  {                                     \
    (a) += G((b), (c), (d)) + (x) + ac; \
    (a) = ROTATE_LEFT((a), (s));        \
    (a) += (b);                         \
  }
#define HH(a, b, c, d, x, s, ac)        \
  {                                     \
    (a) += H((b), (c), (d)) + (x) + ac; \
    (a) = ROTATE_LEFT((a), (s));        \
    (a) += (b);                         \
  }
#define II(a, b, c, d, x, s, ac)        \
  {                                     \
    (a) += I((b), (c), (d)) + (x) + ac; \
    (a) = ROTATE_LEFT((a), (s));        \
    (a) += (b);                         \
  }

/* Default construct. */
inline MD5::MD5() { reset(); }

/* Construct a MD5 object with a input buffer. */
inline MD5::MD5(const void* input, size_t length) {
  reset();
  update(input, length);
}

/* Construct a MD5 object with a string. */
inline MD5::MD5(const std::string& str) {
  reset();
  update(str);
}

/* Construct a MD5 object with a file. */
inline MD5::MD5(std::ifstream& in) {
  reset();
  update(in);
}

/* Return the message-digest */
inline const byte* MD5::digest() {
  if (!_finished) {
    _finished = true;
    final();
  }
  return _digest;
}

/* Reset the calculate state */
inline void MD5::reset() {
  _finished = false;
  /* reset number of bits. */
  _count[0] = _count[1] = 0;
  /* Load magic initialization constants. */
  _state[0] = 0x67452301;
  _state[1] = 0xefcdab89;
  _state[2] = 0x98badcfe;
  _state[3] = 0x10325476;
}

/* Updating the context with a input buffer. */
inline void MD5::update(const void* input, size_t length) {
  update((const byte*) input, length);
}

/* Updating the context with a string. */
inline void MD5::update(const std::string& str) {
  update((const byte*) str.c_str(), str.length());
}

/* Updating the context with a file. */
inline void MD5::update(std::ifstream& in) {
  if (!in) {
    return;
  }

  std::streamsize length;
  char buffer[BUFFER_SIZE];
  while (!in.eof()) {
    in.read(buffer, BUFFER_SIZE);
    length = in.gcount();
    if (length > 0) {
      update(buffer, length);
    }
  }
  in.close();
}

/* MD5 block update operation. Continues an MD5 message-digest
operation, processing another message block, and updating the
context.
*/
inline void MD5::update(const byte* input, size_t length) {
  uint32 i, index, partLen;

  _finished = false;

  /* Compute number of bytes mod 64 */
  index = (uint32)((_count[0] >> 3) & 0x3f);

  /* update number of bits */
  if ((_count[0] += ((uint32) length << 3)) < ((uint32) length << 3)) {
    ++_count[1];
  }
  _count[1] += ((uint32) length >> 29);

  partLen = 64 - index;

  /* transform as many times as possible. */
  if (length >= partLen) {
    memcpy(&_buffer[index], input, partLen);
    transform(_buffer);

    for (i = partLen; i + 63 < length; i += 64) {
      transform(&input[i]);
    }
    index = 0;

  } else {
    i = 0;
  }

  /* Buffer remaining input */
  memcpy(&_buffer[index], &input[i], length - i);
}

/* MD5 finalization. Ends an MD5 message-_digest operation, writing the
the message _digest and zeroizing the context.
*/
inline void MD5::final() {
  byte bits[8];
  uint32 oldState[4];
  uint32 oldCount[2];
  uint32 index, padLen;

  /* Save current state and count. */
  memcpy(oldState, _state, 16);
  memcpy(oldCount, _count, 8);

  /* Save number of bits */
  encode(_count, bits, 8);

  /* Pad out to 56 mod 64. */
  index = (uint32)((_count[0] >> 3) & 0x3f);
  padLen = (index < 56) ? (56 - index) : (120 - index);
  update(PADDING, padLen);

  /* Append length (before padding) */
  update(bits, 8);

  /* Store state in digest */
  encode(_state, _digest, 16);

  /* Restore current state and count. */
  memcpy(_state, oldState, 16);
  memcpy(_count, oldCount, 8);
}

/* MD5 basic transformation. Transforms _state based on block. */
inline void MD5::transform(const byte block[64]) {
  uint32 a = _state[0], b = _state[1], c = _state[2], d = _state[3], x[16];

  decode(block, x, 64);

  /* Round 1 */
  FF(a, b, c, d, x[0], S11, 0xd76aa478);  /* 1 */
  FF(d, a, b, c, x[1], S12, 0xe8c7b756);  /* 2 */
  FF(c, d, a, b, x[2], S13, 0x242070db);  /* 3 */
  FF(b, c, d, a, x[3], S14, 0xc1bdceee);  /* 4 */
  FF(a, b, c, d, x[4], S11, 0xf57c0faf);  /* 5 */
  FF(d, a, b, c, x[5], S12, 0x4787c62a);  /* 6 */
  FF(c, d, a, b, x[6], S13, 0xa8304613);  /* 7 */
  FF(b, c, d, a, x[7], S14, 0xfd469501);  /* 8 */
  FF(a, b, c, d, x[8], S11, 0x698098d8);  /* 9 */
  FF(d, a, b, c, x[9], S12, 0x8b44f7af);  /* 10 */
  FF(c, d, a, b, x[10], S13, 0xffff5bb1); /* 11 */
  FF(b, c, d, a, x[11], S14, 0x895cd7be); /* 12 */
  FF(a, b, c, d, x[12], S11, 0x6b901122); /* 13 */
  FF(d, a, b, c, x[13], S12, 0xfd987193); /* 14 */
  FF(c, d, a, b, x[14], S13, 0xa679438e); /* 15 */
  FF(b, c, d, a, x[15], S14, 0x49b40821); /* 16 */

  /* Round 2 */
  GG(a, b, c, d, x[1], S21, 0xf61e2562);  /* 17 */
  GG(d, a, b, c, x[6], S22, 0xc040b340);  /* 18 */
  GG(c, d, a, b, x[11], S23, 0x265e5a51); /* 19 */
  GG(b, c, d, a, x[0], S24, 0xe9b6c7aa);  /* 20 */
  GG(a, b, c, d, x[5], S21, 0xd62f105d);  /* 21 */
  GG(d, a, b, c, x[10], S22, 0x2441453);  /* 22 */
  GG(c, d, a, b, x[15], S23, 0xd8a1e681); /* 23 */
  GG(b, c, d, a, x[4], S24, 0xe7d3fbc8);  /* 24 */
  GG(a, b, c, d, x[9], S21, 0x21e1cde6);  /* 25 */
  GG(d, a, b, c, x[14], S22, 0xc33707d6); /* 26 */
  GG(c, d, a, b, x[3], S23, 0xf4d50d87);  /* 27 */
  GG(b, c, d, a, x[8], S24, 0x455a14ed);  /* 28 */
  GG(a, b, c, d, x[13], S21, 0xa9e3e905); /* 29 */
  GG(d, a, b, c, x[2], S22, 0xfcefa3f8);  /* 30 */
  GG(c, d, a, b, x[7], S23, 0x676f02d9);  /* 31 */
  GG(b, c, d, a, x[12], S24, 0x8d2a4c8a); /* 32 */

  /* Round 3 */
  HH(a, b, c, d, x[5], S31, 0xfffa3942);  /* 33 */
  HH(d, a, b, c, x[8], S32, 0x8771f681);  /* 34 */
  HH(c, d, a, b, x[11], S33, 0x6d9d6122); /* 35 */
  HH(b, c, d, a, x[14], S34, 0xfde5380c); /* 36 */
  HH(a, b, c, d, x[1], S31, 0xa4beea44);  /* 37 */
  HH(d, a, b, c, x[4], S32, 0x4bdecfa9);  /* 38 */
  HH(c, d, a, b, x[7], S33, 0xf6bb4b60);  /* 39 */
  HH(b, c, d, a, x[10], S34, 0xbebfbc70); /* 40 */
  HH(a, b, c, d, x[13], S31, 0x289b7ec6); /* 41 */
  HH(d, a, b, c, x[0], S32, 0xeaa127fa);  /* 42 */
  HH(c, d, a, b, x[3], S33, 0xd4ef3085);  /* 43 */
  HH(b, c, d, a, x[6], S34, 0x4881d05);   /* 44 */
  HH(a, b, c, d, x[9], S31, 0xd9d4d039);  /* 45 */
  HH(d, a, b, c, x[12], S32, 0xe6db99e5); /* 46 */
  HH(c, d, a, b, x[15], S33, 0x1fa27cf8); /* 47 */
  HH(b, c, d, a, x[2], S34, 0xc4ac5665);  /* 48 */

  /* Round 4 */
  II(a, b, c, d, x[0], S41, 0xf4292244);  /* 49 */
  II(d, a, b, c, x[7], S42, 0x432aff97);  /* 50 */
  II(c, d, a, b, x[14], S43, 0xab9423a7); /* 51 */
  II(b, c, d, a, x[5], S44, 0xfc93a039);  /* 52 */
  II(a, b, c, d, x[12], S41, 0x655b59c3); /* 53 */
  II(d, a, b, c, x[3], S42, 0x8f0ccc92);  /* 54 */
  II(c, d, a, b, x[10], S43, 0xffeff47d); /* 55 */
  II(b, c, d, a, x[1], S44, 0x85845dd1);  /* 56 */
  II(a, b, c, d, x[8], S41, 0x6fa87e4f);  /* 57 */
  II(d, a, b, c, x[15], S42, 0xfe2ce6e0); /* 58 */
  II(c, d, a, b, x[6], S43, 0xa3014314);  /* 59 */
  II(b, c, d, a, x[13], S44, 0x4e0811a1); /* 60 */
  II(a, b, c, d, x[4], S41, 0xf7537e82);  /* 61 */
  II(d, a, b, c, x[11], S42, 0xbd3af235); /* 62 */
  II(c, d, a, b, x[2], S43, 0x2ad7d2bb);  /* 63 */
  II(b, c, d, a, x[9], S44, 0xeb86d391);  /* 64 */

  _state[0] += a;
  _state[1] += b;
  _state[2] += c;
  _state[3] += d;
}

/* Encodes input (ulong) into output (byte). Assumes length is
a multiple of 4.
*/
inline void MD5::encode(const uint32* input, byte* output, size_t length) {
  for (size_t i = 0, j = 0; j < length; ++i, j += 4) {
    output[j] = (byte)(input[i] & 0xff);
    output[j + 1] = (byte)((input[i] >> 8) & 0xff);
    output[j + 2] = (byte)((input[i] >> 16) & 0xff);
    output[j + 3] = (byte)((input[i] >> 24) & 0xff);
  }
}

/* Decodes input (byte) into output (ulong). Assumes length is
a multiple of 4.
*/
inline void MD5::decode(const byte* input, uint32* output, size_t length) {
  for (size_t i = 0, j = 0; j < length; ++i, j += 4) {
    output[i] = ((uint32) input[j]) | (((uint32) input[j + 1]) << 8) |
                (((uint32) input[j + 2]) << 16) |
                (((uint32) input[j + 3]) << 24);
  }
}

/* Convert byte array to hex string. */
inline std::string MD5::bytesToHexString(const byte* input, size_t length) {
  std::string str;
  str.reserve(length << 1);
  for (size_t i = 0; i < length; ++i) {
    int t = input[i];
    int a = t / 16;
    int b = t % 16;
    str.append(1, HEX[a]);
    str.append(1, HEX[b]);
  }
  return str;
}

/* Convert digest to string value */
inline std::string MD5::toString() { return bytesToHexString(digest(), 16); }

#undef S11
#undef S12
#undef S13
#undef S14
#undef S21
#undef S22
#undef S23
#undef S24
#undef S31
#undef S32
#undef S33
#undef S34
#undef S41
#undef S42
#undef S43
#undef S44

/* F, G, H and I are basic MD5 functions.
 */
#undef F
#undef G
#undef H
#undef I

/* ROTATE_LEFT rotates x left n bits.
 */
#undef ROTATE_LEFT

/* FF, GG, HH, and II transformations for rounds 1, 2, 3, and 4.
Rotation is separate from addition to prevent recomputation.
*/
#undef FF
#undef GG
#undef HH
#undef II

}  // namespace ErrorDetection
}  // namespace apsara

#endif  /// APSARA_ERRORDETECTION_MD5_H
