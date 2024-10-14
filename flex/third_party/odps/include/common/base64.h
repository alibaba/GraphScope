#ifndef APSARA_BASE64_H
#define APSARA_BASE64_H

#include <iostream>
#include "common/exception.h"
#include "common/string_piece.h"

namespace apsara {
class BadBase64Exception : public apsara::ExceptionBase {
 public:
  APSARA_DEFINE_EXCEPTION(BadBase64Exception, apsara::ExceptionBase);
};

void Base64Encoding(
    std::istream&, std::ostream&, char makeupChar = '=',
    const char* alphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/");

void Base64Decoding(std::istream&, std::ostream&, char plus = '+',
                    char slash = '/');

namespace common {

bool Base64Encode(const StringPiece& input, std::string* output);
bool Base64Encode(const StringPiece& input, char* output, size_t* len);

bool Base64Decode(const StringPiece& input, std::string* output);
bool Base64Decode(const StringPiece& input, char* output, size_t* len);

}  // namespace common

}  // namespace apsara

#include <assert.h>
#include <iostream>
#include <string>

using namespace apsara;

/*
 * in  xxxxxxxx xxxxxxxx xxxxxxxx
 * out 11111122 22223333 33444444
 */
inline void apsara::Base64Encoding(std::istream& is, std::ostream& os,
                                   char makeupChar, const char* alphabet) {
  int out[4];
  int remain = 0;
  while (!is.eof()) {
    int byte1 = is.get();
    if (byte1 < 0) {
      break;
    }
    int byte2 = is.get();
    int byte3;
    if (byte2 < 0) {
      byte2 = 0;
      byte3 = 0;
      remain = 1;
    } else {
      byte3 = is.get();
      if (byte3 < 0) {
        byte3 = 0;
        remain = 2;
      }
    }
    out[0] = static_cast<unsigned>(byte1) >> 2;
    out[1] = ((byte1 & 0x03) << 4) + (static_cast<unsigned>(byte2) >> 4);
    out[2] = ((byte2 & 0x0F) << 2) + (static_cast<unsigned>(byte3) >> 6);
    out[3] = byte3 & 0x3F;

    if (remain == 1) {
      os.put(out[0] = alphabet[out[0]]);
      os.put(out[1] = alphabet[out[1]]);
      os.put(makeupChar);
      os.put(makeupChar);
    } else if (remain == 2) {
      os.put(out[0] = alphabet[out[0]]);
      os.put(out[1] = alphabet[out[1]]);
      os.put(out[2] = alphabet[out[2]]);
      os.put(makeupChar);
    } else {
      os.put(out[0] = alphabet[out[0]]);
      os.put(out[1] = alphabet[out[1]]);
      os.put(out[2] = alphabet[out[2]]);
      os.put(out[3] = alphabet[out[3]]);
    }
  }
}

// Base64Decoding
inline void apsara::Base64Decoding(std::istream& is, std::ostream& os,
                                   char plus, char slash) {
  int out[3];
  int byte[4];
  int bTmp;
  int bTmpNext;
  const int numOfAlpha = 26;
  const int numOfDecimalNum = 10;
  const int numOfBase64 = 64;
  int index = -1;

  while (is.peek() >= 0) {
    byte[0] = byte[1] = byte[2] = byte[3] = 0;
    out[0] = out[1] = out[2] = 0;
    bTmp = 0;

    for (int i = 0; i < 4; i++) {
      bTmp = is.get();

      if (bTmp == '=') {
        index = i;
        break;
      } else if (bTmp >= 'A' && bTmp <= 'Z') {
        byte[i] = bTmp - 'A';
      } else if (bTmp >= 'a' && bTmp <= 'z') {
        byte[i] = bTmp - 'a' + numOfAlpha;
      } else if (bTmp >= '0' && bTmp <= '9') {
        byte[i] = bTmp + numOfAlpha * 2 - '0';
      } else if (bTmp == plus) {
        byte[i] = numOfAlpha * 2 + numOfDecimalNum;
      } else if (bTmp == slash) {
        byte[i] = numOfBase64 - 1;
      } else if (bTmp < 0) {
        APSARA_THROW(
            BadBase64Exception,
            "\n\nInvaild input!\nInput must be a multiple of four!!\n");
      } else {
        APSARA_THROW(
            BadBase64Exception,
            "\n\nInvaild input!\nPlease input the character in the string "
            "below:"
            "\nABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
            "=\n");
      }
    }

    out[0] = (byte[0] << 2) + (static_cast<unsigned>(byte[1]) >> 4);
    out[1] = ((byte[1] & 0x0F) << 4) + (static_cast<unsigned>(byte[2]) >> 2);
    out[2] = ((byte[2] & 0x03) << 6) + byte[3];

    if (bTmp == '=') {
      if (index == 0 || index == 1) {
        APSARA_THROW(
            BadBase64Exception,
            "\n\nInvaild input!\nInput must be a multiple of four!!\n= must be "
            "the third or fourth place in the last four characters!\n");
      } else if (index == 2) {
        bTmpNext = is.get();
        if (bTmpNext == '=') {
          if (is.peek() < 0) {
            os.put(out[0]);
          } else {
            APSARA_THROW(BadBase64Exception,
                         "\n\nInvaild input!\nPlease do not append any "
                         "character after == !\n");
          }
        } else {
          APSARA_THROW(BadBase64Exception,
                       "\n\nInvaild input!\nPlease do not append any character "
                       "after = except = !\n");
        }
      } else {
        if (is.peek() < 0) {
          os.put(out[0]);
          os.put(out[1]);
        } else {
          APSARA_THROW(
              BadBase64Exception,
              "\n\nInvaild input!\nInput must be a multiple of four!!\nPlease "
              "do not append any character after the first = !\n");
        }
      }
    } else {
      os.put(out[0]);
      os.put(out[1]);
      os.put(out[2]);
    }
  }
}

namespace apsara {
namespace common {

namespace {

static const unsigned char EncodeTable[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
static const unsigned char* DecodeTable = NULL;

inline void FillDecodeTable() {
  // double buffer is used here for thread safe.
  static unsigned char DecodeTableBuff[256];
  unsigned char buff[256];
  if (DecodeTable != NULL) {
    return;
  }
  ::memset(buff, 0x80, sizeof(buff));

  for (size_t k = 0; k < sizeof(EncodeTable); ++k) {
    buff[(size_t) EncodeTable[k]] = k;
  }
  // to mark those valid characters in encoded string, but not in these
  // 64 bases list.
  buff[(size_t) '\r'] = buff[(size_t) '\n'] = 0x4F;
  buff[(size_t) '='] = 0x40;

  ::memcpy(DecodeTableBuff, buff, sizeof(DecodeTableBuff));
  DecodeTable = DecodeTableBuff;
}

// Get the next 4 characters from input string, '\r\n' will be trimmed off.
// The input string starts from 'p', and ends before 'q'. 'buff' is for
// storing the return characters.
// The return value, -1: error, there aren't 4 characters available, or get
// invalid character. 0-4 mean the number of valid characters, '=' is excluded.
inline int GetNext4EncodedCharacters(const unsigned char*& p,
                                     const unsigned char* q,
                                     unsigned char* buff) {
  int k = 0;
  unsigned char c = 0;
  while (k < 4 && p < q) {
    c = DecodeTable[*p];
    if ((c & 0xC0) == 0) {  // normal valid characters
      *buff++ = c;
      ++p;
      ++k;
    } else if (c & 0x80) {  // not ( '\r' or '\n' or '=' )
      return -1;
    } else if (*p == '=') {
      break;
    } else {  // ( '\r' or '\n' )
      ++p;
    }
  }
  // success. this should be most of the cases, return as soon as possible
  if (k == 4) {
    return 4;
  }
  // get a '='
  if (p < q && *p == '=') {
    ++p;
    // there should be 4 characters in the last encode group
    int tail = 4 - k - 1;
    // there should not be more than 2 '=' in the end
    if (tail > 1) {
      return -1;
    }
    while (tail > 0 && p < q && ((DecodeTable[*p] & 0x40) == 0x40)) {
      if (*p == '=') {
        --tail;
      }
      ++p;
    }
    // any character not ('\r' or '\n' or '=') appears after '='
    if (tail != 0) {
      return -1;
    }
    // only ('\r' || '\n') is allowed at the end
    while (p < q) {
      if ((DecodeTable[*p] & 0x4F) == 0x4F) {
        ++p;
      } else {
        return -1;
      }
    }

    return k;
  }
  // for ( '\r' or '\n' ) at very end
  while (p < q && (DecodeTable[*p] & 0x4F) == 0x4F) {
    ++p;
  }
  if (k == 0 && p == q) {
    return 0;
  }

  return -1;
}

inline size_t ExpectedEncodeLength(size_t len) {
  size_t encodedLen = ((len * 4 / 3 + 3) / 4) * 4;
  return encodedLen;
}

inline size_t ExpectedDecodeLength(size_t len) {
  return (size_t)((len + 3) / 4 * 3);
}

}  // anonymous namespace

inline bool Base64Encode(const StringPiece& input, std::string* output) {
  assert(output);
  output->resize(ExpectedEncodeLength(input.size()));

  char* buff = const_cast<char*>(output->data());
  size_t len = output->size();

  if (!Base64Encode(input, buff, &len)) {
    output->clear();
    return false;
  }
  output->resize(len);
  return true;
}

inline bool Base64Encode(const StringPiece& input, char* output, size_t* len) {
  assert(output);

  char* buff = output;
  if (__builtin_expect(*len < ExpectedEncodeLength(input.size()), 0)) {
    return false;
  }

  unsigned char* p = (unsigned char*) input.data();
  unsigned char* q = p + input.size();
  unsigned char c1, c2, c3;

  // process 3 char every loop
  for (; p + 3 <= q; p += 3) {
    c1 = *p;
    c2 = *(p + 1);
    c3 = *(p + 2);

    *buff++ = EncodeTable[c1 >> 2];
    *buff++ = EncodeTable[((c1 << 4) | (c2 >> 4)) & 0x3f];
    *buff++ = EncodeTable[((c2 << 2) | (c3 >> 6)) & 0x3f];
    *buff++ = EncodeTable[c3 & 0x3f];
  }

  // the reminders
  if (q - p == 1) {
    c1 = *p;
    *buff++ = EncodeTable[(c1 & 0xfc) >> 2];
    *buff++ = EncodeTable[(c1 & 0x03) << 4];
    *buff++ = '=';
    *buff++ = '=';
  } else if (q - p == 2) {
    c1 = *p;
    c2 = *(p + 1);
    *buff++ = EncodeTable[(c1 & 0xfc) >> 2];
    *buff++ = EncodeTable[((c1 & 0x03) << 4) | ((c2 & 0xf0) >> 4)];
    *buff++ = EncodeTable[((c2 & 0x0f) << 2)];
    *buff++ = '=';
  }

  *len = buff - output;
  return true;
}

inline bool Base64Decode(const StringPiece& input, std::string* output) {
  assert(output);
  output->resize(ExpectedDecodeLength(input.size()));

  char* buff = const_cast<char*>(output->data());
  size_t len = output->size();

  if (!Base64Decode(input, buff, &len)) {
    output->clear();
    return false;
  }
  output->resize(len);
  return true;
}

inline bool Base64Decode(const StringPiece& input, char* output, size_t* len) {
  assert(output && len);

  char* buff = output;
  if (__builtin_expect(*len < ExpectedDecodeLength(input.size()), 0)) {
    return false;
  }
  if (__builtin_expect(!DecodeTable, 0)) {
    FillDecodeTable();
  }
  if (input.empty()) {
    *len = buff - output;
    return true;
  }

  const unsigned char* p = (unsigned char*) input.data();
  const unsigned char* q = (unsigned char*) input.data() + input.size();

  // handle 4 bytes in every loop
  while (true) {
    char ch = 0;
    unsigned char encoded[4];
    int len = GetNext4EncodedCharacters(p, q, encoded);
    if (__builtin_expect(len == 4, 1)) {
      ch = encoded[0] << 2;   // all 6 bits
      ch |= encoded[1] >> 4;  // 2 high bits
      *buff++ = ch;
      ch = encoded[1] << 4;   // 4 low bits
      ch |= encoded[2] >> 2;  // 4 high bits
      *buff++ = ch;
      ch = encoded[2] << 6;  // 2 low bits
      ch |= encoded[3];
      *buff++ = ch;
    } else if (len >= 2) {
      ch = encoded[0] << 2;   // all 6 bits
      ch |= encoded[1] >> 4;  // 2 high bits
      *buff++ = ch;
      if (len == 3) {
        ch = encoded[1] << 4;   // 4 low bits
        ch |= encoded[2] >> 2;  // 4 high bits
        *buff++ = ch;
      }
    } else if (len == 0) {
      break;
    } else {
      return false;
    }
  }

  *len = buff - output;
  return true;
}

}  // namespace common
}  // namespace apsara

#endif
