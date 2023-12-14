/*
 * This implementation of Keyed-Hash Message Authentication Code (HMAC)
 * is consistent with the FIPS PUB 198 standard. The HMAC specification in
 * this standard is a generalization of Internet RFC 2104 and ANSI X9.71.
 *
 * The cryptographic strength of HMAC depends on the properties of the
 * underlying hash function. Currently, we only support the SHA-1 function
 * as the underlying hash. The HMAC function is operated as below:
 *
 *         HMAC(k,msg) = H((k0^OPAD)||H((k0^IPAD)||msg))
 *
 * where k is the key and k0 is derived from the k. The HMAC is used to
 * guarantee that the accompanying message is authentic. Any problem please
 * contact jun.li@alibaba-inc.com
 */

#ifndef APSARA_SECURITY_HMAC_H
#define APSARA_SECURITY_HMAC_H

#include <stdint.h>
#include "common/sha1.h"

namespace apsara {
namespace security {

#define HMAC_KEY_SIZE SHA1_DIGEST_BYTES
#define HMAC_SIZE SHA1_DIGEST_BYTES
#define HMAC_WORDS SHA1_DIGEST_WORDS

class HMAC {
 public:
  HMAC(const uint8_t* key, size_t lkey);
  HMAC(const HMAC& hm) : in(hm.in), out(hm.out) {}

  void init(const uint8_t* key, size_t lkey);

  void add(const uint8_t* data, size_t len) { in.add(data, len); }

  uint8_t* result() {
    out.add(in.result(), SHA1_DIGEST_BYTES);
    return out.result();
  }

 private:
  SHA1 in, out;
};

}  // end of namespace security
}  // end of namespace apsara

namespace apsara {
namespace security {

template <typename T>
inline void axor(T* p1, const T* p2, size_t len) {
  for (; len != 0; --len)
    *p1++ ^= *p2++;
}

inline HMAC::HMAC(const uint8_t* key, size_t lkey) { init(key, lkey); }

inline void HMAC::init(const uint8_t* key, size_t lkey) {
  in.init();
  out.init();

  uint8_t ipad[SHA1_INPUT_BYTES];
  uint8_t opad[SHA1_INPUT_BYTES];
  memset(ipad, 0x36, sizeof(ipad));
  memset(opad, 0x5c, sizeof(opad));

  if (lkey <= SHA1_INPUT_BYTES) {
    axor(ipad, key, lkey);
    axor(opad, key, lkey);
  } else {
    SHA1 tmp;
    tmp.add(key, lkey);
    const uint8_t* key2 = tmp.result();
    axor(ipad, key2, SHA1_DIGEST_BYTES);
    axor(opad, key2, SHA1_DIGEST_BYTES);
  }

  in.add((uint8_t*) ipad, sizeof(ipad));
  out.add((uint8_t*) opad, sizeof(opad));
}

}  // end of namespace security
}  // end of namespace apsara

#endif
