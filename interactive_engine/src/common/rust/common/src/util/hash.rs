//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

/// Murmur hash function.
///
/// Given the key to hash and a seed, return hash value as `i64`.
pub fn murmur_hash64_with_seed(key: &[u8], seed: u64) -> i64 {
    let m: u64 = 0xc6a4a7935bd1e995;
    let r: u8 = 47;

    let len = key.len();
    let mut h: u64 = seed ^ ((len as u64).wrapping_mul(m));

    let endpos = len - (len & 7);
    let mut i = 0;
    while i != endpos {
        let mut k: u64;

        k = key[i + 0] as u64;
        k |= (key[i + 1] as u64) << 8;
        k |= (key[i + 2] as u64) << 16;
        k |= (key[i + 3] as u64) << 24;
        k |= (key[i + 4] as u64) << 32;
        k |= (key[i + 5] as u64) << 40;
        k |= (key[i + 6] as u64) << 48;
        k |= (key[i + 7] as u64) << 56;

        k = k.wrapping_mul(m);
        k ^= k >> r;
        k = k.wrapping_mul(m);
        h ^= k;
        h = h.wrapping_mul(m);

        i += 8;
    };

    let over = len & 7;
    if over == 7 { h ^= (key[i + 6] as u64) << 48; }
    if over >= 6 { h ^= (key[i + 5] as u64) << 40; }
    if over >= 5 { h ^= (key[i + 4] as u64) << 32; }
    if over >= 4 { h ^= (key[i + 3] as u64) << 24; }
    if over >= 3 { h ^= (key[i + 2] as u64) << 16; }
    if over >= 2 { h ^= (key[i + 1] as u64) << 8; }
    if over >= 1 { h ^= key[i + 0] as u64; }
    if over > 0 { h = h.wrapping_mul(m); }

    h ^= h >> r;
    h = h.wrapping_mul(m);
    h ^= h >> r;
    h as i64
}

/// Murmur hash function.
///
/// Hash input key with default seed.
pub fn murmur_hash64(key: &[u8]) -> i64 {
    murmur_hash64_with_seed(key, 0xc70f6907)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_murmur_hash() {
        // simple tests.
        assert_eq!(murmur_hash64("abcd".as_bytes()), -2416373452805698776);
        assert_eq!(murmur_hash64("1234567".as_bytes()), 7888703982793837558);
        assert_eq!(murmur_hash64("什么东西我不懂".as_bytes()), 5547184402929393830);

        // test user provided cases.
        assert_eq!(murmur_hash64("100021".as_bytes()), 5937486153913251841);
        assert_eq!(murmur_hash64("100427".as_bytes()), -167525241807007284);
        assert_eq!(murmur_hash64("100003542849".as_bytes()), 5449144759114248468);
        assert_eq!(murmur_hash64("2088712468157371".as_bytes()), 4325002293867226414);

        // test some unicode strings.
        assert_eq!(murmur_hash64("뭥땹界壭쎧電ඹ焬绺೥爦埣⫱㕯盽".as_bytes()), -5024059114194069619);
        assert_eq!(murmur_hash64("䱶磗潨ꑔऽ⬸믎僆液몧눒凞杛䏴펧姶ꕹ퇝".as_bytes()), 3213313641453169253);
        assert_eq!(murmur_hash64("鈧紹몮舟ꆎ鲨㷒㦪嬡臵䗽⠆損䃆ਰ鏟广戜Ὑ".as_bytes()), -3113457293858082421);

        // test some account id.
        assert_eq!(murmur_hash64("2088112001803138".as_bytes()), 2240385760819582610);
        assert_eq!(murmur_hash64("2088311465207164".as_bytes()), 7223159068034626273);
    }
}
