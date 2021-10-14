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

use byteorder::{WriteBytesExt, BigEndian};
use std::io::Write;
use super::hash;

pub fn long_pk_to_id(label: i32, pk: i64) -> i64 {
    let mut buf = Vec::new();
    buf.write_i32::<BigEndian>(label).unwrap();
    let pk_str = pk.to_string();
    buf.write_i32::<BigEndian>(pk_str.len() as i32).unwrap();
    buf.write(pk_str.as_bytes()).unwrap();
    hash::murmur_hash64(&buf)
}

pub fn string_pk_to_id(label: i32, pk: &str) -> i64 {
    let mut buf = Vec::new();
    buf.write_i32::<BigEndian>(label).unwrap();
    buf.write_i32::<BigEndian>(pk.len() as i32).unwrap();
    buf.write(pk.as_bytes()).unwrap();
    hash::murmur_hash64(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_long_pk_to_id() {
        let cases = vec![
            (2084074232, 3373548221656802790, -8102589321042382292),
            (1380428816, -6614423657011891678, 2732694405214561224),
            (665595485, 1117510391399097087, 5759704121568900359),
            (1915051164, 241858611053359179, 2309645313960879173),
            (1271131470, 8671147185093348716, -8769579223657495601),
            (1163248002, 960730879526329731, 6894916460948382810),
            (842269488, 5465833541203573144, 1445617516917868989),
            (2065891742, -8985648913838296654, -4684620253208902799),
            (138981496, -1191928344301443265, 9064107917133953479),
            (1059800558, -7781570975187399919, -6111963754413656979)
        ];
        for case in cases {
            assert_eq!(long_pk_to_id(case.0, case.1), case.2);
        }
    }

    #[test]
    fn test_string_pk_to_id() {
        let cases = vec![
            (1930565880, "8106375201729201875", -8111834287691673090),
            (1890633079, "-3253202085218022333", -9040742215164978554),
            (720655986, "2354947115317842184", -3474122099525199089),
            (294449307, "-8208685350722911427", -7688343419408310164),
            (1199263631, "-7840208051834815053", 1568853012377706537),
            (504480353, "3386474773734294640", -5007885445112420227),
            (1264074325, "8145125039620792326", 4893766408812482265),
            (446777634, "5043094402836962015", -956466205410845033),
            (599086102, "7612074827539939056", 8117480166954139738),
            (1072772066, "-7516176534380788154", -4511435562269086109)
        ];
        for case in cases {
            assert_eq!(string_pk_to_id(case.0, case.1), case.2);
        }
    }
}
