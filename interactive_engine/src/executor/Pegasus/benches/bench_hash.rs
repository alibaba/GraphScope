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

//#![feature(test)]
//extern crate test;
//use test::Bencher;
//use std::collections::HashMap;
//
//
//#[derive(Debug, Copy, Clone, Ord, PartialOrd, PartialEq, Eq, Hash)]
//pub struct ChannelId(pub usize);
//
//#[bench]
//fn search_in_hash_table_1(bench : &mut Bencher) {
//    let mut map = HashMap::new();
//
//    for i in 0..1024usize {
//        map.insert(ChannelId(i), i);
//    }
//    let key = ChannelId(5);
//    bench.iter(|| {
//        map.get(&key)
//    })
//}
//
//#[bench]
//fn search_in_hash_table_2(bench : &mut Bencher) {
//    let mut map = HashMap::new();
//    for i in 0..1024usize {
//        map.insert(i, i);
//    }
//    let key = 5usize;
//    bench.iter(|| {
//        map.get(&key)
//    })
//}
//
