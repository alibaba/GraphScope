//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

#![feature(test)]
extern crate test;

use std::collections::HashMap;

use ahash::AHashMap;
use nohash_hasher::IntMap;
use pegasus::Tag;

#[bench]
fn tag_root_hash_w_r_hit(b: &mut test::Bencher) {
    let mut map = HashMap::with_capacity(1024);
    b.iter(|| {
        map.insert(Tag::Root, ());
        map.remove(&Tag::Root);
    })
}

#[bench]
fn tag_one_hash_w_r_hit(b: &mut test::Bencher) {
    let mut map = HashMap::with_capacity(1024);
    b.iter(|| {
        map.insert(Tag::One(77), ());
        map.remove(&Tag::One(77));
    })
}

#[bench]
fn tag_one_a_hash_w_r_hit(b: &mut test::Bencher) {
    let mut map = AHashMap::with_capacity(1024);
    b.iter(|| {
        map.insert(Tag::One(77), ());
        map.remove(&Tag::One(77));
    })
}

#[bench]
fn tag_one_int_map_w_r_hit(b: &mut test::Bencher) {
    let mut map = IntMap::default();
    b.iter(|| {
        map.insert(77, ());
        map.remove(&77);
    })
}

// #[bench]
// fn tag_one_hash_w_r_hit_1000(b: &mut test::Bencher) {
//     let mut map = HashMap::new();
//     for i in 0..1024u32 {
//         map.insert(Tag::One(i), ());
//     }
//     b.iter(|| {
//         map.insert(Tag::One(2000), ());
//         map.remove(&Tag::One(2000));
//     })
// }
//
// #[bench]
// fn tag_one_hash_w_r_hit_100_000(b: &mut test::Bencher) {
//     let mut map = HashMap::new();
//     for i in 0..100_000u32 {
//         map.insert(Tag::One(i), ());
//     }
//     b.iter(|| {
//         map.insert(Tag::One(100_001), ());
//         map.remove(&Tag::One(100_001));
//     })
// }

#[bench]
fn tag_two_hash_w_r_hit(b: &mut test::Bencher) {
    let mut map = HashMap::with_capacity(1024);
    b.iter(|| {
        map.insert(Tag::Two(3, 6), ());
        map.remove(&Tag::Two(3, 6));
    })
}

#[bench]
fn tag_two_a_hash_w_r_hit(b: &mut test::Bencher) {
    let mut map = AHashMap::with_capacity(1024);
    b.iter(|| {
        map.insert(Tag::Two(3, 6), ());
        map.remove(&Tag::Two(3, 6));
    })
}

// #[bench]
// fn tag_two_hash_w_r_hit_1000(b: &mut test::Bencher) {
//     let mut map = HashMap::new();
//     for i in 0..1000 {
//         map.insert(Tag::Two(3, i), ());
//     }
//     b.iter(|| {
//         map.insert(Tag::Two(3, 2000), ());
//         map.remove(&Tag::Two(3, 2000));
//     })
// }

#[bench]
fn tag_three_hash_w_r_hit(b: &mut test::Bencher) {
    let mut map = HashMap::with_capacity(1024);
    b.iter(|| {
        map.insert(Tag::Three(3, 6, 9), ());
        map.remove(&Tag::Three(3, 6, 9));
    })
}

#[bench]
fn tag_three_a_hash_w_r_hit(b: &mut test::Bencher) {
    let mut map = AHashMap::with_capacity(1024);
    b.iter(|| {
        map.insert(Tag::Three(3, 6, 9), ());
        map.remove(&Tag::Three(3, 6, 9));
    })
}
