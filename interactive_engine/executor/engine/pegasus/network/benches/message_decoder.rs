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

// Run with : cargo +nightly bench --features benchmark

#![feature(test)]
extern crate test;
use pegasus_common::codec::AsBytes;
use pegasus_network::{
    MessageDecoder, MessageHeader, ReentrantDecoder, ReentrantSlabDecoder, SimpleBlockDecoder,
};
use test::Bencher;

fn decode_bench<D: MessageDecoder>(b: &mut Bencher, decoder: &mut D) {
    let mut readers = Vec::with_capacity(5);
    let mut header = MessageHeader::default();
    for i in 0..5 {
        header.length = i * 17 + 17;
        let mut r = Vec::with_capacity(header.required_length());
        r.extend_from_slice(header.as_bytes());
        r.extend(vec![8u8; header.length as usize]);
        readers.push(r);
        header.sequence += 1;
    }

    let mut i = 0;
    b.iter(|| {
        let offset = i % 5;
        let mut reader = &readers[offset][0..];
        let msg = decoder
            .decode_next(&mut reader)
            .unwrap()
            .unwrap();
        let (header, payload) = msg.separate();
        assert_eq!(header.sequence as usize, offset);
        assert_eq!(header.length as usize, offset * 17 + 17);
        assert_eq!(payload.len(), offset * 17 + 17);
        i += 1;
    })
}

#[bench]
fn simple_decoder_bench(b: &mut Bencher) {
    let mut decoder = SimpleBlockDecoder::new();
    decode_bench(b, &mut decoder);
}

#[bench]
fn reentrant_decoder_bench(b: &mut Bencher) {
    let mut decoder = ReentrantDecoder::new();
    decode_bench(b, &mut decoder);
}

#[bench]
fn reentrant_shared_decoder_bench(b: &mut Bencher) {
    let mut decoder = ReentrantSlabDecoder::new(1 << 16);
    decode_bench(b, &mut decoder);
}
