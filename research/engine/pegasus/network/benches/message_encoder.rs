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
use pegasus_common::codec::Encode;
use pegasus_common::io::WriteExt;
use pegasus_network::{MessageEncoder, MessageHeader, SimpleEncoder, SlabEncoder, MESSAGE_HEAD_SIZE};
use test::Bencher;

struct Message {
    content: Vec<u8>,
    repeat: usize,
}

impl Message {
    pub fn new(repeat: usize) -> Self {
        Message { content: vec![8; 17], repeat }
    }
}

impl Encode for Message {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write(&self.content)?;
        for _ in 0..self.repeat {
            writer.write(&self.content)?;
        }
        Ok(())
    }
}

fn encode_bench<E: MessageEncoder<Message>>(encoder: &mut E, b: &mut Bencher) {
    let mut header = MessageHeader::default();
    header.channel_id = 1;
    let mut message = Message::new(0);
    b.iter(|| {
        header.sequence += 1;
        message.repeat = (header.sequence % 5) as usize;
        let binary = encoder.encode(&mut header, &message).unwrap();
        assert_eq!(binary.len(), header.required_length());
    });
}

#[bench]
fn simple_encoder_bench(b: &mut Bencher) {
    let mut encoder = SimpleEncoder::default();
    encode_bench(&mut encoder, b);
}

#[bench]
fn slab_encoder_bench(b: &mut Bencher) {
    let mut encoder = SlabEncoder::new(1 << 16);
    encode_bench(&mut encoder, b);
}
