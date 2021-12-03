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

use std::io;
use std::io::Write;

use pegasus_common::bytes::BytesSlab;
use pegasus_common::codec::{AsBytes, Encode};

use crate::message::{MessageHeader, Payload, MESSAGE_HEAD_SIZE};

#[enum_dispatch]
pub trait MessageEncoder<T: Encode>: Send {
    fn encode(&mut self, header: &mut MessageHeader, msg: &T) -> io::Result<Payload>;
}

#[allow(dead_code)]
pub struct SimpleEncoder<T> {
    _ph: std::marker::PhantomData<T>,
}

#[allow(dead_code)]
impl<T> Default for SimpleEncoder<T> {
    fn default() -> Self {
        SimpleEncoder { _ph: std::marker::PhantomData }
    }
}

impl<T> Clone for SimpleEncoder<T> {
    fn clone(&self) -> Self {
        SimpleEncoder::default()
    }
}

///
unsafe impl<T> Send for SimpleEncoder<T> {}

impl<T: Encode> MessageEncoder<T> for SimpleEncoder<T> {
    fn encode(&mut self, header: &mut MessageHeader, msg: &T) -> io::Result<Payload> {
        let mut buffer = vec![0u8; MESSAGE_HEAD_SIZE];
        msg.write_to(&mut buffer)?;
        header.length = (buffer.len() - MESSAGE_HEAD_SIZE) as u64;
        let mut writer = &mut buffer[0..];
        writer.write_all(header.as_bytes())?;
        buffer.shrink_to_fit();
        Ok(Payload::Owned((buffer, 0)))
    }
}

pub struct SlabEncoder<T> {
    pub cap: usize,
    slab: BytesSlab,
    empty_head: Vec<u8>,
    _ph: std::marker::PhantomData<T>,
}

impl<T> SlabEncoder<T> {
    pub fn new(cap: usize) -> Self {
        SlabEncoder {
            cap,
            slab: BytesSlab::new(cap),
            empty_head: vec![0u8; MESSAGE_HEAD_SIZE],
            _ph: std::marker::PhantomData,
        }
    }
}

impl<T> Clone for SlabEncoder<T> {
    fn clone(&self) -> Self {
        SlabEncoder {
            cap: self.cap,
            slab: BytesSlab::new(self.cap),
            empty_head: vec![0u8; MESSAGE_HEAD_SIZE],
            _ph: std::marker::PhantomData,
        }
    }
}

unsafe impl<T> Send for SlabEncoder<T> {}

impl<T: Encode> MessageEncoder<T> for SlabEncoder<T> {
    fn encode(&mut self, header: &mut MessageHeader, msg: &T) -> std::io::Result<Payload> {
        assert_eq!(self.slab.len(), 0);
        self.slab.ensure_capacity(MESSAGE_HEAD_SIZE + 1);
        self.slab.write_all(&self.empty_head)?;
        msg.write_to(&mut self.slab)?;
        header.length = (self.slab.len() - MESSAGE_HEAD_SIZE) as u64;
        {
            let rewrite_head = &mut self.slab.as_mut()[0..MESSAGE_HEAD_SIZE];
            rewrite_head.copy_from_slice(header.as_bytes());
        }
        let bytes = self.slab.extract();
        Ok(Payload::Shared(bytes))
    }
}

#[enum_dispatch(MessageEncoder<T>)]
pub enum GeneralEncoder<T: Encode> {
    Simple(SimpleEncoder<T>),
    Slab(SlabEncoder<T>),
}

impl<T: Encode> Clone for GeneralEncoder<T> {
    fn clone(&self) -> Self {
        match self {
            GeneralEncoder::Simple(e) => GeneralEncoder::Simple(e.clone()),
            GeneralEncoder::Slab(e) => GeneralEncoder::Slab(e.clone()),
        }
    }
}

#[cfg(test)]
mod test {
    use pegasus_common::io::WriteExt;

    use super::*;

    struct Array;

    impl Encode for Array {
        fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
            let bytes = vec![8u8; 1024];
            writer.write_all(&bytes[..])?;
            let bytes = vec![9u8; 1024];
            writer.write_all(&bytes[..])
        }
    }

    fn encode_test<E: MessageEncoder<Array>>(encoder: &mut E) {
        let mut header = MessageHeader::default();
        header.channel_id = 1;
        let msg = Array;
        let payload = encoder.encode(&mut header, &msg).unwrap();
        assert_eq!(payload.len(), header.required_length());
        let bytes = payload.as_ref();
        let header = MessageHeader::from_bytes(&bytes[0..MESSAGE_HEAD_SIZE]);
        assert_eq!(header.channel_id, 1);
        assert_eq!(header.length, 2048);
        assert_eq!(header.sequence, 0);
        let content = &bytes[MESSAGE_HEAD_SIZE..];
        assert_eq!(&content[0..1024], vec![8u8; 1024].as_slice());
        assert_eq!(&content[1024..], vec![9u8; 1024].as_slice());
    }

    #[test]
    fn default_encode_test() {
        let mut encoder = SimpleEncoder::default();
        encode_test(&mut encoder);
    }

    #[test]
    fn bytes_encode_test() {
        let mut encoder = SlabEncoder::new(1 << 16);
        encode_test(&mut encoder);
    }
}
