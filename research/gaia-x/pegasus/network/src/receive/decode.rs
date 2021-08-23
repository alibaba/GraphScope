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
use std::io::Read;

use pegasus_common::bytes::BytesSlab;
use pegasus_common::codec::AsBytes;

use crate::message::{Message, MessageHeader};

#[enum_dispatch]
pub trait MessageDecoder {
    fn decode_next<R: Read>(&mut self, reader: &mut R) -> io::Result<Option<Message>>;
}

/// 简化版decoder, 依赖底层阻塞IO，并且不设置读超时时间；
/// 每次decode 都有可能阻塞在IO读上，直到读到足够的内容， 或被期间的错误打断；
/// 由于每次读都有可能永久阻塞，因此无法做连接的心跳检测。
///
/// 仅供测试和实验使用；
#[allow(dead_code)]
pub struct SimpleBlockDecoder {
    header_bin: Vec<u8>,
}

#[allow(dead_code)]
impl SimpleBlockDecoder {
    pub fn new() -> Self {
        SimpleBlockDecoder { header_bin: crate::message::DEFAULT_MESSAGE_HEADER_BYTES.clone() }
    }
}

#[allow(dead_code)]
impl MessageDecoder for SimpleBlockDecoder {
    fn decode_next<R: Read>(&mut self, reader: &mut R) -> io::Result<Option<Message>> {
        let mut bytes = std::mem::replace(&mut self.header_bin, vec![]);
        // blocking read until enough bytes were read;
        reader.read_exact(&mut bytes)?;
        let header = MessageHeader::from_bytes(&bytes).clone();
        self.header_bin = bytes;
        if header.length == 0 {
            Ok(Some(Message::new_uncheck(header, vec![])))
        } else {
            let mut payload = vec![0u8; header.length as usize];
            reader.read_exact(&mut payload[..])?;
            Ok(Some(Message::new_uncheck(header, payload)))
        }
    }
}

pub struct ReentrantDecoder {
    header_bin: Vec<u8>,
    header_cur: usize,
    in_progress: Option<(Vec<u8>, usize, MessageHeader)>,
}

impl ReentrantDecoder {
    pub fn new() -> Self {
        ReentrantDecoder {
            header_bin: vec![0u8; crate::message::MESSAGE_HEAD_SIZE],
            header_cur: 0,
            in_progress: None,
        }
    }
}

impl MessageDecoder for ReentrantDecoder {
    fn decode_next<R: Read>(&mut self, reader: &mut R) -> io::Result<Option<Message>> {
        if let Some((mut p, cur, h)) = self.in_progress.take() {
            let empty = &mut p[cur..];
            match try_read(reader, empty) {
                Ok(size) => {
                    if size > 0 && p.len() == cur + size {
                        Ok(Some(Message::new_uncheck(h, p)))
                    } else {
                        self.in_progress = Some((p, cur + size, h));
                        Ok(None)
                    }
                }
                Err(e) => {
                    self.in_progress = Some((p, cur, h));
                    Err(e)
                }
            }
        } else {
            let h_cur = self.header_cur;
            let size = try_read(reader, &mut self.header_bin[h_cur..])? + h_cur;
            if size == self.header_bin.len() {
                self.header_cur = 0;
                let header = MessageHeader::from_bytes(&self.header_bin);
                if header.length == 0 {
                    Ok(Some(Message::new_uncheck(header.clone(), vec![])))
                } else {
                    let payload = vec![0; header.length as usize];
                    self.in_progress = Some((payload, 0, header.clone()));
                    self.decode_next(reader)
                }
            } else {
                self.header_cur = size;
                Ok(None)
            }
        }
    }
}

pub struct ReentrantSlabDecoder {
    slab: BytesSlab,
    header_bin: Vec<u8>,
    header_cur: usize,
    in_progress: Option<(usize, MessageHeader)>,
}

impl ReentrantSlabDecoder {
    pub fn new(cap: usize) -> Self {
        ReentrantSlabDecoder {
            slab: BytesSlab::new(cap),
            header_bin: vec![0u8; crate::message::MESSAGE_HEAD_SIZE],
            header_cur: 0,
            in_progress: None,
        }
    }
}

impl MessageDecoder for ReentrantSlabDecoder {
    fn decode_next<R: Read>(&mut self, reader: &mut R) -> io::Result<Option<Message>> {
        if let Some((cur, header)) = self.in_progress.take() {
            let empty = &mut self.slab.as_mut()[cur..];
            let length = try_read(reader, empty)?;
            if length == empty.len() {
                let payload = self.slab.extract();
                Ok(Some(Message::new_uncheck(header, payload)))
            } else {
                assert!(length < empty.len());
                self.in_progress = Some((cur + length, header));
                Ok(None)
            }
        } else {
            let header_empty = &mut self.header_bin[self.header_cur..];
            let length = try_read(reader, header_empty)?;
            if length == header_empty.len() {
                self.header_cur = 0;
                let header = MessageHeader::from_bytes(&self.header_bin);
                if header.length == 0 {
                    Ok(Some(Message::new_uncheck(header.clone(), vec![])))
                } else {
                    self.slab.resize(header.length as usize, 0);
                    self.in_progress = Some((0, header.clone()));
                    self.decode_next(reader)
                }
            } else {
                assert!(length < header_empty.len());
                self.header_cur += length;
                Ok(None)
            }
        }
    }
}

#[inline]
fn try_read<R: io::Read>(reader: &mut R, bytes: &mut [u8]) -> io::Result<usize> {
    loop {
        match reader.read(bytes) {
            Ok(size) => return Ok(size),
            Err(err) => match err.kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => return Ok(0),
                io::ErrorKind::Interrupted => (),
                _ => return Err(err),
            },
        }
    }
}

#[enum_dispatch(MessageDecoder)]
pub enum GeneralDecoder {
    Simple(SimpleBlockDecoder),
    Reentrant(ReentrantDecoder),
    ReentrantSlab(ReentrantSlabDecoder),
}

pub fn get_reentrant_decoder(slab_size: usize) -> GeneralDecoder {
    if slab_size == 0 {
        ReentrantDecoder::new().into()
    } else {
        ReentrantSlabDecoder::new(slab_size).into()
    }
}

#[cfg(test)]
mod test {
    use pegasus_common::codec::Encode;
    use pegasus_common::io::WriteExt;

    use super::*;
    use crate::message::DEFAULT_MESSAGE_HEADER_BYTES;
    use crate::send::MessageEncoder;
    use crate::send::SimpleEncoder;

    struct Array(u8);

    impl Encode for Array {
        fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
            let bytes = vec![self.0; 256];
            writer.write_all(&bytes[..])
        }
    }

    fn decoder_test<D: MessageDecoder>(decoder: &mut D) {
        let mut encoder = SimpleEncoder::default();
        let mut header = MessageHeader::default();
        header.sequence = 1;
        header.channel_id = 1;
        let mut content = Vec::with_capacity(1 << 12);
        content.extend(&*DEFAULT_MESSAGE_HEADER_BYTES);
        content.extend(&*DEFAULT_MESSAGE_HEADER_BYTES);
        content.extend(&*DEFAULT_MESSAGE_HEADER_BYTES);
        for i in 1..9 {
            let b = encoder.encode(&mut header, &Array(i)).unwrap();
            content.extend_from_slice(b.as_ref());
            header.sequence += 1;
        }

        header.sequence = 0;
        header.length = 0;
        content.extend_from_slice(header.as_bytes());
        let mut reader = &content[0..];
        let mut i = 1u8;
        loop {
            let msg = decoder
                .decode_next(&mut reader)
                .unwrap()
                .unwrap();
            let (h, p) = msg.separate();
            if h.channel_id == 0 {
                continue;
            }

            assert_eq!(h.channel_id, 1);
            if h.sequence == 0 {
                assert_eq!(p.len(), 0);
                break;
            } else {
                assert_eq!(h.sequence, i as u64);
                assert_eq!(p.as_ref(), vec![i; 256].as_slice());
                i += 1;
            }
        }
    }

    #[test]
    fn default_decoder_test() {
        let mut decoder = SimpleBlockDecoder::new();
        decoder_test(&mut decoder);
    }

    #[test]
    fn reentrant_decoder_test() {
        let mut decoder = ReentrantDecoder::new();
        decoder_test(&mut decoder)
    }

    #[test]
    fn reentrant_shared_decoder_test() {
        let mut decoder = ReentrantSlabDecoder::new(1 << 16);
        decoder_test(&mut decoder)
    }
}
