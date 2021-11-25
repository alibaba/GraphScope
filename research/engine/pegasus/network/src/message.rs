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

use pegasus_common::bytes::Bytes;
use pegasus_common::codec::{AsBytes, Buf};

/// 协议消息头，描述每个IPC 消息的基本信息，主要包括:
/// - channel id : 消息所属的IPC channel;
/// - length     : 除去消息头，消息内容的长度；
/// - sequence   : 该消息在其所属channel中的序号，序号的大小表明了消息的顺序；
///
/// 按照约定：
/// - id 为0 的channel是内部保留channel, 当前主要用于维护网络连接的心跳；用户创建IPC channel时需要注意channel_id不为 0；
/// - sequence 为0 的消息头是内部保留消息头， 当前主要用于描述 IPC channel 主动关闭的事件；
///
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Default)]
pub struct MessageHeader {
    pub channel_id: u128,
    pub length: u64,
    pub sequence: u64,
}

impl MessageHeader {
    pub fn new(channel_id: u128) -> Self {
        MessageHeader { channel_id, length: 0, sequence: 0 }
    }

    #[inline]
    pub fn required_length(&self) -> usize {
        MESSAGE_HEAD_SIZE + self.length as usize
    }
}

lazy_static! {
    pub(crate) static ref DEFAULT_MESSAGE_HEADER: MessageHeader = MessageHeader::default();
    pub(crate) static ref DEFAULT_MESSAGE_HEADER_BYTES: Vec<u8> = default_message_header_bytes();
}

fn default_message_header_bytes() -> Vec<u8> {
    let header = MessageHeader::default();
    let mut bytes = vec![];
    bytes.extend_from_slice(header.as_bytes());
    bytes
}

pub enum Payload {
    Owned((Vec<u8>, usize)),
    Shared(Bytes),
}

impl Payload {
    pub fn len(&self) -> usize {
        match self {
            Payload::Owned((v, offset)) => v.len() - offset,
            Payload::Shared(b) => b.len(),
        }
    }

    pub fn advance(&mut self, size: usize) {
        if size > 0 {
            match self {
                Payload::Owned((_, offset)) => *offset += size,
                Payload::Shared(bytes) => {
                    bytes.advance(size);
                }
            }
        }
    }
}

impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        match self {
            Payload::Owned((vec, offset)) => &vec[*offset..],
            Payload::Shared(b) => b.as_ref(),
        }
    }
}

impl From<Vec<u8>> for Payload {
    fn from(vec: Vec<u8>) -> Self {
        Payload::Owned((vec, 0))
    }
}

impl From<&Vec<u8>> for Payload {
    fn from(vec: &Vec<u8>) -> Self {
        Payload::Owned((vec.clone(), 0))
    }
}

impl From<MessageHeader> for Payload {
    fn from(header: MessageHeader) -> Self {
        let bytes = header.as_bytes();
        let mut vec = Vec::with_capacity(bytes.len());
        vec.extend_from_slice(bytes);
        Payload::Owned((vec, 0))
    }
}

impl From<Bytes> for Payload {
    fn from(b: Bytes) -> Self {
        Payload::Shared(b)
    }
}

pub const MESSAGE_HEAD_SIZE: usize = std::mem::size_of::<MessageHeader>();

pub struct Message {
    header: MessageHeader,
    payload: Payload,
}

impl Message {
    #[allow(dead_code)]
    pub fn new<P: Into<Payload>>(mut header: MessageHeader, payload: P) -> Option<Self> {
        let payload = payload.into();
        let size = payload.as_ref().len();
        if size >= std::u32::MAX as usize {
            warn!("too large message payload, len={}", size);
            return None;
        }

        header.length = size as u64;
        Some(Message { header, payload })
    }

    pub fn new_uncheck<P: Into<Payload>>(header: MessageHeader, payload: P) -> Self {
        Message { header, payload: payload.into() }
    }

    pub fn separate(self) -> (MessageHeader, Payload) {
        (self.header, self.payload)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn owned_payload_test() {
        let bytes = vec![8u8; 1024];
        let mut payload: Payload = bytes.into();
        assert_eq!(payload.len(), 1024);
        assert_eq!(payload.as_ref(), vec![8u8; 1024].as_slice());
        payload.advance(512);
        assert_eq!(payload.len(), 512);
        assert_eq!(payload.as_ref(), vec![8u8; 512].as_slice())
    }
}
