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

use std::collections::HashMap;
use std::io;
use std::io::Read;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_queue::SegQueue;
use crossbeam_utils::sync::ShardedLock;
use pegasus_common::channel::{MPMCSender, MessageSender};

use crate::message::{Message, Payload};
use crate::receive::MessageDecoder;
use crate::NetError;

/// Inbound mailbox of a data receiver;
struct Inbox {
    pub channel_id: u128,
    tx: AtomicPtr<MessageSender<Payload>>,
    buffer: SegQueue<Payload>,
    is_exhaust: AtomicBool,
}

impl Inbox {
    fn new(channel_id: u128) -> Self {
        Inbox {
            channel_id,
            tx: AtomicPtr::default(),
            buffer: SegQueue::new(),
            is_exhaust: AtomicBool::new(false),
        }
    }

    fn reset_tx(&self, inbox: MessageSender<Payload>) -> Result<(), NetError> {
        let tx = Box::into_raw(Box::new(inbox));
        if self.tx.swap(tx, Ordering::SeqCst).is_null() {
            debug!("user inbox of channel {} is registered;", self.channel_id);
            // reset success;
            // let tx = self.tx.load(Ordering::SeqCst);
            assert!(!tx.is_null());
            while let Ok(pre) = self.buffer.pop() {
                if let Err(_) = unsafe { (*tx).send(pre) } {
                    error!("Inbox#reset_tx: send pre data in buffer failure;");
                    break;
                }
            }

            if self.is_exhaust.load(Ordering::SeqCst) {
                unsafe { (*tx).close() };
            }
            Ok(())
        } else {
            error!("Inbox#reset_tx: user tx of channel {} can't be reset;", self.channel_id);
            Err(NetError::ChannelRxReset(self.channel_id))
        }
    }

    fn push(&self, msg: Payload) {
        if !self.buffer.is_empty() {
            self.buffer.push(msg);
        } else {
            let tx = self.tx.load(Ordering::SeqCst);
            if tx.is_null() {
                self.buffer.push(msg);
            } else {
                if let Err(_) = unsafe { (*tx).send(msg) } {
                    error!("Inbox#push: send data failure;");
                }
            }
        }
    }

    fn close(&self) {
        self.is_exhaust.store(true, Ordering::SeqCst);
        let tx = self.tx.load(Ordering::SeqCst);
        if !tx.is_null() {
            debug!("close user inbox of channel {};", self.channel_id);
            unsafe { (*tx).close() };
        }
    }

    fn is_registered(&self) -> bool {
        !self.tx.load(Ordering::SeqCst).is_null()
    }
}

impl Drop for Inbox {
    fn drop(&mut self) {
        let tx = self.tx.load(Ordering::SeqCst);
        if !tx.is_null() {
            unsafe {
                std::ptr::drop_in_place(tx);
            }
        }
    }
}

struct InboxTable {
    shards: usize,
    table: Vec<ShardedLock<HashMap<u128, Inbox>>>,
}

impl InboxTable {
    pub fn new() -> Self {
        let mut table = Vec::with_capacity(1024);
        for _ in 0..1024 {
            table.push(ShardedLock::new(HashMap::new()));
        }
        InboxTable { shards: 1024, table }
    }
    pub fn dispatch(&self, ch_id: u128, msg: Payload) -> bool {
        self.call_with_lock(ch_id, |inbox| {
            inbox.push(msg);
            inbox.is_registered()
        })
    }

    pub fn register(&self, ch_id: u128, tx: MessageSender<Payload>) -> Result<(), NetError> {
        self.call_with_lock(ch_id, |inbox| inbox.reset_tx(tx))
    }

    pub fn close(&self, ch_id: u128) -> bool {
        self.call_with_lock(ch_id, |inbox| {
            inbox.close();
            inbox.is_registered()
        })
    }

    #[inline]
    fn call_with_lock<T, F>(&self, ch_id: u128, func: F) -> T
    where
        F: FnOnce(&Inbox) -> T,
    {
        let offset = (ch_id % self.shards as u128) as usize;
        {
            //std::sync::atomic::fence(Ordering::Release);
            let lock = self.table[offset]
                .read()
                .expect("InboxTable#dispatch: read lock poisoned");
            if let Some(inbox) = lock.get(&ch_id) {
                return func(inbox);
            }
        }
        {
            //std::sync::atomic::fence(Ordering::Acquire);
            let mut lock = self.table[offset]
                .write()
                .expect("InboxTable#dispatch: write lock poisoned");
            let inbox = lock
                .entry(ch_id)
                .or_insert_with(|| Inbox::new(ch_id));
            func(inbox)
        }
    }

    fn take(&self, ch_id: u128) -> Option<Inbox> {
        let offset = (ch_id % self.shards as u128) as usize;
        let mut lock = self.table[offset]
            .write()
            .expect("InboxTable#dispatch: write lock poisoned");
        lock.remove(&ch_id)
    }
}

struct ReadOptInboxTable {
    share: Arc<InboxTable>,
    cache: HashMap<u128, Inbox>,
}

impl ReadOptInboxTable {
    pub fn new() -> Self {
        ReadOptInboxTable { share: Arc::new(InboxTable::new()), cache: HashMap::new() }
    }

    pub fn dispatch(&mut self, ch_id: u128, msg: Payload) {
        if let Some(inbox) = self.cache.get(&ch_id) {
            inbox.push(msg);
        } else {
            if self.share.dispatch(ch_id, msg) {
                if let Some(inbox) = self.share.take(ch_id) {
                    self.cache.insert(ch_id, inbox);
                }
            }
        }
    }

    pub fn close(&self, ch_id: u128) {
        if let Some(inbox) = self.cache.get(&ch_id) {
            inbox.close();
        } else {
            if self.share.close(ch_id) {
                self.share.take(ch_id);
            }
        }
    }
}

/// The `NetReceiver` is used to receive messages from a network connection, and dispatch it to a
/// mailbox registered by the network applications;
#[allow(dead_code)]
pub(crate) struct NetReceiver<R: Read, D: MessageDecoder> {
    hb_sec: u64,
    reader: R,
    addr: SocketAddr,
    decoder: D,
    last_recv: Instant,
    inbox_table: ReadOptInboxTable,
}

impl<R: Read, D: MessageDecoder> NetReceiver<R, D> {
    pub fn new(hb_sec: u64, addr: SocketAddr, reader: R, decoder: D) -> Self {
        NetReceiver {
            hb_sec,
            reader,
            addr,
            decoder,
            last_recv: Instant::now(),
            inbox_table: ReadOptInboxTable::new(),
        }
    }

    pub fn recv(&mut self) -> Result<(), NetError> {
        if let Some(msg) = decode_next(&mut self.reader, &mut self.decoder)? {
            let (header, payload) = msg.separate();
            if header.channel_id == 0 {
                // This is a heartbeat signal;
            } else if header.sequence == 0 {
                assert_eq!(payload.len(), 0);
                debug!("receive  exhaust signal of channel {} from {:?};", header.channel_id, self.addr);
                self.inbox_table.close(header.channel_id);
            } else {
                self.inbox_table
                    .dispatch(header.channel_id, payload);
            }
            self.last_recv = Instant::now();
        } else {
            let elapsed = self.last_recv.elapsed().as_secs();
            if elapsed > self.hb_sec * 2 {
                return Err(NetError::HBAbnormal(self.addr));
            }
        }
        Ok(())
    }

    pub(crate) fn get_inbox_register(&self) -> InboxRegister {
        InboxRegister { addr: self.addr, inner: self.inbox_table.share.clone() }
    }
}

#[inline(always)]
fn decode_next<R: Read, D: MessageDecoder>(reader: &mut R, decoder: &mut D) -> io::Result<Option<Message>> {
    decoder.decode_next(reader)
}

#[allow(dead_code)]
pub(crate) struct InboxRegister {
    pub addr: SocketAddr,
    inner: Arc<InboxTable>,
}

impl InboxRegister {
    pub(crate) fn register(&self, channel_id: u128, tx: &MessageSender<Payload>) -> Result<(), NetError> {
        self.inner.register(channel_id, tx.clone())
    }
}

#[cfg(test)]
mod test {
    use std::io::Read;
    use std::time::Duration;

    use pegasus_common::channel::MPMCReceiver;
    use pegasus_common::codec::{AsBytes, Encode};
    use pegasus_common::io::WriteExt;

    use super::*;
    use crate::message::{MessageHeader, DEFAULT_MESSAGE_HEADER_BYTES};
    use crate::receive::decode::*;
    use crate::send::MessageEncoder;
    use crate::send::SimpleEncoder;

    #[test]
    fn inbox_test() {
        {
            let inbox = Inbox::new(1);
            let (tx, rx) = pegasus_common::channel::unbound();
            inbox.reset_tx(tx).unwrap();
            inbox.push(vec![1u8; 128].into());
            inbox.push(vec![2u8; 128].into());
            inbox.push(vec![3u8; 128].into());
            assert_eq!(rx.recv().unwrap().as_ref(), vec![1u8; 128].as_slice());
            assert_eq!(rx.recv().unwrap().as_ref(), vec![2u8; 128].as_slice());
            assert_eq!(rx.recv().unwrap().as_ref(), vec![3u8; 128].as_slice());
            inbox.close();
            if let Err(e) = rx.recv() {
                assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe);
            } else {
                panic!("expect broken pipe error;");
            }
        }

        {
            let inbox = Inbox::new(1);
            inbox.push(vec![1u8; 128].into());
            inbox.push(vec![2u8; 128].into());
            let (tx, rx) = pegasus_common::channel::unbound();
            inbox.reset_tx(tx).unwrap();
            let first = rx.recv().unwrap();
            assert_eq!(first.as_ref(), vec![1u8; 128].as_slice());
            let second = rx.recv().unwrap();
            assert_eq!(second.as_ref(), vec![2u8; 128].as_slice());
            inbox.push(vec![3u8; 128].into());
            let third = rx.recv().unwrap();
            assert_eq!(third.as_ref(), vec![3u8; 128].as_slice());
            let empty = rx.try_recv().unwrap();
            assert!(empty.is_none());
            inbox.close();
            if let Err(e) = rx.recv() {
                assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe);
            } else {
                panic!("expect broken pipe error;");
            }
        }

        {
            let inbox = Inbox::new(1);
            inbox.push(vec![1u8; 128].into());
            inbox.push(vec![2u8; 128].into());
            inbox.push(vec![3u8; 128].into());
            inbox.close();
            let (tx, rx) = pegasus_common::channel::unbound();
            inbox.reset_tx(tx).unwrap();
            assert_eq!(rx.recv().unwrap().as_ref(), vec![1u8; 128].as_slice());
            assert_eq!(rx.recv().unwrap().as_ref(), vec![2u8; 128].as_slice());
            assert_eq!(rx.recv().unwrap().as_ref(), vec![3u8; 128].as_slice());
            if let Err(e) = rx.recv() {
                assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe);
            } else {
                panic!("expect broken pipe error;");
            }
        }
    }

    fn net_rx_test<D: MessageDecoder>(decoder: D) {
        let mut bin_stream = Vec::with_capacity(1 << 12);
        // prepare binary stream for read;
        {
            struct Array(u8);

            impl Encode for Array {
                fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
                    let bytes = vec![self.0; 256];
                    writer.write_all(&bytes[..])
                }
            }

            // write some heart beat messages;
            bin_stream.extend(&*DEFAULT_MESSAGE_HEADER_BYTES);
            bin_stream.extend(&*DEFAULT_MESSAGE_HEADER_BYTES);
            bin_stream.extend(&*DEFAULT_MESSAGE_HEADER_BYTES);
            // write one object into channel 1..9;
            let mut encoder = SimpleEncoder::default();
            for i in 1..9 {
                let mut header = MessageHeader::default();
                header.sequence = 1;
                header.channel_id = i as u128;
                let b = encoder.encode(&mut header, &Array(i)).unwrap();
                bin_stream.extend_from_slice(b.as_ref());
                header.sequence = 0;
                header.length = 0;
                bin_stream.extend_from_slice(header.as_bytes());
            }
        }

        let reader = &bin_stream[0..];
        //let decoder = SimpleBlockDecoder::new();
        let mut net_rx = NetReceiver::new(5, "127.0.0.1:8080".parse().unwrap(), reader, decoder);
        let register = net_rx.get_inbox_register();
        let mut user_rx = vec![None; 9];

        for i in 5..9 {
            let (tx, rx) = pegasus_common::channel::unbound();
            register.register(i as u128, &tx).unwrap();
            tx.close();
            user_rx[i as usize] = Some(rx);
        }

        while let Ok(_) = net_rx.recv() {
            //
        }

        for i in 1..5 {
            let (tx, rx) = pegasus_common::channel::unbound();
            register.register(i as u128, &tx).unwrap();
            tx.close();
            user_rx[i as usize] = Some(rx);
        }

        for (i, rx) in user_rx.into_iter().enumerate() {
            if i > 0 {
                assert!(rx.is_some());
                let rx = rx.unwrap();
                let data = rx.recv().unwrap();
                assert_eq!(data.as_ref(), vec![i as u8; 256].as_slice());
                if let Err(e) = rx.recv() {
                    assert_eq!(e.kind(), std::io::ErrorKind::BrokenPipe);
                } else {
                    panic!("expect broken pipe error;");
                }
            }
        }
    }

    #[test]
    fn net_rx_on_simple_decoder_test() {
        let decoder = SimpleBlockDecoder::new();
        net_rx_test(decoder);
    }

    #[test]
    fn net_rx_on_reentrant_decoder_test() {
        let decoder = ReentrantDecoder::new();
        net_rx_test(decoder);
    }

    #[test]
    fn net_rx_on_reentrant_slab_decoder_test() {
        let decoder = ReentrantSlabDecoder::new(1 << 16);
        net_rx_test(decoder);
    }

    fn net_rx_on_hb_abnormal_test<D: MessageDecoder>(decoder: D) {
        struct MockReader;

        impl Read for MockReader {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                // mock not ready read;
                Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
            }
        }

        let mut header = MessageHeader::default();
        header.channel_id = 0;
        header.sequence = 0;
        let reader = header.as_bytes();
        let reader = std::io::Read::chain(reader, MockReader);
        let mut net_rx = NetReceiver::new(1, "127.0.0.1:8080".parse().unwrap(), reader, decoder);
        let start = Instant::now();
        loop {
            if let Err(e) = net_rx.recv() {
                println!("get error {}", e);
                match e {
                    NetError::HBAbnormal(addr) => {
                        assert_eq!(addr, "127.0.0.1:8080".parse().unwrap());
                        break;
                    }
                    _ => panic!("unexpected error {}", e),
                }
            } else {
                std::thread::sleep(Duration::from_millis(10))
            }
        }
        assert!(start.elapsed().as_secs() > 2);
    }

    #[test]
    fn net_rx_on_hb_abnormal_reentrant_decoder_test() {
        let decoder = ReentrantDecoder::new();
        net_rx_on_hb_abnormal_test(decoder);
    }

    #[test]
    fn net_rx_on_hb_abnormal_reentrant_slab_decoder_test() {
        let decoder = ReentrantSlabDecoder::new(1 << 10);
        net_rx_on_hb_abnormal_test(decoder)
    }
}
