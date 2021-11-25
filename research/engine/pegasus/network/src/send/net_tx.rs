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
use std::net::SocketAddr;
use std::sync::{Arc, Weak};
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError};

use crate::message::{Payload, DEFAULT_MESSAGE_HEADER_BYTES};

pub enum NetData {
    AppData(u128, Payload),
    Heartbeat(Payload),
}

#[allow(dead_code)]
pub struct NetSender<W: Write> {
    addr: SocketAddr,
    outbox: Receiver<NetData>,
    outbox_tx: (Weak<Sender<NetData>>, Option<Arc<Sender<NetData>>>),
    conn: W,
    next: Option<NetData>,
}

impl<W: Write> NetSender<W> {
    pub fn new(addr: SocketAddr, conn: W) -> Self {
        let (outbox_tx, outbox_rx) = crossbeam_channel::unbounded();
        let outbox_tx = Arc::new(outbox_tx);
        NetSender {
            addr,
            outbox: outbox_rx,
            outbox_tx: (Arc::downgrade(&outbox_tx), Some(outbox_tx)),
            conn,
            next: None,
        }
    }

    #[allow(dead_code)]
    pub fn get_outbox_tx(&self) -> &Option<Arc<Sender<NetData>>> {
        &self.outbox_tx.1
    }

    #[allow(dead_code)]
    pub fn take_outbox_tx(&mut self) -> Option<Arc<Sender<NetData>>> {
        self.outbox_tx.1.take()
    }

    pub fn send_heart_beat(&self) {
        if let Some(tx) = self.outbox_tx.0.upgrade() {
            if let Err(_) = tx.send(NetData::Heartbeat((&*DEFAULT_MESSAGE_HEADER_BYTES).into())) {
                error!("send heart beat to {:?} failure;", self.addr);
            }
        }
    }

    #[allow(dead_code)]
    pub fn try_send(&mut self, timeout: u64) -> io::Result<bool> {
        if let Some(msg) = self.next.take() {
            if let Some(msg) = self.try_send_inner(msg)? {
                self.next = Some(msg);
                return Ok(false);
            }
        }
        if timeout > 0 {
            let timeout = Duration::from_millis(timeout);
            match self.outbox.recv_timeout(timeout) {
                Ok(msg) => {
                    if let Some(msg) = self.try_send_inner(msg)? {
                        self.next = Some(msg);
                        return Ok(false);
                    } else {
                        loop {
                            match self.outbox.try_recv() {
                                Ok(msg) => {
                                    if let Some(msg) = self.try_send_inner(msg)? {
                                        self.next = Some(msg);
                                        return Ok(false);
                                    }
                                }
                                Err(TryRecvError::Empty) => {
                                    self.conn.flush()?;
                                    return Ok(false);
                                }
                                Err(TryRecvError::Disconnected) => {
                                    self.conn.flush()?;
                                    return Ok(true);
                                }
                            }
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => return Ok(false),
                Err(RecvTimeoutError::Disconnected) => return Ok(true),
            }
        } else {
            loop {
                match self.outbox.try_recv() {
                    Ok(msg) => {
                        if let Some(msg) = self.try_send_inner(msg)? {
                            self.next = Some(msg);
                            return Ok(false);
                        }
                    }
                    Err(TryRecvError::Empty) => {
                        self.conn.flush()?;
                        return Ok(false);
                    }
                    Err(TryRecvError::Disconnected) => {
                        self.conn.flush()?;
                        return Ok(true);
                    }
                }
            }
        }
    }

    #[inline]
    fn try_send_inner(&mut self, data: NetData) -> io::Result<Option<NetData>> {
        Ok(match data {
            NetData::AppData(ch_id, mut p) => match self.try_write(&mut p) {
                Ok(finish) => {
                    if finish {
                        None
                    } else {
                        Some(NetData::AppData(ch_id, p))
                    }
                }
                Err(e) => {
                    super::report_network_error(ch_id, self.addr);
                    return Err(e);
                }
            },
            NetData::Heartbeat(mut p) => {
                if !self.try_write(&mut p)? {
                    Some(NetData::Heartbeat(p))
                } else {
                    None
                }
            }
        })
    }

    /// 将发送队列里的数据依次写入底层数据传输链路。
    ///
    /// # Note :
    /// 这个方法依赖[`write_all`] 方法写入每组数据，由于底层传输链路的IO特性，[`write_all`] 可能多次发起写入
    /// 操作来完成所有数据的写入并返回， 期间的任何一次写操作可能会出现异常并抛出错误， [`write_all`]只会对非
    /// 致命的异常[`ErrorKind::Interrupted`]进行重试， 其他异常将中断[`write_all`]操作， 并将错误信息返回调
    /// 用方，一旦此类现象发生， 调用方无法确切得知哪些数据已被写入， 哪些数据未被写入， 因此只能废弃整个写入连接，
    /// 上层应用可以通过调用 [`check_has_network_error`] 方法来检查是否存在写入异常，并籍此来决定是否放弃当前
    /// 建立在这个写入连接上的任务. 但通常情况下， 由于数据的完整性被破坏， 上层应用无法获取正确的数据， 放弃当前
    /// 任务并等连接重建后提交新的任务进行重试， 是最可靠的方式。
    ///
    /// 通常情况下，如果[`write_all`]方法返回异常，也确实意味着整个连接已不可用， 但也存在一些特殊情况，主要为：
    /// * [`ErrorKind::WouldBlock`] : 底层IO为非阻塞IO， 在写入操作无法立即完成时返回此类错误，提示写入方稍后
    /// 重试.
    /// * [`ErrorKind::TimedOut`] : 底层IO设置了写入超时时间， 写入操作无法在设定的时间内完成时， 返回此类错误
    /// 提升写入发起方稍后重试.
    ///
    /// 以上错误类型并不意味着写入连接不可用。 但由于写入方收到错误消息后， 仍然不能判断哪些数据被写入哪些需要重试，
    /// 因此也将导致后续所有的写入无法进行，只能放弃整个写入连接；
    ///
    /// 综上，调用该方法前，调用方需要确认底层IO为阻塞IO， 并且未设置写入超时时间；如果底层IO是非阻塞IO或设置了写
    /// 超时时间，调用[`try_send`]方法更为合适；
    ///
    /// [`write_all`]: https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all
    /// [`ErrorKind::Interrupted`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.Interrupted
    /// [`ErrorKind::WouldBlock`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.WouldBlock
    /// [`ErrorKind::TimedOut`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.TimedOut
    /// [`check_has_network_error`]: #method.check_has_network_error
    /// [`try_send`]: #method.try_send
    pub fn send(&mut self, timeout: u64) -> io::Result<bool> {
        if timeout > 0 {
            match self
                .outbox
                .recv_timeout(Duration::from_millis(timeout))
            {
                Ok(msg) => {
                    self.write(msg)?;
                    loop {
                        match self.outbox.try_recv() {
                            Ok(msg) => self.write(msg)?,
                            Err(TryRecvError::Empty) => {
                                self.conn.flush()?;
                                return Ok(false);
                            }
                            Err(TryRecvError::Disconnected) => return Ok(true),
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => return Ok(false),
                Err(RecvTimeoutError::Disconnected) => return Ok(true),
            }
        } else {
            loop {
                match self.outbox.try_recv() {
                    Ok(msg) => self.write(msg)?,
                    Err(TryRecvError::Empty) => {
                        self.conn.flush()?;
                        return Ok(false);
                    }
                    Err(TryRecvError::Disconnected) => {
                        self.conn.flush()?;
                        return Ok(true);
                    }
                }
            }
        }
    }

    pub fn take_writer(self) -> W {
        self.conn
    }

    #[inline]
    fn write(&mut self, data: NetData) -> io::Result<()> {
        match data {
            NetData::AppData(ch_id, data) => {
                if let Err(e) = self.conn.write_all(data.as_ref()) {
                    super::report_network_error(ch_id, self.addr);
                    return Err(e);
                }
            }
            NetData::Heartbeat(data) => self.conn.write_all(data.as_ref())?,
        }
        Ok(())
    }

    #[allow(dead_code)]
    #[inline]
    fn try_write(&mut self, buf: &mut Payload) -> io::Result<bool> {
        loop {
            match self.conn.write(buf.as_ref()) {
                Ok(size) => {
                    let buf_len = buf.len();
                    if size == 0 && buf_len != 0 {
                        return Err(io::Error::from(io::ErrorKind::WriteZero));
                    }
                    if size < buf.len() {
                        buf.advance(size);
                    } else {
                        break;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
                    return Ok(false);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn net_send(block: bool, timeout: u64) {
        let writer: Vec<u8> = Vec::with_capacity(1 << 20);
        let mut net_tx = NetSender::new("0.0.0.0:0".parse::<SocketAddr>().unwrap(), writer);
        let mailbox = net_tx.take_outbox_tx().unwrap();
        mailbox
            .send(NetData::AppData(1, vec![1u8; 256].into()))
            .unwrap();
        mailbox
            .send(NetData::AppData(1, vec![2u8; 256].into()))
            .unwrap();
        mailbox
            .send(NetData::AppData(1, vec![3u8; 256].into()))
            .unwrap();
        mailbox
            .send(NetData::AppData(1, vec![4u8; 256].into()))
            .unwrap();
        mailbox
            .send(NetData::AppData(1, vec![5u8; 256].into()))
            .unwrap();
        mailbox
            .send(NetData::AppData(1, vec![6u8; 256].into()))
            .unwrap();
        mailbox
            .send(NetData::AppData(1, vec![7u8; 256].into()))
            .unwrap();
        mailbox
            .send(NetData::AppData(1, vec![8u8; 256].into()))
            .unwrap();

        std::mem::drop(mailbox);
        if block {
            while !net_tx.send(timeout).unwrap() {}
        } else {
            while !net_tx.try_send(timeout).unwrap() {}
        }

        assert_eq!(net_tx.conn.len(), 256 * 8);
        let mut content = net_tx.conn.as_slice();
        for i in 1..9u8 {
            assert_eq!(&content[0..256], vec![i; 256].as_slice());
            content = &content[256..];
        }
    }

    #[test]
    fn net_send_block() {
        net_send(true, 0)
    }

    #[test]
    fn net_send_block_timeout() {
        net_send(true, 100);
    }

    #[test]
    fn net_send_nonblock() {
        net_send(false, 0);
    }

    #[test]
    fn net_send_nonblock_timeout() {
        net_send(false, 100)
    }
}
