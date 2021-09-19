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
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crossbeam_channel::Sender;
use crossbeam_utils::sync::ShardedLock;
use pegasus_common::codec::Encode;

use crate::config::{BlockMode, ConnectionParams, DEFAULT_SLAB_SIZE};
use crate::message::MessageHeader;
use crate::{NetError, Server};

mod encode;
pub use encode::{GeneralEncoder, MessageEncoder, SimpleEncoder, SlabEncoder};

mod net_tx;
use net_tx::{NetData, NetSender};

pub struct IPCSender<T: Encode> {
    pub target: SocketAddr,
    pub channel_id: u128,
    sequence: u64,
    encoder: GeneralEncoder<T>,
    outbox_tx: Sender<NetData>,
    close_guard: Arc<AtomicUsize>,
}

impl<T: Encode> IPCSender<T> {
    pub fn send(&mut self, msg: &T) -> io::Result<()> {
        let mut header = MessageHeader::new(self.channel_id);
        header.sequence = self.sequence;
        let payload = self.encoder.encode(&mut header, msg)?;
        self.outbox_tx
            .send(NetData::AppData(self.channel_id, payload))
            .map_err(|_| {
                error!("DefaultAppSender#send: network outbox disconnected;");
                io::Error::from(io::ErrorKind::BrokenPipe)
            })?;
        self.sequence += 1;
        Ok(())
    }

    pub fn close(&mut self) -> io::Result<()> {
        if self.close_guard.fetch_sub(1, Ordering::SeqCst) == 1 {
            let mut header = MessageHeader::new(self.channel_id);
            header.sequence = 0;
            self.outbox_tx
                .send(NetData::AppData(self.channel_id, header.into()))
                .map_err(|_| {
                    error!("DefaultAppSender#close: network outbox disconnected;");
                    io::Error::from(io::ErrorKind::BrokenPipe)
                })?;
        }
        Ok(())
    }
}

impl<T: Encode + 'static> IPCSender<T> {
    fn new(target: SocketAddr, channel_id: u128, outbox_tx: Sender<NetData>) -> Self {
        IPCSender {
            target,
            channel_id,
            sequence: 1,
            encoder: SlabEncoder::new(DEFAULT_SLAB_SIZE).into(),
            outbox_tx,
            close_guard: Arc::new(AtomicUsize::new(1)),
        }
    }

    pub fn reset_slab_size(&mut self, slab_size: usize) {
        if slab_size == 0 {
            self.encoder = SimpleEncoder::default().into();
        } else {
            self.encoder = SlabEncoder::new(slab_size).into();
        }
    }
}

impl<T: Encode + 'static> Clone for IPCSender<T> {
    fn clone(&self) -> Self {
        self.close_guard.fetch_add(1, Ordering::SeqCst);
        IPCSender {
            target: self.target,
            channel_id: self.channel_id,
            sequence: 1,
            encoder: self.encoder.clone(),
            outbox_tx: self.outbox_tx.clone(),
            close_guard: self.close_guard.clone(),
        }
    }
}

lazy_static! {
    static ref REMOTE_MSG_SENDER: ShardedLock<HashMap<(u64, u64), (SocketAddr, Weak<Sender<NetData>>)>> =
        ShardedLock::new(HashMap::new());
    static ref NETWORK_SEND_ERRORS: Mutex<HashMap<u128, Vec<SocketAddr>>> = Mutex::new(HashMap::new());
}

#[inline]
fn report_network_error(ch_id: u128, addr: SocketAddr) {
    error!("IPC channel[{}]: fail to send data to {:?};", ch_id, addr);
    let mut lock = NETWORK_SEND_ERRORS
        .lock()
        .expect("fail to lock NETWORK_SEND_ERRORS");
    lock.entry(ch_id)
        .or_insert_with(|| vec![])
        .push(addr);
}

#[inline]
pub fn check_has_network_error(ch_id: u128) -> Option<Vec<SocketAddr>> {
    if let Ok(ref mut lock) = NETWORK_SEND_ERRORS.try_lock() {
        lock.remove(&ch_id)
    } else {
        None
    }
}

pub fn check_remotes_send_ready(local: u64, remotes: &[u64]) -> bool {
    let lock = REMOTE_MSG_SENDER
        .read()
        .expect("REMOTE_MSG_SEND read lock poisoned");
    for id in remotes {
        if *id != local {
            if !lock.contains_key(&(local, *id)) {
                return false;
            }
        }
    }
    true
}

pub(crate) fn add_remote_sender(local_id: u64, server: &Server, tx: &Arc<Sender<NetData>>) {
    let tx = Arc::downgrade(tx);
    let mut lock = REMOTE_MSG_SENDER
        .write()
        .expect("REMOTE_MSG_SENDER write lock poisoned");
    lock.insert((local_id, server.id), (server.addr, tx));
}

pub(crate) fn remove_remote_sender(local_id: u64, remote_id: u64) {
    let mut lock = REMOTE_MSG_SENDER
        .write()
        .expect("REMOTE_MSG_SENDER write lock poisoned");
    lock.remove(&(local_id, remote_id));
}

pub fn fetch_remote_sender<T: Encode + 'static>(
    channel_id: u128, local: u64, remotes: &[u64],
) -> Result<Vec<IPCSender<T>>, NetError> {
    let lock = REMOTE_MSG_SENDER
        .read()
        .expect("REMOTE_MSG_SEND read lock poisoned");
    let mut app_senders = Vec::with_capacity(remotes.len());
    for id in remotes {
        if *id != local {
            if let Some((addr, tx)) = lock.get(&(local, *id)) {
                if let Some(tx) = tx.upgrade() {
                    let tx = tx.deref().clone();
                    let sender = IPCSender::<T>::new(*addr, channel_id, tx);
                    app_senders.push(sender);
                } else {
                    return Err(NetError::NotConnected(*id));
                }
            } else {
                return Err(NetError::NotConnected(*id));
            }
        }
    }
    Ok(app_senders)
}

pub(crate) fn start_net_sender(
    local_id: u64, remote: Server, params: &ConnectionParams, state: &Arc<AtomicBool>, conn: TcpStream,
) {
    let mut is_block = !params.is_nonblocking;
    let params = params.get_write_params();
    match params.mode {
        BlockMode::Blocking(timeout) => {
            conn.set_write_timeout(timeout).ok();
            is_block = false;
        }
        _ => (),
    }
    conn.set_nodelay(params.nodelay).ok();
    let disconnected = state.clone();
    let timeout = params.wait_data as u64;
    let guard = if params.buffer > 0 {
        let writer = std::io::BufWriter::with_capacity(params.buffer, conn);
        let mut net_tx = NetSender::new(remote.addr, writer);
        let tx = net_tx.get_outbox_tx().as_ref().expect("");
        add_remote_sender(local_id, &remote, tx);
        std::thread::Builder::new()
            .name(format!("net-sender-{}", remote.id))
            .spawn(move || {
                busy_send(&mut net_tx, is_block, timeout, local_id, remote.id);
                disconnected.store(true, Ordering::SeqCst);
                net_tx
                    .take_writer()
                    .get_ref()
                    .shutdown(std::net::Shutdown::Write)
                    .ok();
            })
            .expect("start net-sender thread failure;")
    } else {
        let mut net_tx = NetSender::new(remote.addr, conn);
        let tx = net_tx.get_outbox_tx().as_ref().expect("");
        add_remote_sender(local_id, &remote, &tx);
        std::thread::Builder::new()
            .name(format!("net-sender-{}", remote.id))
            .spawn(move || {
                busy_send(&mut net_tx, is_block, timeout, local_id, remote.id);
                disconnected.store(true, Ordering::SeqCst);
                net_tx
                    .take_writer()
                    .shutdown(std::net::Shutdown::Write)
                    .ok();
            })
            .expect("start net-sender thread failure;")
    };
    crate::add_network_thread(local_id, guard);
}

fn busy_send<W: Write>(net_tx: &mut NetSender<W>, block: bool, timeout: u64, local: u64, remote: u64) {
    let heart_beat_tick = crossbeam_channel::tick(Duration::from_secs(5));
    while !crate::is_shutdown(local) {
        let result = if block { net_tx.send(timeout) } else { net_tx.try_send(timeout) };
        match result {
            Ok(true) => {
                info!("finish sending all data to {:?}", remote);
                break;
            }
            Err(e) => {
                error!("fail to send data to {:?}, caused by {};", remote, e);
                break;
            }
            _ => {
                if let Ok(_) = heart_beat_tick.try_recv() {
                    net_tx.send_heart_beat();
                }
            }
        }
    }
    info!("IPC sender to {:?} exit;", remote);
    remove_remote_sender(local, remote);
}
