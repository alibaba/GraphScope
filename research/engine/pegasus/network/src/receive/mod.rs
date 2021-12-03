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
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam_utils::sync::ShardedLock;
use pegasus_common::channel::{MPMCReceiver, MPMCSender, MessageReceiver};
use pegasus_common::codec::Decode;

use crate::message::Payload;
use crate::{NetError, Server};

mod decode;
mod net_rx;
pub use decode::{MessageDecoder, ReentrantDecoder, ReentrantSlabDecoder, SimpleBlockDecoder};
use net_rx::{InboxRegister, NetReceiver};

use crate::config::{BlockMode::Blocking, ConnectionParams};

/// The receiver for network's applications to receive data from all remote peers;
pub struct IPCReceiver<T> {
    inbox: MessageReceiver<Payload>,
    _ph: std::marker::PhantomData<T>,
}

impl<T: Decode> IPCReceiver<T> {
    pub fn new(inbox: MessageReceiver<Payload>) -> Self {
        IPCReceiver { inbox, _ph: std::marker::PhantomData }
    }

    pub fn recv(&self) -> io::Result<Option<T>> {
        if let Some(payload) = self.inbox.try_recv()? {
            let mut reader = payload.as_ref();
            let item = T::read_from(&mut reader)?;
            Ok(Some(item))
        } else {
            Ok(None)
        }
    }
}

lazy_static! {
    static ref REMOTE_RECV_REGISTER: ShardedLock<HashMap<(u64, u64), InboxRegister>> =
        ShardedLock::new(HashMap::new());
}

pub fn check_remotes_read_ready(local: u64, remotes: &[u64]) -> bool {
    let lock = REMOTE_RECV_REGISTER
        .read()
        .expect("failure to lock REMOTE_RECV_REGISTER");
    for id in remotes.iter() {
        if *id != local {
            if !lock.contains_key(&(local, *id)) {
                return false;
            }
        }
    }
    true
}

fn add_remote_register(local: u64, remote: u64, register: InboxRegister) {
    let mut lock = REMOTE_RECV_REGISTER
        .write()
        .expect("failure to lock REMOTE_RECV_REGISTER");
    lock.insert((local, remote), register);
}

fn remove_remote_register(local: u64, remote: u64) -> Option<InboxRegister> {
    let mut lock = REMOTE_RECV_REGISTER
        .write()
        .expect("failure to lock REMOTE_RECV_REGISTER");
    lock.remove(&(local, remote))
}

pub fn register_remotes_receiver<T: Decode + 'static>(
    channel_id: u128, local: u64, remotes: &[u64],
) -> Result<IPCReceiver<T>, NetError> {
    let (tx, rx) = pegasus_common::channel::unbound::<Payload>();
    let lock = REMOTE_RECV_REGISTER
        .read()
        .expect("failure to lock REMOTE_RECV_REGISTER");
    for id in remotes.iter() {
        if *id != local {
            if let Some(register) = lock.get(&(local, *id)) {
                register.register(channel_id, &tx)?;
            } else {
                error!("server with id = {} is not connect;", id);
                return Err(NetError::NotConnected(*id));
            }
        }
    }
    tx.close();
    Ok(IPCReceiver::new(rx))
}

pub fn start_net_receiver(
    local: u64, remote: Server, hb_sec: u32, params: &ConnectionParams, state: &Arc<AtomicBool>,
    conn: TcpStream,
) {
    //    let decoder = DefaultBlockDecoder::new(conn);
    if let Blocking(timeout) = params.get_read_params().mode {
        conn.set_read_timeout(timeout).ok();
    }

    let slab_size = params.get_read_params().slab_size;
    let decoder = self::decode::get_reentrant_decoder(slab_size);
    let mut net_recv = NetReceiver::new(hb_sec as u64, remote.addr, conn, decoder);
    let register = net_recv.get_inbox_register();
    add_remote_register(local, remote.id, register);
    let disconnected = state.clone();
    let guard = std::thread::Builder::new()
        .name(format!("net-recv-{}-{}", remote.id, local))
        .spawn(move || {
            while !crate::is_shutdown(local) {
                if let Err(e) = net_recv.recv() {
                    error!("fail to read data from server {:?}, caused by {:?};", remote, e);
                    break;
                }
            }
            disconnected.store(true, Ordering::SeqCst);
            remove_remote_register(local, remote.id);
            info!("IPC receiver recv from {:?} exit;", remote);
        })
        .expect("start net recv thread failure;");
    crate::add_network_thread(local, guard);
}
