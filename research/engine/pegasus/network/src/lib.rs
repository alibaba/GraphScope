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

#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate enum_dispatch;

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_utils::sync::ShardedLock;
use pegasus_common::codec::*;

use crate::config::ConnectionParams;

lazy_static! {
    static ref SHUTDOWN_HOOK: ShardedLock<HashMap<u64, Arc<AtomicBool>>> = ShardedLock::new(HashMap::new());
    static ref NETWORK_THREADS: Mutex<HashMap<u64, Vec<JoinHandle<()>>>> = Mutex::new(HashMap::new());
}

pub fn start_up<D: ServerDetect + 'static, A: ToSocketAddrs>(
    server_id: u64, conf: ConnectionParams, addr: A, detect: D,
) -> Result<SocketAddr, NetError> {
    info!("start server {} ...", server_id);
    let mut mgr = manager::ServerManager::new(server_id, conf, detect);
    {
        let mut lock = SHUTDOWN_HOOK
            .write()
            .expect("SHUTDOWN_HOOK write lock failure;");
        if lock.contains_key(&server_id) {
            return Err(NetError::ServerStarted(server_id));
        } else {
            lock.insert(server_id, Arc::new(AtomicBool::new(false)));
        }
    }

    let addr = mgr.bind(addr)?;
    let guard = std::thread::Builder::new()
        .name(format!("net-manager-{}", server_id))
        .spawn(move || {
            while !is_shutdown(server_id) {
                mgr.refresh();
                std::thread::sleep(Duration::from_secs(2));
            }
            info!("net-manager-{} exit;", server_id);
        })
        .expect("spawn network manager thread failure;");

    add_network_thread(server_id, guard);
    Ok(addr)
}

#[inline]
pub fn shutdown(server_id: u64) {
    info!("server {} shutdown...", server_id);
    let mut lock = SHUTDOWN_HOOK
        .write()
        .expect("SHUTDOWN_HOOK write lock failure;");
    if let Some(hook) = lock.remove(&server_id) {
        hook.store(true, Ordering::SeqCst);
    }
}

#[inline]
pub fn is_shutdown(server_id: u64) -> bool {
    let lock = SHUTDOWN_HOOK
        .read()
        .expect("SHUTDOWN_HOOK read lock failure;");
    if let Some(hook) = lock.get(&server_id) {
        hook.load(Ordering::SeqCst)
    } else {
        true
    }
}

#[inline]
pub fn get_shutdown_hook(server_id: u64) -> Option<Arc<AtomicBool>> {
    let lock = SHUTDOWN_HOOK
        .read()
        .expect("SHUTDOWN_HOOK read lock failure;");
    lock.get(&server_id).map(|hook| hook.clone())
}

#[inline]
pub fn await_termination(server_id: u64) {
    let resources = {
        let mut lock = NETWORK_THREADS
            .lock()
            .expect("fetch lock of NETWORK_THREADS failure;");
        lock.remove(&server_id)
    };
    if let Some(mut resources) = resources {
        debug!("wait {} resources terminate;", resources.len());
        for g in resources.drain(..) {
            if let Err(err) = g.join() {
                error!("network#wait_termination: found error: {:?};", err);
            }
        }
    }
}

pub(crate) fn add_network_thread(server_id: u64, guard: JoinHandle<()>) {
    let mut lock = NETWORK_THREADS
        .lock()
        .expect("fetch lock of NETWORK_THREADS failure;");
    lock.entry(server_id)
        .or_insert_with(|| vec![])
        .push(guard);
}

pub mod config;
mod error;
mod manager;
mod message;
mod receive;
mod send;
mod state;
mod transport;

pub use error::NetError;
pub use manager::ServerDetect;
pub use manager::SimpleServerDetector;
#[cfg(feature = "benchmark")]
pub use message::{MessageHeader, MESSAGE_HEAD_SIZE};
pub use receive::IPCReceiver;
#[cfg(feature = "benchmark")]
pub use receive::{MessageDecoder, ReentrantDecoder, ReentrantSlabDecoder, SimpleBlockDecoder};
pub use send::{check_has_network_error, IPCSender};
#[cfg(feature = "benchmark")]
pub use send::{MessageEncoder, SimpleEncoder, SlabEncoder};
pub use state::check_connect;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Server {
    pub id: u64,
    pub addr: SocketAddr,
}

pub struct InterProcessesComm<T: Codec> {
    /// integer unique id of a application use this network library, used to distinguish communication_old
    /// messages between different applications;
    pub channel_id: u128,
    /// Data senders, each sender responsible for sending data to one remote process;
    senders: Vec<IPCSender<T>>,
    /// Data receiver, responsible for receiving data from all remote processes;
    receive: IPCReceiver<T>,
}

impl<T: Codec> InterProcessesComm<T> {
    pub fn take(self) -> (Vec<IPCSender<T>>, IPCReceiver<T>) {
        (self.senders, self.receive)
    }
}

/// 创建用于多个进程间通信的数据通道(channel), 数据以FIFO顺序被读取;
/// - 参数 `channel_id` 指定将要创建的channel的128位整数id; `channel_id`用于区分不同的数据通道；
/// - 参数 `targets` 指定将要建立通信的进程id, 每个进程需要提前分配一个唯一的64位整数id;
pub fn ipc_channel<T: Codec + 'static>(
    channel_id: u128, local: u64, remotes: &[u64],
) -> Result<InterProcessesComm<T>, NetError> {
    if channel_id == 0 {
        return Err(NetError::IllegalChannelId);
    }
    let txs = crate::send::fetch_remote_sender::<T>(channel_id, local, remotes)?;
    let rx = crate::receive::register_remotes_receiver(channel_id, local, remotes)?;
    Ok(InterProcessesComm { channel_id, senders: txs, receive: rx })
}

/// 创建特定channel id 的多进程间通信channel的发送端；
///
/// # Arguments
///
/// * `channel_id` : 期望获取发送端的目标channel 的id;
/// * `targets`    : 期望与其建立IPC channel的服务进程的id列表；
///
/// # Returns
///
/// * `Ok(Vec<IPCSender<T>>)` : 其中每个sender负责发送数据给参数`targets`中的一个server，返回senders和
/// 参数`targets`保持相同的顺序， 即 `senders[i]` 负责与 `targets[i]`的通信；
/// * `Err(NetError)`   : 获取channel senders失败， 可能的原因包括非法的`channel_id`, 或尝试连接未建立连接的
/// 服务进程等；
///
pub fn ipc_channel_send<T: Codec + 'static>(
    channel_id: u128, local: u64, remotes: &[u64],
) -> Result<Vec<IPCSender<T>>, NetError> {
    if channel_id == 0 {
        return Err(NetError::IllegalChannelId);
    }
    crate::send::fetch_remote_sender::<T>(channel_id, local, remotes)
}

/// 创建指定channel_id 的多进程间通信channel的接收端recv； 该recv将负责接收来自多个服务进程上发送的数据；
///
///
pub fn ipc_channel_recv<T: Codec + 'static>(
    channel_id: u128, local: u64, targets: &[u64],
) -> Result<IPCReceiver<T>, NetError> {
    if channel_id == 0 {
        return Err(NetError::IllegalChannelId);
    }
    crate::receive::register_remotes_receiver(channel_id, local, targets)
}

pub fn check_ipc_ready(local: u64, remotes: &[u64]) -> bool {
    crate::state::check_connect(local, remotes)
        && crate::send::check_remotes_send_ready(local, remotes)
        && crate::receive::check_remotes_read_ready(local, remotes)
}
