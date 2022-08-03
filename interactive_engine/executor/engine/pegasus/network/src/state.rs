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
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam_utils::sync::ShardedLock;

#[allow(dead_code)]
struct ConnectionState {
    pub local_id: u64,
    pub remote_id: u64,
    addr: SocketAddr,
    disconnected: Arc<AtomicBool>,
}

impl ConnectionState {
    pub fn is_connected(&self) -> bool {
        !self.disconnected.load(Ordering::SeqCst)
    }
}

lazy_static! {
    static ref CONNECTION_STATES: ShardedLock<HashMap<(u64, u64), ConnectionState>> =
        ShardedLock::new(HashMap::new());
    static ref ADDR_TO_ID: ShardedLock<HashMap<SocketAddr, u64>> = ShardedLock::new(HashMap::new());
}

pub fn add_connection(local_id: u64, remote_id: u64, addr: SocketAddr) -> Option<Arc<AtomicBool>> {
    let disconnected = Arc::new(AtomicBool::new(false));
    {
        let mut states = CONNECTION_STATES
            .write()
            .expect("lock poisoned");
        let st = ConnectionState { local_id, remote_id, addr, disconnected: disconnected.clone() };
        if let Some(s) = states.get_mut(&(local_id, remote_id)) {
            if !s.is_connected() {
                *s = st;
            } else {
                error!(
                    "add connection to server[id={},addr={:?}] been refused, \
                server[id={},addr={:?}] is in use;",
                    remote_id, addr, s.remote_id, s.addr
                );
                return None;
            }
        } else {
            states.insert((local_id, remote_id), st);
        }
    }
    {
        let mut addr_to_id = ADDR_TO_ID.write().expect("lock poisoned");
        addr_to_id.insert(addr, remote_id);
    }
    Some(disconnected)
}

pub fn is_connected(local_id: u64, remote_id: u64) -> bool {
    let states = CONNECTION_STATES.read().expect("lock poisoned");
    local_id == remote_id
        || states
            .get(&(local_id, remote_id))
            .map(|s| s.is_connected())
            .unwrap_or(false)
}

pub fn check_connect(local: u64, remotes: &[u64]) -> bool {
    let states = CONNECTION_STATES.read().expect("lock poisoned");
    for id in remotes {
        if *id != local
            && !states
                .get(&(local, *id))
                .map(|s| s.is_connected())
                .unwrap_or(false)
        {
            return false;
        }
    }
    true
}
