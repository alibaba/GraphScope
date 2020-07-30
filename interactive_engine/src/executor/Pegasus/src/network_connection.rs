//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::net::{TcpStream, Shutdown};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Condvar};
use std::time::Duration;

/// A Tcp connection representation encapsulating many connection information;
pub struct Connection {
    /// index of the remote node of this connection;
    pub index: usize,
    /// the ip:port socket address of remote node;
    pub name: String,
    /// connection on tcp
    conn: TcpStream,
    ///  global flag used to present the state of connection;
    poison: Arc<AtomicBool>
}

impl Connection {
    pub fn new(index: usize, name: String, conn: TcpStream) -> Self {
        Connection {
            index,
            name,
            conn,
            poison: Arc::new(AtomicBool::new(false))
        }
    }

    /// poison current connection after error(e.g. broken pipe...) occurred;
    /// this connection won't be used evermore;
    pub fn poisoned(&self) {
        self.poison.store(true, Ordering::SeqCst);
    }

    #[inline]
    pub fn is_poisoned(&self) -> bool {
        self.poison.load(Ordering::SeqCst)
    }


    pub fn shutdown(&self) -> ::std::io::Result<()> {
        self.poisoned();
        self.conn.shutdown(Shutdown::Both)
    }

    /// To test if this connection is still available;
    /// return false if disconnected;
    pub fn sniff(&mut self) -> bool {
        if crate::network::EMPTY_HEAD.write_to(&mut self.conn).is_err() {
            self.poisoned();
            false
        } else { true }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if self.shutdown().is_err() {
            warn!("Connection to {} with address {} is dropped failed.", self.index, self.name);
        }
    }
}

impl Clone for Connection {
    fn clone(&self) -> Self {
        Connection {
            index: self.index,
            name: self.name.clone(),
            conn: self.conn.try_clone().expect("Connection#clone: try clone tcpstream failure"),
            poison: self.poison.clone()
        }
    }
}

impl ::std::ops::Deref for Connection {
    type Target = TcpStream;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl ::std::ops::DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}


pub struct ConnectManager {
    connection: Mutex<Option<Connection>>,
    cond: Condvar,
    signal: Arc<AtomicBool>
}

impl ConnectManager {
    pub fn new(signal: &Arc<AtomicBool>) -> Self {
        ConnectManager {
            connection: Mutex::new(None),
            cond: Condvar::new(),
            signal: signal.clone()
        }
    }

    pub fn set_connection(&self, conn: Connection) {
        let mut locked = self.connection.lock().expect("lock poison");
        locked.replace(conn);
        self.cond.notify_all();
    }

    /// Waiting to fetch the latest connection, this function will block the current thread, until
    /// a new connection is available;
    /// Return `None` if no connection will be updated; Also means that the network was been closed;
    pub fn wait_connection(&self) -> Option<Connection> {
        let mut locked = self.connection.lock().expect("lock poison");
        loop {
            if let Some(conn) = locked.as_ref() {
                if !conn.is_poisoned() {
                    return Some(conn.clone());
                }
            }
            if self.signal.load(Ordering::SeqCst) {
                return None;
            }
            locked.take();
            let result = self.cond.wait_timeout(locked,
                                                Duration::from_secs(1)).expect("wait poison");
            locked = result.0;
        }
    }
}
