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

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

use crate::{NetError, Server};

pub const DEFAULT_HEARTBEAT_INTERVAL_SEC: usize = 5;
pub const DEFAULT_SEND_BUFFER_SIZE: usize = 1440;
pub const DEFAULT_WAIT_USER_DATA_MILLSEC: usize = 100;
pub const DEFAULT_SLAB_SIZE: usize = 1 << 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockMode {
    Blocking(Option<Duration>),
    Nonblocking,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct WriteParams {
    pub mode: BlockMode,
    pub buffer: usize,
    pub nodelay: bool,
    pub wait_data: usize,
    pub heartbeat: usize,
}

impl Default for WriteParams {
    fn default() -> Self {
        WriteParams {
            mode: BlockMode::Nonblocking,
            buffer: DEFAULT_SEND_BUFFER_SIZE,
            nodelay: false,
            wait_data: DEFAULT_WAIT_USER_DATA_MILLSEC,
            heartbeat: DEFAULT_HEARTBEAT_INTERVAL_SEC,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ReadParams {
    pub mode: BlockMode,
    pub slab_size: usize,
}

impl Default for ReadParams {
    fn default() -> Self {
        ReadParams { mode: BlockMode::Nonblocking, slab_size: DEFAULT_SLAB_SIZE }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ConnectionParams {
    pub is_nonblocking: bool,
    write: WriteParams,
    read: ReadParams,
}

impl ConnectionParams {
    pub fn nonblocking() -> Self {
        let write = WriteParams::default();
        let read = ReadParams::default();
        ConnectionParams { is_nonblocking: true, write, read }
    }

    pub fn blocking() -> Self {
        let mut write = WriteParams::default();
        write.mode = BlockMode::Blocking(None);
        let mut read = ReadParams::default();
        read.mode = BlockMode::Blocking(None);
        ConnectionParams { is_nonblocking: false, write, read }
    }

    pub fn set_read_timeout(&mut self, timeout: Duration) {
        if !self.is_nonblocking {
            self.read.mode = BlockMode::Blocking(Some(timeout));
        }
    }

    pub fn set_write_timeout(&mut self, timeout: Duration) {
        if !self.is_nonblocking {
            self.write.mode = BlockMode::Blocking(Some(timeout));
        }
    }

    pub fn set_read_slab_size(&mut self, size: usize) {
        self.read.slab_size = size;
    }

    pub fn set_write_wait_data(&mut self, time_in_ms: usize) {
        self.write.wait_data = time_in_ms;
    }

    pub fn set_nodelay(&mut self) {
        self.write.nodelay = true;
    }

    pub fn set_write_buffer_size(&mut self, bytes: usize) {
        self.write.buffer = bytes;
    }

    pub fn set_heartbeat_interval(&mut self, interval: usize) {
        self.write.heartbeat = interval;
    }

    pub(crate) fn get_write_params(&self) -> &WriteParams {
        &self.write
    }

    pub(crate) fn get_read_params(&self) -> &ReadParams {
        &self.read
    }

    pub(crate) fn get_hb_interval_sec(&self) -> u32 {
        self.write.heartbeat as u32
    }
}

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    pub server_id: u64,
    pub servers_size: usize,
    nonblocking: Option<bool>,
    read_timeout_ms: Option<u32>,
    write_timeout_ms: Option<u32>,
    read_slab_size: Option<u32>,
    no_delay: Option<bool>,
    send_buffer: Option<u32>,
    heartbeat_sec: Option<u32>,
    servers: Option<Vec<Option<ServerAddr>>>,
}

#[derive(Debug, Deserialize)]
pub struct ServerAddr {
    ip: String,
    port: u16,
}

impl ServerAddr {
    pub fn new(ip: String, port: u16) -> Self {
        ServerAddr {
            ip,
            port
        }
    }

    pub fn get_ip(&self) -> &str {
        &self.ip
    }

    pub fn get_port(&self) -> u16 {
        self.port
    }
}

pub fn read_from<P: AsRef<Path>>(path: P) -> Result<NetworkConfig, NetError> {
    let config_str = std::fs::read_to_string(path)?;
    NetworkConfig::parse(&config_str)
}

impl NetworkConfig {
    pub fn new(server_id: u64, addr: ServerAddr, servers_size: usize) -> Self {
        let servers = if servers_size > 0 {
            let mut servers = Vec::with_capacity(servers_size);
            for _ in 0..servers_size {
                servers.push(None);
            }
            servers[server_id as usize] = Some(addr);
            Some(servers)
        } else {
            assert!((server_id as usize) < servers_size, "index out of bound, server_id({}) can't larger than server_size({})", server_id, servers_size);
            None
        };
        NetworkConfig {
            server_id,
            servers_size,
            nonblocking: None,
            read_timeout_ms: None,
            write_timeout_ms: None,
            read_slab_size: None,
            no_delay: None,
            send_buffer: None,
            heartbeat_sec: None,
            servers
        }
    }

    pub fn with(server_id: u64,  addrs: Vec<ServerAddr>) -> Self {
        let servers_size = addrs.len();
        let servers = if servers_size > 0 {
            let mut servers = Vec::with_capacity(servers_size);
            for s in addrs {
                servers.push(Some(s));
            }
            Some(servers)
        } else {
            assert!((server_id as usize) < servers_size, "index out of bound, server_id({}) can't larger than server_size({})", server_id, servers_size);
            None
        };
        NetworkConfig {
            server_id,
            servers_size,
            nonblocking: None,
            read_timeout_ms: None,
            write_timeout_ms: None,
            read_slab_size: None,
            no_delay: None,
            send_buffer: None,
            heartbeat_sec: None,
            servers
        }
    }

    pub fn parse(content: &str) -> Result<Self, NetError> {
        Ok(toml::from_str(&content)?)
    }

    pub fn nonblocking(&mut self, v: Option<bool>) -> &mut Self {
        match v {
            Some(true) => {
                self.set_nonblocking();
            }
            _ => ()
        }
        self
    }

    pub fn set_nonblocking(&mut self) -> &mut Self {
        self.nonblocking = Some(true);
        self
    }

    pub fn read_timeout_ms(&mut self, v: Option<u32>) -> &mut Self {
        if let Some(v) = v {
            self.set_read_timeout_ms(v);
        }
        self
    }

    pub fn set_read_timeout_ms(&mut self, timeout: u32) -> &mut Self {
        self.read_timeout_ms = Some(timeout);
        self
    }

    pub fn write_timeout_ms(&mut self, v: Option<u32>) -> &mut Self {
        if let Some(v) = v {
            self.set_write_timeout_ms(v);
        }
        self
    }

    pub fn set_write_timeout_ms(&mut self, timeout: u32) -> &mut Self {
        self.write_timeout_ms = Some(timeout);
        self
    }

    pub fn read_slab_size(&mut self, v: Option<u32>) -> &mut Self {
        if let Some(v) = v {
            self.set_read_slab_size(v);
        }
        self
    }

    pub fn set_read_slab_size(&mut self, slab_size: u32) -> &mut Self {
        self.send_buffer = Some(slab_size);
        self
    }


    pub fn no_delay(&mut self, v: Option<bool>) -> &mut Self {
        match v {
            Some(true) => {
                self.set_no_delay();
            },
            _ => ()
        }
        self
    }

    pub fn set_no_delay(&mut self) -> &mut Self {
        self.no_delay = Some(true);
        self
    }

    pub fn send_buffer(&mut self, v: Option<u32>) -> &mut Self {
        if let Some(v) = v {
            self.set_send_buffer(v);
        }
        self
    }

    pub fn set_send_buffer(&mut self, buffer_size: u32) -> &mut Self {
        self.send_buffer = Some(buffer_size);
        self
    }

    pub fn heartbeat_sec(&mut self, v: Option<u32>) -> &mut Self {
        if let Some(v) = v {
            self.set_heartbeat_sec(v);
        }
        self
    }

    pub fn set_heartbeat_sec(&mut self, seconds: u32) -> &mut Self {
        self.heartbeat_sec = Some(seconds);
        self
    }

    pub fn set_server_addr(&mut self, server_id: u64, addr: ServerAddr) -> Result<&mut Self, NetError> {
        if server_id as usize >= self.servers_size {
            Err(NetError::InvalidConfig(Some(format!("invalid server_id({}) larger than server size {}", server_id, self.servers_size))))
        } else {
            if let Some(ref mut servers) = self.servers {
               servers[server_id as usize] = Some(addr);
            } else {
                unreachable!("servers is none while server size = {}", self.servers_size);
            }
            Ok(self)
        }
    }

    pub fn local_addr(&self) -> Result<SocketAddr, NetError> {
        let index = self.server_id as usize;
        assert!(index < self.servers_size);
        if let Some(ref servers) = self.servers {
            if let Some(ref addr) = servers[index] {
                Ok(SocketAddr::new(addr.ip.parse()?, addr.port))
            } else {
                Err(NetError::InvalidConfig(Some("local server address not found".to_owned())))
            }
        } else {
            Err(NetError::InvalidConfig(Some("local server address not found".to_owned())))
        }
    }

    pub fn get_connection_param(&self) -> ConnectionParams {
        let mut params = if self.nonblocking.unwrap_or(false) {
            ConnectionParams::nonblocking()
        } else {
            let mut params = ConnectionParams::blocking();
            if let Some(w_t) = self.write_timeout_ms {
                if w_t > 0 {
                    params.set_write_timeout(Duration::from_millis(w_t as u64));
                }
            }
            if let Some(r_t) = self.read_timeout_ms {
                if r_t > 0 {
                    params.set_read_timeout(Duration::from_millis(r_t as u64));
                }
            }
            params
        };

        if let Some(rss) = self.read_slab_size {
            params.set_read_slab_size(rss as usize);
        }

        if self.no_delay.unwrap_or(false) {
            params.set_nodelay();
        }

        if let Some(buffer_size) = self.send_buffer {
            if buffer_size > 0 {
                params.set_write_buffer_size(buffer_size as usize);
            }
        }

        if let Some(hb_itvl) = self.heartbeat_sec {
            if hb_itvl > 0 {
                params.set_heartbeat_interval(hb_itvl as usize);
            }
        }

        params
    }

    pub fn get_servers(&self) -> Result<Option<Vec<Server>>, NetError> {
        if let Some(ref server_configs) = self.servers {
            let mut servers = Vec::with_capacity(server_configs.len());
            for (id, p) in server_configs.iter().enumerate() {
                if let Some(peer) = p {
                    let ip = peer.ip.parse()?;
                    let addr = SocketAddr::new(ip, peer.port);
                    let server = Server { id: id as u64, addr };
                    servers.push(server);
                } else {
                    return Err(NetError::InvalidConfig(Some(format!("addredd of server {} not found", id))));
                }
            }
            Ok(Some(servers))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config::BlockMode;

    #[test]
    fn toml_config_test() {
        let content = r#"
            server_id = 0
            nonblocking = false
            read_timeout_ms = 8
            write_timeout_ms = 8

            [[servers]]
            ip = '127.0.0.1'
            port = 8080

            [[servers]]
            ip = '127.0.0.1'
            port = 8081
        "#;

        let config = NetworkConfig::parse(content).unwrap();
        println!("get config {:?}", config);
        assert_eq!(config.server_id, 0);
        assert_eq!(config.nonblocking, Some(false));
        let params = config.get_connection_param();
        assert!(!params.is_nonblocking);
        let wp = params.get_write_params();
        assert_eq!(wp.mode, BlockMode::Blocking(Some(Duration::from_millis(8))));
        let rp = params.get_read_params();
        assert_eq!(rp.mode, BlockMode::Blocking(Some(Duration::from_millis(8))));
        assert_eq!(rp.slab_size, DEFAULT_SLAB_SIZE);
        assert_eq!(wp.nodelay, false);
        assert_eq!(wp.buffer, DEFAULT_SEND_BUFFER_SIZE);
        assert_eq!(wp.heartbeat, DEFAULT_HEARTBEAT_INTERVAL_SEC);
        let peers = config.get_servers().unwrap().unwrap();
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].id, 0);
        assert_eq!(peers[0].addr, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(peers[1].id, 1);
        assert_eq!(peers[1].addr, "127.0.0.1:8081".parse().unwrap());
    }
}
