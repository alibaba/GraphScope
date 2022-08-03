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
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::receive::start_net_receiver;
use crate::send::start_net_sender;
use crate::transport::ConnectionParams;
use crate::{NetError, Server};

pub fn listen_on<A: ToSocketAddrs>(
    server_id: u64, params: ConnectionParams, addr: A,
) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let bind_addr = listener.local_addr()?;
    info!("network listen on {:?}", bind_addr);
    listener.set_nonblocking(true).ok();
    let hb_sec = params.get_hb_interval_sec();
    let guard = std::thread::Builder::new()
        .name("network-listener".to_owned())
        .spawn(move || {
            while !crate::is_shutdown(server_id) {
                match listener.accept() {
                    Ok((mut stream, addr)) => {
                        if let Ok(Some((remote_id, hb))) = super::check_connection(&mut stream) {
                            info!("accept new connection from server {} on {:?}", remote_id, addr);
                            if !crate::state::is_connected(server_id, remote_id) {
                                // create network communication_old channel for lib user;
                                let mut write_half = stream
                                    .try_clone()
                                    .expect("clone tcp stream failure;");
                                if let Err(e) = super::setup_connection(server_id, hb_sec, &mut write_half)
                                {
                                    error!("write pass phrase to {:?} failure: {}", addr, e);
                                } else {
                                    let hook = crate::state::add_connection(server_id, remote_id, addr)
                                        // add connection should never fail;
                                        .expect("add connection failure");
                                    let remote = Server { id: remote_id, addr };
                                    if params.is_nonblocking {
                                        stream.set_nonblocking(true).ok();
                                    }
                                    start_net_sender(server_id, remote, &params, &hook, write_half);
                                    start_net_receiver(server_id, remote, hb, &params, &hook, stream);
                                }
                            } else {
                                warn!("server {} is connected and already in use;", remote_id);
                            }
                        } else {
                            warn!("illegal connection from {:?}, ignored;", addr);
                        }
                    }
                    Err(e) => {
                        if e.kind() != std::io::ErrorKind::WouldBlock {
                            error!("TcpListener call accept error: {:?}", e);
                        }
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
            }
            info!("network listener exit;")
        })
        .expect("create listener thread failure;");
    crate::add_network_thread(server_id, guard);
    Ok(bind_addr)
}

/// 尝试建立新的TCP连接：
/// - 参数 `addr`为期望建立连接的对端服务监听的 socket 地址；
/// - 参数`server_id` 是对端服务的序号；
///
/// 按照约定， 用连续的整数为每个服务分配一个唯一id 作为标识。 大序号server主动连接小序号server,
/// 小序号server等待大序号server的连接请求。
/// 由于TCP的双向通信， 读写复用同一个连接, 因此一旦listener accept 到一个大序号server的连接，
/// 该连接将既用于读也用于写， 小序号server不必再发起新的连接请求;
///
/// 如果参数中的`server_id` 大于等于当前服务的id，并不会发起连接，返回`Ok(())`;
///
pub fn connect<A: ToSocketAddrs>(
    local_id: u64, remote_id: u64, params: ConnectionParams, addr: A,
) -> Result<(), NetError> {
    // 连接请求可能会失败， 或许由于对端服务器未启动端口监听，调用方需要根据返回内容确定是否重试;
    let mut conn = TcpStream::connect(addr)?;
    let addr = conn.peer_addr()?;
    debug!("connect to server {:?};", addr);
    let hb_sec = params.get_hb_interval_sec();
    super::setup_connection(local_id, hb_sec, &mut conn)?;
    debug!("setup connection to {:?} success;", addr);
    if let Some((id, hb_sec)) = super::check_connection(&mut conn)? {
        if id == remote_id {
            info!("connect server {} on {:?} success;", remote_id, addr);
            if let Some(state) = crate::state::add_connection(local_id, remote_id, addr) {
                let remote = Server { id: remote_id, addr };
                if params.is_nonblocking {
                    conn.set_nonblocking(true).ok();
                }
                let read_half = conn
                    .try_clone()
                    .expect("clone tcp stream failure;");
                start_net_sender(local_id, remote, &params, &state, conn);
                start_net_receiver(local_id, remote, hb_sec, &params, &state, read_half);
            } else {
                return Err(NetError::ConflictConnect(remote_id));
            }
        } else {
            error!("invalid server id, expected {}, actual {}", remote_id, id);
            return Err(NetError::UnexpectedServer((remote_id, id)));
        }
    }
    Ok(())
}
