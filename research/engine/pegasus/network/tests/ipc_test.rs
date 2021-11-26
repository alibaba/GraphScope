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
use std::time::Duration;

use pegasus_common::codec::*;
use pegasus_network::{config::ConnectionParams, Server, ServerDetect};

struct MockServerDetect {
    servers: Vec<Server>,
}

impl ServerDetect for MockServerDetect {
    fn fetch(&self) -> Vec<Server> {
        self.servers.clone()
    }
}

struct Entry {
    data: Vec<u8>,
}

impl Entry {
    pub fn new(value: u8) -> Self {
        Entry { data: vec![value; 256] }
    }
}

impl Encode for Entry {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&self.data)
    }
}

impl Decode for Entry {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let mut data = vec![0u8; 256];
        reader.read_exact(&mut data[0..])?;
        Ok(Entry { data })
    }
}

#[test]
fn ipc_test() {
    {
        ipc_with_conf(ConnectionParams::blocking());
    }
    {
        let mut conf = ConnectionParams::blocking();
        conf.set_write_timeout(Duration::from_secs(1));
        ipc_with_conf(conf);
    }
    {
        let mut conf = ConnectionParams::blocking();
        conf.set_write_timeout(Duration::from_secs(1));
        conf.set_read_timeout(Duration::from_secs(1));
        ipc_with_conf(conf);
    }
    {
        let conf = ConnectionParams::nonblocking();
        ipc_with_conf(conf);
    }
}

fn ipc_with_conf(conf: ConnectionParams) {
    pegasus_common::logs::init_log();
    let mut servers = vec![];
    servers.push(Server { id: 0, addr: "127.0.0.1:1234".parse().unwrap() });
    servers.push(Server { id: 1, addr: "127.0.0.1:1235".parse().unwrap() });
    servers.push(Server { id: 2, addr: "127.0.0.1:1236".parse().unwrap() });
    let g1 = mock_process_0(servers.clone(), conf);
    let g2 = mock_process_1(servers.clone(), conf);
    let g3 = mock_process_2(servers, conf);
    g1.join().unwrap();
    g2.join().unwrap();
    g3.join().unwrap();
}

fn mock_process_0(servers: Vec<Server>, conf: ConnectionParams) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("process-0".to_owned())
        .spawn(move || {
            let detector = MockServerDetect { servers };
            let addr = pegasus_network::start_up(0, conf, "127.0.0.1:1234", detector).unwrap();
            info!("server 0 start at {:?}", addr);
            let remotes = vec![1, 2];
            while !pegasus_network::check_connect(0, &remotes) {
                std::thread::sleep(Duration::from_secs(1));
            }

            let ipc_ch = pegasus_network::ipc_channel::<Entry>(1, 0, &remotes).unwrap();
            let (mut sends, recv) = ipc_ch.take();
            let entry = Entry::new(0);
            sends[0].send(&entry).unwrap();
            sends[1].send(&entry).unwrap();
            sends[0].close().unwrap();
            sends[1].close().unwrap();
            let mut receives = vec![];
            loop {
                match recv.recv() {
                    Ok(Some(entry)) => {
                        receives.extend_from_slice(&entry.data);
                    }
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::BrokenPipe {
                            info!("received all;");
                            break;
                        } else {
                            panic!("unexpected error {}", e);
                        }
                    }
                    _ => (),
                }
            }
            receives.sort();
            assert_eq!(&receives[0..256], vec![1u8; 256].as_slice());
            assert_eq!(&receives[256..512], vec![2u8; 256].as_slice());
            pegasus_network::shutdown(0);
            pegasus_network::await_termination(0);
        })
        .unwrap()
}

fn mock_process_1(servers: Vec<Server>, conf: ConnectionParams) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("process-1".to_owned())
        .spawn(move || {
            let detector = MockServerDetect { servers };
            let addr = pegasus_network::start_up(1, conf, "127.0.0.1:1235", detector).unwrap();
            info!("server 1 start at {:?}", addr);
            let remotes = vec![0, 2];

            while !pegasus_network::check_connect(1, &remotes) {
                std::thread::sleep(Duration::from_secs(1));
            }

            let ipc_ch = pegasus_network::ipc_channel::<Entry>(1, 1, &remotes).unwrap();
            let (mut sends, recv) = ipc_ch.take();
            let entry = Entry::new(1);
            sends[0].send(&entry).unwrap();
            sends[1].send(&entry).unwrap();
            sends[0].close().unwrap();
            sends[1].close().unwrap();
            let mut receives = vec![];
            loop {
                match recv.recv() {
                    Ok(Some(entry)) => {
                        receives.extend_from_slice(&entry.data);
                    }
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::BrokenPipe {
                            info!("received all;");
                            break;
                        } else {
                            panic!("unexpected error {}", e);
                        }
                    }
                    _ => (),
                }
            }
            info!("received all;");
            receives.sort();
            assert_eq!(&receives[0..256], vec![0u8; 256].as_slice());
            assert_eq!(&receives[256..512], vec![2u8; 256].as_slice());
            pegasus_network::shutdown(1);
            pegasus_network::await_termination(1);
        })
        .unwrap()
}

fn mock_process_2(servers: Vec<Server>, conf: ConnectionParams) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("process-2".to_owned())
        .spawn(move || {
            let detector = MockServerDetect { servers };
            let addr = pegasus_network::start_up(2, conf, "127.0.0.1:1236", detector).unwrap();
            info!("server 2 start at {:?}", addr);
            let remotes = vec![0, 1];
            while !pegasus_network::check_connect(2, &remotes) {
                std::thread::sleep(Duration::from_secs(1));
            }
            let ipc_ch = pegasus_network::ipc_channel::<Entry>(1, 2, &remotes).unwrap();
            let (mut sends, recv) = ipc_ch.take();
            let entry = Entry::new(2);
            sends[0].send(&entry).unwrap();
            sends[1].send(&entry).unwrap();
            sends[0].close().unwrap();
            sends[1].close().unwrap();
            let mut receives = vec![];
            loop {
                match recv.recv() {
                    Ok(Some(entry)) => {
                        receives.extend_from_slice(&entry.data);
                    }
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::BrokenPipe {
                            info!("received all;");
                            break;
                        } else {
                            panic!("unexpected error {}", e);
                        }
                    }
                    _ => (),
                }
            }
            receives.sort();
            assert_eq!(&receives[0..256], vec![0u8; 256].as_slice());
            assert_eq!(&receives[256..512], vec![1u8; 256].as_slice());
            pegasus_network::shutdown(2);
            pegasus_network::await_termination(2);
        })
        .unwrap()
}
