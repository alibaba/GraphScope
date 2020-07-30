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

use pegasus::network_connection::Connection;
use pegasus::network::reconnect;

use std::net::{TcpListener, TcpStream};
use std::thread;
use std::thread::sleep;
use std::io::{Result, Error, ErrorKind};
use std::borrow::{Borrow, BorrowMut};

pub trait NetworkCenter {
    fn get_address(&self, remote_index: usize) -> Option<&String>;
    fn set_address(&mut self, self_index: usize, self_address: String);
}

pub struct PegasusNetworkCenter {
    ip_list: Vec<String>,
}

impl PegasusNetworkCenter {
    pub fn new() -> PegasusNetworkCenter {
        PegasusNetworkCenter {
            ip_list: vec![],
        }
    }

    pub fn initialize(&mut self, ip_list: Vec<String>) {
        self.ip_list = ip_list;
    }
}

impl NetworkCenter for PegasusNetworkCenter {
    fn get_address(&self, remote_index: usize) -> Option<&String> {
        self.ip_list.get(remote_index).clone()
    }

    fn set_address(&mut self, self_index: usize, self_address: String) {
        self.ip_list[self_index] = self_address;
    }
}

impl Clone for PegasusNetworkCenter {
    fn clone(&self) -> Self {
        PegasusNetworkCenter {
            ip_list: self.ip_list.clone(),
        }
    }
}

//use std::rc::Rc;
//use std::cell::RefCell;
//impl<T: NetworkCenter> NetworkCenter for Rc<RefCell<T>> {
//    fn get_address(&self, remote_index: usize) -> Option<&String> {
//        self.borrow().get_address(remote_index)
//    }
//
//    fn set_address(&mut self, self_index: usize, self_address: String) {
//        self.borrow_mut().set_address(self_index, self_address)
//    }
//}

pub struct NetworkManager {
    index: usize,
    peers: usize,
    listener: TcpListener,
    connections: Vec<Option<Connection>>,
}

impl NetworkManager {
    pub fn new(index: usize, peers: usize, listener: TcpListener) -> Self {
        NetworkManager {
            index,
            peers,
            listener,
            connections: Vec::with_capacity(peers),
        }
    }

    pub fn initialize(listener: TcpListener) -> Self {
        NetworkManager {
            index: 0,
            peers: 0,
            listener,
            connections: Vec::new(),
        }
    }

    pub fn update_number(&mut self, index: usize, peers: usize) {
        self.index = index;
        self.peers = peers;
    }

    pub fn is_serving(&self) -> bool {
        if self.peers == 0 || self.connections.len() != self.peers {
            return false;
        }
        for (index, connection) in self.connections.iter().enumerate() {
            if index != self.index && (connection.is_none() || connection.as_ref().unwrap().is_poisoned()) {
                return false;
            }
        }
        return true;
    }

    pub fn check_network_status(&mut self, network_center: Box<dyn NetworkCenter>) -> (Vec<(usize, String)>, Vec<(usize, String)>) {
        let mut start_addresses = Vec::new();
        let mut await_addresses = Vec::new();

        while self.connections.len() < self.peers {
            self.connections.push(None);
        }

        for (index, connection) in self.connections.iter().enumerate() {
            if connection.is_none() || connection.as_ref().unwrap().is_poisoned() {
                if index == self.index {
                    continue;
                } else if index < self.index {
                    start_addresses.push((index, network_center.get_address(index).expect(format!("get address of {} failed.", index).as_str()).clone()));
                } else {
                    await_addresses.push((index, network_center.get_address(index).expect(format!("get address of {} failed.", index).as_str()).clone()));
                }
            }
        }
        return (start_addresses, await_addresses);
    }

    pub fn reconnect(&mut self, start_addresses: Vec<(usize, String)>, await_addresses: Vec<(usize, String)>, retry_times: u64) -> Result<Vec<(usize, String, TcpStream)>> {
        let self_index = self.index;
        let listener = self.listener.try_clone().expect("Clone tcp listener failed.");
        reconnect(self_index, listener, start_addresses, await_addresses, retry_times)
    }

    pub fn reset_network(&mut self, connection: Connection) {
        self.connections[connection.index].replace(connection);
    }
}
