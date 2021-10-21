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

//! Timely server implementation.

pub mod server;
pub mod client;
pub mod async_client;
pub mod queue;
pub mod allocate;
pub mod prepare;
pub mod manager;
pub mod query_manager;
pub mod async_channel;
pub mod future_set;
pub mod network_manager;

pub use self::client::Client;
pub use self::async_client::AsyncClient;

use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Error;
use std::sync::mpsc::*;
use std::result::Result;


pub type ClientID = u64;
pub type ServerTimestamp = u64;

pub static DEFAULT_INPUT_BATCHES: usize = 256;

/// Messages for client-server interaction

/// I for input
/// O for output
/// DD for dataflow description
#[allow(dead_code)]
pub enum Message<I, O, DD> {
    /// Client send this this message to server to setup a prepared dataflow with
    /// its id, dataflow name and dataflow description.
    Prepare(ClientID, ClientSender<I, O, DD>, String, DD),

    /// Client send a dataflow plan, timeout (in ms) and input batch limit to server,
    /// hoping it to be executed immediately.
    /// The timeout value `0` stands for never timeout.
    /// The batch limit should fall into (0, 1024).
    /// Format: (client_id, timeout_ms, batch_limit, dataflow_desc)
    Execute(ClientID, ClientSender<I, O, DD>, u64, usize, DD),

    /// Cancel a dataflow. Sent from client to server.
    Cancel(ClientID, DataflowId),

    /// Notification sending from server to client when preparing finish.
    DataflowLaunched(DataflowId),

    /// Notification sending from server to client when dataflow runs out of time of cancelled.
    DataflowTimeout(DataflowId),

    /// Client send input data to some dataflow.
    Request(ClientID, ClientSender<I, O, DD>, I),

    /// Message send from server back to client after `Request`(s) messages (maybe none)
    /// indicating end of `Request`(s).
    RequestEnd,

    /// Output data send back from server to client.
    Response(Vec<O>),

    /// Notification sending from server to client informing all output data have
    /// been delivered (with Response variable).
    ResponseEnd,

    /// Client send this message to unregister server-side resources.
    Disconnect(ClientID),

    /// TimelyServer suspend and will restart sooner or later, notify clients.
    ServiceSuspend,

    /// TimelyServer send this message to client to indicate connect success.
    ConnectSuccess,
}

impl<I, O, DD> Message<I, O, DD> {
    /// Get simple name of message.
    fn name(&self) -> &'static str {
        match self {
            Message::Prepare(_, _, _, _) => "Prepare",
            Message::DataflowLaunched(_) => "DataflowLaunched",
            Message::DataflowTimeout(_) => "DataflowTimeout",
            Message::Execute(_, _, _, _, _) => "Execute",
            Message::Cancel(_, _) => "Cancel",
            Message::Request(_, _, _) => "Request",
            Message::RequestEnd => "RequestEnd",
            Message::Response(_) => "Response",
            Message::ResponseEnd => "ResponseEnd",
            Message::Disconnect(_) => "Disconnect",
            Message::ServiceSuspend => "ServiceSuspend",
            Message::ConnectSuccess => "ConnectSuccess",
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct DataflowId {
    worker: usize,
    seq: usize,
}

impl Display for DataflowId {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "{}-{}", self.worker, self.seq)
    }
}


/// Sender for rpc_timely-server to send message to client
pub enum ClientSender<I, O, DD> {
    /// sync sender (std sender)
    SyncSender(Sender<Message<I, O, DD>>),

    /// async sender (futures sender)
    AsyncSender(async_channel::AsyncSSender<Message<I, O, DD>>),
}

impl<I, O, DD> ClientSender<I, O, DD> {
    fn send(&self, message: Message<I, O, DD>) -> Result<(), String> {
        match self {
            ClientSender::SyncSender(sender) => {
                match sender.send(message) {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        let err_msg = format!("{}", e);
                        return Err(err_msg);
                    }
                }
            }
            ClientSender::AsyncSender(sender) => {
                match sender.send(message) {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        let err_msg = format!("{}", e);
                        return Err(err_msg);
                    }
                }
            }
        }
    }
}

use maxgraph_common::proto::hb::*;

/// Runtime info sent by hb thread
pub struct RuntimeInfo {
    server_status: RuntimeHBReq_RuntimeStatus,
    server_port: u16,
    worker_num_per_process: u32,
    process_partition_list: Vec<u32>,
}

impl RuntimeInfo {
    pub fn new(worker_num_per_process: u32,
               process_partition_list: Vec<u32>) -> RuntimeInfo {
        RuntimeInfo {
            server_status: RuntimeHBReq_RuntimeStatus::DOWN,
            server_port: 0,
            worker_num_per_process,
            process_partition_list,
        }
    }

    pub fn get_server_status(&self) -> RuntimeHBReq_RuntimeStatus { self.server_status }
    pub fn get_server_port(&self) -> u16 { self.server_port }
    pub fn get_worker_num_per_process(&self) -> u32 {
        self.worker_num_per_process
    }
    pub fn get_process_partition_list(&self) -> &Vec<u32> {
        &self.process_partition_list
    }
    pub fn change_server_status(&mut self, server_status: RuntimeHBReq_RuntimeStatus) { self.server_status = server_status; }
    pub fn change_server_port(&mut self, server_port: u16) { self.server_port = server_port; }
    pub fn change_port_and_status(&mut self, port: u16, status: RuntimeHBReq_RuntimeStatus) {
        self.server_port = port;
        self.server_status = status;
    }
}

#[derive(Clone, Debug)]
pub struct RuntimeAddress {
    ip: String,
    store_port: i32,
}

impl RuntimeAddress {
    pub fn new(ip: String,
               store_port: i32) -> Self {
        RuntimeAddress {
            ip,
            store_port
        }
    }

    pub fn get_ip(&self) -> &String {
        &self.ip
    }

    pub fn get_port(&self) -> &i32 {
        &self.store_port
    }
}
