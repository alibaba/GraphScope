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

//! In process client of `TimelyServer`, used to send request and receive response.

use std::cell::RefCell;
use std::sync::mpsc::*;
use std::sync::*;
use std::collections::HashMap;
use std::fmt::Debug;
use std::thread::Thread;

use crossbeam_channel::{Sender as CSender};

use super::Message;
use super::ClientSender;
use super::prepare::PreparedError;
use server::{DataflowId, DEFAULT_INPUT_BATCHES};
use server::ClientID;

/// In process client of `TimelyServer`, used to send request and receive response.
///
/// # Example
/// ```rust,no_run
/// extern crate maxgraph_runtime;
/// extern crate timely_communication;
/// extern crate rpc_timely;
///
/// use maxgraph_runtime::server::TimelyServer;
/// use timely_communication::initialize::Configuration;
/// use rpc_timely::dataflow::operators::{Input, Map};
/// use maxgraph_runtime::server::allocate;
///
/// let server: TimelyServer<u32, u32, String> = TimelyServer::new();
/// let config = Configuration::Thread;
/// let (builder, _others) = config.try_build().unwrap();
/// let network_guards = Box::new(allocate::CommsGuard::new());
/// let _guard = server.run(builder, network_guards, |scope, desc, input| {
///     input.as_ref().unwrap()
///          .map(|_| 1)
///     // your server logic goes here
/// }).unwrap();
/// let client = server.new_client().expect("build new client failed");
/// client.prepare("dataflow_name".to_owned(), "dataflow_description_object".to_owned()).unwrap();
/// let input = 1000_u32;
/// client.send_query("dataflow_name".to_owned(), input).unwrap();
/// let results = client.recv().unwrap();
/// ::std::mem::drop(server);
/// ```
pub struct Client<I, O, DD> {
    /// process unique client id
    id: ClientID,
    /// request sender. ClientMessage can be Connect/Disconnect/Request
    dispatch_send: CSender<Message<I, O, DD>>,
    /// response receiver. ClientMessage can be Response/ResponseEnd
    resp_recv: RefCell<Option<Receiver<Message<I, O, DD>>>>,
    /// input sender to each dataflow keyed by name
    dataflow_inputs: Weak<RwLock<HashMap<String, CSender<Message<I, O, DD>>>>>,
    /// rpc_timely worker thread
    worker_thread: Thread,
}

impl<I: Debug, O: Debug, DD: Debug> Client<I, O, DD> {
    /// Create a new inner-process client.
    pub(super) fn new(id: u64,
                      dispatch_send: CSender<Message<I, O, DD>>,
                      dataflow_inputs: Weak<RwLock<HashMap<String, CSender<Message<I, O, DD>>>>>, worker_thread: Thread) -> Client<I, O, DD>{

        Client {
            id,
            dispatch_send,
            resp_recv: RefCell::new(None),
            dataflow_inputs,
            worker_thread,
        }
    }

    /// Update resp_send and resp_recv
    ///
    /// Before client executing query, it should update resp_send and resp_recv.
    /// And the new resp_send will be send to rpc_timely server.
    #[inline]
    fn update_channel(&self) -> Sender<Message<I, O, DD>> {
        let (sender, receiver) = channel();
        self.resp_recv.borrow_mut().replace(receiver);
        sender
    }

    /// Prepare a dataflow.
    ///
    /// The description will be send to the function in `TimelyServer::new()`.
    /// Block until dataflow get prepared.
    pub fn prepare(&self, name: String, desc: DD) -> Result<(), PreparedError> {
        info!("prepare dataflow , name {:?}, desc : {:?}", name, desc);

        match self.remove(name.clone()) {
            Err(msg) => {
                error!("remove old prepare {} failed. cause {}", name, msg);
                return Err(PreparedError::FailedToRemoveOld);
            }
            _ => info!("remove old prepare {} first. ", name)
        };

        let sender = self.update_channel();

        self.dispatch_send.send(Message::Prepare(self.id, ClientSender::SyncSender(sender), name.clone(), desc))
            .expect(format!("send prepare failed, client id: {}", self.id).as_str());

        self.worker_thread.unpark();

        let msg = self.resp_recv.borrow().as_ref().unwrap().recv().unwrap();
        match msg {
            Message::DataflowLaunched(id) => {
                debug!("dataflow with name {} is prepared with id of {} ", name, id);
                Ok(())
            }
            Message::ServiceSuspend => {
                error!("dataflow with name {} is prepared failed, and client need to reconnect", name);
                Err(PreparedError::Unexpected)
            }
            msg => {
                error!("Unexpected message got after prepare: {}", msg.name());
                Err(PreparedError::Unexpected)
            }
        }
    }

    pub fn remove(&self, name: String) -> Result<(), String> {
        info!("Remove dataflow , name : {}", name);
        if let Some(inputs) = self.dataflow_inputs.upgrade() {
            // This should be the last message as the write lock guard, prevent any query in.
            Ok(inputs.write().unwrap().remove(&name)
                .map(|s| s.send(Message::RequestEnd).unwrap()).unwrap_or(()))
        } else {
            Err("maybe server disposed".to_owned())
        }
    }

    pub fn execute(&self, desc: DD) -> Result<DataflowId, String> {
        self.execute_opt(0, DEFAULT_INPUT_BATCHES, desc)
    }

    pub fn execute_opt(&self, timeout_ms: u64, input_batches: usize, desc: DD) -> Result<DataflowId, String> {

        let sender = self.update_channel();

        self.dispatch_send.send(Message::Execute(self.id, ClientSender::SyncSender(sender), timeout_ms, input_batches, desc))
            .expect(format!("Send execute plan failed, client id : {}", self.id).as_str());

        self.worker_thread.unpark();

        let msg = self.resp_recv.borrow().as_ref().unwrap().recv().unwrap();
        match msg {
            Message::DataflowLaunched(id) => {
                debug!("dataflow with id {} is in executing ...", id);
                Ok(id)
            }
            Message::ServiceSuspend => {
                let err_msg = format!("Recv unexpected serviceSuspend, so client need to reconncet");
                error!("{}", err_msg);
                Err(err_msg)
            }
            msg => {
                let err_msg = format!("Unexpected message got after execute: {}", msg.name());
                error!("Unexpected message got after execute: {}", msg.name());
                Err(err_msg)
            }
        }
    }

    pub fn cancel_execution(&self, dataflow_id: DataflowId) -> Result<(), String> {
        self.dispatch_send.send(Message::Cancel(self.id, dataflow_id))
            .map_err(|_| "Send Message::Cancel failed".to_owned())?;

        self.worker_thread.unpark();
        Ok(())
    }

    /// Send input to an dataflow specified by `name` string.
    pub fn send_query(&self, name: String, req: I) -> Result<(), String> {
        let sender = self.update_channel();
        if let Some(inputs) = self.dataflow_inputs.upgrade() {
            inputs.read().unwrap().get(&name)
                .map(|s| {
                    Ok({
                        s.send(Message::Request(self.id, ClientSender::SyncSender(sender), req)).expect(format!("Send request failed, client id : {}", self.id).as_str());

                        // notify rpc_timely worker thread
                        self.worker_thread.unpark();
                    })
                }).unwrap_or_else(|| Err(format!("No such dataflow : {}", name)))
        } else {
            Err("maybe server disposed".to_owned())
        }
    }

    /// Wait for response of the query just send.
    pub fn recv(&self) -> Result<Vec<O>, String> {
        // TODO support streaming response
        // TODO support timeout
        let mut results = vec![];
        loop {
            match self.resp_recv.borrow().as_ref().unwrap().recv() {
                Ok(Message::Response(mut data)) => {
//                    debug!("client {} get response: {:?}", self.id, data);
                    if results.is_empty() {
                        ::std::mem::swap(&mut results, &mut data)
                    } else {
                        results.extend(data);
                    }
                }
                Ok(Message::ResponseEnd) => {
                    return Ok(results);
                }
                Ok(Message::ServiceSuspend) => {
                    error!("Server suspend, try to reconnect. ");
                    return Err(format!("Server suspend, try again later, client.id: {}", self.id));
                }
                Ok(Message::DataflowTimeout(id)) => {
                    return Err(format!("Dataflow timeout or cancelled: {}", id));
                }
                Err(err) => return Err(format!("error when client receive: {}", err)),
                Ok(msg) => return Err(format!("Unexpected message received: {}", msg.name()))
            }
        }
    }
}


