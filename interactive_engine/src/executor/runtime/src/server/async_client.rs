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

//! In process async client of `TimelyServer`, used to send request and receive response asynchronously.

use std::sync::*;
use std::collections::HashMap;
use std::fmt::Debug;
use std::thread::Thread;

use crossbeam_channel::{Sender as CSender};

use super::Message;
use super::ClientSender;
use super::prepare::PreparedError;
use server::DataflowId;
use server::ClientID;
use server::async_channel::{AsyncSReceiver, AsyncSSender, async_mpsc};
use server::future_set::{ExecuteFuture, PrepareFuture, RecvStream};


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
pub struct AsyncClient<I, O, DD> {
    /// process unique client id
    id: ClientID,
    /// request sender. ClientMessage can be Connect/Disconnect/Request
    dispatch_send: CSender<Message<I, O, DD>>,
    /// response receiver. ClientMessage can be Response/ResponseEnd
    resp_recv_async: Option<AsyncSReceiver<Message<I, O, DD>>>,
    /// input sender to each dataflow keyed by name
    dataflow_inputs: Weak<RwLock<HashMap<String, CSender<Message<I, O, DD>>>>>,
    /// rpc_timely worker thread
    worker_thread: Thread,
}

impl<I: Debug, O: Debug, DD: Debug> AsyncClient<I, O, DD> {

    pub(super) fn new(id: u64, dispatch_send: CSender<Message<I, O, DD>>, dataflow_inputs: Weak<RwLock<HashMap<String,
        CSender<Message<I, O, DD>>>>>, worker_thread: Thread) -> AsyncClient<I, O, DD> {

        AsyncClient {
            id,
            dispatch_send,
            resp_recv_async: None,
            dataflow_inputs,
            worker_thread,
        }
    }

    /// Update resp_send and resp_recv
    ///
    /// Before client executing query, it should update resp_send and resp_recv.
    /// And the new resp_send will be send to rpc_timely server.
    #[inline]
    fn update_channel(&mut self) -> AsyncSSender<Message<I, O, DD>> {
        let (sender, receiver) = async_mpsc();
        self.resp_recv_async.replace(receiver);
        sender
    }

    /// Prepare a dataflow.
    ///
    /// The description will be send to the function in `TimelyServer::new()`.
    /// Block until dataflow get prepared.
    pub fn prepare_async(&mut self, name: String, desc: DD) -> Result<PrepareFuture<I, O, DD>, PreparedError> {
        info!("prepare dataflow , name {:?}, desc : {:?}", name, desc);

        match self.remove(name.clone()) {
            Err(msg) => {
                error!("remove old prepare {} failed. cause {}", name, msg);
                return Err(PreparedError::FailedToRemoveOld);
            }
            _ => info!("remove old prepare {} first. ", name)
        };

        let sender = self.update_channel();

        self.dispatch_send.send(Message::Prepare(self.id, ClientSender::AsyncSender(sender), name.clone(), desc))
            .expect(format!("send prepare failed, client id: {}", self.id).as_str());

        // notify rpc_timely worker thread
        self.worker_thread.unpark();
        return Ok(PrepareFuture::new(name, self.resp_recv_async.take().unwrap()));
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

    pub fn execute_opt_async(&mut self, timeout_ms: u64, input_batches: usize, desc: DD) -> Result<ExecuteFuture<I, O, DD>, String> {
        let sender = self.update_channel();
        self.dispatch_send.send(Message::Execute(self.id, ClientSender::AsyncSender(sender), timeout_ms, input_batches,desc))
            .expect(format!("Send execute plan failed, client id : {}", self.id).as_str());

        // notify rpc_timely worker thread
        self.worker_thread.unpark();

        debug!("send execute message and unpark worker thread");

        return Ok(ExecuteFuture::new(self.resp_recv_async.clone().take().unwrap()));
    }


    pub fn cancel_execution(&self, dataflow_id: DataflowId) ->Result<(), String> {
        self.dispatch_send.send(Message::Cancel(self.id, dataflow_id))
            .map_err(|_| "Send Message::Cancel failed".to_owned())?;

        self.worker_thread.unpark();

        Ok(())
    }

    /// Send input to an dataflow specified by `name` string.
    pub fn send_query_async(&mut self, name: String, req: I) -> Result<(), String> {

        let sender = self.update_channel();
        if let Some(inputs) = self.dataflow_inputs.upgrade() {
            inputs.read().unwrap().get(&name)
                .map(|s| {
                    Ok({
                        s.send(Message::Request(self.id, ClientSender::AsyncSender(sender), req)).expect(format!("Send request failed, client id : {}", self.id).as_str());
                        self.worker_thread.unpark();
                    })
                }).unwrap_or_else(|| Err(format!("No such dataflow : {}", name)))
        } else {
            Err("maybe server disposed".to_owned())
        }
    }

    pub fn recv_async_stream(&mut self) -> RecvStream<I, O, DD> {
        return RecvStream::new(self.resp_recv_async.take().unwrap());
    }
}

