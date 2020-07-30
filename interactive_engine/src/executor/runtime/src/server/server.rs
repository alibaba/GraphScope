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

//! Create a rpc_timely service and submit dataflows to it.

use std::rc::Rc;
use std::cell::RefCell;
use std::time::{Instant, Duration};
use std::fmt::Debug;
use std::mem;
use std::collections::HashMap;
use std::sync::atomic::{Ordering, AtomicUsize, AtomicBool};
use std::sync::{Arc, RwLock};
use std::error::Error as StdError;
use std::panic::{self, AssertUnwindSafe};
use std::thread::Thread;

use crossbeam_channel::{self, Sender as CSender, Receiver as CReceiver};

use super::queue::*;
use super::*;
use store::store_service::StoreServiceManager;

use pegasus::Pegasus;
use maxgraph_common::proto::lambda_service_grpc::LambdaServiceClient;
use utils::get_lambda_service_client;
use store::remote_store_service::RemoteStoreServiceManager;
use store::task_partition_manager::TaskPartitionManager;
use dataflow::manager::context::BuilderContext;
use maxgraph_store::api::graph_partition::GraphPartitionManager;


/// TimelyServer context per worker.
struct Context<I: Send + 'static, O: Send + 'static, DD: Send + 'static> {
    /// We distinguish queries by timestamp, so record timestamp-to-client mapping to know where to send
    /// Response and ResponseEnd message.
    ts_client_mapping: HashMap<(DataflowId, ServerTimestamp), ClientID>,

    /// Sender to clients, used to send response messages like PrepareEnd, ResponseEnd, etd.
    clients: HashMap<ClientID, ClientSender<I, O, DD>>,
}

impl<I: Send + 'static, O: Send + 'static, DD: Send + 'static> Context<I, O, DD> {
    /// Create a shared `Context`.
    #[inline]
    fn new_shared() -> Rc<RefCell<Context<I, O, DD>>> {
        Rc::new(RefCell::new(Context {
            ts_client_mapping: Default::default(),
            clients: Default::default(),
        }))
    }

    /// Search client sender by dataflow id and timestamp.
    /// Returns Error if client id could not be found by (dataflow_id, timestamp) tuple.
    /// Otherwise return sender of client if found.
    #[inline]
    fn get_client_of_timestamp(&self, dataflow_id: &DataflowId, timestamp: &ServerTimestamp) -> Result<Option<&ClientSender<I, O, DD>>, ()> {
        match self.ts_client_mapping.get(&(*dataflow_id, *timestamp)) {
            Some(client_id) => Ok(self.get_client(client_id)),
            None => Err(())
        }
    }

    #[inline]
    fn add_timestamp_mapping(&mut self, dataflow_id: DataflowId, timestamp: ServerTimestamp, client_id: ClientID) {
        self.ts_client_mapping.insert((dataflow_id, timestamp), client_id);
    }

    /// Remove `(dataflow_id, timestamp)` to `client_id` mapping, and `client_id` to `Sender` mapping.
    ///
    /// Returns Error if client id could not be found by `(dataflow_id, timestamp)` tuple.
    /// Otherwise remove and return sender of client if found.
    ///
    /// # Attention
    /// Client sender will also be removed.
    #[inline]
    fn remove_client_and_timestamp_mapping(&mut self, dataflow_id: &DataflowId, timestamp: &ServerTimestamp) -> Result<Option<ClientSender<I, O, DD>>, ()> {
        match self.ts_client_mapping.remove(&(*dataflow_id, *timestamp)) {
            Some(client_id) => Ok(self.clients.remove(&client_id)),
            None => Err(())
        }
    }

    /// On-going timestamps.
    #[inline]
    fn timestamps(&self) -> Vec<(DataflowId, ServerTimestamp)> {
        self.ts_client_mapping.keys().cloned().collect()
    }

    /// Find all senders belonging to the same dataflow, and do something with it.
    fn search_and_remove_client<F>(&mut self, dataflow_id: &DataflowId, mut func: F)
        where F: FnMut(&ClientID, ClientSender<I, O, DD>)
    {
        let mut client_id = None;
        for (k, v) in self.ts_client_mapping.iter() {
            if k.0 == *dataflow_id {
                client_id = Some(v);
            }
        }

        if client_id.is_some() {
            let client_id = client_id.unwrap();
            match self.clients.remove(client_id) {
                Some(sender) => func(client_id, sender),
                None => { error!("for_each_dataflow_client: no client found for {}", client_id) }
            }
        }
    }

    /// Get client sender by client id.
    #[inline]
    fn get_client(&self, client_id: &ClientID) -> Option<&ClientSender<I, O, DD>> {
        self.clients.get(client_id)
    }

    /// Add a new client.
    #[inline]
    fn client_connect(&mut self, client_id: ClientID, sender: ClientSender<I, O, DD>) {
        self.clients.insert(client_id, sender);
    }

    /// Send `Message::ServiceSuspend` to all clients.
    #[inline]
    fn send_suspend(&self) {
        for (id, sender) in self.clients.iter() {
            match sender.send(Message::ServiceSuspend) {
                Ok(()) => debug!("Send serviceSuspend to client {} success", id),
                Err(e) => error!("Send serviceSuspend to client {} failed, caused: {}", id, e),
            }
        }
    }

    /// Check whether client is exists
    #[inline]
    fn check_client_id(&self, client_id: &ClientID) -> Option<(DataflowId, ServerTimestamp)> {
        if self.clients.contains_key(client_id) {
            for (k, id) in self.ts_client_mapping.iter() {
                if id == client_id {
                    return Some(k.clone());
                }
            }
        }
        None
    }

    /// Update client sender
    #[inline]
    fn update_client_sender(&mut self, client_id: ClientID, sender: ClientSender<I, O, DD>, dataflow_id: DataflowId, timestamp: ServerTimestamp) {
        if let Some((dataflow_id_old, timestamp_old)) = self.check_client_id(&client_id) {
            if let Err(_) = self.remove_client_and_timestamp_mapping(&dataflow_id_old, &timestamp_old) {
                let err_msg = format!("Remove dataflow_id: {} and timestamp: {} failed.", dataflow_id_old, timestamp_old);
                panic!("{}", err_msg);
            }
        }
        self.client_connect(client_id, sender);
        self.add_timestamp_mapping(dataflow_id, timestamp, client_id);
    }
}
