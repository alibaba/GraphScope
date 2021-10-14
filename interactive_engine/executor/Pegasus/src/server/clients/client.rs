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

use super::*;
use std::net::{TcpStream, ToSocketAddrs};
use std::io;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashMap;
use std::thread::JoinHandle;
use std::time::Duration;
use std::io::BufWriter;
use crossbeam_channel::{Receiver, Sender, RecvTimeoutError};
use tokio::reactor::Handle;
use crossbeam_queue::SegQueue;
use crate::serialize::write_binary;

/// Future callback for async submitted tasks;
/// The callback was registered to the client-end service when task being submitted, and
/// would be called when any concerned response was ready;
pub trait Callback: Send + 'static {
    /// Be invoked to handle ready response; Return `true` to indicate that this callback should
    /// be retired.
    fn accept(&mut self, res: BinaryMessage<TaskResponseHeader>) -> bool;
}

impl<F: Fn(BinaryMessage<TaskResponseHeader>) -> bool + Send + 'static> Callback for F {
    #[inline]
    fn accept(&mut self, res: BinaryMessage<TaskResponseHeader>) -> bool {
        (*self)(res)
    }
}

pub struct AsyncClient {
    /// unique client identifier in current process;
    pub identifier: u32,
    /// new task request dispatch
    tx: Sender<(u32, TaskRequest<Bytes>)>,
    /// async callback registers;
    callbacks: Arc<SegQueue<(u64, Box<dyn Callback>)>>,

    tasks: Arc<RwLock<HashMap<u64, (u32, u32)>>>
}

impl AsyncClient {
    pub fn new(id:  u32, tx: &Sender<(u32, TaskRequest<Bytes>)>,
               queue: &Arc<SegQueue<(u64, Box<dyn Callback>)>>,
               tasks: &Arc<RwLock<HashMap<u64, (u32, u32)>>>) -> Self {
        AsyncClient {
            identifier: id,
            tx: tx.clone(),
            callbacks: queue.clone(),
            tasks: tasks.clone(),
        }
    }

    pub fn new_task<R, F>(&self, task_id: u64, workers: u32, processes: u32,
                          req: R, func: F) -> Result<(), ClientError>
        where R: CPSerialize, F: Callback
    {
        let length = req.serialize_len();
        // let req_body = write_binary(req)?;
        let header = TaskRequestHeader::new(task_id, 1, length as u64, workers, processes);
        let req = TaskRequest::new(header, req);
        let binary_body = write_binary(&req)?;
        let req = TaskRequest::new(header, binary_body);
        self.tx.send((self.identifier, req))
            .map_err(|err| {
                let (_, r) = err.0;
                ClientError::NewTaskError((r.header, "send error".to_owned()))
            })?;

        self.callbacks.push((task_id, Box::new(func)));
        self.tasks.write().expect("write lock poison").insert(task_id, (processes, 0));
        Ok(())
    }

    /// Count number of running tasks;
    pub fn running_tasks(&self) -> usize {
        self.tasks.read().expect("read lock poison").len()
    }

    /// Count number of submitting tasks;
    pub fn submitting_tasks(&self) -> usize {
        self.tasks.read().expect("read lock poison")
            .iter()
            .filter(|(_, (v1, v2))| *v2 < *v1)
            .count()
    }

    pub fn waiting_tasks_done(&self) {
        while self.running_tasks() > 0 {
            ::std::thread::sleep(Duration::from_millis(10));
        }
    }
}

pub type ClientResponse = BinaryMessage<TaskResponseHeader>;

pub struct ClientService {
    tx: Sender<(u32, TaskRequest<Bytes>)>,
    threads: usize,
    rx: Option<Receiver<(u32, TaskRequest<Bytes>)>>,
    task2client: Arc<RwLock<HashMap<u64, u32>>>,
    clients: Arc<RwLock<Vec<Option<UnboundedSender<ClientResponse>>>>>,
    client_id_gen: Arc<AtomicUsize>,
    runtime: Option<tokio::runtime::Runtime>,
    client_send_guard: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub enum ClientError {
    IOError(io::Error),
    StartError(String),
    ServiceAlreadyStarted,
    NewTaskError((TaskRequestHeader, String)),
}

impl ::std::convert::From<io::Error> for ClientError {
    fn from(err: io::Error) -> Self {
        ClientError::IOError(err)
    }
}

#[inline]
fn send_request<W: Write>(req: Bytes, writes: &mut [W]) -> Result<(), io::Error> {
    // TODO : If some send success, some send failure, send cancel to successes as rollback;
    for write in writes {
        loop {
            match write.write_all(&req) {
                Ok(_) => break,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted
                    || e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e)
            }
        }
    }
    Ok(())
}

pub type ClientResponseReader<R> = BinaryReader<TaskResponseHeader, R>;

fn start_client_send<W: Write + Send + 'static>(rx: Receiver<(u32, TaskRequest<Bytes>)>,
                     mut writes: Vec<BufWriter<W>>,
                     clients: &Arc<RwLock<Vec<Option<UnboundedSender<ClientResponse>>>>>,
                     task2client: &Arc<RwLock<HashMap<u64, u32>>>) -> JoinHandle<()> {
    let task2client = task2client.clone();
    let clients = clients.clone();
    ::std::thread::Builder::new().name("client-send".to_owned())
        .spawn(move || {
            info!("client-send start work ...");
            loop {
                match rx.recv_timeout(Duration::from_millis(2)) {
                    Ok((client, req)) => {
                        let task_id = *req.task_id();
                        let processes = req.header.processes as usize;
                        let sends = &mut writes[0..processes];
                        debug!("send request {}", task_id);
                        if let Err(e) = send_request(req.take_body(), sends) {
                            error!("Error#client-send: send request of {} failure: {:?}", task_id, e);
                            let client_id = client as usize;
                            let h = TaskResponseHeader::on_error(ErrorCode::SendRequestError, task_id);
                            let clients = clients.read().expect("clients read lock poison");
                            if let Some(Some(client)) = clients.get(client_id) {
                                client.unbounded_send(ClientResponse::new(h, None))
                                    .map_err(|_e| {
                                        error!("Error#client-send: client {} disconnected;", client_id);
                                    }).ok();
                            } else {
                                error!("Error#client-send: client {} not found;", client_id);
                            }
                        } else {
                            task2client.write().expect("task2client write lock poison")
                                .insert(task_id, client);
                        }
                    },
                    Err(RecvTimeoutError::Timeout) => {
                        // debug!("flush buffer");
                        for w in writes.iter_mut() {
                            w.flush().map_err(|e| {
                                error!("Error#ClientService: flush network failure, {:?}", e);
                            }).ok();
                        }
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        for w in writes.iter_mut() {
                            w.flush().map_err(|e| {
                                error!("Error#ClientService: flush network failure, {:?}", e);
                            }).ok();
                        }
                        warn!("client-send disconnected;");
                        break
                    }
                }
            }

            info!("client-send exit...");

        }).expect("start client-send failure")
}

impl ClientService {
    pub fn new() -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        ClientService {
            threads: 8,
            tx,
            rx: Some(rx),
            task2client: Arc::new(RwLock::new(HashMap::new())),
            clients: Arc::new(RwLock::new(Vec::new())),
            client_id_gen: Arc::new(AtomicUsize::new(0)),
            runtime: None,
            client_send_guard: None,
        }
    }

    pub fn new_blocking_client(&self) {
        unimplemented!()
    }

    pub fn new_async_client(&mut self) -> AsyncClient {
        let identifier = self.client_id_gen.fetch_add(1, Ordering::SeqCst) as u32;
        let callbacks_queue = Arc::new(SegQueue::new());
        let tasks = Arc::new(RwLock::new(HashMap::new()));
        let client = AsyncClient::new(identifier, &self.tx, &callbacks_queue, &tasks);
        let (tx, rx) = unbounded();
        let mut clients = self.clients.write().expect("ClientService: client write lock poison");
        clients.push(Some(tx));

        let mut callbacks: HashMap<u64, Box<dyn Callback>> = HashMap::new();

        let f = rx.for_each(move |res| {
            debug!("get response {:?} ;", res.header);
            if res.header.is_ok() && res.header.is_body_empty() {
                let mut tasks = tasks.write().expect("write lock poison");
                if let Some((_, p2)) = tasks.get_mut(&res.header.task_id) {
                    *p2 +=1;
                }
            } else {
                // dispatch response to corresponding callback;
                let task_id = res.header.task_id;
                if let Some(mut callback) = callbacks.remove(&task_id) {
                    if !callback.accept(res) {
                        callbacks.insert(task_id, callback);
                    } else {
                        tasks.write().expect("write lock poison").remove(&task_id);
                    }
                } else {
                    let mut dispatched = false;
                    while let Ok((t_id, mut cb)) = callbacks_queue.pop() {
                        if t_id == task_id {
                            if !cb.accept(res) {
                                callbacks.insert(t_id, cb);
                            } else {
                                tasks.write().expect("write lock poison").remove(&task_id);
                            }
                            dispatched = true;
                            break;
                        } else {
                            callbacks.insert(t_id, cb);
                        }
                    }
                    if !dispatched {
                        error!("Error#ClientService: unknown task response of {}", task_id);
                    }
                }
            }
            Ok(())
        });
        if let Some(ref mut runtime) = self.runtime {
            runtime.spawn(f);
        } else {
            tokio::spawn(f);
        }
        client
    }

    pub fn start_service<A: ToSocketAddrs>(&mut self, addresses: &Vec<A>) -> Result<(), ClientError> {
        if let Some(rx) = self.rx.take() {
            let mut writes = Vec::with_capacity(addresses.len());
            let mut runtime = tokio::runtime::Builder::new()
                .core_threads(self.threads)
                .name_prefix("client-service-").build()
                .map_err(|err| ClientError::StartError(format!("start runtime failure {:?}", err)))?;

            for addr in addresses {
                let connection = TcpStream::connect(addr)?;
                connection.set_nodelay(true).expect("set no delay failure");
                let write = BufWriter::with_capacity(1 << 12, connection.try_clone()?);
                writes.push(write);
                let read = tokio::net::TcpStream::from_std(connection, &Handle::default())?;

                let task2client = self.task2client.clone();
                let clients = self.clients.clone();
                let future = ClientResponseReader::new(read)
                    .for_each(move |req: BinaryMessage<TaskResponseHeader>| {
                        // dispatch response according to task_id;
                        // 1. find client by task id;
                        let task_id = req.header.task_id;
                        debug!("get response of task {}", task_id);
                        let client_id = {
                            let lock = task2client.read().expect("read lock poison");
                            lock.get(&task_id).map(|id| *id)
                        };

                        if let Some(client_id) = client_id {
                            let lock = clients.read().expect("client read lock poison");
                            // 2. dispatch response to client through registered channel;
                            if let Some(Some(client)) = lock.get(client_id as usize) {
                                client.unbounded_send(req).map_err(|_| {
                                    error!("Error#ClientService: client {} disconnected, abandon inbound message;", client_id);
                                }).ok();
                            } else {
                                error!("Error#ClientService: unknown client id {}", client_id);
                            }
                        } else {
                            error!("Error#ClientService: unknow task_id {}", task_id);
                        }
                        Ok(())
                    })
                    .map_err(|e| error!("Error#ClientService: map error: {:?}", e));
                runtime.spawn(future);
            }

            let guard = start_client_send(rx, writes, &self.clients, &self.task2client);
            self.client_send_guard.replace(guard);
            self.runtime.replace(runtime);
            Ok(())
        } else {
            error!("Error#Client: service already started");
            Err(ClientError::ServiceAlreadyStarted)
        }
    }
}
