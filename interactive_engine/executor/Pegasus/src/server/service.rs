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

use std::net::SocketAddr;
use std::io;
use std::thread::JoinHandle;
use super::*;
use crate::operator::sink::BinarySinker;
use crate::channel::{IOResult, IOError};
use crate::serialize::write_binary;
use tokio::net::TcpListener;
use crossbeam_channel::{Receiver, Sender};

pub struct Sink {
    inner: UnboundedSender<Bytes>
}

impl Clone for Sink {
    #[inline]
    fn clone(&self) -> Self { Sink { inner: self.inner.clone() } }
}

impl Sink {
    pub fn new(sender: UnboundedSender<Bytes>) -> Self {
        Sink { inner: sender }
    }
}

impl BinarySinker for Sink {
    #[inline]
    fn sink(&mut self, res: Bytes) -> IOResult<()> {
        self.inner.unbounded_send(res).map_err(|_err| {
            IOError::BrokenPipe("sink send error".to_owned())
        })
    }
}

type Request = TaskRequest<Option<Bytes>>;

pub trait TaskGenerator : Send + Sync {
    fn create_task(&self, task: Request, runtime: &Pegasus, sink: &mut Sink) -> Result<(), String>;
}

pub struct GenericGenerator {
    runtime: Pegasus,
    inner: Arc<dyn TaskGenerator>,
    req_rx: Receiver<(Request, Sink)>,
    guards: Option<Vec<JoinHandle<()>>>
}

fn create_task(inner: &Arc<dyn TaskGenerator>, runtime: &Pegasus, task: TaskRequest<Option<Bytes>>, mut sink: Sink) {
    let task_id = *task.task_id();
    let state = match inner.create_task(task, runtime, &mut sink) {
        Ok(()) => ResponseType::OK,
        Err(e) => {
            error!("Create task failure, caused by {}", e);
            ResponseType::Error(ErrorCode::CreateTaskError)
        }
    };
    // return empty ok response to notify client that task has been submitted successfully;
    let res = new_empty_response(task_id, state);
    let bytes = write_binary(&res)
        .expect("empty response encode failure");
    sink.sink(bytes).map_err(|err| {
        error!("Error#GenericGenerator: send create task response failure, caused by {:?}", err);
    }).ok();
}

impl GenericGenerator {

    fn new<G: TaskGenerator + 'static>(runtime: Pegasus,
                                       req_rx: Receiver<(Request, Sink)>,
                                       generator: G) -> Self {
        GenericGenerator {
            runtime,
            inner: Arc::new(generator),
            req_rx,
            guards: None,
        }
    }

    fn run(&mut self, threads: usize) {
        let mut guards = Vec::new();
        for i in 0..threads {
            let rx = self.req_rx.clone();
            let generator = self.inner.clone();
            let runtime = self.runtime.clone();
            let h = ::std::thread::Builder::new().name(format!("generator-{}", i))
                .spawn(move || {
                    info!("generator: {} start to work... ", i);
                    while let Ok((req, sink)) = rx.recv() {
                        create_task(&generator, &runtime, req, sink);
                    }
                    info!("generator-{} exit", i);
                }).expect("build generator thread failure");
            guards.push(h);
        }
        self.guards.replace(guards);
    }

    fn join(&mut self) {
        if let Some(guards) = self.guards.take() {
            for g in guards {
                g.join().unwrap();
            }
        }
    }
}

pub struct Service {
    tcp_listener: Option<TcpListener>,
    generator: GenericGenerator,
    req_tx: Sender<(Request, Sink)>,
}

pub type TaskDescReader<R> = BinaryReader<TaskRequestHeader, R>;

impl Service {

    pub fn new<G: TaskGenerator + 'static>(runtime: Pegasus, generator: G) -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        let generator = GenericGenerator::new(runtime, rx, generator);
        Service {
            tcp_listener: None,
            generator,
            req_tx: tx,
        }
    }

    pub fn bind(&mut self) -> io::Result<SocketAddr> {
        let addr = "0.0.0.0:0".parse::<SocketAddr>().unwrap();
        let listener = TcpListener::bind(&addr)?;
        let addr = listener.local_addr()?;
        info!("Service start on {}", addr);
        self.tcp_listener.replace(listener);
        Ok(addr)
    }

    pub fn start(self, generators: usize) {
        let Service { mut tcp_listener, mut generator, req_tx} = self;
        generator.run(generators);
        if let Some(listener) = tcp_listener.take() {
            let server = listener.incoming()
                .map_err(|e| error!("Error#Service: accept connection error: {:?}", e))
                .for_each(move |socket| {
                    // let req_tx_cp = req_tx.clone();
                    let (tx, rx) = unbounded();
                    socket.set_nodelay(true)
                        .map_err(|e| error!("Error#Service: set no delay error: {:?} .", e)).ok();
                    info!("new connections : {:?}", socket.peer_addr());
                    let (reader, writer) = socket.split();
                    let req_tx = req_tx.clone();
                    let reader = TaskDescReader::new(reader).
                        for_each(move |req| {
                            debug!("new request : {:?}", req.header);
                            let sink = Sink::new(tx.clone());
                            req_tx.send((TaskRequest::from(req), sink))
                                .map_err(|_| {
                                    error!("Error#Service: failed to deliver request;");
                                }).ok();
                            Ok(())
                        })
                        .map_err(|err| error!("Error#Service: read request failure: {:?}", err))
                        .map(|_| ());
                    tokio::spawn(reader);

                    let writer = rx.fold(writer, |write, res| {
                        debug!("write response");
                        tokio::io::write_all(write, res)
                            .map(|(w, _)| w)
                            .map_err(|e| {
                                error!("Write response error, caused by {:?}", e);
                            })
                    }).map_err(|e| error!("write response err {:?}", e));
                    tokio::spawn(writer.map(|_| ()))
                });
            tokio::run(server);
        } else {
            error!("Error#Service: the service is not init;");
        }
        generator.join();
    }
}
