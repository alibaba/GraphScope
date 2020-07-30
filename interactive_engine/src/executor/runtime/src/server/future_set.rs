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

use futures::{Future, Async, Poll, Stream};
use tokio_sync::semaphore::{Permit, Semaphore};

use super::Message;
use server::async_channel::{AsyncSReceiver, TryPark};
use server::prepare::PreparedError;
use server::DataflowId;
use std::sync::Arc;

use crossbeam_channel::{TryRecvError};

/// ClientFuture is a future for fetching a client of rpc_timely-server
/// ClientFuture need mpmc future
/// At the same time, there will be no only one receiver being parked, so need a queue to record these tasks
pub struct ClientFuture {
    permit: Option<Permit>,
    semaphore: Arc<Semaphore>,
}

impl ClientFuture {
    pub fn new(semaphore: Arc<Semaphore>) -> Self{
        ClientFuture {
            permit: Some(Permit::new()),
            semaphore,
        }
    }
}

impl Future for ClientFuture {
    type Item = Permit;
    type Error = String;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut permit = self.permit.take().unwrap();
        match permit.poll_acquire(&self.semaphore) {
            Ok(Async::Ready(())) => Ok(Async::Ready(permit)),
            Ok(Async::NotReady) => {
                self.permit = Some(permit);
                Ok(Async::NotReady)
            },
            Err(_) => {
                let err_msg = format!("Acquire client semaphore error");
                error!("{}", err_msg);
                return Err(err_msg);
            }
        }
    }
}

/// ExecuteFuture is a future to launched a dataflow
pub struct ExecuteFuture<I, O, DD> {
    receiver: AsyncSReceiver<Message<I, O, DD>>,
}


impl<I, O, DD> ExecuteFuture<I, O, DD> {
    pub fn new(receiver_async: AsyncSReceiver<Message<I, O, DD>>) -> Self {
        ExecuteFuture {
            receiver: receiver_async,
        }
    }
}

impl<I, O, DD> Future for ExecuteFuture<I, O, DD> {
    type Item = DataflowId;
    type Error = String;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let message = self.receiver.try_recv();

            if let Err(error) = message {
                match error {
                    TryRecvError::Empty => {
                        match self.receiver.try_park() {
                            TryPark::NotEmpty => {
                                continue;
                            }
                            TryPark::Parked => {
                                return Ok(Async::NotReady);
                            }
                        }
                    }
                    TryRecvError::Disconnected => {
                        let error_msg = format!("Receiver is disconnected.");
                        error!("{}", error_msg);
                        return Err(error_msg);
                    }
                }
            } else if let Ok(message) = message {
                match message {
                    Message::DataflowLaunched(dataflow_id) => {
                        debug!("dataflow with id {} is in executing ...", dataflow_id);
                        return Ok(Async::Ready(dataflow_id));
                    }
                    Message::ServiceSuspend => {
                        let err_msg = format!("Launch dataflow failed caused by receiving serviceSuspend, so client need to reconncet");
                        error!("{}", err_msg);
                        return Err(err_msg);
                    }
                    msg => {
                        let err_msg = format!("Launch dataflow failed caused by unexpected message got: {}", msg.name());
                        error!("{}", err_msg);
                        return Err(err_msg);
                    }
                }
            }

        }
    }
}

/// PrepareFuture is a future to prepare query
pub struct PrepareFuture<I, O, DD> {
    name: String,
    receiver: AsyncSReceiver<Message<I, O, DD>>,
}

impl<I, O, DD> PrepareFuture<I, O, DD> {
    pub fn new(name: String, receiver_async: AsyncSReceiver<Message<I, O, DD>>) -> Self {
        PrepareFuture {
            name,
            receiver: receiver_async,
        }
    }
}

impl<I, O, DD> Future for PrepareFuture<I, O, DD> {
    type Item = ();
    type Error = PreparedError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let message = self.receiver.try_recv();
            if let Err(error) = message {
                match error {
                    TryRecvError::Empty => {
                        match self.receiver.try_park() {
                            TryPark::NotEmpty => {
                                continue;
                            }
                            TryPark::Parked => {
                                return Ok(Async::NotReady);
                            }
                        }
                    }
                    TryRecvError::Disconnected => {
                        let error_msg = format!("Client receiver is disconnected.");
                        error!("{}", error_msg);
                        return Err(PreparedError::Unexpected);
                    }
                }
            } else if let Ok(message) = message {
                match message {
                    Message::DataflowLaunched(id) => {
                        debug!("dataflow with name {} is prepared with id of {} ", self.name, id);
                        return Ok(Async::Ready(()))
                    }
                    Message::ServiceSuspend => {
                        error!("dataflow with name {} is prepared failed, and client need to reconnect", self.name);
                        return Err(PreparedError::Unexpected)
                    }
                    msg => {
                        error!("Unexpected message got after prepare: {}", msg.name());
                        return Err(PreparedError::Unexpected)
                    }
                }
            }
        }
    }
}


/// RecvStream return query result with a stream
pub struct RecvStream<I, O, DD> {
    receiver: AsyncSReceiver<Message<I, O, DD>>,
}

impl<I, O, DD> RecvStream<I, O, DD> {
    pub fn new(receiver_async: AsyncSReceiver<Message<I, O, DD>>) -> Self {
        RecvStream {
            receiver: receiver_async,
        }
    }
}

impl<I, O, DD> Stream for RecvStream<I, O, DD> {
    type Item = Vec<O>;
    type Error = String;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            let message = self.receiver.try_recv();
            if let Err(error) = message {
                match error {
                    TryRecvError::Empty => {
                        match self.receiver.try_park() {
                            TryPark::NotEmpty => {
                                continue;
                            }
                            TryPark::Parked => {
                                return Ok(Async::NotReady);
                            }
                        }
                    },
                    TryRecvError::Disconnected => {
                        let error_msg = format!("Receiver is disconnected.");
                        error!("{}", error_msg);
                        return Ok(Async::Ready(None));
                    }
                }
            } else if let Ok(message) = message {
                match message {
                    Message::Response(data) => {
                        return Ok(Async::Ready(Some(data)));
                    }
                    Message::ResponseEnd => {
                        return Ok(Async::Ready(None));
                    }
                    Message::ServiceSuspend => {
                        error!("Server suspend, try to reconnect. ");
                        return Err(format!("Server suspend, try again later"));
                    }
                    Message::DataflowTimeout(id) => {
                        let err_msg = format!("Dataflow timeout or cancelled: {}. ", id);
                        error!("{}", err_msg);
                        return Err(err_msg);
                    }
                    msg => {
                        let err_msg = format!("Unexpected message received: {}", msg.name());
                        error!("{}", err_msg);
                        return Err(err_msg);
                    }
                }
            }
        }
    }
}


#[cfg(test)]
mod tests {
    extern crate timely_communication;
    extern crate timely;
    extern crate tokio_sync;

    use timely::dataflow::Stream as TimelyStream;
    use timely::dataflow::operators::*;
    use timely::dataflow::scopes::Child;
    use timely_communication::allocator::Generic;
    use timely::worker::Timeout;
    use server::ServerTimestamp;
    use timely::worker::Worker;
    use super::*;
    use timely_communication::Configuration;
    use std::sync::Arc;
    use server::server::TimelyServer;
    use server::allocate;
    use server::future_set::{ClientFuture};

    use futures::Future;
    use futures::stream::Stream;
    use futures::sink::Sink;
    use futures::sync::mpsc::channel;
    use tokio_sync::semaphore::Semaphore;
    use store::store_service::StoreServiceManager;
    use maxgraph_common::proto::lambda_service_grpc::LambdaServiceClient;
    use store::remote_store_service::RemoteStoreServiceManager;
    use maxgraph_store::api::graph_partition::ConstantPartitionManager;
    use store::task_partition_manager::TaskPartitionManager;
    use dataflow::manager::context::BuilderContext;

    fn build_mock_async_execute() -> impl Clone + for<'a> Fn(&mut Child<'a, Worker<Generic>, ServerTimestamp>, String,
        Option<&TimelyStream<Child<'a, Worker<Generic>, ServerTimestamp>, i64>>, Arc<BuilderContext>) -> TimelyStream<Child<'a, Worker<Generic>, ServerTimestamp>, i64>
    {
        move |scope, desc, _input, _context| {
            assert_eq!(desc, "desc_2");
            (0..3).to_stream(scope)
                .flat_map(|x| (0..4).map(move |i| i + x))
                .exchange(|x| *x as u64)
        }
    }

    #[test]
    fn mock_async_execute() {
        let config = Configuration::Process(1);

        let semaphore = Arc::new(Semaphore::new(2));

        let (sender, receiver) = channel(1024);
        let (s, r) = ::std::sync::mpsc::channel();

        let timely_server: Arc<TimelyServer<i64, i64, String>> = Arc::new(TimelyServer::new());
        let timely_server_clone = timely_server.clone();
        let backed_root_dir = String::from("./mock_async_execute");
        let backed_root_dir_clone = backed_root_dir.clone();
        ::std::fs::create_dir_all(backed_root_dir.clone()).expect("create backed_root_dir failed.");
        let task_partition_manager = TaskPartitionManager::empty();
        let partition_manager = Arc::new(ConstantPartitionManager::new());

        let _server_handle = ::std::thread::Builder::new().name("rpc_timely-server".to_owned()).spawn(move || {
            let (builder, _others) = config.try_build(Some(backed_root_dir_clone)).unwrap();
            let network_guards = Box::new(allocate::CommsGuard::new());
            let _guard = timely_server_clone.run(builder, network_guards, 0, 0, vec![], task_partition_manager, partition_manager, Arc::new(build_mock_async_execute()), false).unwrap();

            s.send("starting").expect("send starting failed.");

        }).expect("create rpc_timely-server thread failed");

        while !timely_server.is_server_running() {
            let _result = ::std::thread::sleep(::std::time::Duration::new(1, 0));
        }

        let future = ClientFuture::new(semaphore.clone())
            .and_then(|mut permit| {
                let mut client = timely_server.new_client_async();
                futures::done(client.execute_opt_async(10000 as u64, 128 as usize, "desc_1".to_owned()))
                    .and_then(move |execute_future| {
                        println!("success");
                        execute_future
                            .map_err(|err| {
                                err
                            })
                            .and_then(move |_dataflow_id| {
                                let stream = client.recv_async_stream()
                                    .then(|recv_message| {
                                        match recv_message {
                                            Ok(recv_result) => {
                                                Ok(recv_result)
                                            },
                                            Err(_) => {
                                                Ok(vec![0_i64; 1])
                                            }
                                        }
                                    })
                                    .map_err(|result: futures::sync::mpsc::SendError<_>| {
                                        result
                                    });

                                sender.send_all(stream)
                                    .map_err(|_err| {
                                        format!("error")
                                    })
                                    .and_then(move|_| {
                                        Ok(())
                                    })
                            })
                            .then(move |_result| {
                                permit.release(&semaphore);
                                Ok(())
                            })
                    })
            });


        r.recv().expect("recv starting failed.");

        let _future_result = future.wait().expect("run future failed.");

        let _result = receiver.for_each(|result| {
            let mut query_result = vec![];
            query_result.extend(result);
            query_result.sort();
            let mut expected = vec![0, 1, 2].iter().flat_map(|x| (0..4).map(move |i| i as i64 + x)).collect::<Vec<_>>();
            expected.sort();
            assert_eq!(query_result, expected);
            Ok(())
        }).wait().expect("receive query result failed.");
        ::std::fs::remove_dir_all(backed_root_dir).expect("remove backed_root_dir failed.");
    }
}

