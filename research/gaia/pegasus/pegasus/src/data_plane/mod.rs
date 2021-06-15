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

use crate::channel_id::{ChannelId, SubChannelId};
use crate::data::Data;
use crate::errors::{BuildJobError, IOError};
use pegasus_common::channel::MPMCSender;
use pegasus_network::{IPCReceiver, IPCSender};
use std::collections::LinkedList;

pub use crate::data_plane::intra_thread::{ThreadPull, ThreadPush};

/// Sending part of a message communication_old which always push message of type `T` into the underlying channel;
#[enum_dispatch]
pub trait Push<T>: Send {
    /// Push message into communication_old channel, returns [`Err(IOError)`] if failed;
    /// Check the error to get more information;
    fn push(&mut self, msg: T) -> Result<(), IOError>;

    /// If an error occurred when invoking the `push` function, the message failed to be pushed can
    /// be get back by invoke this function;
    fn check_failure(&mut self) -> Option<T> {
        None
    }

    /// Since some implementation may buffer messages, override this method
    /// to do flush;
    /// For the no-buffer communication_old implementations, invoke this should have no side-effect;
    fn flush(&mut self) -> Result<(), IOError> {
        Ok(())
    }

    /// Close the current [`Push`], it can't push messages any more;
    fn close(&mut self) -> Result<(), IOError>;
}

/// Abstraction of the receive side of a data-plane communication_old which transforms messages of type [`T`];
/// Same as the [`Push`] abstraction, the [`Dataplane`] encapsulate the underlying communication_old implementation,
/// and provide a general [`Pull`] abstraction for applications to consume messages;
#[enum_dispatch]
pub trait Pull<T>: Send {
    /// Pull message out of the data-plane communication_old;
    /// This function won't block, it is more like the 'try_pull' semantic;
    ///
    /// Returns [`Ok(Some(T))`] immediately if any messages were available in the communication_old, otherwise
    /// returns [`Ok(None)`].
    ///
    /// Error([`Err(IOError)`]) occurs if the communication_old is in exception; Check the returned [`IOError`]
    /// for more details about the error;
    fn pull(&mut self) -> Result<Option<T>, IOError>;
}

impl<T, P: ?Sized + Push<T>> Push<T> for Box<P> {
    #[inline]
    fn push(&mut self, msg: T) -> Result<(), IOError> {
        (**self).push(msg)
    }

    #[inline]
    fn check_failure(&mut self) -> Option<T> {
        (**self).check_failure()
    }

    #[inline]
    fn flush(&mut self) -> Result<(), IOError> {
        (**self).flush()
    }

    #[inline]
    fn close(&mut self) -> Result<(), IOError> {
        (**self).close()
    }
}

impl<T, P: ?Sized + Pull<T>> Pull<T> for Box<P> {
    #[inline]
    fn pull(&mut self) -> Result<Option<T>, IOError> {
        (**self).pull()
    }
}

mod inter_processes;
mod intra_process;
mod intra_thread;

use crate::data_plane::inter_processes::{CombinationPull, RemotePush};
use crate::ServerConf;
use intra_process::IntraProcessPull;
use intra_process::IntraProcessPush;

#[enum_dispatch(Push<T>)]
pub enum GeneralPush<T: Data> {
    IntraThread(ThreadPush<T>),
    IntraProcess(IntraProcessPush<T>),
    InterProcesses(RemotePush<T>),
}

impl<T: Data> GeneralPush<T> {
    pub fn is_local(&self) -> bool {
        match self {
            GeneralPush::InterProcesses(_) => false,
            _ => true,
        }
    }
}

#[enum_dispatch(Pull<T>)]
pub enum GeneralPull<T: Data> {
    IntraThread(ThreadPull<T>),
    IntraProcess(IntraProcessPull<T>),
    InterProcesses(CombinationPull<T>),
}

pub(crate) fn pipeline<T: Data>(id: SubChannelId) -> (ThreadPush<T>, ThreadPull<T>) {
    intra_thread::pipeline(id)
}

pub struct ChannelResource<T: Data> {
    pub ch_id: SubChannelId,
    pushes: Vec<GeneralPush<T>>,
    pull: GeneralPull<T>,
}

impl<T: Data> ChannelResource<T> {
    pub fn take(self) -> (Vec<GeneralPush<T>>, GeneralPull<T>) {
        (self.pushes, self.pull)
    }
}

pub fn build_local_channels<T: Data>(
    id: ChannelId, workers: usize,
) -> LinkedList<ChannelResource<T>> {
    let mut list = LinkedList::new();
    if workers == 0 {
        return list;
    }

    if workers == 1 {
        let ch_id = (id, 0u32).into();
        let (tx, rx) = intra_thread::pipeline::<T>(ch_id);
        let pushes = vec![tx.into()];
        list.push_back(ChannelResource { ch_id, pushes, pull: rx.into() });
        return list;
    }

    let mut ch_txs = Vec::with_capacity(workers);
    let mut ch_rxs = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tx, rx) = pegasus_common::channel::unbound::<T>();
        ch_txs.push(tx);
        ch_rxs.push(rx);
    }

    for (i, recv) in ch_rxs.into_iter().enumerate() {
        let (tx, rx) = intra_thread::pipeline::<T>((id, i).into());
        let mut pushes = Vec::<GeneralPush<T>>::with_capacity(workers);
        for j in 0..workers {
            let p = IntraProcessPush::<T>::new((id, j).into(), ch_txs[j].clone());
            pushes.push(p.into());
        }
        let mut send = std::mem::replace(&mut pushes[i], tx.into());
        send.close().ok();
        let ch_id = (id, i).into();
        let pull = IntraProcessPull::<T>::new(ch_id, rx, recv).into();
        let ch = ChannelResource { ch_id, pushes, pull };
        list.push_back(ch);
    }
    for tx in ch_txs {
        tx.close();
    }
    list
}

pub fn build_channels<T: Data>(
    id: ChannelId, workers: usize, server_index: usize, server_conf: &ServerConf,
) -> Result<LinkedList<ChannelResource<T>>, BuildJobError> {
    let servers = server_conf.get_servers();
    if servers.is_empty() {
        return Ok(build_local_channels(id, workers));
    }
    assert!(
        server_index < servers.len(),
        "invalid server index: {} out of bound: {}",
        server_index,
        servers.len()
    );
    if servers.len() == 1 && server_index == 0 {
        return Ok(build_local_channels(id, workers));
    }
    let my_server_id = servers[server_index];

    let worker_offset = workers * server_index as usize;
    // prepare local channels;
    let mut to_local_pushes = LinkedList::new();
    let mut local_pull = LinkedList::new();
    {
        let mut ch_txs = Vec::with_capacity(workers);
        let mut ch_rxs = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx, rx) = pegasus_common::channel::unbound::<T>();
            ch_txs.push(tx);
            ch_rxs.push(rx);
        }

        for (i, recv) in ch_rxs.into_iter().enumerate() {
            let mut pushes = Vec::<GeneralPush<T>>::with_capacity(workers);
            for j in 0..workers {
                let p =
                    IntraProcessPush::<T>::new((id, j + worker_offset).into(), ch_txs[j].clone());
                pushes.push(p.into());
            }
            let (tx, rx) = intra_thread::pipeline::<T>((id, i + worker_offset).into());
            let mut send = std::mem::replace(&mut pushes[i], tx.into());
            send.close().ok();
            let pull = IntraProcessPull::<T>::new((id, i + worker_offset).into(), rx, recv);
            to_local_pushes.push_back(pushes);
            local_pull.push_back(pull);
        }

        for t in ch_txs {
            t.close();
        }
    }

    // prepare remote channels
    let mut remote_sends = Vec::<Vec<IPCSender<T>>>::new();
    let mut remote_recv = LinkedList::<IPCReceiver<T>>::new();
    {
        for i in 0..workers {
            let ch_id = encode_channel_id(id, i as u32);
            let sends = pegasus_network::ipc_channel_send::<T>(ch_id, my_server_id, &servers)?;
            remote_sends.push(sends);
            let recv = pegasus_network::ipc_channel_recv::<T>(ch_id, my_server_id, &servers)?;
            remote_recv.push_back(recv);
        }
    }

    let mut ch_res = LinkedList::new();
    for i in 0..workers {
        let peers = workers * servers.len();
        let mut pushes = Vec::with_capacity(peers);
        let mut idx = 0;
        for server_id in servers.iter() {
            if *server_id == my_server_id {
                if let Some(p) = to_local_pushes.pop_front() {
                    pushes.extend(p);
                }
            } else {
                for j in 0..workers {
                    let worker_index = idx * workers + j;
                    let send = remote_sends[j][idx].clone();
                    let p = RemotePush::new((id, worker_index).into(), send);
                    pushes.push(p.into());
                }
                idx += 1;
            }
        }
        let local = local_pull.pop_front().expect("local recv lost");
        let remote = remote_recv.pop_front().expect("remote recv lost");
        let ch_id = (id, worker_offset + i).into();
        let pull = CombinationPull::<T>::new(ch_id, local, remote);
        ch_res.push_back(ChannelResource { ch_id, pushes, pull: pull.into() });
    }

    // do clean
    for t in remote_sends {
        for mut tx in t {
            tx.close().ok();
        }
    }

    Ok(ch_res)
}

#[inline]
fn encode_channel_id(id: ChannelId, worker_index: u32) -> u128 {
    let mut ch_id = (id.job_seq as u128) << 64;
    ch_id |= (id.index as u128) << 32;
    ch_id |= worker_index as u128;
    ch_id
}

#[cfg(test)]
mod test {
    use super::*;
    use pegasus_network::config::ConnectionParams;
    use pegasus_network::Server;

    fn push_pull_ch(workers: usize, ch_res: ChannelResource<u64>) {
        let ch_id = ch_res.ch_id;
        let (mut pushes, mut pull) = ch_res.take();
        assert_eq!(pushes.len(), workers);
        let limit = workers as u64 * 1024;
        for i in 0..limit {
            let offset = i as usize % workers;
            pushes[offset].push(i).unwrap();
        }

        for mut p in pushes {
            p.close().unwrap();
        }

        let mut count = 0;
        loop {
            match pull.pull() {
                Ok(Some(item)) => {
                    assert_eq!(item as usize % workers, ch_id.worker as usize);
                    count += 1;
                }
                Ok(None) => {
                    println!("has received {}, expect {}", count, limit);
                    std::thread::sleep(std::time::Duration::from_millis(100))
                }
                Err(e) => {
                    if e.is_source_exhaust() {
                        break;
                    } else {
                        panic!("get error {}", e);
                    }
                }
            }
        }
        println!("finish receive;");
        assert_eq!(count, limit);
    }

    fn run_channel_test(workers: usize, resources: &mut LinkedList<ChannelResource<u64>>) {
        let mut thread_guards = vec![];
        while let Some(ch) = resources.pop_front() {
            thread_guards.push(std::thread::spawn(move || push_pull_ch(workers, ch)));
        }

        for g in thread_guards {
            g.join().unwrap()
        }
    }

    #[test]
    fn intra_process_ch_1() {
        let mut resources = build_local_channels([1, 1].into(), 1);
        run_channel_test(1, &mut resources);
    }

    #[test]
    fn intra_process_ch_2() {
        let mut resources = build_local_channels([1, 2].into(), 2);
        run_channel_test(2, &mut resources);
    }

    #[test]
    fn intra_process_ch_3() {
        let mut resources = build_local_channels([1, 3].into(), 3);
        run_channel_test(3, &mut resources);
    }

    fn channel_test(ch_index: usize, workers: usize, server_index: usize, servers: &[u64]) {
        let mut ch_resources =
            build_channels([1, ch_index].into(), workers, server_index, servers).unwrap();
        if servers.is_empty() {
            run_channel_test(workers, &mut ch_resources);
        } else {
            let total_workers = workers * servers.len();
            run_channel_test(total_workers, &mut ch_resources);
        }
    }

    #[test]
    fn test_channel_empty_server() {
        channel_test(1, 2, 0, &vec![]);
    }

    #[test]
    fn test_channel_between_2_servers() {
        pegasus_common::logs::init_log();
        let mut servers = vec![];
        servers.push(Server { id: 0, addr: "127.0.0.1:2333".parse().unwrap() });
        servers.push(Server { id: 1, addr: "127.0.0.1:2334".parse().unwrap() });
        let servers_bk = servers.clone();
        let s1 = std::thread::Builder::new()
            .name("server_0".to_owned())
            .spawn(move || {
                println!("start server 0");
                pegasus_network::start_up(
                    0,
                    ConnectionParams::nonblocking(),
                    "127.0.0.1:2333",
                    servers_bk,
                )
                .unwrap();
                while !pegasus_network::check_connect(0, &vec![0, 1]) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
                channel_test(1, 2, 0, &vec![0, 1]);
                pegasus_network::shutdown(0);
            })
            .unwrap();

        let s2 = std::thread::Builder::new()
            .name("server_1".to_owned())
            .spawn(move || {
                println!("start server 1");
                pegasus_network::start_up(
                    1,
                    ConnectionParams::nonblocking(),
                    "127.0.0.1:2334",
                    servers,
                )
                .unwrap();
                while !pegasus_network::check_connect(0, &vec![0, 1]) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
                channel_test(1, 2, 1, &vec![0, 1]);
                pegasus_network::shutdown(1);
            })
            .unwrap();
        s1.join().unwrap();
        s2.join().unwrap();
    }
}
