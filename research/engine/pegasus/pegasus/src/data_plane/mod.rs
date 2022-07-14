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

use std::collections::LinkedList;

use pegasus_common::channel::MPMCSender;
use pegasus_network::{IPCReceiver, IPCSender};

use crate::channel_id::ChannelId;
use crate::data::Data;
use crate::errors::{BuildJobError, IOError};

/// Sending part of a message communication_old which always push message of type `T` into the underlying channel;
#[enum_dispatch]
pub trait Push<T>: Send {
    /// Push message into communication_old channel, returns [`Err(IOError)`] if failed;
    /// Check the error to get more information;
    fn push(&mut self, msg: T) -> Result<(), IOError>;

    /// If an error occurred when invoking the `push` function, the message failed to be pushed can
    /// be get back by invoke this function;
    fn check_failed(&mut self) -> Option<T> {
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

/// Abstraction of the receive side of a channel which transforms messages of type [`T`];
#[enum_dispatch]
pub trait Pull<T>: Send {
    /// Pull message out of the underlying channel;
    ///
    /// This function won't block;
    ///
    /// Returns [`Ok(Some(T))`] immediately if any message is available in the channel, otherwise
    /// returns [`Ok(None)`].
    ///
    /// Error([`Err(IOError)`]) occurs if the channel is in exception; Check the returned [`IOError`]
    /// for more details about the error;
    fn next(&mut self) -> Result<Option<T>, IOError>;

    /// Check if there is any message in the channel;
    fn has_next(&mut self) -> Result<bool, IOError>;
}

impl<T, P: ?Sized + Push<T>> Push<T> for Box<P> {
    #[inline]
    fn push(&mut self, msg: T) -> Result<(), IOError> {
        (**self).push(msg)
    }

    #[inline]
    fn check_failed(&mut self) -> Option<T> {
        (**self).check_failed()
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
    fn next(&mut self) -> Result<Option<T>, IOError> {
        (**self).next()
    }

    fn has_next(&mut self) -> Result<bool, IOError> {
        (**self).has_next()
    }
}

mod inter_processes;
mod intra_process;
pub(crate) mod intra_thread;

use inter_processes::{CombinationPull, RemotePush};
use intra_process::{IntraProcessPull, IntraProcessPush};
use intra_thread::{ThreadPull, ThreadPush};

use crate::config::ServerConf;

#[enum_dispatch(Push<T>)]
pub enum GeneralPush<T: Data> {
    IntraThread(ThreadPush<T>),
    IntraProcess(IntraProcessPush<T>),
    InterProcesses(RemotePush<T>),
}

impl<T: Data> GeneralPush<T> {
    #[inline]
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

pub(crate) fn pipeline<T: Data>(id: ChannelId) -> (ThreadPush<T>, ThreadPull<T>) {
    intra_thread::pipeline(id)
}

pub struct ChannelResource<T: Data> {
    pub ch_id: ChannelId,
    pushes: Vec<GeneralPush<T>>,
    pull: GeneralPull<T>,
}

impl<T: Data> ChannelResource<T> {
    pub fn take(self) -> (Vec<GeneralPush<T>>, GeneralPull<T>) {
        (self.pushes, self.pull)
    }
}

pub fn build_local_channels<T: Data>(id: ChannelId, workers: usize) -> LinkedList<ChannelResource<T>> {
    let mut list = LinkedList::new();
    if workers == 0 {
        return list;
    }

    if workers == 1 {
        let (tx, rx) = intra_thread::pipeline::<T>(id);
        let pushes = vec![tx.into()];
        list.push_back(ChannelResource { ch_id: id, pushes, pull: rx.into() });
        return list;
    }

    let mut ch_txs = Vec::with_capacity(workers);
    let mut ch_rxs = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tx, rx) = pegasus_common::channel::unbound::<T>();
        ch_txs.push(tx);
        ch_rxs.push(rx);
    }

    for (_, recv) in ch_rxs.into_iter().enumerate() {
        let (tx, rx) = intra_thread::pipeline::<T>(id);
        let mut pushes = Vec::<GeneralPush<T>>::with_capacity(workers);
        for j in 0..workers {
            let p = IntraProcessPush::<T>::new(id, ch_txs[j].clone());
            pushes.push(p.into());
        }
        pushes.push(tx.into());
        // let mut send = std::mem::replace(&mut pushes[i], tx.into());
        // send.close().ok();
        let pull = IntraProcessPull::<T>::new(id, rx, recv).into();
        let ch = ChannelResource { ch_id: id, pushes, pull };
        list.push_back(ch);
    }
    for tx in ch_txs {
        tx.close();
    }
    list
}

pub fn build_channels<T: Data>(
    id: ChannelId, local_workers: u32, server_index: u32, server_conf: &ServerConf,
) -> Result<LinkedList<ChannelResource<T>>, BuildJobError> {
    let workers = local_workers as usize;
    let servers = server_conf.get_servers();
    if servers.is_empty() {
        return Ok(build_local_channels(id, workers));
    }

    let server_index = server_index as usize;

    if servers.len() == 1 && server_index == 0 {
        return Ok(build_local_channels(id, workers));
    }
    let my_server_id = servers[server_index];

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

        for recv in ch_rxs.into_iter() {
            let mut pushes = Vec::<GeneralPush<T>>::with_capacity(workers + 1);
            for j in 0..workers {
                let p = IntraProcessPush::<T>::new(id, ch_txs[j].clone());
                pushes.push(p.into());
            }
            let (tx, rx) = intra_thread::pipeline::<T>(id);
            pushes.push(tx.into());
            let pull = IntraProcessPull::<T>::new(id, rx, recv);
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
            // use send[j] to send  message to worker[i] at server j
            let sends = pegasus_network::ipc_channel_send::<T>(ch_id, my_server_id, &servers)?;
            remote_sends.push(sends);
            let recv = pegasus_network::ipc_channel_recv::<T>(ch_id, my_server_id, &servers)?;
            remote_recv.push_back(recv);
        }
    }

    let mut ch_res = LinkedList::new();
    // prepare channel resources for each local worker;
    let total_peers = workers * servers.len() as usize;
    for _ in 0..workers {
        let mut pushes = Vec::with_capacity(total_peers + 1);
        let mut to_local = None;
        let mut idx = 0;
        for server_id in servers.iter() {
            if *server_id == my_server_id {
                if let Some(mut p) = to_local_pushes.pop_front() {
                    to_local = p.pop();
                    pushes.extend(p);
                }
            } else {
                for j in 0..workers {
                    let send = remote_sends[j][idx].clone();
                    let p = RemotePush::new(id, send);
                    pushes.push(p.into());
                }
                idx += 1;
            }
        }
        if let Some(local_push) = to_local {
            pushes.push(local_push);
        }
        assert_eq!(pushes.len(), total_peers + 1);
        let local = local_pull.pop_front().expect("local recv lost");
        let remote = remote_recv
            .pop_front()
            .expect("remote recv lost");
        let pull = CombinationPull::<T>::new(id, local, remote);
        ch_res.push_back(ChannelResource { ch_id: id, pushes, pull: pull.into() });
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
    use pegasus_network::config::ConnectionParams;
    use pegasus_network::Server;

    use super::*;

    fn push_pull_ch(index: usize, workers: usize, ch_res: ChannelResource<u64>) {
        let (mut pushes, mut pull) = ch_res.take();
        if workers > 1 {
            let mut push = pushes.swap_remove(index);
            push.close().unwrap();
        }
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
            match pull.next() {
                Ok(Some(_item)) => {
                    assert_eq!(_item as usize % workers, index as usize);
                    count += 1;
                }
                Ok(None) => {
                    println!("worker[{}] has received {}, expect {}", index, count, limit);
                    std::thread::sleep(std::time::Duration::from_millis(50))
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
        println!("worker[{}] finish receive {};", index, count);
        assert_eq!(count, limit);
    }

    fn run_channel_test(offset: usize, workers: usize, resources: &mut LinkedList<ChannelResource<u64>>) {
        let mut thread_guards = vec![];
        let mut index = offset;
        while let Some(ch) = resources.pop_front() {
            thread_guards.push(std::thread::spawn(move || push_pull_ch(index, workers, ch)));
            index += 1;
        }

        for g in thread_guards {
            g.join().unwrap()
        }
    }

    #[test]
    fn intra_process_ch_1() {
        let mut resources = build_local_channels(ChannelId::new(1, 1), 1);
        run_channel_test(0, 1, &mut resources);
    }

    #[test]
    fn intra_process_ch_2() {
        let mut resources = build_local_channels(ChannelId::new(1, 2), 2);
        run_channel_test(0, 2, &mut resources);
    }

    #[test]
    fn intra_process_ch_3() {
        let mut resources = build_local_channels(ChannelId::new(1, 3), 3);
        run_channel_test(0, 3, &mut resources);
    }

    fn channel_test(ch_index: usize, workers: usize, server_index: usize, servers: &ServerConf) {
        let mut ch_resources = build_channels(
            ChannelId::new(1, ch_index as u32),
            workers as u32,
            server_index as u32,
            servers,
        )
        .unwrap();
        let servers_len = servers.len();
        if servers_len == 0 {
            run_channel_test(0, workers, &mut ch_resources);
        } else {
            let total_workers = workers * servers_len;
            let offset = server_index * workers;
            run_channel_test(offset, total_workers, &mut ch_resources);
        }
    }

    #[test]
    fn test_channel_empty_server() {
        channel_test(1, 2, 0, &ServerConf::Local);
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
                pegasus_network::start_up(0, ConnectionParams::nonblocking(), "127.0.0.1:2333", servers_bk)
                    .unwrap();
                let server_conf = ServerConf::Partial(vec![0, 1]);
                while !pegasus_network::check_ipc_ready(0, &vec![0, 1]) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
                channel_test(2, 3, 0, &server_conf);
                pegasus_network::shutdown(0);
            })
            .unwrap();

        let s2 = std::thread::Builder::new()
            .name("server_1".to_owned())
            .spawn(move || {
                println!("start server 1");
                pegasus_network::start_up(1, ConnectionParams::nonblocking(), "127.0.0.1:2334", servers)
                    .unwrap();
                while !pegasus_network::check_ipc_ready(0, &vec![0, 1]) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
                let server_conf = ServerConf::Partial(vec![0, 1]);
                channel_test(2, 3, 1, &server_conf);
                pegasus_network::shutdown(1);
            })
            .unwrap();
        s1.join().unwrap();
        s2.join().unwrap();
    }

    #[test]
    fn multi_channels_test() {
        pegasus_common::logs::init_log();
        let mut servers = vec![];
        servers.push(Server { id: 2, addr: "127.0.0.1:2335".parse().unwrap() });
        servers.push(Server { id: 3, addr: "127.0.0.1:2336".parse().unwrap() });
        let server_size = 2;
        let mut server_list = Vec::with_capacity(server_size);
        for _ in 0..server_size {
            server_list.push(servers.clone());
        }
        // build two channels for each worker
        let mut threads = vec![];
        let mut index = 0;
        let start_index = 2;
        for server in server_list {
            threads.push(std::thread::spawn(move || {
                pegasus_network::start_up(
                    index + start_index,
                    ConnectionParams::nonblocking(),
                    server[index as usize].addr,
                    server,
                )
                .unwrap();
                while !pegasus_network::check_ipc_ready(index + start_index, &vec![2, 3]) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
                let worker_id = index as usize;
                let local_workers = 1;
                let server_conf = ServerConf::Partial(vec![2, 3]);
                let mut ch_resources_first = build_channels::<Vec<u64>>(
                    ChannelId::new(1, 0),
                    local_workers,
                    index as u32,
                    &server_conf,
                )
                .unwrap();
                let mut ch_resources_second = build_channels::<Vec<u64>>(
                    ChannelId::new(1, 1),
                    local_workers,
                    index as u32,
                    &server_conf,
                )
                .unwrap();
                let mut channels = vec![];
                channels.push(ch_resources_first.pop_front().unwrap());
                channels.push(ch_resources_second.pop_front().unwrap());
                let mut channel_threads = vec![];
                let mut channel_index = 0;
                for ch in channels {
                    channel_threads.push(std::thread::spawn(move || {
                        let (mut pushes, mut pull) = ch.take();
                        let mut removed_push = pushes.swap_remove(worker_id);
                        removed_push.close().unwrap();
                        let message_size = 1024;
                        for i in 0..pushes.len() {
                            if i != worker_id {
                                for j in 0..message_size {
                                    let mut buffer = vec![];
                                    buffer.push((channel_index << 32) + j as u64);
                                    pushes[i].push(buffer).unwrap();
                                }
                            }
                            pushes[i].close().unwrap();
                        }
                        let mut count = 0;
                        loop {
                            match pull.next() {
                                Ok(Some(mut _item)) => {
                                    for i in _item {
                                        let recv_channel_index = i >> 32;
                                        let message_index = i ^ (recv_channel_index << 32);
                                        assert_eq!(recv_channel_index, channel_index);
                                        assert_eq!(count, message_index);
                                    }
                                    count += 1;
                                }
                                Ok(None) => std::thread::sleep(std::time::Duration::from_millis(50)),
                                Err(e) => {
                                    if e.is_source_exhaust() {
                                        info!(
                                            "worker[{}] has received {}, expect {}",
                                            worker_id, count, message_size
                                        );
                                        break;
                                    } else {
                                        panic!("get error {}", e);
                                    }
                                }
                            }
                        }
                    }));
                    channel_index += 1;
                }
                for g in channel_threads {
                    g.join().unwrap();
                }
                pegasus_network::shutdown(index + start_index);
            }));
            index += 1;
        }
        for g in threads {
            g.join().unwrap();
        }
    }

    #[test]
    fn large_buffer_test() {
        pegasus_common::logs::init_log();
        let mut servers = vec![];
        servers.push(Server { id: 4, addr: "127.0.0.1:2337".parse().unwrap() });
        servers.push(Server { id: 5, addr: "127.0.0.1:2338".parse().unwrap() });
        let server_size = 2;
        let mut server_list = Vec::with_capacity(server_size);
        for _ in 0..server_size {
            server_list.push(servers.clone());
        }
        // build two channels for each worker
        let mut threads = vec![];
        let mut index = 0;
        let start_index = 4;
        for server in server_list {
            threads.push(std::thread::spawn(move || {
                pegasus_network::start_up(
                    index + start_index,
                    ConnectionParams::nonblocking(),
                    server[index as usize].addr,
                    server,
                )
                .unwrap();
                while !pegasus_network::check_ipc_ready(index + start_index, &vec![4, 5]) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
                let local_workers = 2;
                let server_conf = ServerConf::Partial(vec![4, 5]);
                let mut ch_resources = build_channels::<Vec<u64>>(
                    ChannelId::new(2, 0),
                    local_workers,
                    index as u32,
                    &server_conf,
                )
                .unwrap();
                let mut channel_threads = vec![];
                let mut local_index = 0;
                while let Some(ch) = ch_resources.pop_front() {
                    channel_threads.push(std::thread::spawn(move || {
                        let worker_id = index * local_workers as u64 + local_index;
                        let (mut pushes, mut pull) = ch.take();
                        let mut removed_push = pushes.swap_remove(worker_id as usize);
                        removed_push.close().unwrap();
                        let message_count = pushes.len() * 64;
                        let message_size = 65536;
                        for i in 0..message_count {
                            let offset = i % pushes.len();
                            let mut message = vec![];
                            for j in 0..message_size {
                                message.push((offset << 32) as u64 + j);
                            }
                            pushes[offset].push(message).unwrap();
                        }
                        for mut push in pushes {
                            push.close().unwrap();
                        }
                        let mut count = 0;
                        loop {
                            match pull.next() {
                                Ok(Some(mut _item)) => {
                                    count += 1;
                                }
                                Ok(None) => {
                                    info!(
                                        "worker[{}] has received {}, expect {}",
                                        worker_id, count, message_count
                                    );
                                    std::thread::sleep(std::time::Duration::from_millis(50))
                                }
                                Err(e) => {
                                    if e.is_source_exhaust() {
                                        info!(
                                            "worker[{}] has received {}, expect {}",
                                            worker_id, count, message_count
                                        );
                                        break;
                                    } else {
                                        panic!("get error {}", e);
                                    }
                                }
                            }
                        }
                    }));
                    local_index += 1;
                }
                for g in channel_threads {
                    g.join().unwrap();
                }
                pegasus_network::shutdown(index + start_index);
            }));
            index += 1;
        }
        for g in threads {
            g.join().unwrap();
        }
    }
}
