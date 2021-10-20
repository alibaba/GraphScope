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

use std::net::{TcpStream, TcpListener};
use std::thread::{self, JoinHandle};
use std::io::{Read, Write, BufWriter, ErrorKind};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::convert::From;
use std::cell::Cell;

use crossbeam_channel::{Sender, Receiver, SendError, RecvTimeoutError, TryRecvError};
use super::*;
use crate::common::{BytesSlab, Bytes};

#[derive(Copy, Clone, Debug, Abomonation, Eq, PartialEq)]
pub struct MessageHeader {
    /// index of job;
    pub task: usize,
    /// index of channel.
    pub channel: usize,
    /// index of worker sending message.
    pub source: usize,
    /// index of worker receiving message.
    pub target: usize,
    /// number of bytes in message.
    pub length: usize,
    /// sequence number.
    pub seqno: usize,
}

impl MessageHeader {
    pub fn new_empty(&self) -> MessageHeader {
        let mut empty = *self;
        empty.length = 0;
        empty
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
}

impl From<MessageHeader> for WorkerId {
    fn from(h: MessageHeader) -> Self {
        WorkerId(h.task, h.target)
    }
}


// TODO(qiuxiafei): DANGER!!! padding! padding! padding!
pub const HEAD_SIZE: usize = ::std::mem::size_of::<MessageHeader>();
pub const EMPTY_HEAD: MessageHeader = MessageHeader { task: 0, channel: 0, source: 0, target: 0, length: 0, seqno: 0 };


impl MessageHeader {
    #[inline(always)]
    pub fn try_read(bytes: &mut [u8]) -> Option<MessageHeader> {
        unsafe { abomonation::decode::<MessageHeader>(bytes) }
            .and_then(|(header, remaining)| {
                if remaining.len() >= header.length {
                    Some(header.clone())
                } else {
                    None
                }
            })
    }

    #[inline(always)]
    pub fn write_to<W: ::std::io::Write>(&self, writer: &mut W) -> ::std::io::Result<()> {
        unsafe { abomonation::encode(self, writer) }
    }


    #[inline(always)]
    pub fn required_bytes(&self) -> usize {
        HEAD_SIZE + self.length
    }
}

/// A `Postman` send mails (data, events, etc.) to a target process.
pub struct Postman {
    pub target: usize,
    smb_rx: Receiver<Bytes>,
    broken: Arc<AtomicBool>,
}

impl Postman {
    pub fn new(target: usize, smb: Receiver<Bytes>, broken: &Arc<AtomicBool>) -> Self {
        Postman {
            target,
            smb_rx: smb,
            broken: broken.clone(),
        }
    }

    pub fn send_to_end(&self, mut connection: Connection) -> ::std::io::Result<()> {
        info!("Start to repeatedly send message to index {}", self.target);
        // start tcp send transform loop;
        loop {
            match self.smb_rx.recv_timeout(Duration::from_secs(2)) {
                Ok(msg) => {
                    match connection.write_all(&msg[..]) {
                        Err(err) => {
                            error!("send message failure: {:?}", err);
                            connection.poisoned();
                            return Err(err);
                        },
                        Ok(()) => ()
                    }
                },
                Err(RecvTimeoutError::Disconnected) => {
                    break
                },
                Err(RecvTimeoutError::Timeout) => {
                    if let Err(e) = connection.flush() {
                        if e.kind() != ErrorKind::Interrupted {
                            error!("network error: flush failure, {:?};", e);
                            return Err(e);
                        }
                    }
                    if !connection.sniff() {
                        return Err(::std::io::Error::new(ErrorKind::Other, "network is disconnected. "))
                    }

                   if self.broken.load(Ordering::Relaxed) {
                       return Err(::std::io::Error::from(ErrorKind::BrokenPipe));
                   }
                }
            }
        }
        info!("Stop to send message to index {}", self.target);
        Ok(())
        //self.writer.get_mut().shutdown(::std::net::Shutdown::Write).expect("Write shutdown failed");
    }

    #[allow(dead_code)]
    #[inline]
    fn write_end<W: Write>(remote: &mut BufWriter<W>) -> ::std::io::Result<()>{
        EMPTY_HEAD.write_to(remote)?;
        remote.flush()
    }
}


pub struct RecvMailBox {
    pub worker_index: WorkerId,
    pub channel_index: ChannelId,
    data_sender: Sender<Bytes>,
}

impl Clone for RecvMailBox {
    fn clone(&self) -> Self {
        RecvMailBox {
            worker_index: self.worker_index,
            channel_index: self.channel_index,
            data_sender: self.data_sender.clone(),
        }
    }
}

impl RecvMailBox {
    pub fn new(index: WorkerId, channel: ChannelId, sender: Sender<Bytes>) -> Self {
        RecvMailBox {
            worker_index: index,
            channel_index: channel,
            data_sender: sender,
        }
    }

    #[inline]
    pub fn push(&self, header: MessageHeader, msg: Bytes) -> Result<(), SendError<Bytes>> {
        debug!("worker[{:?}]: channel-{} recv {:?}", self.worker_index, self.channel_index, header);
        self.data_sender.send(msg)
    }
}

struct Dispatcher {
    id: (WorkerId, usize),
    guard: HashSet<WorkerId>,
    peers: i64,
    departed: bool,
    rmb: Option<RecvMailBox>,
    stash: Vec<(MessageHeader ,Bytes)>,
    exhausted: Cell<bool>,
}

impl Dispatcher {
    pub fn new(worker: WorkerId, channel: usize, peers: i64, rmb: Option<RecvMailBox>) -> Self {
        // None mailbox indicate a invalid dispatcher, maybe only a place holder,
        // for this case, exhaust flag is set;
        let valid = rmb.is_some();
        Dispatcher {
            id: (worker, channel),
            guard: HashSet::new(),
            peers,
            departed: false,
            rmb,
            stash: Vec::new(),
            exhausted: Cell::new(!valid),
        }
    }

    #[inline]
    pub fn reset_rmb(&mut self, rmb: RecvMailBox) {
        self.exhausted.replace(false);
        if self.rmb.is_none() {
            debug!("recv mailbox {:?} registered;", self.id);
            for (header, msg) in self.stash.drain(..) {
                if !self.departed {
                    if let Err(_e) = rmb.push(header, msg) {
                        error!("rmb[{}-{}] disconnected", rmb.worker_index, rmb.channel_index);
                        self.departed = true;
                    }
                }
            }
            self.rmb.replace(rmb);
        }
    }

    #[inline]
    pub fn reset_peers(&mut self, peers: u32) {
        self.peers = peers as i64;
    }

    #[inline]
    pub fn decrease(&mut self, source: WorkerId) {
        self.exhausted.replace(false);
        self.guard.insert(source);
        debug!("{} stop to send to network mailbox {:?}", source, self.id);
    }

    #[inline]
    pub fn is_exhausted(&self) -> bool {
        if !self.exhausted.get() {
            let exhausted = self.departed || (self.stash.is_empty() &&
                (self.peers == -1 && self.guard.len() == 0 || self.guard.len() as i64 == self.peers));
            if exhausted {
                debug!("network mailbox {:?} exhausted;", self.id);
                self.exhausted.replace(exhausted);
            }
            exhausted
        } else {
            true
        }
    }

    #[inline]
    pub fn is_stashed(&self) -> bool {
        self.rmb.is_none()
    }

    #[inline]
    pub fn set_departed(&mut self) {
        self.departed = true;
        self.stash.clear();
    }

    #[inline]
    pub fn send(&mut self, header: MessageHeader, msg: Bytes) {
        self.exhausted.replace(false);
        if !self.departed {
            if let Some(ref rmb) = self.rmb {
                match rmb.push(header, msg) {
                    Err(_) => {
                        error!("rmb[{}-{}] disconnected;", rmb.worker_index, rmb.channel_index);
                        self.departed = true;
                    },
                    _ => ()
                }
            } else {
                warn!("recv mailbox {:?} not registered, push {:?} to stash;",self.id, header);
                self.stash.push((header, msg));
            }
        } else {
            warn!("recv mailbox {:?} is departed, abondon message {:?}", self.id, header)
        }
    }
}

pub struct Switchboard {
    rmb_rx: Receiver<(RecvMailBox, usize)>,
    switch: HashMap<WorkerId, Vec<Dispatcher>>,
    need_clean: bool,
}

impl Switchboard {
    pub fn new(rx: Receiver<(RecvMailBox, usize)>) -> Self {
        Switchboard {
            rmb_rx: rx,
            switch: HashMap::new(),
            need_clean: false,
        }
    }

    pub fn send_to(&mut self, header: MessageHeader, msg: Bytes) {
        let target_id = WorkerId::from(header);
        self.check_switch();
        let dispatch = self.ensure(target_id, header.channel);
        dispatch.send(header, msg);
    }

    pub fn clean(&mut self, source: WorkerId, target: WorkerId, channel: usize) {
        self.check_switch();
        let dispatch = self.ensure(target, channel);
        dispatch.decrease(source);
        self.need_clean = true;
    }

    pub fn clean_up(&mut self) {
        if self.need_clean {
            self.switch.retain(|id, chs| {
                let retain = chs.iter().any(|d| !d.is_exhausted());
                if !retain {
                    debug!("clean all network mailboxes of worker {}", id)
                }
                retain
            });
            self.need_clean = false;
        }
    }

    pub fn check_switch(&mut self) {
        loop {
            match self.rmb_rx.try_recv() {
                Ok((rmb, peers)) => {
                    let d = self.ensure(rmb.worker_index, rmb.channel_index.0);
                    d.reset_rmb(rmb);
                    d.reset_peers(peers as u32);
                },
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    error!("Switch board disconnected;");
                    for (_id, entry) in self.switch.iter_mut() {
                        for d in entry.iter_mut() {
                            if d.is_stashed() {
                                d.set_departed();
                            }
                        }
                    }
                    break
                }
            }
        }
    }

    fn ensure(&mut self, target: WorkerId, channel: usize) -> &mut Dispatcher {
        let chs = self.switch.entry(target).or_insert(Vec::new());

        while chs.len() <= channel {
            chs.push(Dispatcher::new(target, chs.len(), -1, None));
        }
        &mut chs[channel]
    }
}

/// A `Deliveryman` receives messages from a `Read` (typically a tcp stream), and deliver them
/// to corresponding channel of corresponding worker.
pub struct Deliveryman {
    pub source: usize,
    switch_board: Switchboard,
    slab: BytesSlab,
}

impl Deliveryman {
    pub fn new(source: usize, recv: Receiver<(RecvMailBox, usize)>) -> Self {
        let switch_board = Switchboard::new(recv);
        Deliveryman {
            source,
            switch_board,
            slab: BytesSlab::new(20),
        }
    }

    pub fn deliver_to_end(&mut self, mut connection: Connection) -> ::std::io::Result<()> {
        info!("Start to repeatedly receive message from index {};", self.source);
        let switch_board = &mut self.switch_board;
        while !connection.is_poisoned() {
            switch_board.check_switch();
            self.slab.ensure_capacity(1);
            match connection.read(self.slab.empty()) {
                Err(e) => {
                    if e.kind() == ErrorKind::WouldBlock ||
                        e.kind() == ErrorKind::TimedOut || e.kind() == ErrorKind::Interrupted {
                        switch_board.clean_up();
                        continue
                    } else {
                        error!("get network error: {:?}", e);
                        connection.poisoned();
                        return Err(e);
                    }
                },
                Ok(read) => {
                    if read > 0 {
                        self.slab.make_valid(read);
                        while let Some(header) = MessageHeader::try_read(self.slab.valid()) {
                            trace!("receive message: {:?}", header);
                            let mut bytes = self.slab.extract(header.required_bytes());
                            if header.is_empty() {
                                if header == EMPTY_HEAD {
                                    // ignore， EMPTY_HEADER is a sniffing signal used for keep-alive in framework level;
                                } else {
                                    let source = WorkerId(header.task, header.source);
                                    let target = WorkerId(header.task, header.target);
                                    switch_board.clean(source, target, header.channel);
                                    //switch_board.clean_up();
                                }
                            } else {
                                bytes.extract_to(HEAD_SIZE);
                                switch_board.send_to(header, bytes);
                            }
                        }
                    } else {
                        // always Ok(0)
                        // as describe in : https://doc.rust-lang.org/std/net/enum.Shutdown.html ,
                        // read '0' may indicates broken of connection;
                        connection.poisoned();
                        return Err(::std::io::Error::new(ErrorKind::Other, "network is disconnected."));
                    }
                }
            }
        }
        info!("Stop to receive message from index {}", self.source);
        Ok(())
    }
}

use network_connection::ConnectManager;
use network_connection::Connection;

pub struct RemoteStreams {
    send_stream: ConnectManager,
    recv_stream: ConnectManager,
}

impl RemoteStreams {
    pub fn new(signal: &Arc<AtomicBool>) -> Self {
        RemoteStreams {
            send_stream: ConnectManager::new(signal),
            recv_stream: ConnectManager::new(signal),
        }
    }

    pub fn set_send(&self, connect: Connection) {
        self.send_stream.set_connection(connect);
    }

    #[inline]
    pub fn wait_send(&self) -> Option<Connection> {
        self.send_stream.wait_connection()
    }

    pub fn set_recv(&self, connect: Connection) {
        self.recv_stream.set_connection(connect);
    }

    #[inline]
    pub fn wait_recv(&self) -> Option<Connection> {
        self.recv_stream.wait_connection()
    }
}

pub struct TcpRemote {
    pub local: usize,
    pub remote: usize,
    broken_signal: Arc<AtomicBool>,
    smb_sx: RwLock<Option<Sender<Bytes>>>,
    rmb_sx: RwLock<Option<Sender<(RecvMailBox, usize)>>>,
    running_guard: Mutex<(Option<JoinHandle<()>>, Option<JoinHandle<()>>)>,
}

impl TcpRemote {
    pub fn new(local: usize, remote: usize) -> Self {
        TcpRemote {
            local,
            remote,
            broken_signal: Arc::new(AtomicBool::new(false)),
            smb_sx: RwLock::new(None),
            rmb_sx: RwLock::new(None),
            running_guard: Mutex::new((None, None)),
        }
    }

    pub fn start(&self, streams: &Arc<RemoteStreams>) {
        let remote = self.remote;
        {
            let (smb_sx, smb_rx) = crossbeam_channel::unbounded();
            self.smb_sx.write().expect("write lock poison").replace(smb_sx);
            let stream_guard = streams.clone();
            let signal = self.broken_signal.clone();
            let join = thread::Builder::new()
                .name(format!("network-{}-{}", self.local, self.remote))
                .spawn(move || {
                    let p = Postman::new(remote, smb_rx, &signal);
                    while let Some(connection) = stream_guard.wait_send() {
                        match p.send_to_end(connection) {
                            Ok(()) => {
                                signal.store(true, Ordering::Relaxed);
                                break
                            },
                            Err(e) => {
                                error!("network error: {:?}", e);
                            }
                        }
                    }
                }).unwrap();
            self.running_guard.lock().expect("lock poison").0.replace(join);
        }

        {
            let (rmb_sx, rmb_rx) = crossbeam_channel::unbounded();
            self.rmb_sx.write().expect("write lock poison").replace(rmb_sx);
            let stream_guard = streams.clone();
            let join = thread::Builder::new()
                .name(format!("network-{}-{}", self.remote, self.local))
                .spawn(move || {
                    let mut d = Deliveryman::new(remote, rmb_rx);
                    while let Some(connection) = stream_guard.wait_recv() {
                        connection.set_read_timeout(Some(Duration::from_millis(64)))
                            .expect("Unreachable: sed read timeout failure");
                        match d.deliver_to_end(connection) {
                            Ok(()) => break,
                            Err(e) => {
                                error!("network error {:?}", e);
                            }
                        }
                    }
                }).unwrap();
            self.running_guard.lock().expect("lock poison").1.replace(join);
        }
    }

    #[inline]
    pub fn get_smb_sx(&self) -> Option<Sender<Bytes>> {
        self.smb_sx.read().expect("read lock poison").as_ref()
            .map(|r| r.clone())
    }

    #[inline]
    pub fn get_rmb_sx(&self) -> Option<Sender<(RecvMailBox, usize)>> {
        self.rmb_sx.read().expect("read lock poison").as_ref()
            .map(|r| r.clone())
    }

    pub fn set_broken(&self) {
        self.broken_signal.store(true, Ordering::Relaxed);
    }

    #[inline]
    pub fn shutdown(&self) {
        self.smb_sx.write().expect("write lock poison").take();
        self.rmb_sx.write().expect("write lock poison").take();
    }

    pub fn await_termination(&self) {
        let mut lock = self.running_guard.lock().expect("running guard lock poison");
        if let Some(running) = lock.0.take() {
            running.join().unwrap();
        }
        if let Some(running) = lock.1.take() {
            running.join().unwrap();
        }
    }
}

/// For each process
pub struct PostOffice {
    pub index: usize,
    pub remote_size: usize,
    shutdown_signal: Arc<AtomicBool>,
    remotes: Vec<Option<(TcpRemote, Arc<RemoteStreams>)>>,
}

impl PostOffice {
    pub fn new(index: usize, remote_size: usize) -> Self {
        debug_assert!(remote_size > 1);
        debug_assert!(index < remote_size);
        let mut remotes = Vec::with_capacity(remote_size);
        let shutdown_signal = Arc::new(AtomicBool::new(false));
        for i in 0..remote_size {
            if index == i {
                remotes.push(None);
            } else {
                let connection = Arc::new(RemoteStreams::new(&shutdown_signal));
                let tcp_remote = TcpRemote::new(index, i);
                tcp_remote.start(&connection);
                remotes.push(Some((tcp_remote, connection)));
            }
        }
        PostOffice {
            index,
            remote_size,
            shutdown_signal,
            remotes,
        }
    }


    pub fn get_smb(&self, index: usize) -> Option<Sender<Bytes>> {
        self.remotes[index].as_ref().and_then(|r| r.0.get_smb_sx())
    }

    pub fn get_all_smbs(&self) -> Option<Vec<Sender<Bytes>>> {
        unimplemented!()
    }

    pub fn get_all_rmbs(&self) -> Option<Vec<Sender<(RecvMailBox, usize)>>> {
        unimplemented!()
    }

    pub fn get_rmb(&self, index: usize) -> Option<Sender<(RecvMailBox, usize)>> {
        self.remotes[index].as_ref().and_then(|r| r.0.get_rmb_sx())
    }

    pub fn shutdown(&self) {
        self.shutdown_signal.store(true, Ordering::Relaxed);
        self.remotes.iter().for_each(|r| {
            if let Some(re) = r {
                re.0.shutdown();
            }
        })
    }

    pub fn await_termination(&self) {
        self.remotes.iter().for_each(|r| {
            if let Some(r) = r {
                r.0.await_termination();
            }
        })
    }

    pub fn reconnect_all(&self, remotes: Vec<Connection>) {
        for connection in remotes.into_iter() {
            if let Some((_, ref connection_manager)) = self.remotes[connection.index] {
                connection_manager.set_send(connection.clone());
                connection_manager.set_recv(connection);
            }
        }
    }

    pub fn re_connect(&self, connection: Connection) {
        if let Some((_, ref connection_manager)) = self.remotes[connection.index] {
            connection_manager.set_send(connection.clone());
            connection_manager.set_recv(connection);
        }
    }
}

#[allow(dead_code)]
// This constant is sent along immediately after establishing a TCP stream, so
// that it is easy to sniff out Timely traffic when it is multiplexed with
// other traffic on the same port.
const HANDSHAKE_MAGIC: u64 = 0xc2f1fb770118ad9d;
const CONNECTION_INTERVAL_TIME: u64 = 10;

use std::thread::sleep;
use std::io::Error;

pub fn reconnect(self_index: usize, listener: TcpListener, start_addresses: Vec<(usize, String)>, await_address: Vec<(usize, String)>, retry_times: u64) -> ::std::io::Result<Vec<(usize, String, TcpStream)>> {
    let start_task = thread::spawn(move || start_connection(self_index, start_addresses, retry_times));
    let await_task = thread::spawn(move || await_connection(self_index, listener, await_address, retry_times));

    let mut result = vec![];
    match start_task.join() {
        Ok(Ok(sub_result)) => result.extend(sub_result.into_iter()),
        Ok(Err(e)) => return Err(e),
        Err(_e) => return Err(Error::new(ErrorKind::Other, "Join start connection failed. ")),
    };

    match await_task.join() {
        Ok(Ok(sub_result)) => result.extend(sub_result.into_iter()),
        Ok(Err(e)) => return Err(e),
        Err(_e) => return Err(Error::new(ErrorKind::Other, "Join await connection failed. ")),
    };

    return Ok(result);
}


pub fn register_tcp_listener() -> TcpListener {
    let default_addr = "0.0.0.0:0".to_string();
    let listener = TcpListener::bind(&default_addr).expect("bind tcp listener address failed");
    return listener;
}

pub fn start_connection(self_index: usize, remote_addresses: Vec<(usize, String)>, mut retry_times: u64) -> ::std::io::Result<Vec<(usize, String, TcpStream)>> {
    let mut result = Vec::with_capacity(remote_addresses.len());

    for (remote_index, remote_address) in remote_addresses {
        if remote_address.is_empty() {
            continue;
        }
        assert!(self_index > remote_index);
        loop {
            match TcpStream::connect(remote_address.as_str()) {
                Ok(mut tcp_stream) => {
                    tcp_stream.set_nodelay(true).map_err(|e| Error::new(e.kind(), format!("set_nodelay call failed, caused: {:?}", e)))?;
                    unsafe { abomonation::encode(&HANDSHAKE_MAGIC, &mut tcp_stream) }.map_err(|e| Error::new(e.kind(), format!("failed to encode/send handshake magic, caused: {:?}", e)))?;
                    unsafe { abomonation::encode(&(self_index as u64), &mut tcp_stream) }.map_err(|e| Error::new(e.kind(), format!("failed to encode/send worker index, caused: {:?}", e)))?;
                    println!("worker {} connect to worker {} success!!", self_index, remote_index);
                    result.push((remote_index, remote_address, tcp_stream));
                    break;
                },
                Err(error) => {
                    sleep(Duration::from_millis(CONNECTION_INTERVAL_TIME));
                    if retry_times == 0 {
                        eprintln!("worker {}: connecting to worker {} failed, caused by {}.", self_index, remote_index, error);
                        break;
                    } else {
                        retry_times -= 1;
                    }
                }
            }
        }
    }
    return Ok(result);
}

pub fn await_connection(self_index: usize, tcp_listener: TcpListener, await_address: Vec<(usize, String)>, mut retry_times: u64) -> ::std::io::Result<Vec<(usize, String, TcpStream)>> {

    if cfg!(target_os = "linux") {
        tcp_listener.set_nonblocking(true).map_err(|e| Error::new(e.kind(), format!("Tcp listener cannot set non-blocking: {:?}", e)))?;
    }

    let mut result = Vec::with_capacity(await_address.len());

    for _ in 0..await_address.len() {
        loop {
            match tcp_listener.accept() {
                Ok((mut tcp_stream, _socket_addr)) => {
                    tcp_stream.set_nodelay(true).map_err(|e| Error::new(e.kind(), format!("Stream set_nodelay call failed, caused: {:?}", e)))?;
                    let mut buffer = [0u8; 16];
                    tcp_stream.read_exact(&mut buffer).map_err(|e| Error::new(e.kind(), format!("failed to read worker index, caused: {:?}", e)))?;
                    let (magic, mut buffer) = unsafe { abomonation::decode::<u64>(&mut buffer) }.expect("failed to decode magic");
                    if magic != &HANDSHAKE_MAGIC {
                        let error = ::std::io::Error::new(::std::io::ErrorKind::InvalidData, "received incorrect timely handshake");
                        eprintln!("Worker {}: connected from other workers failed, caused by {}.", self_index, error);
                        continue;
                    }
                    let remote_index = unsafe { abomonation::decode::<u64>(&mut buffer) }
                        .ok_or(Error::new(ErrorKind::Other, "failed to decode worker index".to_owned()))
                        .map_err(|e| e )?.0.clone() as usize;
                    println!("worker {} connected by {} success!!!", self_index, remote_index);
                    assert!(self_index < remote_index);
                    result.push((remote_index, tcp_stream.peer_addr().unwrap().to_string(), tcp_stream));
                    break;
                },
                Err(error) => {
                    sleep(Duration::from_millis(CONNECTION_INTERVAL_TIME));
                    if retry_times == 0 {
                        eprintln!("Worker {}: connected from other workers failed, caused by: {}", self_index, error);
                        return Ok(result);
                    } else {
                        retry_times -= 1;
                    }
                }
            }
        }
    }
    return Ok(result);
}








