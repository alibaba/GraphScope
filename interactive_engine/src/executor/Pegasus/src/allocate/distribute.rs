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

use std::marker::PhantomData;
use super::*;
use crate::allocate::Process;
use crate::network::{RecvMailBox, MessageHeader, PostOffice};
use crossbeam_channel::{Sender, Receiver, TryRecvError};
use crate::common::{Bytes, SLAB};

/// Push-Pll Pair across network:
/// Push -- gateway -- TCP/xxx -- gateway -- Pull
pub struct PusherOnMergeQueue<D> {
    header: MessageHeader,
    sender: Sender<Bytes>,
    closed: bool,
    _ph: PhantomData<D>,
}

impl<D> PusherOnMergeQueue<D> {
    pub fn new(id: AllocateId, target: usize, sender: &Sender<Bytes>) -> Self {
        let header = MessageHeader {
            task: id.0,
            channel: id.2,
            source: id.1,
            target,
            length: 0,
            seqno: 0,
        };

        PusherOnMergeQueue {
            header,
            sender: sender.clone(),
            closed: false,
            _ph: PhantomData,
        }
    }

    #[inline]
    fn destroy(&mut self) -> Result<(), IOError> {
        if !self.closed {
            let header = self.header.new_empty();
            send_binary::<()>(header, None, &self.sender);
            self.closed = true;
        }
        Ok(())
    }
}

#[inline]
fn send_binary<D: Data>(header: MessageHeader, msg: Option<D>, sender: &Sender<Bytes>) {
    SLAB.with(|slab| {
        let mut slab = slab.borrow_mut();
        let capacity = header.required_bytes();
        if slab.empty().len() < capacity {
            if let Some(bytes) = slab.extract_valid() {
                sender.send(bytes).expect("send failure, disconnected");
            }
            slab.ensure_capacity(capacity);
        };
        let mut bytes = slab.empty();
        debug_assert!(bytes.len() >= capacity);
        // TODO: Handle write header failure;
        header.write_to(&mut bytes).expect("write message header failure");
        // TODO: Handle Serialize message failure
        if let Some(msg) = msg {
            bincode::serialize_into(bytes, &msg).expect("serialize message failure");
        }
        slab.make_valid(capacity);
        if let Some(msg) = slab.extract_valid() {
            sender.send(msg).expect("send failure, disconnected");
        }
    });
}

impl<D: Data> Push<D> for PusherOnMergeQueue<D> {
    fn push(&mut self, msg: D) -> Result<(), IOError> {
        if log_enabled!(log::Level::Debug) {
            debug!("PusherOnMergeQueue-[{}] start to push msg to {}/{}", self.header.source,
                   self.header.target, self.header.channel);
        }

        self.header.seqno += 1;
        let mut header = self.header;
        // TODO : Handle serialize failure;
        let length = bincode::serialized_size(&msg).expect("measure serialized size error");
        debug_assert!(length > 0);
        header.length = length as usize;
        send_binary(header, Some(msg), &self.sender);
        Ok(())
    }

    #[inline]
    fn close(&mut self) -> Result<(), IOError> {
        self.destroy()
    }
}

impl<D> Drop for PusherOnMergeQueue<D> {
    fn drop(&mut self) {
        if let Err(e) = self.destroy() {
            error!("drop PusherOnMergeQueue failure, caused by {:?}", e);
        }
    }
}

pub struct RecvMailBoxPull<D> {
    id: (WorkerId, ChannelId),
    receiver: Receiver<Bytes>,
    disconnected: bool,
    _ph: PhantomData<D>,
}

impl<D> RecvMailBoxPull<D> {
    pub fn new(worker: WorkerId, ch: ChannelId, receiver: Receiver<Bytes>) -> Self {
        RecvMailBoxPull {
            id: (worker, ch),
            receiver,
            disconnected: false,
            _ph: PhantomData,
        }
    }

    #[inline]
    pub fn is_disconnected(&self) -> bool {
        self.disconnected
    }
}

impl<D: Data> Pull<D> for RecvMailBoxPull<D> {
    fn pull(&mut self) -> Result<Option<D>, IOError> {
        if !self.disconnected {
            match self.receiver.try_recv() {
                Ok(bytes) => {
                    let msg = bincode::deserialize(&bytes[..]).expect("deserialize error");
                    Ok(Some(msg))
                },
                Err(TryRecvError::Empty) => {
                    Ok(None)
                },
                Err(TryRecvError::Disconnected) => {
                    self.disconnected = true;
                    warn!("RecvMailBox {}-{} disconnected", self.id.0, self.id.1);
                    Ok(None)
                }
            }
        } else {
            Err(IOError::BrokenPipe("disconnected".into()))
        }
    }
}

#[allow(dead_code)]
pub struct CombinedDataPull<D> {
    index: ChannelId,
    local: Box<dyn Pull<D>>,
    remote: RecvMailBoxPull<D>,
}

impl<D> CombinedDataPull<D> {
    pub fn new(index: ChannelId, local: Box<dyn Pull<D>>, remote: RecvMailBoxPull<D>) -> Self {
        CombinedDataPull {
            index,
            local,
            remote,
        }
    }
}

impl<D: Data> Pull<D> for CombinedDataPull<D> {
    fn pull(&mut self) -> Result<Option<D>, IOError> {
        match self.local.pull() {
            Ok(Some(data)) => {
                return Ok(Some(data))
            },
            _ => {}
        }

        if self.remote.is_disconnected() {
            Ok(None)
        } else {
            self.remote.pull()
        }
    }
}

pub struct Distribute {
    peers: usize,
    index: usize,
    local: Process,
    network: Arc<PostOffice>,
}

impl Distribute {
    pub fn new(index: usize, peers: usize) -> Self {
        Distribute {
            peers,
            index,
            local: Process::new(),
            network: Arc::new(PostOffice::new(index, peers)),
        }
    }

    pub fn setup_network(&self, connections: Vec<crate::network_connection::Connection>) {
        self.network.reconnect_all(connections);
    }

    pub fn get_network(&self) -> Arc<PostOffice> {
        self.network.clone()
    }
}

impl RuntimeEnv for Distribute {
    fn peers(&self) -> usize {
        self.peers
    }

    fn index(&self) -> usize {
        self.index
    }

    fn allocate<T: Data>(&self, id: AllocateId, peers: ParallelConfig) -> Option<(Vec<Box<dyn Push<T>>>, Box<dyn Pull<T>>)> {
        if peers.processes == 1 {
            self.local.allocate(id, peers)
        } else if self.index < peers.processes {
            debug_assert!(peers.processes <= self.peers);
            let (sx, rx) = crossbeam_channel::unbounded();
            let rmb = RecvMailBox::new(id.get_worker_id(), id.get_channel_id(), sx);
            let remote_pull = RecvMailBoxPull::new(id.get_worker_id(), id.get_channel_id(), rx);
            let mut pushes = Vec::new();
            let mut inner_pull = None;
            for i in 0..peers.processes {
                if i != self.index {
                    // its a safe unwrap
                    let smb = self.network.get_smb(i).expect("smb not found;");
                    let rmb_sx = self.network.get_rmb(i).expect("rmb not found");
                    // TODO:
                    rmb_sx.send((rmb.clone(), peers.workers)).expect("register rmb failure");
                    // some workers on process i
                    for j in 0..peers.workers {
                        let target = i * peers.workers + j;
                        let p = PusherOnMergeQueue::new(id, target, &smb);
                        pushes.push(Box::new(p) as Box<dyn Push<T>>);
                    }
                } else {
                    // some workers in current process;
                    let pl = ParallelConfig::new(1, peers.workers);
                    let id = AllocateId(id.0, id.1 - self.index * peers.workers, id.2);
                    // TODO
                    let (ps, pull) = self.local.allocate(id, pl).expect("local allocate failure");
                    for p in ps {
                        pushes.push(p);
                    }
                    inner_pull.replace(pull);
                }
            }

            debug_assert!(inner_pull.is_some());
            let inner_pull = inner_pull.expect("inner pull lost");
            let pull = CombinedDataPull::new(id.get_channel_id(), inner_pull, remote_pull);
            let pull = Box::new(pull) as Box<dyn Pull<T>>;
            Some((pushes, pull))
        } else {
            None
        }
    }

    fn shutdown(&self) {
        self.network.shutdown();
    }

    fn await_termination(&self) {
        self.network.await_termination();
    }
}

#[cfg(test)]
mod test {
    use crate::allocate::{Distribute, RuntimeEnv, AllocateId, ParallelConfig};

    #[test]
    fn test_allocate() {
        let d0 = Distribute::new(0, 3);
        d0.allocate::<u64>(AllocateId(0, 0, 0), ParallelConfig::new(3, 3));
        d0.allocate::<u64>(AllocateId(0, 1, 0), ParallelConfig::new(3, 3));
        d0.allocate::<u64>(AllocateId(0, 2, 0), ParallelConfig::new(3, 3));
        let d1 = Distribute::new(1, 3);
        d1.allocate::<u64>(AllocateId(0, 3, 0), ParallelConfig::new(3, 3));
        d1.allocate::<u64>(AllocateId(0, 4, 0), ParallelConfig::new(3, 3));
        d1.allocate::<u64>(AllocateId(0, 5, 0), ParallelConfig::new(3, 3));
        let d2 = Distribute::new(2, 3);
        d2.allocate::<u64>(AllocateId(0, 6, 0), ParallelConfig::new(3, 3));
        d2.allocate::<u64>(AllocateId(0, 7, 0), ParallelConfig::new(3, 3));
        d2.allocate::<u64>(AllocateId(0, 8, 0), ParallelConfig::new(3, 3));
    }
}



















