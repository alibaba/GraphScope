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

use std::collections::HashMap;
use std::any::Any;
use std::sync::{Arc, Mutex};
use super::*;
use crossbeam_channel::{Receiver, Sender, TryRecvError, SendError};


pub struct Process {
    channels: Arc<Mutex<HashMap<(usize, usize), Box<dyn Any + Send>>>>,
}

impl Process {
    pub fn new() -> Self {
        Process {
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl RuntimeEnv for Process {

    #[inline]
    fn peers(&self) -> usize { 1 }

    #[inline]
    fn index(&self) -> usize { 0 }

    fn allocate<T: Data>(&self, id: AllocateId, peers: ParallelConfig) -> Option<(Vec<Box<dyn Push<T>>>, Box<dyn Pull<T>>)> {
        if peers.processes == 1 {
            if peers.workers == 1 {
                let (ps, pr) = Thread::pipeline::<T>();
                Some((vec![Box::new(ps) as Box<dyn Push<T>>], Box::new(pr) as Box<dyn Pull<T>>))
            } else {
                let mut locked = self.channels.lock().expect("lock poisoned");
                let (pushes, pull , tmp) = {
                    let entry = locked.entry((id.0, id.2)).or_insert_with(|| {
                        let mut pushes = Vec::with_capacity(peers.workers);
                        let mut pulles = Vec::with_capacity(peers.workers);
                        for _i in 0..peers.workers {
                            let (sx, rx): (Sender<T>, Receiver<T>) = crossbeam_channel::unbounded();
                            pushes.push(sx);
                            pulles.push(rx);
                        }

                        let mut to_box = Vec::new();
                        for pull in pulles {
                            to_box.push(Some((pushes.clone(), pull)));
                        }
                        Box::new(to_box)
                    });

                    let vector = entry.downcast_mut::<Vec<Option<(Vec<Sender<T>>, Receiver<T>)>>>().expect("cast error.");
                    let (pushes, pull): (Vec<Sender<T>>, Receiver<T>) = vector[id.1].take().expect("channel missed");
                    let pushes = pushes.into_iter().map(|p| {
                        Box::new(ProcessPush::new(p)) as Box<dyn Push<T>>
                    }).collect::<Vec<_>>();

                    let pull = ProcessPull::new(id, pull);
                    let tmp = vector.iter().all(|opt| opt.is_none());
                    (pushes, pull, tmp)
                };
                if tmp {
                    locked.remove(&(id.0, id.2));
                }
                Some((pushes, Box::new(pull)))
            }
        } else {  None }
    }

    fn shutdown(&self) { () }

    fn await_termination(&self) { () }
}

pub struct ProcessPush<T> {
    sender: Sender<T>,
}

impl<T> ProcessPush<T> {
    pub fn new(sender: Sender<T>) -> Self {
        ProcessPush {
            sender,
        }
    }
}

impl<T> Clone for ProcessPush<T> {
    fn clone(&self) -> Self {
        ProcessPush {
            sender: self.sender.clone(),
        }
    }
}

impl<T: Send + Debug> Push<T> for ProcessPush<T> {
    #[inline]
    fn push(&mut self, msg: T) -> Result<(), IOError> {
        match self.sender.send(msg) {
            Err(SendError(msg)) => {
                Err(IOError::BrokenPipe(format!("error when ProcessPush send: {:?}", msg)))
            },
            _ => Ok(())
        }
    }

    #[inline]
    fn close(&mut self) -> Result<(), IOError> {
        Ok(())
    }
}

pub struct ProcessPull<T> {
    index: AllocateId,
    recv: Receiver<T>,
}

impl<T> ProcessPull<T> {
    pub fn new(index: AllocateId, recver: Receiver<T>) -> Self {
        ProcessPull {
            index,
            recv: recver,
        }
    }
}

impl<T: Send> Pull<T> for ProcessPull<T> {
    #[inline]
    fn pull(&mut self) -> Result<Option<T>, IOError> {
        match self.recv.try_recv() {
            Ok(data) => Ok(Some(data)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => {
                debug!("channel {:?} disconnected", self.index);
                Ok(None)
            }
        }
    }
}
