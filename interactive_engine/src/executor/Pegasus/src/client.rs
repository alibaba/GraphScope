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

use std::collections::HashSet;
use super::*;
use crossbeam_channel::Sender;

pub struct Client<T> {
    input_send: Sender<(u32, Option<T>)>,
    running: HashSet<u32>,
}

impl<T: Data> Client<T> {
    pub fn new(sx: Sender<(u32, Option<T>)>) -> Self {
        Client {
            input_send: sx,
            running: HashSet::new(),
        }
    }

    /// Create input session with id `id`;
    pub fn input(&mut self, id: u32) -> Result<InputSession<T>, String> {
        if !self.running.insert(id) {
            Err(format!("job created with session id {} is still in running", id))
        } else {
            Ok(InputSession::new(id, &self.input_send))
        }
    }
}

pub struct InputSession<D> {
    id: u32,
    send: Sender<(u32, Option<D>)>
}

impl<D: Data> InputSession<D> {
    pub fn new(id: u32, send: &Sender<(u32, Option<D>)>) -> Self {
        InputSession {
            id,
            send: send.clone(),
        }
    }

    pub fn give(&self, record: D) -> Result<(), D> {
        self.send.send((self.id, Some(record)))
            .map_err(|e| {
                let (_i, r) = e.0;
                r.unwrap()
            })
    }
}

impl<D> Drop for InputSession<D> {
    fn drop(&mut self) {
        loop {
            if let Ok(()) = self.send.send((self.id, None)) {
                break
            }
        }
    }
}
