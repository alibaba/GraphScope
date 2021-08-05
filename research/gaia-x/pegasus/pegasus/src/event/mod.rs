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

use crate::graph::Port;
use crate::progress::EndSignal;
use crate::Tag;
use pegasus_common::codec::*;

#[derive(Debug, Clone)]
pub struct Event {
    pub from_worker: u32,
    pub target_port: Port,
    signal: Signal,
}

impl Event {
    pub fn new(worker: u32, target: Port, signal: Signal) -> Self {
        Event { from_worker: worker, target_port: target, signal }
    }

    pub fn take_signal(self) -> Signal {
        self.signal
    }
}

impl Encode for Event {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u32(self.from_worker)?;
        writer.write_u32(self.target_port.index as u32)?;
        writer.write_u32(self.target_port.port as u32)?;
        match &self.signal {
            Signal::EndSignal(end) => {
                writer.write_u8(0)?;
                end.write_to(writer)?;
            }
            Signal::CancelSignal(tag) => {
                writer.write_u8(1)?;
                tag.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for Event {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Event> {
        let from_worker = reader.read_u32()?;
        let index = reader.read_u32()? as usize;
        let port = reader.read_u32()? as usize;
        let target_port = Port::new(index, port);
        let e = reader.read_u8()?;
        let signal = match e {
            0 => {
                let end = EndSignal::read_from(reader)?;
                Signal::EndSignal(end)
            }
            1 => {
                let tag = Tag::read_from(reader)?;
                Signal::CancelSignal(tag)
            }
            _ => unreachable!(),
        };
        Ok(Event { from_worker, target_port, signal })
    }
}

#[derive(Debug, Clone)]
pub enum Signal {
    EndSignal(EndSignal),
    CancelSignal(Tag),
}

pub mod emitter;
