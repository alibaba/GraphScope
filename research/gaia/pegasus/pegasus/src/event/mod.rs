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

use crate::Tag;
use pegasus_common::codec::*;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EndOfStream {
    All,
    OneOf(u32),
}

/// Different kinds of events used to describe and control the tagged logic stream;
///
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum EventKind {
    /// Record how many messages was pushed into a stream;
    Pushed(usize),
    /// Indicates a stream was end(End Of Stream); Use a `usize` to specific the source of stream;
    EOS(EndOfStream),
    ///
    Discard(u32),
}

impl EventKind {
    pub fn end_of(source: u32) -> Self {
        EventKind::EOS(EndOfStream::OneOf(source))
    }

    pub fn end_all() -> Self {
        EventKind::EOS(EndOfStream::All)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Event {
    pub tag: Tag,
    pub ch: u32,
    pub kind: EventKind,
}

impl Event {
    pub fn new(tag: Tag, ch: u32, kind: EventKind) -> Self {
        Event { tag, ch, kind }
    }
}

impl Encode for Event {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.tag.write_to(writer)?;
        writer.write_u32(self.ch)?;
        writer.write_all(self.kind.as_bytes())
    }
}

impl Decode for Event {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Event> {
        let tag = Tag::read_from(reader)?;
        let ch = reader.read_u32()?;
        let mut bytes = [0u8; std::mem::size_of::<EventKind>()];
        reader.read_exact(&mut bytes[0..])?;
        let kind = unsafe { std::mem::transmute_copy(&bytes) };
        Ok(Event::new(tag, ch, kind))
    }
}

mod io;
mod manager;
mod receive;
mod send;
mod utils;

pub use io::{EventBus, EventEntrepot};
pub use manager::EventManager;
pub use receive::{ChannelRxState, Panel};
pub use send::ChannelTxState;
