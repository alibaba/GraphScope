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

pub use concise::correlate::subtask::*;
pub use concise::correlate::Tumbling;
pub use concise::*;
pub use iteration::{IterCondition, Iteration};
pub use primitive::binary::Binary;
pub use primitive::branch::Branch;
pub use primitive::sink::{FromStream, Sink};
pub use primitive::source::{IntoDataflow, Source};
pub use primitive::unary::Unary;

use crate::data::DataSet;
use crate::progress::EndSignal;
use crate::Tag;

pub enum Res<D> {
    Data(D),
    Halt,
}

pub enum BranchEnum {
    Left,
    Right,
}

pub enum ResultSet<'a, D> {
    Data(&'a mut DataSet<D>),
    End,
}

/// Represents a notification which always indicates that data of a scope in the input stream
/// has exhaust;
#[derive(Clone)]
pub struct Notification {
    /// The port of operator's input this notification belongs to;
    pub port: usize,
    /// The end signal of the scope this notification belongs to;
    end: EndSignal,
}

impl Notification {
    pub fn new(port: usize, end: EndSignal) -> Self {
        Notification { port, end }
    }

    pub fn tag(&self) -> &Tag {
        &self.end.tag
    }

    pub fn take_end(self) -> EndSignal {
        self.end
    }
}

pub(crate) mod concise;
pub mod error;
pub mod function;
pub(crate) mod iteration;
pub mod meta;
pub(crate) mod primitive;
pub(crate) mod scope;
