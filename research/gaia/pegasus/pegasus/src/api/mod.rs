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

pub(crate) mod concise;
#[macro_use]
pub mod function;
pub(crate) mod iteration;
pub mod meta;
pub(crate) mod multiplex;
pub mod notify;
pub(crate) mod primitive;
pub(crate) mod scope;
pub mod state;

pub use concise::dedup::Dedup;
pub use concise::exchange::Exchange;
pub use concise::filter::Filter;
pub use concise::fold::Fold;
pub use concise::map::Map;
pub use concise::reduce::*;
pub use iteration::{Iteration, LoopCondition};
pub use multiplex::subtask::{SubTask, SubtaskResult};
pub use multiplex::Multiplexing;
pub use primitive::binary::{Binary, BinaryInput, BinaryNotification, BinaryNotify, BinaryState};
pub use primitive::branch::{Branch, Condition, IntoBranch};
pub use primitive::sink::{ResultSet, Sink};
pub use primitive::source::{ExternSource, FromStream, IntoStream, NonBlockReceiver};
pub use primitive::unary::{LazyUnary, Unary, UnaryNotify, UnaryState};
pub use scope::enter::complete;
pub use scope::enter::{EnterScope, ScopeInput, ScopeInputEmitter};
pub use scope::leave::LeaveScope;
