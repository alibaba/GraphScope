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

use crate::api::meta::OperatorMeta;
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::{Data, Tag};
use std::cell::RefCell;

/// Represents the input of a scope at runtime;
pub struct ScopeInput<D> {
    /// The id of scope this input belongs to;
    pub id: u32,
    /// Indicates if this is the last input of the scope;
    pub is_last: bool,
    /// The data will enter a scope;
    pub(crate) data: D,
}

impl<D> ScopeInput<D> {
    pub fn new(id: u32, is_last: bool, data: D) -> Self {
        ScopeInput { id, is_last, data }
    }

    pub(crate) fn take(self) -> D {
        self.data
    }
}

pub trait ScopeInputEmitter<D: Send>: Send + Clone {
    /// If a `ScopeInputEmitter` has no other peers(return `false`), it means that all the scopes
    /// this emitter emit data to are only visible to this emitter, these scopes' open or close states
    /// are managed by this emitter only. Other emitter in other worker peers are totally independent,
    /// if different emitter emit data into same scopes, or open or close same scopes will incur unexpected
    /// behaviors in this situation.
    fn has_peers(&self) -> bool {
        true
    }

    /// Compute which scope this data will enter, implement of this trait function will take the data and
    /// return a  optional [`ScopeInput`] value back; `None` represents no scope this data will enter;
    ///
    /// [`ScopeInput`]: struct.ScopeInput.html
    ///
    fn get_scope(&mut self, data: D) -> Option<ScopeInput<D>>;
}

impl<D: Send, F: FnMut(D) -> Option<ScopeInput<D>> + Send + Clone> ScopeInputEmitter<D> for F {
    fn get_scope(&mut self, data: D) -> Option<ScopeInput<D>> {
        (*self)(data)
    }
}

pub trait EnterScope<D: Data> {
    /// Enter new scope statically, which means all streaming input data will enter a same default scope,
    /// and the scope can be known before dataflow running;
    fn enter(&self) -> Result<Stream<D>, BuildJobError>;

    /// Enter new scopes dynamically, which means different streaming input data may enter different scopes.
    /// Which scope a data will enter can only be decided at runtime, depending on the data itself and the
    /// user defined program;
    ///
    /// User should define a function which take each streaming data as input, and return a [`ScopeInput`]
    /// which contains the id of scope the data expect to enter; The system will create a new scope context
    /// if it not exist, and feed the data into the scope's input; Error(kind=[`IllegalScopeInput`]) will
    /// be thrown if the scope is exist and has been closed(completed) already;
    ///
    /// [`ScopeInput`]: struct.ScopeInput.html
    /// [`IllegalScopeInput`]: ../errors/enum.ErrorKind.html
    ///
    fn dyn_enter<B, F>(&self, func: B) -> Result<Stream<D>, BuildJobError>
    where
        B: FnOnce(&OperatorMeta) -> F,
        F: ScopeInputEmitter<D> + 'static;
}

thread_local! {

    pub(crate) static CURRENT_SCOPE : RefCell<Option<Tag>> = RefCell::new(None);

    pub(crate) static EXTRA_COMPLETES: RefCell<Vec<Tag>> = RefCell::new(Vec::new());
}

#[inline]
pub fn complete(id: u32) {
    let complete = CURRENT_SCOPE.with(|cur| cur.borrow().as_ref().map(|tag| Tag::inherit(tag, id)));

    if let Some(complete) = complete {
        EXTRA_COMPLETES.with(|cpe| cpe.borrow_mut().push(complete));
    }
}
