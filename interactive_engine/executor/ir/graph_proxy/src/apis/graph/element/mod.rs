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

use dyn_type::{BorrowObject, Object};
pub use edge::Edge;
use ir_common::LabelId;
pub use path::GraphPath;
pub use path::VertexOrEdge;
pub use property::{Details, DynDetails, PropKey, PropertyValue};
pub use vertex::Vertex;

use crate::apis::ID;

mod edge;
mod path;
mod property;
mod vertex;

/// An `Element` is an abstraction of the data filed in an IR `Record`.
pub trait Element {
    /// Try to turn the `Element` into a `GraphElement`,
    /// `None` by default, if it is not a `GraphElement`
    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        None
    }
    /// The length of the `Element`
    fn len(&self) -> usize;
    /// Turn the `Element` into a `BorrowObject`.
    fn as_borrow_object(&self) -> BorrowObject;
}

/// `GraphElement` is a special `Element` with extra properties of `id` and `label`.
pub trait GraphElement: Element {
    fn id(&self) -> ID;
    fn label(&self) -> Option<LabelId>;
    /// To obtain the data maintained by the graph_element, mostly is a hash-table with key-value mappings,
    /// `None` by default, if there is no data.
    fn details(&self) -> Option<&DynDetails> {
        None
    }
}

impl Element for () {
    fn len(&self) -> usize {
        0
    }

    fn as_borrow_object(&self) -> BorrowObject {
        BorrowObject::None
    }
}

impl Element for Object {
    fn len(&self) -> usize {
        match self {
            Object::None => 0,
            Object::Vector(v) => v.len(),
            Object::KV(kv) => kv.len(),
            _ => 1,
        }
    }

    fn as_borrow_object(&self) -> BorrowObject {
        self.as_borrow()
    }
}

impl<'a> Element for BorrowObject<'a> {
    fn len(&self) -> usize {
        match self {
            BorrowObject::None => 0,
            BorrowObject::Vector(v) => v.len(),
            BorrowObject::KV(kv) => kv.len(),
            _ => 1,
        }
    }

    fn as_borrow_object(&self) -> BorrowObject<'a> {
        *self
    }
}
