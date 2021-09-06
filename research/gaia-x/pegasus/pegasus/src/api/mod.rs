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

pub use concise::*;
pub use iteration::{IterCondition, Iteration};
pub use primitive::binary::Binary;
pub use primitive::branch::Branch;
pub use primitive::sink::{FromStream, Sink};
pub use primitive::source::{IntoDataflow, Source};
pub use primitive::unary::Unary;

pub mod notification {

    use crate::progress::Weight;
    use crate::Tag;

    #[derive(Debug, Clone)]
    pub struct EndScope {
        pub(crate) port: usize,
        pub(crate) tag: Tag,
        pub(crate) weight: Weight,
    }

    #[derive(Debug, Clone)]
    pub struct CancelScope {
        pub(crate) port: usize,
        pub(crate) tag: Tag,
    }

    impl EndScope {
        pub fn tag(&self) -> &Tag {
            &self.tag
        }

        pub fn port(&self) -> usize {
            self.port
        }
    }

    impl CancelScope {
        pub fn tag(&self) -> &Tag {
            &self.tag
        }

        pub fn port(&self) -> usize {
            self.port
        }
    }
}

pub(crate) mod concise;
pub mod error;
pub mod function;
pub(crate) mod iteration;
pub mod meta;
pub(crate) mod primitive;
pub(crate) mod scope;
