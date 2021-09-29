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

use crate::object::RawType;
use std::fmt;

#[derive(Debug, PartialEq)]
pub struct CastError {
    pub kind: RawType,
    target: &'static str,
}

impl CastError {
    pub fn new<T>(kind: RawType) -> Self {
        let target = std::any::type_name::<T>();
        CastError { kind, target }
    }
}

impl fmt::Display for CastError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.kind {
            RawType::Byte => write!(f, "can't cast i8 into {}", self.target),
            RawType::Integer => write!(f, "can't cast i32 into {}", self.target),
            RawType::Long => write!(f, "can't cast i64 into {}", self.target),
            RawType::ULLong => write!(f, "can't cast u128 into {}", self.target),
            RawType::Float => write!(f, "can't cast f64 into {}", self.target),
            RawType::Blob(len) => write!(f, "can't cast Blob({}) into {}", len, self.target),
            RawType::String => write!(f, "can't cast String into {}", self.target),
            RawType::Unknown => write!(f, "can't cast unknown dyn type into {}", self.target),
        }
    }
}

impl std::error::Error for CastError {}
