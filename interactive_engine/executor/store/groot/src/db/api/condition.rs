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

use crate::db::api::types::PropertyValue;
use crate::db::api::PropertyId;

#[allow(dead_code)]
pub enum Condition {
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
    LessThan(PropertyId, PropertyValue),
    LessEqual(PropertyId, PropertyValue),
    GreaterThan(PropertyId, PropertyValue),
    GreaterEqual(PropertyId, PropertyValue),
    Equal(PropertyId, PropertyValue),
    NotEqual(PropertyId, PropertyValue),
}
