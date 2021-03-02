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
use crate::{Data, Tag};

pub enum ResultSet<D> {
    Data(Vec<D>),
    End,
}

pub trait Sink<D: Data> {
    fn sink_by<B, F>(&self, construct: B) -> Result<(), BuildJobError>
    where
        B: FnOnce(&OperatorMeta) -> F,
        F: Fn(&Tag, ResultSet<D>) + Send + 'static;
}
