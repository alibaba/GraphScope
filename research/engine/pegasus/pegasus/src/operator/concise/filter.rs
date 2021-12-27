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

use crate::api::function::FnResult;
use crate::api::Filter;
use crate::api::Map;
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;

impl<D: Data> Filter<D> for Stream<D> {
    fn filter<F>(self, func: F) -> Result<Stream<D>, BuildJobError>
    where
        F: Fn(&D) -> FnResult<bool> + Send + 'static,
    {
        self.filter_map(move |item| if (func)(&item)? { Ok(Some(item)) } else { Ok(None) })
    }
}
