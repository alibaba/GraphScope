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

use crate::communication::decorator::count::CountedPush;
use crate::communication::decorator::exchange::ExchangePush;
use crate::data::{Data, DataSet};
use crate::data_plane::Push;
use crate::errors::IOError;

pub mod count;
pub mod exchange;

pub enum DataPush<T: Data> {
    Count(CountedPush<T>),
    Exchange(ExchangePush<T>),
}

impl<T: Data> Push<DataSet<T>> for DataPush<T> {
    fn push(&mut self, msg: DataSet<T>) -> Result<(), IOError> {
        match self {
            DataPush::Count(push) => push.push(msg),
            DataPush::Exchange(push) => push.push(msg),
        }
    }

    fn check_failure(&mut self) -> Option<DataSet<T>> {
        match self {
            DataPush::Count(push) => push.check_failure(),
            DataPush::Exchange(push) => push.check_failure(),
        }
    }

    fn flush(&mut self) -> Result<(), IOError> {
        match self {
            DataPush::Count(push) => push.flush(),
            DataPush::Exchange(push) => push.flush(),
        }
    }

    fn close(&mut self) -> Result<(), IOError> {
        match self {
            DataPush::Count(push) => push.close(),
            DataPush::Exchange(push) => push.close(),
        }
    }
}
