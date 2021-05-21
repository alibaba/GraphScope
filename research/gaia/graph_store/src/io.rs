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

use bincode::Result as BincodeResult;
use bincode::{deserialize_from, serialize_into};
use serde::{de, ser};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

#[inline(always)]
pub fn export<T, P>(obj: &T, path: P) -> BincodeResult<()>
where
    T: ser::Serialize,
    P: AsRef<Path>,
{
    let mut writer = BufWriter::new(File::create(path)?);

    serialize_into(&mut writer, &obj)
}

#[inline(always)]
pub fn import<T, P>(path: P) -> BincodeResult<T>
where
    T: de::DeserializeOwned,
    P: AsRef<Path>,
{
    let mut reader = BufReader::new(File::open(path)?);

    deserialize_from(&mut reader)
}
