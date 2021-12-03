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

mod schema;
mod data_type;
mod type_def;
mod relation;
mod prop_def;
mod test_util;
pub mod prelude;

use std::fmt::Debug;
use self::data_type::DataType;
use self::type_def::TypeDef;

pub type LabelId = u32;
pub type PropId = u32;

pub trait Schema: Debug + Send + Sync {
    fn get_prop_id(&self, name: &str) -> Option<PropId>;
    fn get_prop_type(&self, label: LabelId, prop_id: PropId) -> Option<&DataType>;
    fn get_prop_types(&self, prop_id: PropId) -> Option<Vec<&data_type::DataType>>;
    fn get_prop_name(&self, prop_id: PropId) -> Option<&str>;
    fn get_label_id(&self, name: &str) -> Option<LabelId>;
    fn get_label_name(&self, label: LabelId) -> Option<&str>;
    fn get_type_def(&self, label: LabelId) -> Option<&TypeDef>;
    fn get_type_defs(&self) -> Vec<&TypeDef>;
    fn get_version(&self) -> u32;
    fn get_partition_num(&self) -> u32;
    fn to_proto(&self) -> Vec<u8>;
}

