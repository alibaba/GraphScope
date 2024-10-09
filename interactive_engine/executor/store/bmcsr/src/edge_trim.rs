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

use std::collections::HashSet;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::schema::CsrGraphSchema;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EdgeTrimJson {
    ie_enable: Vec<Vec<String>>,
    oe_enable: Vec<Vec<String>>,
}

impl EdgeTrimJson {
    pub fn get_enable_indexs(&self, schema: &CsrGraphSchema) -> (HashSet<usize>, HashSet<usize>) {
        let mut ie_index_set = HashSet::<usize>::new();
        let mut oe_index_set = HashSet::<usize>::new();

        let vertex_label_num = schema.vertex_type_to_id.len();
        let edge_label_num = schema.edge_type_to_id.len();

        for ie in &self.ie_enable {
            let src_label_name = ie.get(0).expect("src label name not found");
            let dst_label_name = ie.get(2).expect("dst label name not found");
            let edge_label_name = ie.get(1).expect("edge label name not found");
            let src_label_id = *schema
                .vertex_type_to_id
                .get(src_label_name)
                .expect("label id not found") as usize;
            let dst_label_id = *schema
                .vertex_type_to_id
                .get(dst_label_name)
                .expect("label id not found") as usize;
            let edge_label_id = *schema
                .edge_type_to_id
                .get(edge_label_name)
                .expect("label id not found") as usize;
            let index = src_label_id * vertex_label_num * edge_label_num
                + dst_label_id * edge_label_num
                + edge_label_id;
            ie_index_set.insert(index);
        }

        for oe in &self.oe_enable {
            let src_label_name = oe.get(0).expect("src label name not found");
            let dst_label_name = oe.get(2).expect("dst label name not found");
            let edge_label_name = oe.get(1).expect("edge label name not found");
            let src_label_id = *schema
                .vertex_type_to_id
                .get(src_label_name)
                .expect("label id not found") as usize;
            let dst_label_id = *schema
                .vertex_type_to_id
                .get(dst_label_name)
                .expect("label id not found") as usize;
            let edge_label_id = *schema
                .edge_type_to_id
                .get(edge_label_name)
                .expect("label id not found") as usize;
            let index = src_label_id * vertex_label_num * edge_label_num
                + dst_label_id * edge_label_num
                + edge_label_id;
            oe_index_set.insert(index);
        }

        (ie_index_set, oe_index_set)
    }
}
