//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

pub mod async_maxgraph_service;
pub mod ctrl_service;
pub mod maxgraph_service;

pub fn generate_task_id(frontend_query_id: String) -> usize {
    let query_id = if frontend_query_id.chars().nth(0).unwrap() == '-' {
        &frontend_query_id[1..frontend_query_id.len()]
    } else {
        &frontend_query_id
    };

    let task_id = query_id.to_owned().parse::<usize>().expect("parser query id failed.");
    info!("frontend query id: {}, self query id: {}", frontend_query_id, task_id);
    task_id
}
