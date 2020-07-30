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

pub mod mock_tunnel;

pub use self::mock_tunnel as tunnel;

use lru_time_cache::LruCache;
use std::sync::Mutex;

lazy_static! {
        pub static ref IDS_CACHE: Mutex<LruCache<String, Vec<i64>>> = {
            let time_to_live = ::std::time::Duration::from_secs(3600);
            Mutex::new(LruCache::<String, Vec<i64>>::with_expiry_duration_and_capacity(time_to_live, 10))
        };
    }

pub fn remove_odps_id_cache(key: &String) {
    let mut id_cache = IDS_CACHE.lock().unwrap();
    id_cache.remove(key);
}
