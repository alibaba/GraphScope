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

#[cfg(test)]
mod tests {
    use dataflow::store::cache::CacheStore;
    use dataflow::message::{PropertyEntity, ValuePayload};

    #[test]
    fn test_cache_store_all() {
        let cache_store = CacheStore::new(&vec![0, 4, 2]);
        assert!(cache_store.get_partition(1).is_none());

        let partition = cache_store.get_partition(2);
        assert!(partition.is_some());
        let p = partition.unwrap();
        let prop_id = 1;
        let value = 3_i32;
        {
            let pe = PropertyEntity::new(prop_id, ValuePayload::Int(value));
            p.write(0, 1, pe);
        }
        {
            let prop = p.get(0, 1, 1);
            assert!(prop.is_some());
            let pe = prop.unwrap();
            assert_eq!(prop_id, pe.get_propid());
            assert_eq!(value, pe.get_value().get_int().unwrap());
        }
    }
}
