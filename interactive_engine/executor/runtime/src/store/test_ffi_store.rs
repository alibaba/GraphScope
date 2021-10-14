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
    use super::*;
    use store::ffi::FFIGraphStore;
    use maxgraph_store::api::{GlobalGraphQuery, Vertex};


    #[test]
    fn test_ffi_get_all_vertices() {
        let store = FFIGraphStore::new(100);

        let label_list = vec![];
        let partition_list = vec![1];
        let mut vertex_list = store.get_all_vertices(0, &label_list, None, None, None, 0, &partition_list);

        let mut vid_list = vec![];
        let result_vid_list = vec![(123, 1), (123, 1), (123, 1)];
        let mut count = 0;
        while let Some(v) = vertex_list.next() {
            vid_list.push((v.get_id(), v.get_label_id()));
            count += 1;
            if count >= 3 {
                break;
            }
        }

        assert_eq!(result_vid_list, vid_list);
    }
}
