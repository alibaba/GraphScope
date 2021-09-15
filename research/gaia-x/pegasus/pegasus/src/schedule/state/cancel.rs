// //
// //! Copyright 2020 Alibaba Group Holding Limited.
// //!
// //! Licensed under the Apache License, Version 2.0 (the "License");
// //! you may not use this file except in compliance with the License.
// //! You may obtain a copy of the License at
// //!
// //! http://www.apache.org/licenses/LICENSE-2.0
// //!
// //! Unless required by applicable law or agreed to in writing, software
// //! distributed under the License is distributed on an "AS IS" BASIS,
// //! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// //! See the License for the specific language governing permissions and
// //! limitations under the License.
//
// use std::collections::HashSet;
//
// use crate::tag::tools::map::TidyTagMap;
// use crate::Tag;
//
// pub struct CancelPolicy {
//     scope_level: u32,
//     port_peers: usize,
//     received: Vec<TidyTagMap<HashSet<u32>>>,
// }
//
// impl CancelPolicy {
//     pub fn new(scope_level: u32, peers: usize) -> Self {
//         let received = (0..scope_level + 1)
//             .map(|i| TidyTagMap::new(i))
//             .collect();
//         CancelPolicy { scope_level, port_peers: peers, received }
//     }
//
//     pub fn on_cancel(&mut self, src: u32, cancel_scope: Tag) -> Option<Tag> {
//         let idx = cancel_scope.len();
//         assert!(idx <= self.scope_level as usize);
//         if self.port_peers != 1 {
//             if let Some(mut worker_set) = self.received[idx].remove(&cancel_scope) {
//                 worker_set.insert(src);
//                 if worker_set.len() == self.port_peers {
//                     Some(cancel_scope)
//                 } else {
//                     self.received[idx].insert(cancel_scope, worker_set);
//                     None
//                 }
//             } else {
//                 let mut worker_set = HashSet::new();
//                 worker_set.insert(src);
//                 self.received[idx].insert(cancel_scope, worker_set);
//                 None
//             }
//         } else {
//             Some(cancel_scope)
//         }
//     }
// }
