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
// use crate::Tag;
// use pegasus_common::rc::RcPointer;
// use std::cell::Cell;
// use std::collections::HashMap;
//
// pub struct Entry {
//     tag: Tag,
//     active: Cell<bool>,
//     blocked: Cell<i64>,
//     parent: Option<RcPointer<Entry>>,
//     is_dirty: RcPointer<Cell<bool>>,
// }
//
// impl Entry {
//     pub fn new(tag: Tag, active: bool, is_dirty: RcPointer<Cell<bool>>) -> Self {
//         Entry { tag, active: Cell::new(active), blocked: Cell::new(0), parent: None, is_dirty }
//     }
//
//     pub fn with(
//         tag: Tag, active: bool, block: bool, parent: Option<RcPointer<Entry>>,
//         is_dirty: RcPointer<Cell<bool>>,
//     ) -> Self {
//         Entry { tag, active: Cell::new(active), blocked: Cell::new(block as i64), parent, is_dirty }
//     }
//
//     #[inline]
//     pub fn set_active(&self) {
//         self.active.set(true);
//     }
//
//     #[inline]
//     pub fn incr_block(&self, size: i64) {
//         let pre = self.blocked.get();
//         let v = pre.checked_add(size).expect("incr block failure");
//         if pre <= 0 && v > 0 {
//             if let Some(ref parent) = self.parent {
//                 parent.incr_block(1);
//             }
//         }
//         self.blocked.set(v);
//     }
//
//     #[inline]
//     pub fn decr_block(&self, size: i64) {
//         let pre = self.blocked.get();
//         let v = pre.checked_sub(size).expect("decr block failure");
//         if pre > 0 && v <= 0 {
//             if let Some(ref parent) = self.parent {
//                 parent.decr_block(1);
//             }
//             if self.get_active().is_some() {
//                 self.is_dirty.set(true);
//             }
//         }
//         self.blocked.set(v);
//     }
//
//     #[inline]
//     pub fn clear_block(&self) {
//         let current = self.blocked.get();
//         if current > 0 {
//             self.blocked.set(0);
//             self.is_dirty.set(true);
//             if let Some(ref parent) = self.parent {
//                 parent.decr_block(1);
//             }
//         }
//     }
//
//     #[inline]
//     pub fn is_active(&self) -> bool {
//         self.active.get()
//     }
//
//     #[inline]
//     pub fn is_blocked(&self) -> bool {
//         self.blocked.get() > 0
//     }
//
//     #[inline]
//     pub fn get_active(&self) -> Option<Tag> {
//         if self.active.get() {
//             Some(self.tag.clone())
//         } else if let Some(ref parent) = self.parent {
//             parent.get_active()
//         } else {
//             None
//         }
//     }
//
//     // #[inline]
//     // fn block_size(&self) -> i64 {
//     //     self.blocked.get()
//     // }
// }
//
// pub struct BlockGuard {
//     inner: RcPointer<Entry>,
// }
//
// impl BlockGuard {
//     pub fn new(entry: RcPointer<Entry>) -> Self {
//         BlockGuard { inner: entry }
//     }
//
//     #[inline]
//     pub fn incr(&self, size: u32) {
//         self.inner.incr_block(size as i64)
//     }
//
//     #[inline]
//     pub fn decr(&self, size: u32) {
//         self.inner.decr_block(size as i64)
//     }
//
//     #[inline]
//     pub fn clear(&self) {
//         self.inner.clear_block()
//     }
//
//     #[inline]
//     pub fn get(&self) -> Option<Tag> {
//         self.inner.get_active()
//     }
// }
//
// pub struct TagAntiChainSet {
//     chain: Vec<HashMap<Tag, RcPointer<Entry>>>,
//     fronts: Vec<Tag>,
//     len: usize,
//     is_dirty: RcPointer<Cell<bool>>,
// }
//
// impl TagAntiChainSet {
//     pub fn new() -> Self {
//         TagAntiChainSet {
//             chain: vec![],
//             fronts: vec![],
//             len: 0,
//             is_dirty: RcPointer::new(Cell::new(false)),
//         }
//     }
//
//     pub fn push(&mut self, tag: Tag) {
//         let entry = self.push_inner(tag, true);
//         if !entry.is_blocked() {
//             // if entry.tag.is_root() {
//             //     debug!("root is_active={} blocked {},", entry.is_active(), entry.block_size());
//             // }
//             self.is_dirty.set(true);
//         }
//     }
//
//     // pub fn push_block(&mut self, tag: Tag) -> BlockGuard {
//     //     self.is_dirty.set(true);
//     //     let entry = self.push_inner(tag, true);
//     //     entry.incr_block(1);
//     //     BlockGuard::new(entry)
//     // }
//
//     fn push_inner(&mut self, tag: Tag, active: bool) -> RcPointer<Entry> {
//         if let Some(entry) = self.get(&tag) {
//             let entry = entry.clone();
//             if active && !entry.is_active() {
//                 entry.set_active();
//                 self.len += 1;
//             }
//             entry
//         } else {
//             let entry = if tag.is_root() {
//                 RcPointer::new(Entry::with(tag.clone(), active, false, None, self.is_dirty.clone()))
//             } else {
//                 let p = tag.to_parent_uncheck();
//                 let parent = self.push_inner(p, false);
//                 RcPointer::new(Entry::with(
//                     tag.clone(),
//                     active,
//                     false,
//                     Some(parent),
//                     self.is_dirty.clone(),
//                 ))
//             };
//
//             while self.chain.len() <= tag.len() {
//                 self.chain.push(HashMap::new());
//             }
//             self.chain[tag.len()].insert(tag, entry.clone());
//             if active {
//                 self.len += 1;
//             }
//             entry
//         }
//     }
//
//     pub fn remove(&mut self, tag: &Tag) {
//         if let Some(e) = self.get(tag) {
//             if e.is_active() {
//                 e.active.set(false);
//                 self.len -= 1;
//             }
//         }
//     }
//
//     pub fn block_anyway(&mut self, tag: &Tag, size: i64) -> BlockGuard {
//         let entry = self.push_inner(tag.clone(), false);
//         entry.incr_block(size);
//         BlockGuard::new(entry.clone())
//     }
//
//     #[allow(dead_code)]
//     pub fn block_if_present(&self, tag: &Tag, size: i64) -> Option<BlockGuard> {
//         if let Some(b) = self.get(tag) {
//             if b.is_active() {
//                 b.incr_block(size);
//                 Some(BlockGuard::new(b.clone()))
//             } else {
//                 None
//             }
//         } else {
//             None
//         }
//     }
//
//     #[allow(dead_code)]
//     pub fn block_if_present_and_not_block(&self, tag: &Tag, size: i64) -> Option<BlockGuard> {
//         if let Some(b) = self.get(tag) {
//             if b.is_active() && !b.is_blocked() {
//                 b.incr_block(size);
//                 return Some(BlockGuard::new(b.clone()));
//             }
//         }
//         None
//     }
//
//     #[inline]
//     fn get(&self, tag: &Tag) -> Option<&RcPointer<Entry>> {
//         let len = tag.len();
//         if self.chain.len() > len {
//             self.chain[len].get(tag)
//         } else {
//             None
//         }
//     }
//
//     fn calculate_fronts(&mut self) {
//         if self.is_dirty.get() {
//             let mut vec = std::mem::replace(&mut self.fronts, vec![]);
//             let len = self.chain.len();
//             let mut count = 0;
//             for i in 1..(len + 1) {
//                 self.chain[len - i].retain(|tag, entry| {
//                     let remove = entry.is_active() && !entry.is_blocked();
//                     if remove {
//                         count += 1;
//                         vec.push(tag.clone());
//                     }
//                     // if tag.is_root() {
//                     //     debug!("root is_active={} blocked {},", entry.is_active(), entry.block_size());
//                     // }
//                     !remove
//                 });
//             }
//             self.len -= count;
//             self.fronts = vec;
//         }
//     }
//
//     pub fn check_has_fronts(&mut self) -> bool {
//         self.calculate_fronts();
//         !self.fronts.is_empty()
//     }
//
//     #[inline]
//     pub fn take_fronts(&mut self) -> &mut Vec<Tag> {
//         if self.len > 0 {
//             self.check_has_fronts();
//         }
//         self.is_dirty.set(false);
//         &mut self.fronts
//     }
//
//     #[allow(dead_code)]
//     #[inline]
//     pub fn len(&self) -> usize {
//         self.len
//     }
//
//     #[inline]
//     pub fn is_dirty(&self) -> bool {
//         self.is_dirty.get()
//     }
// }
//
// #[cfg(test)]
// mod test {
//     use super::*;
//
//     #[test]
//     fn test_push() {
//         let mut chain = TagAntiChainSet::new();
//         chain.push(tag!(0));
//         chain.push(tag!(1));
//         chain.push(tag![0, 0]);
//         chain.push(tag![0, 1]);
//         chain.push(tag![0, 2]);
//         chain.push(tag![0, 2]);
//         assert_eq!(chain.len(), 5);
//         assert!(chain.is_dirty());
//         let fronts = std::mem::replace(chain.take_fronts(), vec![]);
//         assert_eq!(fronts.len(), 5);
//         assert_eq!(chain.len(), 0);
//         assert!(!chain.is_dirty());
//         let mut a = fronts[0..3].to_vec();
//         a.sort_by(|t1, t2| t1.as_slice().cmp(&t2.as_slice()));
//         assert_eq!(a, vec![tag![0, 0], tag![0, 1], tag![0, 2]]);
//         let mut b = fronts[3..].to_vec();
//         b.sort_by(|t1, t2| t1.as_slice().cmp(&t2.as_slice()));
//         assert_eq!(b, vec![tag!(0), tag!(1)]);
//     }
//
//     #[test]
//     fn test_block() {
//         let root = crate::tag::ROOT.clone();
//         let mut chain = TagAntiChainSet::new();
//         chain.push(tag!(0));
//         chain.push(tag![0, 0]);
//         chain.push(tag![0, 1]); //
//         chain.push(tag![0, 2]);
//         chain.push(tag!(1));
//         chain.push(tag![1, 2]);
//         chain.push(tag![1, 3]);
//         chain.push(root.clone());
//         let block = chain.block_if_present(&tag![0, 1], 1);
//         assert!(block.is_some());
//         let block = block.unwrap();
//         let fronts = std::mem::replace(chain.take_fronts(), vec![]);
//         assert_eq!(fronts.len(), 5);
//         assert_eq!(&fronts[4], &tag!(1));
//         assert!(!chain.is_dirty());
//         block.incr(1024);
//         assert!(!chain.is_dirty());
//         block.decr(1024);
//         assert!(!chain.is_dirty());
//         let fronts = std::mem::replace(chain.take_fronts(), vec![]);
//         assert_eq!(fronts.len(), 0);
//         block.decr(1);
//         assert!(chain.is_dirty());
//         let fronts = std::mem::replace(chain.take_fronts(), vec![]);
//         assert_eq!(fronts.len(), 3);
//         assert_eq!(&fronts[0], &tag![0, 1]);
//         assert_eq!(&fronts[1], &tag!(0));
//         assert_eq!(&fronts[2], &root);
//     }
//
//     #[test]
//     fn test_block_anyway() {
//         let root = crate::tag::ROOT.clone();
//         let mut chain = TagAntiChainSet::new();
//         let guard = chain.block_anyway(&tag![0, 1], 1024);
//         chain.push(tag!(0));
//         chain.push(tag!(1));
//         chain.push(root.clone());
//         assert_eq!(chain.len(), 3);
//         let fronts = std::mem::replace(chain.take_fronts(), vec![]);
//         assert_eq!(fronts.len(), 1);
//         assert_eq!(&fronts[0], &tag!(1));
//         assert_eq!(chain.len(), 2);
//         guard.decr(1023);
//         assert!(!chain.is_dirty());
//         let fronts = std::mem::replace(chain.take_fronts(), vec![]);
//         assert_eq!(fronts.len(), 0);
//         guard.decr(16);
//         assert!(chain.is_dirty());
//         let fronts = std::mem::replace(chain.take_fronts(), vec![]);
//         assert_eq!(fronts.len(), 2);
//         assert_eq!(&fronts[0], &tag!(0));
//         assert_eq!(&fronts[1], &root);
//     }
//
//     #[test]
//     fn test_block_anyway_2() {
//         let root = crate::tag::ROOT.clone();
//         let mut chain = TagAntiChainSet::new();
//
//         let guard = chain.block_anyway(&tag![0, 1], 1024);
//         guard.decr(1024);
//
//         let fronts = std::mem::replace(chain.take_fronts(), vec![]);
//         assert!(fronts.is_empty());
//
//         let blocked = guard.get();
//         assert!(blocked.is_none());
//         assert!(!chain.is_dirty());
//
//         guard.incr(1024);
//         chain.push(tag!(0));
//         chain.push(root.clone());
//
//         assert!(!chain.is_dirty());
//         let blocked = guard.get();
//         assert!(blocked.is_some());
//         assert_eq!(blocked, Some(tag!(0)));
//         let fronts = std::mem::replace(chain.take_fronts(), vec![]);
//         assert!(fronts.is_empty());
//
//         guard.decr(1024);
//         assert!(chain.is_dirty());
//         let fronts = std::mem::replace(chain.take_fronts(), vec![]);
//         assert_eq!(fronts.len(), 2);
//         assert_eq!(&fronts[0], &tag!(0));
//         assert_eq!(&fronts[1], &root);
//     }
//
//     #[test]
//     fn test_block_anyway_3() {
//         let root = crate::tag::ROOT.clone();
//         let mut chain = TagAntiChainSet::new();
//         let guard = chain.block_anyway(&tag![0, 0, 1], 1024);
//         guard.decr(1024);
//         let fronts = std::mem::replace(chain.take_fronts(), vec![]);
//         assert!(fronts.is_empty());
//         let blocked = guard.get();
//         assert!(blocked.is_none());
//
//         guard.incr(1024);
//         chain.push(tag![0, 0]);
//         assert!(!chain.is_dirty());
//         let blocked = guard.get();
//         assert!(blocked.is_some());
//         assert_eq!(blocked, Some(tag![0, 0]));
//
//         chain.push(tag!(0));
//         chain.push(root.clone());
//         assert!(!chain.is_dirty());
//
//         let fronts = std::mem::replace(chain.take_fronts(), vec![]);
//         assert!(fronts.is_empty());
//
//         guard.decr(1024);
//         assert!(chain.is_dirty());
//
//         let fronts = std::mem::replace(chain.take_fronts(), vec![]);
//         assert_eq!(fronts.len(), 3);
//         assert_eq!(&fronts[0], &tag![0, 0]);
//         assert_eq!(&fronts[1], &tag!(0));
//         assert_eq!(&fronts[2], &root);
//     }
// }
