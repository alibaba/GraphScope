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
// use std::collections::HashMap;
//
// pub struct TagNode {
//     tag: Tag,
//     children: HashMap<u32, TagNode>,
//     is_present: bool,
// }
//
// impl TagNode {
//     pub fn new(tag: Tag) -> Self {
//         TagNode { tag, children: HashMap::new(), is_present: false }
//     }
//
//     pub fn get_child(&mut self, index: u32) -> &mut TagNode {
//         let tag = &self.tag;
//         self.children.entry(index).or_insert_with(|| {
//             let tag = Tag::inherit(tag, index);
//             TagNode::new(tag)
//         })
//     }
//
//     pub fn add_child(&mut self, index: u32, tag: Tag) {
//         if tag.len() == self.tag.len() + 1 {
//             if let Some(child) = self.children.get_mut(&index) {
//                 assert_eq!(child.tag, tag);
//                 child.is_present = true;
//             } else {
//                 let mut node = TagNode::new(tag);
//                 node.is_present = true;
//                 self.children.insert(index, node);
//             }
//         }
//     }
// }
//
// pub struct TagTree {
//     root: TagNode,
// }
//
// impl TagTree {
//     pub fn new() -> Self {
//         let root = TagNode::new(crate::tag::ROOT.clone());
//         TagTree { root }
//     }
//
//     pub fn add_node(&mut self, tag: Tag) {
//         if tag.is_root() {
//             self.root.is_present = true;
//         } else if tag.len() == 1 {
//             let index = tag.current_uncheck();
//             self.root.add_child(index, tag);
//         } else {
//             let mut node = &mut self.root;
//             let last = tag.len() - 1;
//             let slice = tag.as_slice();
//             for offset in &slice[0..last] {
//                 node = node.get_child(*offset);
//             }
//             node.add_child(slice[last], tag);
//         }
//     }
//
//     pub fn fold_into(&mut self, target: &mut Vec<Tag>) {
//         if self.root.is_present {
//             target.push(self.root.tag.clone());
//         } else {
//             let mut stack = Vec::new();
//             for node in self.root.children.values() {
//                 if node.is_present {
//                     target.push(node.tag.clone());
//                 } else {
//                     stack.push(node);
//                 }
//             }
//
//             while let Some(node) = stack.pop() {
//                 if node.is_present {
//                     target.push(node.tag.clone());
//                 } else {
//                     for child in node.children.values() {
//                         stack.push(child);
//                     }
//                 }
//             }
//         }
//     }
// }
//
// #[cfg(test)]
// mod test {
//     use super::*;
//     use crate::tag::tools::tree::TagTree;
//
//     #[test]
//     fn test_fold() {
//         let mut q = TagTree::new();
//         {
//             let mut v = vec![
//                 tag!(0, 0, 0),
//                 tag!(0, 0, 1),
//                 tag!(0, 1, 0),
//                 tag!(0, 1, 1),
//                 tag!(0, 2, 0),
//                 tag!(0, 2, 1),
//                 tag!(1, 0, 0),
//                 tag!(1, 0, 1),
//                 tag!(1, 1, 0),
//                 tag!(1, 1, 1),
//             ];
//             q.add_node(v[0].clone());
//             q.add_node(v[1].clone());
//             q.add_node(v[2].clone());
//             q.add_node(v[3].clone());
//             q.add_node(v[4].clone());
//             q.add_node(v[5].clone());
//             q.add_node(v[6].clone());
//             q.add_node(v[7].clone());
//             q.add_node(v[8].clone());
//             q.add_node(v[9].clone());
//             v.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));
//             let mut vec = Vec::new();
//             q.fold_into(&mut vec);
//             vec.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));
//             assert_eq!(vec, v);
//         }
//         {
//             let mut v = vec![tag!(0, 0), tag!(0, 1), tag!(0, 2), tag!(1, 0), tag!(1, 1)];
//             q.add_node(v[0].clone());
//             q.add_node(v[1].clone());
//             q.add_node(v[2].clone());
//             q.add_node(v[3].clone());
//             q.add_node(v[4].clone());
//             let mut vec = Vec::new();
//             q.fold_into(&mut vec);
//             v.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));
//             vec.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));
//             assert_eq!(vec, v);
//         }
//         {
//             let mut v = vec![tag!(0), tag!(1)];
//             q.add_node(tag!(0));
//             q.add_node(tag!(1));
//             let mut vec = Vec::new();
//             q.fold_into(&mut vec);
//             v.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));
//             vec.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));
//             assert_eq!(vec, v);
//         }
//         q.add_node(crate::tag::ROOT.clone());
//         let mut vec = Vec::new();
//         q.fold_into(&mut vec);
//         assert_eq!(vec, vec![crate::tag::ROOT.clone()]);
//     }
// }
