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

use crate::Tag;
use ahash::AHashMap;
use pegasus_common::rc::RcPointer;
use std::cell::{RefCell, RefMut};
use std::collections::HashSet;

#[derive(Clone)]
enum Fence {
    Little(u128),
    Large(HashSet<u32>),
}

impl Fence {
    pub fn new(guard: u32) -> Self {
        if guard < 64 {
            Fence::Little(0)
        } else {
            Fence::Large(HashSet::new())
        }
        //Fence::Large(HashSet::new())
    }

    #[inline]
    pub fn insert(&mut self, sig: u32) -> bool {
        match self {
            Fence::Little(f) => {
                assert!(sig < 128);
                if sig == 0 {
                    if *f & 0 == 0 {
                        *f |= 1;
                        true
                    } else {
                        false
                    }
                } else {
                    let offset = 1u128 << sig;
                    if *f & offset == 0 {
                        *f |= offset;
                        true
                    } else {
                        false
                    }
                }
            }
            Fence::Large(f) => f.insert(sig),
        }
    }

    #[inline]
    pub fn is_passed(&self, guard: usize) -> bool {
        match self {
            Fence::Little(f) => {
                let guard = (1u128 << guard) - 1;
                debug_worker!("guard is {}, fence is {}", guard, f);
                *f >= guard
            }
            Fence::Large(f) => f.len() >= guard,
        }
    }
}

pub struct CountDownLatchNode<T> {
    pub guard: usize,
    content: RefCell<Option<T>>,
    fence: RefCell<Fence>,
    children: RefCell<Vec<RcPointer<CountDownLatchNode<T>>>>,
    count_downed: RefCell<Vec<T>>,
}

impl<T> CountDownLatchNode<T> {
    pub fn new(guard: usize, content: T) -> Self {
        let fence = Fence::new(guard as u32);
        CountDownLatchNode {
            guard,
            content: RefCell::new(Some(content)),
            fence: RefCell::new(fence),
            children: RefCell::new(Vec::new()),
            count_downed: RefCell::new(Vec::new()),
        }
    }

    pub fn count_down(&self, sig: u32) -> RefMut<Vec<T>> {
        if self.fence.borrow_mut().insert(sig) {
            let children = self.children.borrow();
            for child in children.iter() {
                for r in child.count_down(sig).drain(..) {
                    self.count_downed.borrow_mut().push(r);
                }
            }
        }

        if self.fence.borrow().is_passed(self.guard) {
            if let Some(c) = self.content.borrow_mut().take() {
                self.count_downed.borrow_mut().push(c);
            }
        }
        self.count_downed.borrow_mut()
    }

    #[inline]
    pub fn is_blocked(&self) -> bool {
        //println!("current len is {}", self.current.len());
        !self.fence.borrow().is_passed(self.guard)
    }

    pub fn spawn(&self, content: Option<T>) -> RcPointer<CountDownLatchNode<T>> {
        let fence = self.fence.borrow().clone();
        let child = CountDownLatchNode {
            guard: self.guard,
            content: RefCell::new(content),
            fence: RefCell::new(fence),
            children: RefCell::new(vec![]),
            count_downed: RefCell::new(vec![]),
        };

        let child = RcPointer::new(child);
        self.children.borrow_mut().push(child.clone());
        child
    }

    pub fn is_empty(&self) -> bool {
        self.content.borrow().is_none()
    }

    pub fn set_content(&self, content: T) {
        self.content.replace(Some(content));
    }
}

pub struct CountDownLatchTree {
    pub guard: usize,
    root: CountDownLatchNode<Tag>,
    leaf: RefCell<AHashMap<Tag, RcPointer<CountDownLatchNode<Tag>>>>,
    count_downed: RefCell<Vec<Tag>>,
}

impl CountDownLatchTree {
    pub fn new(guard: usize) -> Self {
        CountDownLatchTree {
            guard,
            root: CountDownLatchNode::new(guard, Tag::Root),
            leaf: RefCell::new(AHashMap::new()),
            count_downed: RefCell::new(vec![]),
        }
    }

    pub fn count_down(&self, tag: Tag, sig: u32) -> RefMut<Vec<Tag>> {
        if tag.is_root() {
            self.root.count_down(sig)
        } else {
            let leaf = self.leaf.borrow();
            if let Some(node) = leaf.get(&tag) {
                if node.is_empty() {
                    node.set_content(tag);
                }
                for e in node.count_down(sig).drain(..) {
                    self.count_downed.borrow_mut().push(e);
                }
            } else {
                std::mem::drop(leaf);
                for e in self.add_node(tag, false).count_down(sig).drain(..) {
                    self.count_downed.borrow_mut().push(e);
                }
            }
            self.count_downed.borrow_mut()
        }
    }

    fn add_node(&self, tag: Tag, is_path: bool) -> RcPointer<CountDownLatchNode<Tag>> {
        if let Some(p) = tag.to_parent() {
            let node = if p.is_root() {
                if is_path {
                    self.root.spawn(None)
                } else {
                    self.root.spawn(Some(tag.clone()))
                }
            } else {
                let leaf = self.leaf.borrow();
                if let Some(p_node) = leaf.get(&p) {
                    if is_path {
                        p_node.spawn(None)
                    } else {
                        p_node.spawn(Some(tag.clone()))
                    }
                } else {
                    std::mem::drop(leaf);
                    let node = self.add_node(p, true);
                    if is_path {
                        node.spawn(None)
                    } else {
                        node.spawn(Some(tag.clone()))
                    }
                }
            };
            self.leaf.borrow_mut().insert(tag, node.clone());
            node
        } else {
            unreachable!("can't add node with tag {:?}", tag);
        }
    }

    #[allow(dead_code)]
    pub fn is_blocked(&self, tag: &Tag) -> Option<bool> {
        if tag.is_root() {
            Some(self.root.is_blocked())
        } else {
            self.leaf.borrow().get(tag).map(|p| p.is_blocked())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn fence_test() {
        let mut fence = Fence::new(8);
        fence.insert(0);
        fence.insert(0);
        fence.insert(0);
        fence.insert(0);
        fence.insert(0);
        fence.insert(0);
        fence.insert(0);
        fence.insert(0);
        assert!(!fence.is_passed(8));
        fence.insert(1);
        fence.insert(4);
        fence.insert(5);
        fence.insert(6);
        fence.insert(6);
        fence.insert(6);
        fence.insert(2);
        fence.insert(3);
        assert!(!fence.is_passed(8));
        fence.insert(7);
        assert!(fence.is_passed(8));
    }

    #[test]
    fn count_down_latch_node_test() {
        let root_cdl = CountDownLatchNode::new(3, "root".to_owned());
        let child_tdl = root_cdl.spawn(Some("root_child".to_owned()));
        root_cdl.count_down(1);
        root_cdl.count_down(2);
        root_cdl.count_down(1);
        assert!(root_cdl.count_down(2).is_empty());
        assert!(root_cdl.is_blocked());
        assert!(child_tdl.count_down(1).is_empty());
        assert!(child_tdl.is_blocked());
        let r = child_tdl.count_down(0);
        assert_eq!(r.len(), 1);
        assert_eq!("root_child", r[0].as_str());
        assert!(!child_tdl.is_blocked());
        assert!(root_cdl.is_blocked())
    }

    #[test]
    fn count_down_tree_test() {
        let cdl_tree = CountDownLatchTree::new(2);
        assert!(cdl_tree.count_down(tag![0, 0], 0).is_empty());
        assert!(cdl_tree.count_down(tag![0, 0], 0).is_empty());
        let tag = cdl_tree.count_down(tag![0, 0], 1).remove(0);
        assert_eq!(tag, tag![0, 0]);
        assert!(cdl_tree.count_down(tag![0], 0).is_empty());
        assert!(cdl_tree.count_down(tag![0], 0).is_empty());
        let tag = cdl_tree.count_down(tag![0], 1).remove(0);
        assert_eq!(tag, tag![0]);
    }
}
