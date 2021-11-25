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

use crate::process::traversal::traverser::Traverser;
use std::marker::PhantomData;

#[enum_dispatch]
pub trait Predicate<T> {
    fn test(&self, entry: &T) -> Option<bool>;
}

impl<T, F: Fn(&T) -> Option<bool>> Predicate<T> for F {
    fn test(&self, entry: &T) -> Option<bool> {
        (self)(entry)
    }
}

impl<T> Predicate<T> for Box<dyn Predicate<T>> {
    fn test(&self, entry: &T) -> Option<bool> {
        (**self).test(entry)
    }
}

#[enum_dispatch]
pub trait BiPredicate<T, K> {
    fn test(&self, left: &T, right: &K) -> Option<bool>;
}

pub mod codec;
mod compare;
mod contains;
mod element;
mod traverser;

use crate::structure::{GraphElement, Tag};
pub use element::*;
pub use traverser::*;

pub enum Filter<T, P: Predicate<T>> {
    Ph(PhantomData<T>),
    Simple(P),
    Chain(Chain<T, P>),
}

impl<T, P: Predicate<T>> Default for Filter<T, P> {
    fn default() -> Self {
        Filter::Ph(std::marker::PhantomData)
    }
}

impl<T, P: Predicate<T>> Filter<T, P> {
    pub fn with(p: P) -> Self {
        Filter::Simple(p)
    }

    pub fn with_chain<F: Into<Filter<T, P>>>(f: F) -> Self {
        Filter::Chain(Chain::new(f))
    }

    pub fn from_vec(vec: Vec<P>) -> Self {
        let chain = Chain::from_vec(vec);
        Filter::Chain(chain)
    }

    pub fn and<F: Into<Filter<T, P>>>(&mut self, f: F) -> &mut Self {
        match self {
            Filter::Ph(_) => {
                let _ = std::mem::replace(self, f.into());
            }
            Filter::Simple(_) => {
                let old = std::mem::replace(self, Filter::Ph(PhantomData));
                let mut upgrade = Filter::with_chain(old);
                upgrade.and(f);
                let _ = std::mem::replace(self, upgrade);
            }
            Filter::Chain(chain) => chain.and(f),
        }
        self
    }

    pub fn or<F: Into<Filter<T, P>>>(&mut self, f: F) -> &mut Self {
        match self {
            Filter::Ph(_) => {
                let _ = std::mem::replace(self, f.into());
            }
            Filter::Simple(_) => {
                let old = std::mem::replace(self, Filter::Ph(PhantomData));
                let mut upgrade = Filter::with_chain(old);
                upgrade.or(f);
                let _ = std::mem::replace(self, upgrade);
            }
            Filter::Chain(chain) => chain.or(f),
        }
        self
    }

    pub fn test(&self, entry: &T) -> Option<bool> {
        match self {
            Filter::Ph(_) => Some(true),
            Filter::Simple(p) => p.test(entry),
            Filter::Chain(chain) => chain.test(entry),
        }
    }

    pub fn for_each<F>(&self, func: &mut F)
    where
        F: FnMut(&P),
    {
        match self {
            Filter::Ph(_) => (),
            Filter::Simple(p) => func(p),
            Filter::Chain(chain) => {
                for n in chain.list.iter() {
                    n.filter.for_each(func);
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Filter::Ph(_) => true,
            Filter::Simple(_) => false,
            Filter::Chain(f) => f.is_empty(),
        }
    }
}

unsafe impl<T, P: Predicate<T> + Send> Send for Filter<T, P> {}

unsafe impl<T, P: Predicate<T> + Sync> Sync for Filter<T, P> {}

impl<T, P: Predicate<T>> From<P> for Filter<T, P> {
    fn from(p: P) -> Self {
        Filter::Simple(p)
    }
}

pub type TraverserFilterChain = Filter<Traverser, traverser::TraverserFilter>;

pub fn without_tag(filter: Filter<GraphElement, ElementFilter>) -> TraverserFilterChain {
    let mut tf = Filter::<Traverser, traverser::TraverserFilter>::default();
    let mut connect = ChainKind::Or;
    match filter {
        Filter::Ph(_) => {}
        Filter::Simple(f) => {
            let next = Filter::with(HasHead::new(f).into());
            match connect {
                ChainKind::And => {
                    tf.and(next);
                }
                ChainKind::Or => {
                    tf.or(next);
                }
            }
        }
        Filter::Chain(chain) => {
            for node in chain.list {
                let next = without_tag(node.filter);
                match connect {
                    ChainKind::And => {
                        tf.and(next);
                    }
                    ChainKind::Or => {
                        tf.or(next);
                    }
                }
                connect = node.next;
            }
        }
    }
    tf
}

pub fn with_tag(
    tags: &mut dyn Iterator<Item = Tag>, filter: Filter<GraphElement, ElementFilter>,
) -> TraverserFilterChain {
    let mut tf = Filter::<Traverser, traverser::TraverserFilter>::default();
    let mut connect = ChainKind::Or;
    match filter {
        Filter::Ph(_) => {}
        Filter::Simple(f) => {
            // TODO: Handle tag unexpected eof;
            let t = tags.next().expect("no tags found");
            let next = Filter::with(HasTag::new(t, f).into());
            match connect {
                ChainKind::And => {
                    tf.and(next);
                }
                ChainKind::Or => {
                    tf.or(next);
                }
            }
        }
        Filter::Chain(chain) => {
            for node in chain.list {
                let next = with_tag(tags, node.filter);
                match connect {
                    ChainKind::And => {
                        tf.and(next);
                    }
                    ChainKind::Or => {
                        tf.or(next);
                    }
                }
                connect = node.next;
            }
        }
    }
    tf
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub(crate) enum ChainKind {
    And,
    Or,
}

struct ChainNode<T, P: Predicate<T>> {
    filter: Filter<T, P>,
    next: ChainKind,
}

impl<T, P: Predicate<T>> ChainNode<T, P> {
    pub fn new(filter: Filter<T, P>) -> Self {
        ChainNode { filter, next: ChainKind::Or }
    }

    pub fn with(filter: Filter<T, P>, next: ChainKind) -> Self {
        ChainNode { filter, next }
    }
}

pub struct Chain<T, P: Predicate<T>> {
    list: Vec<ChainNode<T, P>>,
}

impl<T, P: Predicate<T>> Chain<T, P> {
    fn new<F: Into<Filter<T, P>>>(f: F) -> Self {
        let node = ChainNode::new(f.into());
        Chain { list: vec![node] }
    }

    fn from_vec(vec: Vec<P>) -> Self {
        let mut list = Vec::with_capacity(vec.len());
        for p in vec {
            list.push(ChainNode::with(Filter::Simple(p), ChainKind::And));
        }

        Chain { list }
    }

    fn and<F: Into<Filter<T, P>>>(&mut self, f: F) {
        if let Some(last) = self.list.last_mut() {
            last.next = ChainKind::And;
        }

        let next = f.into();
        self.add_next(next);
    }

    fn or<F: Into<Filter<T, P>>>(&mut self, f: F) {
        if let Some(last) = self.list.last_mut() {
            last.next = ChainKind::Or;
        }
        let next = f.into();
        self.add_next(next);
    }

    #[inline]
    fn add_next(&mut self, next: Filter<T, P>) {
        match next {
            Filter::Ph(_) => {
                let node = ChainNode::new(next);
                self.list.push(node);
            }
            Filter::Simple(_) => {
                let node = ChainNode::new(next);
                self.list.push(node);
            }
            Filter::Chain(mut chain) => {
                let len = chain.list.len();
                if len == 0 {
                    self.list.push(ChainNode::new(Filter::default()));
                } else if len == 1 {
                    let next = chain.list.swap_remove(0);
                    self.list.push(next);
                } else {
                    self.list.push(ChainNode::new(Filter::Chain(chain)));
                }
            }
        }
    }

    fn test(&self, entry: &T) -> Option<bool> {
        let mut result = false;
        let mut next = ChainKind::Or;
        for f in self.list.iter() {
            if let Some(r) = f.filter.test(entry) {
                match next {
                    ChainKind::And => result &= r,
                    ChainKind::Or => result |= r,
                }
                next = f.next;
                if (result && next == ChainKind::Or) || (!result && next == ChainKind::And) {
                    return Some(result);
                }
            } else {
                return None;
            }
        }
        Some(result)
    }

    fn is_empty(&self) -> bool {
        self.list.is_empty()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    struct Person {
        pub id: u64,
        pub name: String,
        pub age: u32,
    }

    impl Person {
        pub fn new(id: u64, name: String, age: u32) -> Self {
            Person { id, name, age }
        }
    }

    #[test]
    pub fn test_single_filter() {
        let filter = Filter::with(|p: &Person| Some(p.age > 30));
        let p1 = Person::new(0, "abc".to_owned(), 31);
        let p2 = Person::new(0, "abc".to_owned(), 29);
        assert_eq!(filter.test(&p1), Some(true));
        assert_eq!(filter.test(&p2), Some(false));
    }

    #[test]
    pub fn test_simple_chain_filter() {
        let func1 = Box::new(|p: &Person| Some(p.age > 30)) as Box<dyn Predicate<Person>>;
        let func2 = Box::new(|p: &Person| Some(p.id % 2 == 0)) as Box<dyn Predicate<Person>>;
        let func3 =
            Box::new(|p: &Person| Some(p.name.starts_with("a"))) as Box<dyn Predicate<Person>>;
        let mut filter = Filter::with_chain(func1);
        // age > 30 && id % 2 == 0 || name.start_with("a")
        filter.and(func2).or(func3);

        let p1 = Person::new(0, "abc".to_owned(), 31);
        let p2 = Person::new(1, "abcd".to_owned(), 32);
        let p3 = Person::new(2, "bcd".to_owned(), 33);
        let p4 = Person::new(3, "bcde".to_owned(), 33);
        assert_eq!(filter.test(&p1), Some(true));
        assert_eq!(filter.test(&p2), Some(true));
        assert_eq!(filter.test(&p3), Some(true));
        assert_eq!(filter.test(&p4), Some(false));
    }

    #[test]
    pub fn test_nested_chain_filter() {
        let func1 = Box::new(|p: &Person| Some(p.age > 30)) as Box<dyn Predicate<Person>>;
        let func2 = Box::new(|p: &Person| Some(p.id % 2 == 0)) as Box<dyn Predicate<Person>>;
        let func3 =
            Box::new(|p: &Person| Some(p.name.starts_with("a"))) as Box<dyn Predicate<Person>>;
        let mut filter = Filter::with_chain(func1);
        let mut nested = Filter::with_chain(func2);
        nested.and(func3);
        // age > 30 || ( id % 2 == 0 && name.start_with("a") )
        filter.or(nested);

        let p1 = Person::new(0, "abc".to_owned(), 30);
        let p2 = Person::new(1, "abcd".to_owned(), 31);
        let p3 = Person::new(2, "bcd".to_owned(), 29);
        let p4 = Person::new(3, "bcde".to_owned(), 29);
        assert_eq!(filter.test(&p1), Some(true));
        assert_eq!(filter.test(&p2), Some(true));
        assert_eq!(filter.test(&p3), Some(false));
        assert_eq!(filter.test(&p4), Some(false));
    }
}
