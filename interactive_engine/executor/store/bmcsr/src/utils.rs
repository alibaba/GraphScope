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

use crate::graph::IndexType;

pub struct Iter<'a, T> {
    inner: Box<dyn Iterator<Item = T> + 'a + Send>,
}

impl<'a, T> Iter<'a, T> {
    pub fn from_iter<I: Iterator<Item = T> + 'a + Send>(iter: I) -> Self {
        Iter { inner: Box::new(iter) }
    }

    pub fn from_iter_box(iter: Box<dyn Iterator<Item = T> + 'a + Send>) -> Self {
        Iter { inner: iter }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = T;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    #[inline(always)]
    fn count(self) -> usize {
        self.inner.count()
    }
}

unsafe impl<'a, T> Send for Iter<'a, T> {}

pub struct Range<I: IndexType> {
    begin: I,
    end: I,
}

pub struct RangeIterator<I: IndexType> {
    cur: I,
    end: I,
}

impl<I: IndexType> Iterator for RangeIterator<I> {
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur == self.end {
            None
        } else {
            let ret = self.cur.clone();
            self.cur += I::new(1);
            Some(ret)
        }
    }
}

impl<I: IndexType> Range<I> {
    pub fn new(begin: I, end: I) -> Self {
        Range { begin, end }
    }

    pub fn into_iter(self) -> RangeIterator<I> {
        RangeIterator { cur: self.begin.clone(), end: self.end.clone() }
    }
}

pub struct LabeledIterator<L: Copy + Send, I: Iterator + Send> {
    labels: Vec<L>,
    iterators: Vec<I>,
    cur: usize,
}

impl<L: Copy + Send, I: Iterator + Send> LabeledIterator<L, I> {
    pub fn new(labels: Vec<L>, iterators: Vec<I>) -> Self {
        Self { labels, iterators, cur: 0 }
    }
}

impl<L: Copy + Send, I: Iterator + Send> Iterator for LabeledIterator<L, I> {
    type Item = (L, I::Item);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.cur == self.labels.len() {
                return None;
            }
            if let Some(item) = self.iterators[self.cur].next() {
                return Some((self.labels[self.cur], item));
            } else {
                self.cur += 1;
            }
        }
    }
}

unsafe impl<L: Copy + Send, I: Iterator + Send> Send for LabeledIterator<L, I> {}

pub struct LabeledRangeIterator<L: Copy + Send, I: Copy + Send + IndexType> {
    labels: Vec<L>,
    iterators: Vec<RangeIterator<I>>,
    cur: usize,
}

impl<L: Copy + Send, I: Copy + Send + IndexType> LabeledRangeIterator<L, I> {
    pub fn new(labels: Vec<L>, iterators: Vec<RangeIterator<I>>) -> Self {
        Self { labels, iterators, cur: 0 }
    }
}

impl<L: Copy + Send, I: Copy + Send + IndexType> Iterator for LabeledRangeIterator<L, I> {
    type Item = (L, I);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.cur == self.labels.len() {
                return None;
            }
            if let Some(item) = self.iterators[self.cur].next() {
                return Some((self.labels[self.cur], item));
            } else {
                self.cur += 1;
            }
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let mut remaining = n;
        while remaining != 0 {
            if self.cur == self.labels.len() {
                return None;
            }
            let cur_remaining = self.iterators[self.cur].end.index() - self.iterators[self.cur].cur.index();
            let cur_cur = self.iterators[self.cur].cur.index();
            if cur_remaining >= remaining {
                self.iterators[self.cur].cur = I::new(cur_cur + remaining);
                return self.next();
            } else {
                remaining -= cur_remaining;
                self.cur += 1;
            }
        }
        None
    }
}
