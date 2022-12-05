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

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Bound, Index, RangeBounds};

use serde::Serialize;

/// `SortedMap` is a data structure with similar characteristics as BTreeMap but
/// slightly different trade-offs: lookup, insertion, and removal are *O*(log(*n*))
/// and elements can be iterated in order cheaply.
///
/// `SortedMap` can be faster than a `BTreeMap` for small sizes (<50) since it
/// stores data in a more compact way. It also supports accessing contiguous
/// ranges of elements as a slice, and slices of already sorted elements can be
/// inserted efficiently.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct SortedMap<K, V, C: AsRef<[(K, V)]> = Vec<(K, V)>> {
    data: C,
    ph1: PhantomData<K>,
    ph2: PhantomData<V>,
}

impl<K, V, C: AsRef<[(K, V)]> + Default> Default for SortedMap<K, V, C> {
    #[inline]
    fn default() -> SortedMap<K, V, C> {
        SortedMap { data: C::default(), ph1: PhantomData, ph2: PhantomData }
    }
}

impl<K: Ord, V> FromIterator<(K, V)> for SortedMap<K, V> {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut data: Vec<(K, V)> = iter.into_iter().collect();

        data.sort_unstable_by(|&(ref k1, _), &(ref k2, _)| k1.cmp(k2));
        data.dedup_by(|&mut (ref k1, _), &mut (ref k2, _)| k1.cmp(k2) == Ordering::Equal);

        SortedMap { data, ph1: PhantomData, ph2: PhantomData }
    }
}

impl<K: Ord, V> SortedMap<K, V> {
    #[inline]
    pub fn into_immutable(self) -> SortedMap<K, V, Box<[(K, V)]>> {
        SortedMap { data: self.data.into_boxed_slice(), ph1: PhantomData, ph2: PhantomData }
    }

    #[inline]
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match self.lookup_index_for(key) {
            Ok(index) => unsafe { Some(&mut self.data.get_unchecked_mut(index).1) },
            Err(_) => None,
        }
    }

    /// Gets a mutable reference to the value in the entry, or insert a new one.
    #[inline]
    pub fn get_mut_or_insert_default(&mut self, key: K) -> &mut V
    where
        K: Eq,
        V: Default,
    {
        let index = match self.lookup_index_for(&key) {
            Ok(index) => index,
            Err(index) => {
                self.data.insert(index, (key, V::default()));
                index
            }
        };
        unsafe { &mut self.data.get_unchecked_mut(index).1 }
    }

    /// Construct a `SortedMap` from a presorted set of elements. This is faster
    /// than creating an empty map and then inserting the elements individually.
    ///
    /// It is up to the caller to make sure that the elements are sorted by key
    /// and that there are no duplicates.
    #[inline]
    pub fn from_presorted_elements(elements: Vec<(K, V)>) -> SortedMap<K, V> {
        SortedMap { data: elements, ph1: PhantomData, ph2: PhantomData }
    }

    #[inline]
    pub fn insert(&mut self, key: K, mut value: V) -> Option<V> {
        match self.lookup_index_for(&key) {
            Ok(index) => {
                let slot = unsafe { self.data.get_unchecked_mut(index) };
                mem::swap(&mut slot.1, &mut value);
                Some(value)
            }
            Err(index) => {
                self.data.insert(index, (key, value));
                None
            }
        }
    }

    #[inline]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        match self.lookup_index_for(key) {
            Ok(index) => Some(self.data.remove(index).1),
            Err(_) => None,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Iterate over elements, sorted by key
    #[inline]
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, (K, V)> {
        self.data.iter_mut()
    }

    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit()
    }

    #[inline]
    pub fn remove_range<R>(&mut self, range: R)
    where
        R: RangeBounds<K>,
    {
        let (start, end) = self.range_slice_indices(range);
        self.data.splice(start..end, std::iter::empty());
    }

    /// Mutate all keys with the given function `f`. This mutation must not
    /// change the sort-order of keys.
    #[inline]
    pub fn offset_keys<F>(&mut self, f: F)
    where
        F: Fn(&mut K),
    {
        self.data
            .iter_mut()
            .map(|&mut (ref mut k, _)| k)
            .for_each(f);
    }

    /// Inserts a presorted range of elements into the map. If the range can be
    /// inserted as a whole in between to existing elements of the map, this
    /// will be faster than inserting the elements individually.
    ///
    /// It is up to the caller to make sure that the elements are sorted by key
    /// and that there are no duplicates.
    #[inline]
    pub fn insert_presorted(&mut self, elements: Vec<(K, V)>) {
        if elements.is_empty() {
            return;
        }

        // debug_assert!(elements.array_windows().all(|[fst, snd]| fst.0 < snd.0));

        let start_index = self.lookup_index_for(&elements[0].0);

        let elements = match start_index {
            Ok(index) => {
                let mut elements = elements.into_iter();
                self.data[index] = elements.next().unwrap();
                elements
            }
            Err(index) => {
                if index == self.data.len() || elements.last().unwrap().0 < self.data[index].0 {
                    // We can copy the whole range without having to mix with
                    // existing elements.
                    self.data
                        .splice(index..index, elements.into_iter());
                    return;
                }

                let mut elements = elements.into_iter();
                self.data
                    .insert(index, elements.next().unwrap());
                elements
            }
        };

        // Insert the rest
        for (k, v) in elements {
            self.insert(k, v);
        }
    }
}

impl<K: Ord, V, C: AsRef<[(K, V)]>> SortedMap<K, V, C> {
    #[inline]
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match self.lookup_index_for(key) {
            Ok(index) => Some(&self.data.as_ref()[index].1),
            Err(_) => None,
        }
    }

    /// Iterate over elements, sorted by key
    #[inline]
    pub fn iter(&self) -> std::slice::Iter<'_, (K, V)> {
        self.data.as_ref().iter()
    }

    /// Iterate over the keys, sorted
    #[inline]
    pub fn keys(&self) -> impl Iterator<Item = &K> + ExactSizeIterator + DoubleEndedIterator {
        self.iter().map(|&(ref k, _)| k)
    }

    /// Iterate over values, sorted by key
    #[inline]
    pub fn values(&self) -> impl Iterator<Item = &V> + ExactSizeIterator + DoubleEndedIterator {
        self.iter().map(|&(_, ref v)| v)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.as_ref().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn range<R>(&self, range: R) -> &[(K, V)]
    where
        R: RangeBounds<K>,
    {
        let (start, end) = self.range_slice_indices(range);
        &self.data.as_ref()[start..end]
    }

    /// Looks up the key in `self.data` via `slice::binary_search()`.
    #[inline(always)]
    pub fn lookup_index_for<Q>(&self, key: &Q) -> Result<usize, usize>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.data
            .as_ref()
            .binary_search_by(|&(ref x, _)| x.borrow().cmp(key))
    }

    #[inline]
    pub fn get_entry_at(&self, index: usize) -> Option<&(K, V)> {
        self.data.as_ref().get(index)
    }

    #[inline]
    pub fn get_entry_at_unchecked(&self, index: usize) -> &(K, V) {
        self.data.as_ref().get(index).unwrap()
    }

    #[inline]
    fn range_slice_indices<R>(&self, range: R) -> (usize, usize)
    where
        R: RangeBounds<K>,
    {
        let start = match range.start_bound() {
            Bound::Included(ref k) => match self.lookup_index_for(k) {
                Ok(index) | Err(index) => index,
            },
            Bound::Excluded(ref k) => match self.lookup_index_for(k) {
                Ok(index) => index + 1,
                Err(index) => index,
            },
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(ref k) => match self.lookup_index_for(k) {
                Ok(index) => index + 1,
                Err(index) => index,
            },
            Bound::Excluded(ref k) => match self.lookup_index_for(k) {
                Ok(index) | Err(index) => index,
            },
            Bound::Unbounded => self.len(),
        };

        (start, end)
    }

    #[inline]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.get(key).is_some()
    }
}

impl<'a, K, Q, V, C: AsRef<[(K, V)]>> Index<&'a Q> for SortedMap<K, V, C>
where
    K: Ord + Borrow<Q>,
    Q: Ord + ?Sized,
{
    type Output = V;

    fn index(&self, key: &Q) -> &Self::Output {
        self.get(key).expect("no entry found for key")
    }
}
