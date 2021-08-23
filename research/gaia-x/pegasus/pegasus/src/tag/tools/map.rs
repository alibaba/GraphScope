use std::borrow::Cow;

use ahash::AHashMap;
use nohash_hasher::IntMap;

use crate::Tag;

pub struct TidyTagMap<V> {
    pub scope_level: u32,
    root: Option<V>,
    first: IntMap<u32, V>,
    others: AHashMap<Tag, V>,
}

impl<V> TidyTagMap<V> {
    pub fn new(scope_level: u32) -> Self {
        TidyTagMap { scope_level, root: None, first: IntMap::default(), others: AHashMap::new() }
    }
}

impl<V> Default for TidyTagMap<V> {
    fn default() -> Self {
        TidyTagMap::new(0)
    }
}

impl<V> TidyTagMap<V> {
    pub fn insert(&mut self, tag: Tag, value: V) -> Option<V> {
        if tag.len() == self.scope_level as usize {
            match tag {
                Tag::Root => self.root.replace(value),
                Tag::One(t) => self.first.insert(t, value),
                _ => self.others.insert(tag, value),
            }
        } else {
            Some(value)
        }
    }

    pub fn get(&self, tag: &Tag) -> Option<&V> {
        if tag.len() == self.scope_level as usize {
            match tag {
                Tag::Root => self.root.as_ref(),
                Tag::One(v) => self.first.get(v),
                _ => self.others.get(tag),
            }
        } else {
            None
        }
    }

    pub fn get_mut(&mut self, tag: &Tag) -> Option<&mut V> {
        if tag.len() == self.scope_level as usize {
            match tag {
                Tag::Root => self.root.as_mut(),
                Tag::One(v) => self.first.get_mut(v),
                _ => self.others.get_mut(tag),
            }
        } else {
            None
        }
    }

    pub fn get_mut_or_else<F>(&mut self, tag: &Tag, or: F) -> &mut V
    where
        F: FnOnce() -> V,
    {
        match tag {
            Tag::Root => self.root.get_or_insert(or()),
            Tag::One(v) => self.first.entry(*v).or_insert_with(or),
            _ => self
                .others
                .entry(tag.clone())
                .or_insert_with(or),
        }
    }

    pub fn remove(&mut self, tag: &Tag) -> Option<V> {
        if tag.len() == self.scope_level as usize {
            match tag {
                Tag::Root => self.root.take(),
                Tag::One(v) => self.first.remove(v),
                _ => self.others.remove(tag),
            }
        } else {
            None
        }
    }

    pub fn contains_key(&self, tag: &Tag) -> bool {
        if tag.len() == self.scope_level as usize {
            match tag {
                Tag::Root => self.root.is_some(),
                Tag::One(v) => self.first.contains_key(v),
                _ => self.others.contains_key(tag),
            }
        } else {
            false
        }
    }

    pub fn is_empty(&self) -> bool {
        if self.scope_level == 0 {
            self.root.is_none()
        } else if self.scope_level == 1 {
            self.first.is_empty()
        } else {
            self.others.is_empty()
        }
    }

    pub fn iter(&self) -> Iter<V> {
        if self.scope_level == 0 {
            Iter::Root(self.root.iter())
        } else if self.scope_level == 1 {
            Iter::First(self.first.iter())
        } else {
            Iter::Others(self.others.iter())
        }
    }

    pub fn iter_mut(&mut self) -> IterMut<V> {
        if self.scope_level == 0 {
            IterMut::Root(self.root.iter_mut())
        } else if self.scope_level == 1 {
            IterMut::First(self.first.iter_mut())
        } else {
            IterMut::Others(self.others.iter_mut())
        }
    }

    pub fn retain<F>(&mut self, mut func: F)
    where
        F: FnMut(&Tag, &mut V) -> bool,
    {
        if self.scope_level == 0 {
            if let Some(mut v) = self.root.take() {
                if func(&Tag::Root, &mut v) {
                    self.root = Some(v);
                }
            }
        } else if self.scope_level == 1 {
            self.first.retain(|t, v| func(&Tag::One(*t), v));
        } else {
            self.others.retain(|t, v| func(t, v))
        }
    }

    pub fn len(&self) -> usize {
        if self.scope_level == 0 {
            if self.root.is_some() {
                1
            } else {
                0
            }
        } else if self.scope_level == 1 {
            self.first.len()
        } else {
            self.others.len()
        }
    }
}

impl<V: Default> TidyTagMap<V> {
    pub fn get_mut_or_insert(&mut self, tag: &Tag) -> &mut V {
        match tag {
            Tag::Root => self.root.get_or_insert_with(V::default),
            Tag::One(v) => self.first.entry(*v).or_insert_with(V::default),
            _ => self
                .others
                .entry(tag.clone())
                .or_insert_with(V::default),
        }
    }
}

pub enum Iter<'a, V> {
    Root(std::option::Iter<'a, V>),
    First(std::collections::hash_map::Iter<'a, u32, V>),
    Others(std::collections::hash_map::Iter<'a, Tag, V>),
}

impl<'a, V> Iterator for Iter<'a, V> {
    type Item = (Cow<'a, Tag>, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Root(r) => r.next().map(|x| (Cow::Owned(Tag::Root), x)),
            Iter::First(x) => x
                .next()
                .map(|(t, x)| (Cow::Owned(Tag::One(*t)), x)),
            Iter::Others(x) => x.next().map(|(t, x)| (Cow::Borrowed(t), x)),
        }
    }
}

pub enum IterMut<'a, V> {
    Root(std::option::IterMut<'a, V>),
    First(std::collections::hash_map::IterMut<'a, u32, V>),
    Others(std::collections::hash_map::IterMut<'a, Tag, V>),
}

impl<'a, V> Iterator for IterMut<'a, V> {
    type Item = (Cow<'a, Tag>, &'a mut V);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            IterMut::Root(r) => r.next().map(|x| (Cow::Owned(Tag::Root), x)),
            IterMut::First(x) => x
                .next()
                .map(|(t, x)| (Cow::Owned(Tag::One(*t)), x)),
            IterMut::Others(x) => x.next().map(|(t, x)| (Cow::Borrowed(t), x)),
        }
    }
}
