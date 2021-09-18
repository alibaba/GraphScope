use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::ops::{Add, AddAssign, Div};

use nohash_hasher::IntSet;
use pegasus_common::codec::Encode;

use crate::api::notification::EndScope;
use crate::codec::{Decode, ReadExt, WriteExt};
use crate::Tag;

#[derive(Clone, Debug)]
enum Mask {
    Single(u32),
    Partial(HashSet<u32>),
    All(u32),
}

impl PartialEq for Mask {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Mask::Single(a), Mask::Single(b)) => a == b,
            (Mask::Single(a), Mask::Partial(b)) => b.len() == 1 && b.contains(a),
            (Mask::Single(a), Mask::All(b)) => *a == 0 && *b == 1,
            (Mask::Partial(a), Mask::Single(b)) => a.len() == 1 && a.contains(b),
            (Mask::Partial(a), Mask::Partial(b)) => a == b,
            (Mask::Partial(a), Mask::All(b)) => {
                if a.len() as u32 == *b {
                    for i in 0..*b {
                        if !a.contains(&i) {
                            return false;
                        }
                    }
                    true
                } else {
                    false
                }
            }
            (Mask::All(a), Mask::Single(b)) => *a == 1 && *b == 0,
            (Mask::All(a), Mask::Partial(b)) => {
                if b.len() == *a as usize {
                    for i in 0..*a {
                        if !b.contains(&i) {
                            return false;
                        }
                    }
                    true
                } else {
                    false
                }
            }
            (Mask::All(a), Mask::All(b)) => *a == *b,
        }
    }
}

impl PartialOrd for Mask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Mask::Single(a), Mask::Single(b)) => {
                if a == b {
                    Some(Ordering::Equal)
                } else {
                    None
                }
            }
            (Mask::Single(a), Mask::Partial(b)) => {
                if b.contains(a) {
                    if b.len() == 1 {
                        Some(Ordering::Equal)
                    } else {
                        Some(Ordering::Less)
                    }
                } else {
                    None
                }
            }
            (Mask::Single(_), Mask::All(_)) => Some(Ordering::Less),
            (Mask::Partial(a), Mask::Single(b)) => {
                if a.contains(b) {
                    if a.len() == 1 {
                        Some(Ordering::Equal)
                    } else {
                        Some(Ordering::Greater)
                    }
                } else {
                    None
                }
            }
            (Mask::Partial(a), Mask::Partial(b)) => {
                if a == b {
                    return Some(Ordering::Equal);
                }

                if a.is_subset(b) {
                    Some(Ordering::Less)
                } else if a.is_superset(b) {
                    Some(Ordering::Greater)
                } else {
                    None
                }
            }
            (Mask::Partial(a), Mask::All(b)) => {
                if a.len() as u32 == *b {
                    for i in 0..*b {
                        if !a.contains(&i) {
                            return None;
                        }
                    }
                    Some(Ordering::Equal)
                } else if a.len() < *b as usize {
                    Some(Ordering::Less)
                } else {
                    None
                }
            }
            (Mask::All(_), Mask::Single(_)) => Some(Ordering::Greater),
            (Mask::All(a), Mask::Partial(b)) => {
                if *a == b.len() as u32 {
                    for i in 0..*a {
                        if !b.contains(&i) {
                            return None;
                        }
                    }
                    Some(Ordering::Equal)
                } else {
                    None
                }
            }
            (Mask::All(a), Mask::All(b)) => {
                if *a == *b {
                    Some(Ordering::Equal)
                } else {
                    None
                }
            }
        }
    }
}

impl Eq for Mask {}

#[derive(Clone, PartialEq, Eq, PartialOrd)]
pub struct Weight {
    mask: Mask,
}

impl Weight {
    pub fn single(index: u32) -> Self {
        Weight { mask: Mask::Single(index) }
    }

    pub fn partial_empty() -> Self {
        Weight { mask: Mask::Partial(Default::default()) }
    }

    pub fn partial_current() -> Self {
        let mut set: HashSet<u32> = Default::default();
        let idx = crate::worker_id::get_current_worker().index;
        set.insert(idx);
        Weight { mask: Mask::Partial(set) }
    }

    pub fn all() -> Self {
        let peers = crate::worker_id::get_current_worker().total_peers();
        Weight { mask: Mask::All(peers) }
    }

    pub fn add_source(&mut self, worker: u32) {
        match self.mask {
            Mask::Single(_) => unreachable!("single weight can't add source"),
            Mask::Partial(ref mut set) => {
                set.insert(worker);
            }
            Mask::All(_) => {}
        }
    }

    pub fn contains_source(&self, worker: u32) -> bool {
        match &self.mask {
            Mask::Single(id) => *id == worker,
            Mask::Partial(set) => set.contains(&worker),
            Mask::All(_) => true,
        }
    }

    #[inline]
    pub fn value(&self) -> usize {
        match self.mask {
            Mask::Single(_) => 1,
            Mask::Partial(ref set) => set.len(),
            Mask::All(ref p) => *p as usize,
        }
    }

    pub fn merge(&mut self, other: Weight) {
        match (&mut self.mask, other.mask) {
            (Mask::Single(a), Mask::Single(b)) => {
                *a = b;
            }
            (Mask::Single(a), Mask::Partial(mut b)) => {
                b.insert(*a);
                self.mask = Mask::Partial(b);
            }
            (Mask::Single(_), Mask::All(b)) => {
                self.mask = Mask::All(b);
            }
            (Mask::Partial(a), Mask::Single(b)) => {
                a.insert(b);
            }
            (Mask::Partial(a), Mask::Partial(b)) => {
                a.extend(b);
            }
            (Mask::Partial(_), Mask::All(b)) => {
                self.mask = Mask::All(b);
            }
            _ => (),
        }
    }
}

impl Debug for Weight {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.mask {
            Mask::Single(x) => write!(f, "{}", x),
            Mask::Partial(ref p) => write!(f, "{:?}", p),
            Mask::All(_) => write!(f, "All"),
        }
    }
}

impl Encode for Weight {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match &self.mask {
            Mask::Single(x) => {
                writer.write_u32(0)?;
                writer.write_u32(*x)
            }
            Mask::Partial(s) => {
                writer.write_u32(1)?;
                writer.write_u32(s.len() as u32)?;
                for x in s.iter() {
                    writer.write_u32(*x)?;
                }
                Ok(())
            }
            Mask::All(a) => {
                writer.write_u32(2)?;
                writer.write_u32(*a)
            }
        }
    }
}

impl Decode for Weight {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let mode = reader.read_u32()?;
        if mode == 0 {
            let x = reader.read_u32()?;
            Ok(Weight { mask: Mask::Single(x) })
        } else if mode == 1 {
            let len = reader.read_u32()? as usize;
            let mut set = HashSet::with_capacity(len);
            for _ in 0..len {
                let x = reader.read_u32()?;
                set.insert(x);
            }
            Ok(Weight { mask: Mask::Partial(set) })
        } else if mode == 2 {
            let x = reader.read_u32()?;
            Ok(Weight { mask: Mask::All(x) })
        } else {
            Err(std::io::ErrorKind::InvalidData)?
        }
    }
}

#[derive(Clone)]
pub struct EndSignal {
    pub tag: Tag,
    pub seq: u64,
    pub(crate) source_weight: Weight,
    pub(crate) update_weight: Option<Weight>,
}

impl From<EndScope> for EndSignal {
    fn from(end: EndScope) -> Self {
        EndSignal::new(end.tag, end.weight)
    }
}

impl EndSignal {
    pub fn new(tag: Tag, weight: Weight) -> Self {
        EndSignal { tag, seq: 0, source_weight: weight, update_weight: None }
    }

    pub fn update_to(&mut self, weight: Weight) {
        self.update_weight = Some(weight);
    }

    pub fn update(&mut self) {
        if let Some(weight) = self.update_weight.take() {
            self.source_weight = weight;
        }
    }

    pub fn take(self) -> (Tag, Weight, Option<Weight>) {
        (self.tag, self.source_weight, self.update_weight)
    }
}

impl Debug for EndSignal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(w) = self.update_weight.as_ref() {
            write!(f, "EOS({:?}: {:?}=>{:?})", self.tag, self.source_weight, w)
        } else {
            write!(f, "EOS({:?}: {:?})", self.tag, self.source_weight)
        }
    }
}

impl Encode for EndSignal {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.tag.write_to(writer)?;
        writer.write_u64(self.seq)?;
        self.source_weight.write_to(writer)?;
        self.update_weight.write_to(writer)
    }
}

impl Decode for EndSignal {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let tag = Tag::read_from(reader)?;
        let seq = reader.read_u64()?;
        let source_weight = Weight::read_from(reader)?;
        let update_weight = Option::<Weight>::read_from(reader)?;
        Ok(EndSignal { tag, seq, source_weight, update_weight })
    }
}

/// represent as (a / b)
#[allow(dead_code)]
#[derive(Copy, Clone)]
pub struct Fraction(u64, u64);

#[allow(dead_code)]
impl Fraction {
    pub fn new(numerator: u64, denominator: u64) -> Self {
        assert_ne!(numerator, 0);
        assert_ne!(denominator, 0);
        Fraction(numerator, denominator)
    }

    pub fn as_f64(&self) -> f64 {
        self.1 as f64 / self.0 as f64
    }
}

impl Add for Fraction {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let d = self.1 * rhs.1;
        let n = self.0 * rhs.1 + self.1 * rhs.0;
        Fraction(n, d)
    }
}

impl AddAssign for Fraction {
    fn add_assign(&mut self, rhs: Self) {
        let d = self.1 * rhs.1;
        let n = self.0 * rhs.1 + self.1 * rhs.0;
        self.0 = n;
        self.1 = d;
    }
}

impl Div<u64> for Fraction {
    type Output = Self;

    fn div(self, rhs: u64) -> Self::Output {
        let d = self.1 * rhs;
        Fraction(self.0, d)
    }
}

impl PartialEq for Fraction {
    fn eq(&self, other: &Self) -> bool {
        (self.1 == other.1 && self.0 == other.0) || (self.as_f64() == other.as_f64())
    }
}

impl Eq for Fraction {}

impl PartialEq<f64> for Fraction {
    fn eq(&self, other: &f64) -> bool {
        let r = self.1 as f64 / self.0 as f64;
        r == *other
    }
}

impl Debug for Fraction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{} / {}]", self.0, self.1)
    }
}

impl Encode for Fraction {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64(self.0)?;
        writer.write_u64(self.1)
    }
}

impl Decode for Fraction {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let n = reader.read_u64()?;
        let d = reader.read_u64()?;
        Ok(Fraction::new(n, d))
    }
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct ScopeEnd {
    tag: Tag,
    last_seq: u64,
    score: Fraction,
    children: IntSet<u32>,
}

#[allow(dead_code)]
impl ScopeEnd {
    pub fn new(tag: Tag, last_seq: u64, score: Fraction) -> Self {
        ScopeEnd { tag, last_seq, score, children: IntSet::default() }
    }

    pub fn update_tag(&mut self, tag: Tag) {
        self.tag = tag;
    }

    pub fn add_child(&mut self, child: u32) {
        self.children.insert(child);
    }

    pub fn score(&self) -> &Fraction {
        &self.score
    }

    pub fn tag(&self) -> &Tag {
        &self.tag
    }

    pub fn children(&self) -> &IntSet<u32> {
        &self.children
    }
}

impl Debug for ScopeEnd {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "End<{:?}({:?})>", self.tag, self.score)
    }
}

impl Encode for ScopeEnd {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.tag.write_to(writer)?;
        self.score.write_to(writer)?;
        writer.write_u32(self.children.len() as u32)?;
        for child in self.children.iter() {
            writer.write_u32(*child)?;
        }
        writer.write_u64(self.last_seq)
    }
}

impl Decode for ScopeEnd {
    fn read_from<R: ReadExt>(_reader: &mut R) -> std::io::Result<Self> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn mask_eq_test() {
        assert_eq!(Mask::Single(0), Mask::Single(0));
        assert_ne!(Mask::Single(0), Mask::Single(1));

        let mut set = HashSet::new();
        set.insert(1);
        assert_eq!(Mask::Single(1), Mask::Partial(set));

        let mut set = HashSet::new();
        set.insert(1);
        set.insert(2);
        assert_ne!(Mask::Single(1), Mask::Partial(set));

        assert_eq!(Mask::Single(0), Mask::All(1));
        assert_ne!(Mask::Single(1), Mask::All(1));

        let mut set = HashSet::new();
        set.insert(1);
        assert_eq!(Mask::Partial(set), Mask::Single(1));

        let mut set1 = HashSet::new();
        set1.insert(0);
        set1.insert(1);
        let mut set2 = HashSet::new();
        set2.insert(0);
        set2.insert(1);
        assert_eq!(Mask::Partial(set1), Mask::Partial(set2));

        let mut set = HashSet::new();
        set.insert(0);
        set.insert(1);
        set.insert(2);
        assert_eq!(Mask::Partial(set), Mask::All(3));

        let mut set1 = HashSet::new();
        set1.insert(0);
        set1.insert(1);
        let mut set2 = HashSet::new();
        set2.insert(0);
        set2.insert(1);
        set2.insert(2);
        assert_ne!(Mask::Partial(set1), Mask::Partial(set2));

        assert_eq!(Mask::All(1), Mask::Single(0));
        assert_eq!(Mask::All(3), Mask::All(3));
        let mut set = HashSet::new();
        set.insert(0);
        set.insert(1);
        set.insert(2);
        assert_eq!(Mask::All(3), Mask::Partial(set));
    }

    #[test]
    fn mask_cmp_test() {
        let mut set1 = HashSet::new();
        set1.insert(0);
        set1.insert(1);
        let mut set2 = HashSet::new();
        set2.insert(0);
        set2.insert(1);
        set2.insert(2);
        assert!(Mask::Partial(set1) <= Mask::Partial(set2));

        let mut set1 = HashSet::new();
        set1.insert(0);
        set1.insert(1);
        set1.insert(2);
        let mut set2 = HashSet::new();
        set2.insert(0);
        set2.insert(1);
        set2.insert(2);
        let a = Mask::Partial(set1);
        let b = Mask::Partial(set2);
        assert!(a <= b);
        assert!(a >= b);
        assert_eq!(a, b);

        let mut set2 = HashSet::new();
        set2.insert(0);
        set2.insert(1);
        set2.insert(2);
        assert!(Mask::Partial(set2) <= Mask::All(4));

        let mut set1 = HashSet::new();
        set1.insert(0);
        set1.insert(1);
        set1.insert(3);
        let mut set2 = HashSet::new();
        set2.insert(0);
        set2.insert(1);
        set2.insert(2);

        assert!(!(Mask::Partial(set1) <= Mask::Partial(set2)));
    }
}
