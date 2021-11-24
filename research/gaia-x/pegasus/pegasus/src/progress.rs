use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::ops::{Add, AddAssign, Div};

use nohash_hasher::IntSet;
use pegasus_common::codec::Encode;

use crate::codec::{Decode, ReadExt, WriteExt};
use crate::Tag;

#[derive(Clone, Debug)]
enum PeerSet {
    None,
    Single(u32),
    // TODO: use bit set instead;
    Partial(HashSet<u32>),
    All(u32),
}

impl PartialEq for PeerSet {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PeerSet::None, PeerSet::None) => true,
            (PeerSet::None, _) => false,
            (_, PeerSet::None) => false,
            (PeerSet::Single(a), PeerSet::Single(b)) => a == b,
            (PeerSet::Single(a), PeerSet::Partial(b)) => b.len() == 1 && b.contains(a),
            (PeerSet::Single(a), PeerSet::All(b)) => *a == 0 && *b == 1,
            (PeerSet::Partial(a), PeerSet::Single(b)) => a.len() == 1 && a.contains(b),
            (PeerSet::Partial(a), PeerSet::Partial(b)) => a == b,
            (PeerSet::Partial(a), PeerSet::All(b)) => {
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
            (PeerSet::All(a), PeerSet::Single(b)) => *a == 1 && *b == 0,
            (PeerSet::All(a), PeerSet::Partial(b)) => {
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
            (PeerSet::All(a), PeerSet::All(b)) => *a == *b,
        }
    }
}

impl Eq for PeerSet {}

#[derive(Clone, PartialEq, Eq)]
pub struct DynPeers {
    mask: PeerSet,
}

impl DynPeers {
    pub fn single(index: u32) -> Self {
        DynPeers { mask: PeerSet::Single(index) }
    }

    pub fn empty() -> Self {
        DynPeers { mask: PeerSet::None }
    }

    pub fn partial_current() -> Self {
        let mut set: HashSet<u32> = Default::default();
        let idx = crate::worker_id::get_current_worker().index;
        set.insert(idx);
        DynPeers { mask: PeerSet::Partial(set) }
    }

    pub fn all() -> Self {
        let peers = crate::worker_id::get_current_worker().total_peers();
        DynPeers { mask: PeerSet::All(peers) }
    }

    pub fn add_source(&mut self, worker: u32) {
        let mask = std::mem::replace(&mut self.mask, PeerSet::None);
        match mask {
            PeerSet::None => self.mask = PeerSet::Single(worker),
            PeerSet::Single(a) => {
                let mut set = HashSet::with_capacity(2);
                set.insert(a);
                set.insert(worker);
                self.mask = PeerSet::Partial(set)
            }
            PeerSet::Partial(mut set) => {
                set.insert(worker);
                self.mask = PeerSet::Partial(set)
            }
            PeerSet::All(a) => self.mask = PeerSet::All(a),
        }
    }

    pub fn contains_source(&self, worker: u32) -> bool {
        match &self.mask {
            PeerSet::None => false,
            PeerSet::Single(id) => *id == worker,
            PeerSet::Partial(set) => set.contains(&worker),
            PeerSet::All(_) => true,
        }
    }

    #[inline]
    pub fn value(&self) -> usize {
        match self.mask {
            PeerSet::None => 0,
            PeerSet::Single(_) => 1,
            PeerSet::Partial(ref set) => set.len(),
            PeerSet::All(ref p) => *p as usize,
        }
    }

    pub fn merge(&mut self, other: DynPeers) {
        let mask = std::mem::replace(&mut self.mask, PeerSet::None);
        match (mask, other.mask) {
            (PeerSet::None, _other) => {
                self.mask = _other;
            }
            (my, PeerSet::None) => {
                self.mask = my;
            }
            (PeerSet::Single(a), PeerSet::Single(b)) => {
                let mut set = HashSet::with_capacity(2);
                set.insert(a);
                set.insert(b);
                self.mask = PeerSet::Partial(set);
            }
            (PeerSet::Single(a), PeerSet::Partial(mut b)) => {
                b.insert(a);
                self.mask = PeerSet::Partial(b);
            }
            (PeerSet::Partial(mut a), PeerSet::Single(b)) => {
                a.insert(b);
                self.mask = PeerSet::Partial(a);
            }
            (PeerSet::Partial(mut a), PeerSet::Partial(b)) => {
                a.extend(b);
                self.mask = PeerSet::Partial(a);
            }
            (_, PeerSet::All(b)) => {
                self.mask = PeerSet::All(b);
            }
            (PeerSet::All(a), _) => {
                self.mask = PeerSet::All(a);
            }
        }
    }
}

impl Debug for DynPeers {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.mask {
            PeerSet::None => write!(f, "P[]"),
            PeerSet::Single(x) => write!(f, "P[{}]", x),
            PeerSet::Partial(ref p) => write!(f, "P[{:?}]", p),
            PeerSet::All(_) => write!(f, "All"),
        }
    }
}

impl Encode for DynPeers {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match &self.mask {
            PeerSet::None => writer.write_u32(0),
            PeerSet::Single(x) => {
                writer.write_u32(1)?;
                writer.write_u32(*x)
            }
            PeerSet::Partial(s) => {
                writer.write_u32(2)?;
                writer.write_u32(s.len() as u32)?;
                for x in s.iter() {
                    writer.write_u32(*x)?;
                }
                Ok(())
            }
            PeerSet::All(a) => {
                writer.write_u32(3)?;
                writer.write_u32(*a)
            }
        }
    }
}

impl Decode for DynPeers {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let mode = reader.read_u32()?;
        if mode == 0 {
            Ok(DynPeers { mask: PeerSet::None })
        } else if mode == 1 {
            let x = reader.read_u32()?;
            Ok(DynPeers { mask: PeerSet::Single(x) })
        } else if mode == 2 {
            let len = reader.read_u32()? as usize;
            let mut set = HashSet::with_capacity(len);
            for _ in 0..len {
                let x = reader.read_u32()?;
                set.insert(x);
            }
            Ok(DynPeers { mask: PeerSet::Partial(set) })
        } else if mode == 3 {
            let x = reader.read_u32()?;
            Ok(DynPeers { mask: PeerSet::All(x) })
        } else {
            Err(std::io::ErrorKind::InvalidData)?
        }
    }
}

#[derive(Clone)]
pub struct EndOfScope {
    /// The tag of scope this end belongs to;
    pub(crate) tag: Tag,
    /// Record how many data has send to a consumer;
    pub(crate) total_send: u64,
    /// Record how many data has send to all consumers;
    pub(crate) global_total_send: u64,
    /// The worker peers who also has send data(and end) of this scope;
    /// It indicates how many the `[EndOfScope]` will be received by consumers;
    peers: DynPeers,
}

impl EndOfScope {
    pub(crate) fn new(tag: Tag, source: DynPeers, total_send: u64, global_total_send: u64) -> Self {
        EndOfScope { tag, peers: source, total_send, global_total_send }
    }

    pub(crate) fn merge(&mut self, other: EndOfScope) {
        assert_eq!(self.tag, other.tag);
        self.peers.merge(other.peers);
        self.total_send += other.total_send;
        self.global_total_send += other.global_total_send;
    }

    pub fn update_peers(&mut self, mut peers: DynPeers) {
        if peers.value() == 0 {
            let owner = if self.tag.len() == 0 {
                0
            } else {
                let peers = crate::worker_id::get_current_worker().total_peers();
                self.tag.current_uncheck() % peers
            };
            peers = DynPeers::single(owner);
        }
        trace_worker!("update peers from {:?} to {:?} of scope {:?}", self.peers, peers, self.tag);
        self.peers = peers;
    }

    pub fn peers_contains(&self, index: u32) -> bool {
        self.peers.contains_source(index)
    }

    pub fn peers(&self) -> &DynPeers {
        &self.peers
    }

    pub(crate) fn contains_source(&self, src: u32) -> bool {
        self.peers.value() > 0 && self.peers.contains_source(src)
    }
}

impl Debug for EndOfScope {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "end({:?}_{})", self.tag, self.total_send)
    }
}

impl Encode for EndOfScope {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.tag.write_to(writer)?;
        self.peers.write_to(writer)?;
        writer.write_u64(self.total_send)?;
        writer.write_u64(self.global_total_send)
    }
}

impl Decode for EndOfScope {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let tag = Tag::read_from(reader)?;
        let peers = DynPeers::read_from(reader)?;
        let total_send = reader.read_u64()?;
        let global_total_send = reader.read_u64()?;
        Ok(EndOfScope { tag, peers, total_send, global_total_send })
    }
}

#[derive(Clone, Debug)]
pub struct EndSyncSignal {
    end: EndOfScope,
    children: DynPeers,
}

impl EndSyncSignal {
    pub fn new(end: EndOfScope, children: DynPeers) -> Self {
        EndSyncSignal { end, children }
    }

    pub fn merge_children(&mut self, other: DynPeers) {
        self.children.merge(other)
    }

    pub fn set_push_count(&mut self, count: u64) {
        self.end.total_send = count;
    }

    pub fn push_count(&self) -> u64 {
        self.end.total_send
    }

    pub fn sources(&self) -> usize {
        self.end.peers.value()
    }

    pub fn tag(&self) -> &Tag {
        &self.end.tag
    }

    pub fn into_end(mut self) -> EndOfScope {
        let child = std::mem::replace(&mut self.children, DynPeers::empty());
        self.end.peers = child;
        self.end
    }

    pub fn take(self) -> (EndOfScope, DynPeers) {
        (self.end, self.children)
    }
}

impl Encode for EndSyncSignal {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.end.write_to(writer)?;
        self.children.write_to(writer)
    }
}

impl Decode for EndSyncSignal {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let end = EndOfScope::read_from(reader)?;
        let children = DynPeers::read_from(reader)?;
        Ok(EndSyncSignal { end, children })
    }
}

/////////////////////////// future use ////////////////////////////////

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
        assert_eq!(PeerSet::Single(0), PeerSet::Single(0));
        assert_ne!(PeerSet::Single(0), PeerSet::Single(1));

        let mut set = HashSet::new();
        set.insert(1);
        assert_eq!(PeerSet::Single(1), PeerSet::Partial(set));

        let mut set = HashSet::new();
        set.insert(1);
        set.insert(2);
        assert_ne!(PeerSet::Single(1), PeerSet::Partial(set));

        assert_eq!(PeerSet::Single(0), PeerSet::All(1));
        assert_ne!(PeerSet::Single(1), PeerSet::All(1));

        let mut set = HashSet::new();
        set.insert(1);
        assert_eq!(PeerSet::Partial(set), PeerSet::Single(1));

        let mut set1 = HashSet::new();
        set1.insert(0);
        set1.insert(1);
        let mut set2 = HashSet::new();
        set2.insert(0);
        set2.insert(1);
        assert_eq!(PeerSet::Partial(set1), PeerSet::Partial(set2));

        let mut set = HashSet::new();
        set.insert(0);
        set.insert(1);
        set.insert(2);
        assert_eq!(PeerSet::Partial(set), PeerSet::All(3));

        let mut set1 = HashSet::new();
        set1.insert(0);
        set1.insert(1);
        let mut set2 = HashSet::new();
        set2.insert(0);
        set2.insert(1);
        set2.insert(2);
        assert_ne!(PeerSet::Partial(set1), PeerSet::Partial(set2));

        assert_eq!(PeerSet::All(1), PeerSet::Single(0));
        assert_eq!(PeerSet::All(3), PeerSet::All(3));
        let mut set = HashSet::new();
        set.insert(0);
        set.insert(1);
        set.insert(2);
        assert_eq!(PeerSet::All(3), PeerSet::Partial(set));
    }
}
