use crate::codec::{Decode, ReadExt, WriteExt};
use crate::Tag;
use pegasus_common::codec::Encode;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};

#[derive(Clone, Debug)]
enum Mask {
    Single,
    Partial(HashSet<u32>),
    All(u32),
}

impl PartialEq for Mask {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Mask::Single => match other {
                Mask::Single => true,
                Mask::Partial(p) => p.len() == 1,
                Mask::All(p) => *p == 1,
            },
            Mask::Partial(p) => match other {
                Mask::Single => p.len() == 1,
                Mask::Partial(o) => p == o,
                Mask::All(a) => p.len() == *a as usize,
            },
            Mask::All(a) => match other {
                Mask::Single => *a == 1,
                Mask::Partial(p) => p.len() == *a as usize,
                Mask::All(o) => a == o,
            },
        }
    }
}

impl Eq for Mask {}

#[derive(Clone, PartialEq, Eq)]
pub struct Weight {
    mask: Mask,
}

impl Weight {
    pub fn single() -> Self {
        Weight { mask: Mask::Single }
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
            Mask::Single => unreachable!("single weight can't add source"),
            Mask::Partial(ref mut set) => {
                set.insert(worker);
            }
            Mask::All(_) => {}
        }
    }

    #[inline]
    pub fn value(&self) -> usize {
        match self.mask {
            Mask::Single => 1,
            Mask::Partial(ref set) => set.len(),
            Mask::All(ref p) => *p as usize,
        }
    }

    pub fn merge(&mut self, other: Weight) {
        let mask = std::mem::replace(&mut self.mask, Mask::Single);
        match mask {
            Mask::Single => {
                assert_eq!(other.mask, Mask::Single);
                self.mask = Mask::Single;
            }
            Mask::Partial(mut set) => match other.mask {
                Mask::Single => {
                    self.mask = Mask::Partial(set);
                }
                Mask::Partial(o_set) => {
                    for x in o_set {
                        set.insert(x);
                    }
                    self.mask = Mask::Partial(set);
                }
                Mask::All(p) => {
                    self.mask = Mask::All(p);
                }
            },
            Mask::All(_) => {
                self.mask = mask;
            }
        }
    }
}

impl Debug for Weight {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.mask {
            Mask::Single => Ok(()),
            Mask::Partial(ref p) => write!(f, "{:?}", p),
            Mask::All(_) => write!(f, "All"),
        }
    }
}

impl Encode for Weight {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match &self.mask {
            Mask::Single => writer.write_u32(0),
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
            Ok(Weight { mask: Mask::Single })
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
