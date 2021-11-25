use std::fmt::{Debug, Formatter};

use bitflags::_core::cmp::Ordering;

use crate::codec::{Decode, Encode, ReadExt, WriteExt};
use crate::stream::Stream;
use crate::{BuildJobError, Data};

/// `Merge` is a [`Binary`] operator that merge the data of the two input streams into one
/// output stream.
///
/// [`Binary`]: crate::api::binary::Binary
pub trait Merge<D: Data> {
    /// Merge two input streams of the **same** data type.
    ///
    /// # Example
    /// ```
    ///   # use pegasus::{JobConf};
    ///   # use pegasus::api::{Sink, Map, Merge, Collect};
    ///
    ///   # let conf = JobConf::new("reduce_example");
    ///     let mut results = pegasus::run(conf, || {
    ///         let id = pegasus::get_current_worker().index;
    ///         move |input, output| {
    ///             let src1 = input.input_from(vec![1, 3, 5, 7, 9])?;
    ///             let (src1, src2) = src1.copied()?;
    ///             src1
    ///                 .merge(src2.map(|d| Ok(d + 1))?)?
    ///                 .collect::<Vec<u32>>()?
    ///                 .sink_into(output)
    ///         }
    ///     })
    ///       .expect("build job failure");
    ///
    ///     let mut expected = results.next().unwrap().unwrap();
    ///     expected.sort();
    ///     assert_eq!(expected, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    /// ```
    fn merge(self, other: Stream<D>) -> Result<Stream<D>, BuildJobError>;

    /// Merge two input streams of different data types `D` and `T` into one output stream.
    /// The output stream carries the data of type [`Either<D, T>`]. If it takes a data
    /// from the `D` side, it outputs a data of type `Either::A<D>`, otherwise, it outputs
    /// a data of type `Either::B<T>`.
    ///
    /// [`Either<D, T>`]: crate::api::merge::Either
    ///
    /// # Example
    /// ```
    /// /*
    ///   # use pegasus::{JobConf};
    ///   # use pegasus::api::{Sink, Map, Merge, Either};
    ///
    ///   # let mut conf = JobConf::new("reduce_example");
    ///   # conf.plan_print = false;
    ///     let mut results = pegasus::run(conf, || {
    ///         let id = pegasus::get_current_worker().index;
    ///         move |input, output| {
    ///             let src1 = input.input_from(vec![1_u32, 3])?;
    ///             let (src1, src2) = src1.copied()?;
    ///             src1
    ///                 .merge_isomer(src2.map(|d| Ok(d as u64 - 1))?)?
    ///                 .sink_into(output)
    ///         }
    ///     })
    ///       .expect("build job failure")
    ///       .map(|x| x.unwrap_or(Either::A(0_u32)))
    ///       .collect::<Vec<Either<u32, u64>>>();
    ///
    ///     results.sort();
    ///     assert_eq!(results, [Either::A(1_u32), Either::A(3_u32), Either::B(0_u64), Either::B(2_u64)]);
    /// */
    /// ```
    fn merge_isomer<T: Data>(self, isomer: Stream<T>) -> Result<Stream<Either<D, T>>, BuildJobError>;
}

pub enum Either<A, B> {
    A(A),
    B(B),
}

impl<A: PartialEq, B: PartialEq> PartialEq for Either<A, B> {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Either::A(a1) => {
                if let Either::A(a2) = other {
                    a1 == a2
                } else {
                    false
                }
            }
            Either::B(b1) => {
                if let Either::B(b2) = other {
                    b1 == b2
                } else {
                    false
                }
            }
        }
    }
}

impl<A: PartialEq + Eq, B: PartialEq + Eq> Eq for Either<A, B> {}

impl<A: PartialOrd, B: PartialOrd> PartialOrd for Either<A, B> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            Either::A(a1) => {
                if let Either::A(a2) = other {
                    a1.partial_cmp(a2)
                } else {
                    None
                }
            }
            Either::B(b1) => {
                if let Either::B(b2) = other {
                    b1.partial_cmp(b2)
                } else {
                    None
                }
            }
        }
    }
}

impl<A: PartialOrd + Ord, B: PartialOrd + Ord> Ord for Either<A, B> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self {
            Either::A(a1) => {
                if let Either::A(a2) = other {
                    a1.cmp(a2)
                } else {
                    // A is by default less than B
                    Ordering::Less
                }
            }
            Either::B(b1) => {
                if let Either::B(b2) = other {
                    b1.cmp(b2)
                } else {
                    Ordering::Greater
                }
            }
        }
    }
}

impl<A: Clone, B: Clone> Clone for Either<A, B> {
    fn clone(&self) -> Self {
        match self {
            Either::A(a) => Either::A(a.clone()),
            Either::B(b) => Either::B(b.clone()),
        }
    }
}

impl<A: Debug, B: Debug> Debug for Either<A, B> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Either::A(a) => write!(f, "A({:?})", a),
            Either::B(b) => write!(f, "B({:?})", b),
        }
    }
}

impl<A: Encode, B: Encode> Encode for Either<A, B> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            Either::A(a) => {
                writer.write_u8(0)?;
                a.write_to(writer)
            }
            Either::B(b) => {
                writer.write_u8(1)?;
                b.write_to(writer)
            }
        }
    }
}

impl<A: Decode, B: Decode> Decode for Either<A, B> {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let m = reader.read_u8()?;
        let e = if m == 0 {
            let a = A::read_from(reader)?;
            Either::A(a)
        } else {
            let b = B::read_from(reader)?;
            Either::B(b)
        };
        Ok(e)
    }
}
