//! Bridge between rust types and raw values

/// Rust C bridge traits
pub mod traits {
    pub use super::{AsRaw, AsRawMut, FromRaw, MatchesRaw};
}

/// A rust type than can identify as a raw value understood by the MPI C API.
pub unsafe trait AsRaw {
    /// The raw MPI C API type
    type Raw;
    /// The raw value
    fn as_raw(&self) -> Self::Raw;
}

unsafe impl<'a, T> AsRaw for &'a T
where
    T: 'a + AsRaw,
{
    type Raw = <T as AsRaw>::Raw;
    fn as_raw(&self) -> Self::Raw {
        (*self).as_raw()
    }
}

/// A rust type than can provide a mutable pointer to a raw value understood by the MPI C API.
pub unsafe trait AsRawMut: AsRaw {
    /// A mutable pointer to the raw value
    fn as_raw_mut(&mut self) -> *mut <Self as AsRaw>::Raw;
}

/// Conversion for the Rust type from the raw MPI handle type.
pub trait FromRaw: AsRaw {
    /// Constructs the Rust type, with all its semantics, from the raw MPI type.
    ///
    /// # Safety
    /// `handle` may be assumed to be a live MPI object.
    unsafe fn from_raw(handle: <Self as AsRaw>::Raw) -> Self;
}

/// A marker trait that indicates the Rust type is exactly equivalent in representation to the Raw
/// type, allowing slices of the type to be used with MPI APIs that accept arrays of its Raw MPI
/// handle.
pub unsafe trait MatchesRaw: AsRaw {}
