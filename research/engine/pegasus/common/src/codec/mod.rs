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

//! The `Serialize` and `Deserialize` traits is paired contract which defines how objects of various
//! structures be encoded into network, and decoded from network;

use std::io;
use std::mem;

pub use bytes::Buf;

pub use crate::io::{ReadExt, WriteExt};

/// The encode interface used for serializing data structures into binary output stream;
///
/// # Examples
///
/// ```
/// use std::io;
/// use pegasus_common::codec::{Encode, WriteExt};
///
/// struct Person {
///     name    : String,
///     age     : u16
/// }
///
/// impl Encode for Person {
///     fn write_to<W: WriteExt>(&self,writer: &mut W) -> io::Result<()> {
///         self.name.write_to(writer)?;
///         writer.write_u16(self.age)
///     }
/// }
///
/// ```
///
/// Users can also use third-party libraries to do serializing, such as [`serde`], [`bincode`], and so on.
///
/// Use these libraries to serialize a typed struct into a byte array, and then write the byte array
/// into the `writer`;
///
/// For example, assuming the `Person` has implement [`bincode`] serializing interface:
///
///
/// ```ignore
/// impl Serialize for Person {
///     fn write_to<W: WriteExt>(&self,writer: &mut W) -> io::Result<()> {
///         let encoded: Vec<u8> = bincode::serialize(&self).unwrap();
///         writer.write_u32(encoded.len())?;
///         writer.write_all(&encoded)
///     }
/// }
///
/// ```
///
/// # Arguments
///
/// The `writer` is a generic type of [`WriteExt`], which may be a `File`, a `TcpStream`, or something elses;
///
/// A type struct can implement `Serialize` by writing each field into the writer in order, as long as all
/// its fields have implement `Serialize`;
///
/// # Return value
///
/// Returns `Result::Err(std::io::Error)` if do serializing failed;
///
/// [`serde`]: https://serde.rs/
/// [`bincode`]: https://github.com/servo/bincode
///
pub trait Encode {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()>;
}

/// The deserialize interface used for decoding tryped structures from binary stream;
///
/// # Examples
/// ```
/// use pegasus_common::codec::*;
/// use std::io;
///
/// struct Person {
///     name    : String,
///     age     : u16
/// }
///
/// impl Decode for Person {
///     fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
///         let name = String::read_from(reader)?;
///         let age = reader.read_u16()?;
///         Ok(Person { name, age })
///     }
/// }
/// ```
/// If users use third-party libraries to do the serializing, they should use the same libraries to do
/// deserializing.
///
/// For example, assuming users use [`bincode`] to do serializing on `Person`,
///
/// ```ignore
/// use std::io;
/// use pegasus_common::codec::*;
/// use std::io::Read;
///
/// impl Encode for Person {
///     fn write_to<W: WriteExt>(&self,writer: &mut W) -> io::Result<()> {
///         unimplemented!("use bincode;")
///     }
/// }
///
/// impl Decode for Person {
///     fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
///         let len = reader.read_u32()? as usize;
///         let bytes = reader.read_to(len)?;
///         let decoded: Person = bincode::deserialize(&bytes[..]).unwrap();
///         Ok(decoded)
///     }
/// }
/// ```
pub trait Decode: Sized {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self>;
}

pub trait Codec: Encode + Decode {}

impl<T: Encode + Decode> Codec for T {}

impl Encode for () {
    fn write_to<W: WriteExt>(&self, _writer: &mut W) -> io::Result<()> {
        Ok(())
    }
}

impl Decode for () {
    fn read_from<R: ReadExt>(_: &mut R) -> io::Result<Self> {
        Ok(())
    }
}

impl Encode for bool {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        if *self {
            writer.write_u8(1)
        } else {
            writer.write_u8(0)
        }
    }
}

impl Decode for bool {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        if reader.read_u8()? > 0 {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

macro_rules! serialize_numbers {
    ($ty: ty, $write: ident, $read: ident) => {
        impl Encode for $ty {
            fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
                writer.$write(*self)
            }
        }

        impl Decode for $ty {
            fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<$ty> {
                Ok(reader.$read()?)
            }
        }
    };
}

serialize_numbers!(u8, write_u8, read_u8);
serialize_numbers!(i8, write_i8, read_i8);
serialize_numbers!(u16, write_u16, read_u16);
serialize_numbers!(i16, write_i16, read_i16);
serialize_numbers!(u32, write_u32, read_u32);
serialize_numbers!(i32, write_i32, read_i32);
serialize_numbers!(u64, write_u64, read_u64);
serialize_numbers!(i64, write_i64, read_i64);
serialize_numbers!(u128, write_u128, read_u128);
serialize_numbers!(i128, write_i128, read_i128);
serialize_numbers!(f32, write_f32, read_f32);
serialize_numbers!(f64, write_f64, read_f64);

pub trait AsBytes: Sized {
    fn as_bytes(&self) -> &[u8];

    fn from_bytes(bytes: &[u8]) -> &Self;
}

impl<T: Copy> AsBytes for T {
    fn as_bytes(&self) -> &[u8] {
        let size = mem::size_of::<T>();
        unsafe { std::slice::from_raw_parts(mem::transmute(self), size) }
    }

    fn from_bytes(bytes: &[u8]) -> &T {
        unsafe { std::mem::transmute(bytes.as_ptr()) }
    }
}

impl Encode for String {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        let bytes = self.as_bytes();
        writer.write_u32(bytes.len() as u32)?;
        writer.write_all(bytes)
    }
}

impl Decode for String {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let len = reader.read_u32()? as usize;
        let mut vec = vec![0u8; len];
        reader.read_exact(&mut vec)?;
        let ptr = vec.as_mut_ptr();
        unsafe {
            ::std::mem::forget(vec);
            Ok(String::from_raw_parts(ptr, len, len))
        }
    }
}

impl<T: Encode> Encode for Vec<T> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u32(self.len() as u32)?;
        for datum in self.iter() {
            datum.write_to(writer)?;
        }
        Ok(())
    }
}

impl<T: Decode> Decode for Vec<T> {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let len = reader.read_u32()? as usize;
        let mut vec = Vec::with_capacity(len);
        for _i in 0..len {
            vec.push(T::read_from(reader)?);
        }
        Ok(vec)
    }
}

#[macro_export]
macro_rules! tuple_impls {

    ( $($name:ident )+ ) => {
        impl<$($name: Encode),*> Encode for ($($name,)*) {
           fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
                let ($($name,)+) = self;
                ($($name.write_to(writer)?,)+);
                Ok(())
           }
        }

        impl<$($name: Decode),*> Decode for ($($name,)*) {
           fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
                Ok(($($name::read_from(reader)?,)+))
           }
        }

    }
}

#[allow(non_snake_case)]
mod tuple {
    use super::*;
    tuple_impls! { A }
    tuple_impls! { A B }
    tuple_impls! { A B C }
    tuple_impls! { A B C D }
    tuple_impls! { A B C D E }
    tuple_impls! { A B C D E F }
    tuple_impls! { A B C D E F G }
    tuple_impls! { A B C D E F G H }
    tuple_impls! { A B C D E F G H I }
    tuple_impls! { A B C D E F G H I J }
    tuple_impls! { A B C D E F G H I J K }
    tuple_impls! { A B C D E F G H I J K L }
}

impl<T: Encode> Encode for Option<T> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Some(obj) => {
                writer.write_u8(1)?;
                obj.write_to(writer)
            }
            None => writer.write_u8(0),
        }
    }
}

impl<T: Decode> Decode for Option<T> {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let opt = reader.read_u8()?;
        if opt == 0 {
            Ok(None)
        } else {
            let obj = T::read_from(reader)?;
            Ok(Some(obj))
        }
    }
}

mod shade;
mod third_party;
pub use shade::ShadeCodec;
#[cfg(feature = "serde")]
pub use third_party::serde_bin as serde;

#[cfg(test)]
mod test {
    use super::*;

    macro_rules! test_serde_num {
        ($ty: ty) => {
            let len = std::mem::size_of::<$ty>() * 1024;
            let mut bytes = vec![0u8; len];
            let mut writer = &mut bytes[0..];
            let end = 1024 as $ty;
            for item in 0..end {
                item.write_to(&mut writer).unwrap();
            }
            let mut reader = &bytes[0..];
            for expected in 0..end {
                let decoded = <$ty>::read_from(&mut reader).unwrap();
                //println!("decoded: {:?}", decoded);
                assert_eq!(expected, decoded)
            }
        };
    }

    macro_rules! test_serde_signed_num {
        ($ty: ty) => {
            let len = std::mem::size_of::<$ty>() * 1024;
            let mut bytes = vec![0u8; len];
            let mut writer = &mut bytes[0..];
            let end = 1024 as $ty;
            for i in 0..end {
                let item = i - 512;
                item.write_to(&mut writer).unwrap();
            }
            let mut reader = &bytes[0..];
            for i in 0..end {
                let expected = i - 512;
                let decoded = <$ty>::read_from(&mut reader).unwrap();
                //println!("decoded: {:?}", decoded);
                assert_eq!(expected, decoded)
            }
        };
    }

    #[test]
    fn serde_u8() {
        let mut bytes = vec![0u8; 1024];
        let mut writer = &mut bytes[0..];
        let max = std::u8::MAX as u32;
        for i in 0..1024u32 {
            let item = (i % max) as u8;
            item.write_to(&mut writer).unwrap();
        }
        for (offset, decode) in bytes.iter().enumerate() {
            let expect = (offset as u32 % max) as u8;
            assert_eq!(expect, *decode);
        }

        let mut reader = &bytes[0..];
        for i in 0..1024 {
            let expect = (i as u32 % max) as u8;
            let de = <u8>::read_from(&mut reader).unwrap();
            assert_eq!(expect, de);
        }
    }

    #[test]
    fn serde_i8() {
        let mut bytes = vec![0u8; 1024];
        let mut writer = &mut bytes[0..];
        let min = std::i8::MIN;
        let max = std::i8::MAX;
        for item in min..max {
            item.write_to(&mut writer).unwrap();
        }
        max.write_to(&mut writer).unwrap();
        let mut reader = &bytes[0..];
        for expected in min..max {
            let decoded = <i8>::read_from(&mut reader).unwrap();
            assert_eq!(expected, decoded);
        }
        let decoded = <i8>::read_from(&mut reader).unwrap();
        assert_eq!(max, decoded);
    }

    #[test]
    fn serde_u16() {
        test_serde_num!(u16);
    }

    #[test]
    fn serde_u32() {
        test_serde_num!(u32);
    }

    #[test]
    fn serde_u64() {
        test_serde_num!(u64);
    }

    #[test]
    fn serde_u128() {
        test_serde_num!(u128);
    }

    #[test]
    fn serde_i16() {
        test_serde_num!(i16);
    }

    #[test]
    fn serde_i16_2() {
        test_serde_signed_num!(i16);
    }

    #[test]
    fn serde_i32() {
        test_serde_num!(i32);
    }

    #[test]
    fn serde_i32_2() {
        test_serde_signed_num!(i32);
    }

    #[test]
    fn serde_i64() {
        test_serde_num!(i64);
    }

    #[test]
    fn serde_i64_2() {
        test_serde_signed_num!(i64);
    }

    #[test]
    fn serde_i128() {
        test_serde_num!(i128);
    }

    #[test]
    fn serde_i128_2() {
        test_serde_signed_num!(i128);
    }

    #[test]
    fn serde_f32() {
        let len = std::mem::size_of::<f32>() * 1024;
        let mut bytes = vec![0u8; len];
        let mut writer = &mut bytes[0..];
        let seed = 3.1415926f32;
        for i in 0..1024 {
            let item = ((i - 512) as f32) * seed;
            item.write_to(&mut writer).unwrap();
        }

        let mut reader = &bytes[0..];
        for i in 0..1024 {
            let expected = ((i - 512) as f32) * seed;
            let decoded = <f32>::read_from(&mut reader).unwrap();
            //println!("expected {}", expected);
            assert_eq!(expected, decoded);
        }
    }

    #[test]
    fn serde_f64() {
        let len = std::mem::size_of::<f64>() * 1024;
        let mut bytes = vec![0u8; len];
        let mut writer = &mut bytes[0..];
        let seed = 3.1415926f64;
        for i in 0..1024 {
            let item = ((i - 512) as f64) * seed;
            item.write_to(&mut writer).unwrap();
        }

        let mut reader = &bytes[0..];
        for i in 0..1024 {
            let expected = ((i - 512) as f64) * seed;
            let decoded = <f64>::read_from(&mut reader).unwrap();
            //println!("expected {}", expected);
            assert_eq!(expected, decoded);
        }
    }

    #[test]
    fn serde_tuple() {
        let len = std::mem::size_of::<(u32, u64)>() * 1024;
        let mut bytes = vec![0u8; len];
        let mut writer = &mut bytes[0..];
        for i in 0..1024 {
            let item = (i as u32, i as u64);
            item.write_to(&mut writer).unwrap();
        }

        let mut reader = &bytes[0..];
        for i in 0..1024 {
            let expected = (i as u32, i as u64);
            let decoded = <(u32, u64)>::read_from(&mut reader).unwrap();
            assert_eq!(expected, decoded);
        }
    }

    #[test]
    fn serde_tuple_tuple() {
        let len = std::mem::size_of::<(u32, (i32, u64))>() * 1024;
        let mut bytes = vec![0u8; len];
        let mut writer = &mut bytes[0..];
        for i in 0..1024 {
            let item = (i as u32, ((i - 512) as i32, i as u64));
            item.write_to(&mut writer).unwrap();
        }
        let mut reader = &bytes[0..];
        for i in 0..1024 {
            let expected = (i as u32, ((i - 512) as i32, i as u64));
            let decoded = <(u32, (i32, u64))>::read_from(&mut reader).unwrap();
            assert_eq!(expected, decoded);
        }
    }

    #[test]
    fn serde_triple() {
        let len = std::mem::size_of::<(u32, i32, u128)>() * 1024;
        let mut bytes = vec![0u8; len];
        let mut writer = &mut bytes[0..];
        for i in 0..1024 {
            let item = (i as u32, (i - 512) as i32, (i * 1024) as u128);
            item.write_to(&mut writer).unwrap();
        }
        let mut reader = &bytes[0..];
        for i in 0..1024 {
            let expected = (i as u32, (i - 512) as i32, (i * 1024) as u128);
            let decoded = <(u32, i32, u128)>::read_from(&mut reader).unwrap();
            assert_eq!(expected, decoded);
        }
    }

    #[test]
    fn serde_string() {
        let data = "Hello world!!!@#$%".to_owned();
        let mut bytes = vec![0u8; 1024];
        let mut writer = &mut bytes[0..];
        data.write_to(&mut writer).unwrap();
        let mut reader = &bytes[0..];
        let decoded = String::read_from(&mut reader).unwrap();
        assert_eq!(data, decoded);
    }

    #[test]
    fn serde_string_tuple() {
        let mut bytes = vec![];
        let str1 = "This is the first string".to_string();
        let str2 = "This is the second string".to_string();
        let item = (str1, str2);
        item.write_to(&mut bytes).unwrap();
        let mut reader = &bytes[0..];
        let decoded = <(String, String)>::read_from(&mut reader).unwrap();
        assert_eq!(item, decoded);
    }

    #[test]
    fn as_bytes_of_copy() {
        #[derive(Copy, Clone, Debug, Eq, PartialEq)]
        struct Object {
            field1: u64,
            field2: u64,
            field3: Option<u32>,
        }

        let obj = Object { field1: 1, field2: 2, field3: Some(3) };
        let bytes = obj.as_bytes();
        let obj2 = Object::from_bytes(bytes);
        //println!("get object: {:?}", obj2);
        assert_eq!(&obj, obj2);
    }
}
