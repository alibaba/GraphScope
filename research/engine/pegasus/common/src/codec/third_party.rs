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

#![allow(unused_imports)]
use crate::io::{ReadExt, WriteExt};

#[cfg(feature = "serde")]
pub mod serde_bin {
    use std::fmt::{Debug, Display};
    use std::io;
    use std::io::Error;
    use std::ops::{Deref, DerefMut};

    use serde::de::{DeserializeSeed, IntoDeserializer, Visitor};
    use serde::Deserializer;

    use super::*;
    use crate::codec::{Decode, Encode};

    pub fn ser_into<T: ?Sized, W>(writer: &mut W, obj: &T) -> io::Result<()>
    where
        T: serde::Serialize,
        W: WriteExt,
    {
        let mut serializer = WriterSerializer::new(writer);
        obj.serialize(&mut serializer)
            .map_err(|err| err.0)?;
        Ok(())
    }

    pub fn de_from<T, R>(reader: &mut R) -> io::Result<T>
    where
        T: serde::de::DeserializeOwned,
        R: ReadExt,
    {
        let mut der = ReaderDeserializer { inner: reader };
        let value = T::deserialize(&mut der).map_err(|err| err.0)?;
        Ok(value)
    }

    struct WriterSerializer<'a, W: WriteExt> {
        inner: &'a mut W,
    }

    impl<'a, W: WriteExt> WriterSerializer<'a, W> {
        fn new(writer: &'a mut W) -> Self {
            WriterSerializer { inner: writer }
        }
    }

    impl<'a, W: WriteExt> Deref for WriterSerializer<'a, W> {
        type Target = W;

        fn deref(&self) -> &Self::Target {
            self.inner
        }
    }

    impl<'a, W: WriteExt> DerefMut for WriterSerializer<'a, W> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.inner
        }
    }

    struct PhantomError(io::Error);

    impl Debug for PhantomError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }

    impl Display for PhantomError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }

    impl std::error::Error for PhantomError {}

    impl serde::ser::Error for PhantomError {
        fn custom<T>(msg: T) -> Self
        where
            T: Display,
        {
            PhantomError(io::Error::new(io::ErrorKind::Other, format!("{}", msg)))
        }
    }

    impl serde::de::Error for PhantomError {
        fn custom<T>(msg: T) -> Self
        where
            T: Display,
        {
            PhantomError(io::Error::new(io::ErrorKind::Other, format!("{}", msg)))
        }
    }

    impl Into<io::Error> for PhantomError {
        fn into(self) -> Error {
            self.0
        }
    }

    impl From<io::Error> for PhantomError {
        fn from(err: io::Error) -> Self {
            PhantomError(err)
        }
    }

    impl<'a, W: WriteExt> serde::ser::SerializeSeq for &mut WriterSerializer<'a, W> {
        type Ok = ();
        type Error = PhantomError;

        fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
        where
            T: serde::Serialize,
        {
            value.serialize(&mut **self)
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }
    }

    impl<'a, W: WriteExt> serde::ser::SerializeTuple for &mut WriterSerializer<'a, W> {
        type Ok = ();
        type Error = PhantomError;

        fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
        where
            T: serde::Serialize,
        {
            value.serialize(&mut **self)
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }
    }

    impl<'a, W: WriteExt> serde::ser::SerializeTupleStruct for &mut WriterSerializer<'a, W> {
        type Ok = ();
        type Error = PhantomError;

        fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
        where
            T: serde::Serialize,
        {
            value.serialize(&mut **self)
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }
    }

    impl<'a, W: WriteExt> serde::ser::SerializeTupleVariant for &mut WriterSerializer<'a, W> {
        type Ok = ();
        type Error = PhantomError;

        fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
        where
            T: serde::Serialize,
        {
            value.serialize(&mut **self)
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }
    }

    impl<'a, W: WriteExt> serde::ser::SerializeMap for &mut WriterSerializer<'a, W> {
        type Ok = ();
        type Error = PhantomError;

        fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
        where
            T: serde::Serialize,
        {
            key.serialize(&mut **self)
        }

        fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
        where
            T: serde::Serialize,
        {
            value.serialize(&mut **self)
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }
    }

    impl<'a, W: WriteExt> serde::ser::SerializeStruct for &mut WriterSerializer<'a, W> {
        type Ok = ();
        type Error = PhantomError;

        fn serialize_field<T: ?Sized>(&mut self, _: &'static str, value: &T) -> Result<(), Self::Error>
        where
            T: serde::Serialize,
        {
            value.serialize(&mut **self)
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }
    }

    impl<'a, W: WriteExt> serde::ser::SerializeStructVariant for &mut WriterSerializer<'a, W> {
        type Ok = ();
        type Error = PhantomError;

        fn serialize_field<T: ?Sized>(&mut self, _: &'static str, value: &T) -> Result<(), Self::Error>
        where
            T: serde::Serialize,
        {
            value.serialize(&mut **self)
        }

        fn end(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }
    }

    impl<'a, W: WriteExt> serde::Serializer for &mut WriterSerializer<'a, W> {
        type Ok = ();
        type Error = PhantomError;
        type SerializeSeq = Self;
        type SerializeTuple = Self;
        type SerializeTupleStruct = Self;
        type SerializeTupleVariant = Self;
        type SerializeMap = Self;
        type SerializeStruct = Self;
        type SerializeStructVariant = Self;

        fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
            self.write_u8(v as u8)?;
            Ok(())
        }

        fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
            self.write_i8(v)?;
            Ok(())
        }

        fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
            self.write_i16(v)?;
            Ok(())
        }

        fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
            self.write_i32(v)?;
            Ok(())
        }

        fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
            self.write_i64(v)?;
            Ok(())
        }

        fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
            self.write_u8(v)?;
            Ok(())
        }

        fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
            self.write_u16(v)?;
            Ok(())
        }

        fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
            self.write_u32(v)?;
            Ok(())
        }

        fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
            self.write_u64(v)?;
            Ok(())
        }

        fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
            self.write_f32(v)?;
            Ok(())
        }

        fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
            self.write_f64(v)?;
            Ok(())
        }

        fn serialize_char(self, _: char) -> Result<Self::Ok, Self::Error> {
            unimplemented!()
        }

        fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
            v.write_to(self.inner)?;
            Ok(())
        }

        fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
            self.write_u64(v.len() as u64)?;
            self.write_all(v)?;
            Ok(())
        }

        fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
            self.write_u8(0)?;
            Ok(())
        }

        fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
        where
            T: serde::Serialize,
        {
            self.write_u8(1)?;
            value.serialize(self)
        }

        fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }

        fn serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok, Self::Error> {
            Ok(())
        }

        fn serialize_unit_variant(
            self, _: &'static str, variant_index: u32, _: &'static str,
        ) -> Result<Self::Ok, Self::Error> {
            self.write_u32(variant_index)?;
            Ok(())
        }

        fn serialize_newtype_struct<T: ?Sized>(
            self, _: &'static str, value: &T,
        ) -> Result<Self::Ok, Self::Error>
        where
            T: serde::Serialize,
        {
            value.serialize(self)
        }

        fn serialize_newtype_variant<T: ?Sized>(
            self, _: &'static str, variant_index: u32, _: &'static str, value: &T,
        ) -> Result<Self::Ok, Self::Error>
        where
            T: serde::Serialize,
        {
            self.write_u32(variant_index)?;
            value.serialize(self)
        }

        fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
            if let Some(len) = len {
                self.write_u32(len as u32)?;
                Ok(self)
            } else {
                Err(io::Error::from(io::ErrorKind::InvalidData))?
            }
        }

        fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
            self.write_u32(len as u32)?;
            Ok(self)
        }

        fn serialize_tuple_struct(
            self, _: &'static str, len: usize,
        ) -> Result<Self::SerializeTupleStruct, Self::Error> {
            self.write_u32(len as u32)?;
            Ok(self)
        }

        fn serialize_tuple_variant(
            self, _: &'static str, variant_index: u32, _: &'static str, len: usize,
        ) -> Result<Self::SerializeTupleVariant, Self::Error> {
            self.write_u32(variant_index)?;
            self.write_u32(len as u32)?;
            Ok(self)
        }

        fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
            if let Some(len) = len {
                self.write_u64(len as u64)?;
                Ok(self)
            } else {
                Err(io::Error::from(io::ErrorKind::InvalidData))?
            }
        }

        fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct, Self::Error> {
            Ok(self)
        }

        fn serialize_struct_variant(
            self, _: &'static str, variant_index: u32, _: &'static str, _: usize,
        ) -> Result<Self::SerializeStructVariant, Self::Error> {
            self.write_u32(variant_index)?;
            Ok(self)
        }

        fn collect_str<T: ?Sized>(self, _: &T) -> Result<Self::Ok, Self::Error>
        where
            T: Display,
        {
            Err(io::Error::from(io::ErrorKind::InvalidData))?
        }
    }

    struct ReaderDeserializer<'a, R: ReadExt> {
        inner: &'a mut R,
    }

    struct SeqAccess<'a, 'de, R: ReadExt> {
        len: usize,
        de: &'a mut ReaderDeserializer<'de, R>,
    }

    impl<'a, 'de, R: ReadExt> SeqAccess<'a, 'de, R> {
        fn new(len: usize, de: &'a mut ReaderDeserializer<'de, R>) -> Self {
            SeqAccess { len, de }
        }
    }

    impl<'de, 'a, R: ReadExt> serde::de::SeqAccess<'de> for SeqAccess<'a, 'de, R> {
        type Error = PhantomError;

        fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
        where
            T: DeserializeSeed<'de>,
        {
            if self.len > 0 {
                self.len -= 1;
                let value = seed.deserialize(&mut *self.de)?;
                Ok(Some(value))
            } else {
                Ok(None)
            }
        }

        fn size_hint(&self) -> Option<usize> {
            Some(self.len)
        }
    }

    struct MapAccess<'a, 'de, R: ReadExt> {
        len: usize,
        de: &'a mut ReaderDeserializer<'de, R>,
    }

    impl<'de, 'a, R: ReadExt> serde::de::MapAccess<'de> for MapAccess<'a, 'de, R> {
        type Error = PhantomError;

        fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
        where
            K: DeserializeSeed<'de>,
        {
            if self.len > 0 {
                self.len -= 1;
                let key = seed.deserialize(&mut *self.de)?;
                Ok(Some(key))
            } else {
                Ok(None)
            }
        }

        fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
        where
            V: DeserializeSeed<'de>,
        {
            let value = seed.deserialize(&mut *self.de)?;
            Ok(value)
        }

        #[inline]
        fn size_hint(&self) -> Option<usize> {
            Some(self.len)
        }
    }

    impl<'de, 'a, R: ReadExt> serde::de::VariantAccess<'de> for &'a mut ReaderDeserializer<'de, R> {
        type Error = PhantomError;

        fn unit_variant(self) -> Result<(), Self::Error> {
            Ok(())
        }

        fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
        where
            T: DeserializeSeed<'de>,
        {
            seed.deserialize(self)
        }

        fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            self.deserialize_tuple(len, visitor)
        }

        fn struct_variant<V>(
            self, fields: &'static [&'static str], visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            self.deserialize_tuple(fields.len(), visitor)
        }
    }

    impl<'de, 'a, R: ReadExt> serde::de::EnumAccess<'de> for &'a mut ReaderDeserializer<'de, R> {
        type Error = PhantomError;
        type Variant = Self;

        fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
        where
            V: DeserializeSeed<'de>,
        {
            let idx = self.inner.read_u32()?;
            let val: Result<_, Self::Error> = seed.deserialize(idx.into_deserializer());

            Ok((val?, self))
        }
    }

    impl<'de, 'a, R: ReadExt> serde::de::Deserializer<'de> for &'a mut ReaderDeserializer<'de, R> {
        type Error = PhantomError;

        fn deserialize_any<V>(self, _: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            unimplemented!()
        }

        fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            let b = self.inner.read_u8()? != 0;
            visitor.visit_bool(b)
        }

        fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_i8(self.inner.read_i8()?)
        }

        fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_i16(self.inner.read_i16()?)
        }

        fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_i32(self.inner.read_i32()?)
        }

        fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_i64(self.inner.read_i64()?)
        }

        fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_u8(self.inner.read_u8()?)
        }

        fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_u16(self.inner.read_u16()?)
        }

        fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_u32(self.inner.read_u32()?)
        }

        fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_u64(self.inner.read_u64()?)
        }

        fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_f32(self.inner.read_f32()?)
        }

        fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_f64(self.inner.read_f64()?)
        }

        fn deserialize_char<V>(self, _: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            unimplemented!()
        }

        fn deserialize_str<V>(self, _: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            unimplemented!()
        }

        fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            let v = String::read_from(self.inner)?;
            visitor.visit_string(v)
        }

        fn deserialize_bytes<V>(self, _: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            unimplemented!()
        }

        fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            let len = self.inner.read_u64()? as usize;
            let mut bytes = vec![0u8; len];
            self.inner.read_exact(&mut bytes)?;
            visitor.visit_byte_buf(bytes)
        }

        fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            let mode = self.inner.read_u8()?;
            if mode == 0 {
                visitor.visit_none()
            } else {
                visitor.visit_some(self)
            }
        }

        fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_unit()
        }

        fn deserialize_unit_struct<V>(self, _: &'static str, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_unit()
        }

        fn deserialize_newtype_struct<V>(self, _: &'static str, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_newtype_struct(self)
        }

        fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            let len = self.inner.read_u32()? as usize;
            visitor.visit_seq(SeqAccess::new(len, self))
        }

        fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_seq(SeqAccess::new(len, self))
        }

        fn deserialize_tuple_struct<V>(
            self, _: &'static str, len: usize, visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            self.deserialize_tuple(len, visitor)
        }

        fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            let len = self.inner.read_u64()? as usize;
            visitor.visit_map(MapAccess { len, de: self })
        }

        fn deserialize_struct<V>(
            self, _: &'static str, fields: &'static [&'static str], visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            self.deserialize_tuple(fields.len(), visitor)
        }

        fn deserialize_enum<V>(
            self, _: &'static str, _: &'static [&'static str], visitor: V,
        ) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            visitor.visit_enum(self)
        }

        fn deserialize_identifier<V>(self, _: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            unimplemented!()
        }

        fn deserialize_ignored_any<V>(self, _: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            unimplemented!()
        }
    }
}
