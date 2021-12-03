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

use crate::generated::common as common_pb;
use crate::structure::codec::ParseError;
use crate::structure::element::{read_id, write_id, Label};
use crate::{FromPb, ID};
use dyn_type::{BorrowObject, Object};
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use pegasus_common::downcast::*;
use std::collections::HashMap;
use std::io;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

pub type PropId = u32;

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum Token {
    Id,
    Label,
    Property(PropKey),
}

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub enum PropKey {
    Str(String),
    Id(PropId),
}

impl From<String> for PropKey {
    fn from(prop_name: String) -> Self {
        PropKey::Str(prop_name)
    }
}

impl From<&String> for PropKey {
    fn from(prop_name: &String) -> Self {
        PropKey::Str(prop_name.clone())
    }
}

impl From<&str> for PropKey {
    fn from(prop_name: &str) -> Self {
        PropKey::Str(prop_name.to_string())
    }
}

impl From<PropId> for PropKey {
    fn from(prop_id: PropId) -> Self {
        PropKey::Id(prop_id)
    }
}

impl FromPb<common_pb::PropertyKey> for PropKey {
    fn from_pb(prop_key: common_pb::PropertyKey) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        match prop_key.item {
            Some(common_pb::property_key::Item::Name(name)) => Ok(PropKey::Str(name)),
            Some(common_pb::property_key::Item::NameId(name_id)) => {
                Ok(PropKey::Id(name_id as PropId))
            }
            _ => Err(ParseError::InvalidData),
        }
    }
}

impl Encode for PropKey {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            PropKey::Str(s) => {
                writer.write_u8(0)?;
                s.write_to(writer)?;
            }
            PropKey::Id(i) => {
                writer.write_u8(1)?;
                writer.write_u32(*i)?;
            }
        }
        Ok(())
    }
}

impl Decode for PropKey {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let kind = <u8>::read_from(reader)?;
        match kind {
            0 => {
                let prop_key = <String>::read_from(reader)?;
                Ok(PropKey::Str(prop_key))
            }
            1 => {
                let prop_key = <PropId>::read_from(reader)?;
                Ok(PropKey::Id(prop_key))
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, "unreachable")),
        }
    }
}

pub trait Details: Send + Sync + AsAny {
    fn get_property(&self, key: &PropKey) -> Option<BorrowObject>;

    fn get_id(&self) -> ID;

    fn get_label(&self) -> &Label;
}

#[derive(Clone)]
pub struct DynDetails {
    inner: Arc<dyn Details>,
}

impl DynDetails {
    pub fn new<P: Details + 'static>(p: P) -> Self {
        DynDetails { inner: Arc::new(p) }
    }
}

impl_as_any!(DynDetails);

impl Details for DynDetails {
    fn get_property(&self, key: &PropKey) -> Option<BorrowObject> {
        self.inner.get_property(key)
    }

    fn get_id(&self) -> ID {
        self.inner.get_id()
    }

    fn get_label(&self) -> &Label {
        self.inner.get_label()
    }
}

impl Encode for DynDetails {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        if let Some(default) = self.inner.as_any_ref().downcast_ref::<DefaultDetails>() {
            // hint to be as DefaultDetails
            writer.write_u8(1)?;
            default.write_to(writer)?;
        } else {
            // TODO(yyy): handle other kinds of details
            // hint to be other Details, not in use now
            writer.write_u8(0)?;
        }
        Ok(())
    }
}

impl Decode for DynDetails {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let kind = <u8>::read_from(reader)?;
        if kind == 1 {
            let details = <DefaultDetails>::read_from(reader)?;
            Ok(DynDetails::new(details))
        } else {
            // TODO(yyy): handle other kinds of details
            // safety: fake never be used
            let fake = DefaultDetails::new(0, Label::Id(0));
            Ok(DynDetails::new(fake))
        }
    }
}

#[allow(dead_code)]
pub struct DefaultDetails {
    id: ID,
    label: Label,
    inner: HashMap<PropKey, Object>,
}

#[allow(dead_code)]
impl DefaultDetails {
    pub fn new(id: ID, label: Label) -> Self {
        DefaultDetails { id, label, inner: HashMap::new() }
    }

    pub fn new_with_prop(id: ID, label: Label, properties: HashMap<PropKey, Object>) -> Self {
        DefaultDetails { id, label, inner: properties }
    }
}

impl_as_any!(DefaultDetails);

impl Deref for DefaultDetails {
    type Target = HashMap<PropKey, Object>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for DefaultDetails {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Details for DefaultDetails {
    fn get_property(&self, key: &PropKey) -> Option<BorrowObject> {
        self.inner.get(key).map(|o| o.as_borrow())
    }

    fn get_id(&self) -> ID {
        self.id
    }

    fn get_label(&self) -> &Label {
        &self.label
    }
}

impl Encode for DefaultDetails {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        write_id(self.id, writer)?;
        self.label.write_to(writer)?;
        writer.write_u64(self.inner.len() as u64)?;
        for (k, v) in &self.inner {
            k.write_to(writer)?;
            v.write_to(writer)?;
        }
        Ok(())
    }
}

impl Decode for DefaultDetails {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let id = read_id(reader)?;
        let label = <Label>::read_from(reader)?;
        let len = reader.read_u64()?;
        let mut map = HashMap::with_capacity(len as usize);
        for _i in 0..len {
            let k = <PropKey>::read_from(reader)?;
            let v = <Object>::read_from(reader)?;
            map.insert(k, v);
        }
        Ok(DefaultDetails { id, label, inner: map })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_ser_dyn_details() {
        let default_details = DefaultDetails::new(1234, Label::Str("test".to_owned()));
        let dyn_details = DynDetails::new(default_details);
        let mut bytes = vec![];
        dyn_details.write_to(&mut bytes).unwrap();

        let mut reader = &bytes[0..];
        let de = <DynDetails>::read_from(&mut reader).unwrap();
        assert_eq!(de.get_id(), 1234);
        let label = de.get_label();
        if let Label::Str(s) = label {
            assert_eq!(s.clone(), "test".to_owned());
        } else {
            unreachable!()
        }
    }
}
