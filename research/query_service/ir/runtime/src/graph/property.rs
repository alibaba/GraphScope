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

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::io;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use dyn_type::{BorrowObject, Object};
use ir_common::error::{ParsePbError, ParsePbResult};
use ir_common::generated::common as pb;
use ir_common::NameOrId;
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use pegasus_common::downcast::*;

use crate::graph::{read_id, write_id, ID};

/// The three types of property to get
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PropKey {
    Id,
    Label,
    Key(NameOrId),
}

impl TryFrom<pb::Property> for PropKey {
    type Error = ParsePbError;

    fn try_from(p: pb::Property) -> ParsePbResult<Self>
    where
        Self: Sized,
    {
        use pb::property::Item;
        if let Some(item) = p.item {
            match item {
                Item::Id(_) => Ok(PropKey::Id),
                Item::Label(_) => Ok(PropKey::Label),
                Item::Key(k) => Ok(PropKey::Key(NameOrId::try_from(k)?)),
            }
        } else {
            Err(ParsePbError::from("empty content provided"))
        }
    }
}

impl Encode for PropKey {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            PropKey::Id => {
                writer.write_u8(0)?;
            }
            PropKey::Label => {
                writer.write_u8(1)?;
            }
            PropKey::Key(key) => {
                writer.write_u8(2)?;
                key.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for PropKey {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let opt = reader.read_u8()?;
        match opt {
            0 => Ok(PropKey::Id),
            1 => Ok(PropKey::Label),
            2 => {
                let key = <NameOrId>::read_from(reader)?;
                Ok(PropKey::Key(key))
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
        }
    }
}

pub trait Details: std::fmt::Debug + Send + Sync + AsAny {
    fn get_property(&self, key: &NameOrId) -> Option<BorrowObject>;

    fn get_id(&self) -> ID;

    fn get_label(&self) -> Option<&NameOrId>;

    fn get(&self, prop_key: &PropKey) -> Option<BorrowObject> {
        match prop_key {
            PropKey::Id => Some(self.get_id().into()),
            PropKey::Label => self
                .get_label()
                .map(|label| label.as_borrow_object()),
            PropKey::Key(k) => self.get_property(k),
        }
    }
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
    fn get_property(&self, key: &NameOrId) -> Option<BorrowObject> {
        self.inner.get_property(key)
    }

    fn get_id(&self) -> ID {
        self.inner.get_id()
    }

    fn get_label(&self) -> Option<&NameOrId> {
        self.inner.get_label()
    }
}

impl fmt::Debug for DynDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynDetails")
            .field("id", &self.get_id())
            .field("label", &self.get_label())
            .field("properties", &format!("{:?}", &self.inner.as_ref()))
            .finish()
    }
}

impl Encode for DynDetails {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        if let Some(default) = self
            .inner
            .as_any_ref()
            .downcast_ref::<DefaultDetails>()
        {
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
            let fake = DefaultDetails::new(0, NameOrId::Id(0));
            Ok(DynDetails::new(fake))
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct DefaultDetails {
    id: ID,
    label: Option<NameOrId>,
    inner: HashMap<NameOrId, Object>,
}

#[allow(dead_code)]
impl DefaultDetails {
    pub fn new(id: ID, label: NameOrId) -> Self {
        DefaultDetails { id, label: Some(label), inner: HashMap::new() }
    }

    pub fn with(id: ID, label: Option<NameOrId>) -> Self {
        DefaultDetails { id, label, inner: HashMap::new() }
    }

    pub fn with_property(id: ID, label: NameOrId, properties: HashMap<NameOrId, Object>) -> Self {
        DefaultDetails { id, label: Some(label), inner: properties }
    }
}

impl_as_any!(DefaultDetails);

impl Deref for DefaultDetails {
    type Target = HashMap<NameOrId, Object>;

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
    fn get_property(&self, key: &NameOrId) -> Option<BorrowObject> {
        self.inner.get(key).map(|o| o.as_borrow())
    }

    fn get_id(&self) -> ID {
        self.id
    }

    fn get_label(&self) -> Option<&NameOrId> {
        self.label.as_ref()
    }
}

impl Encode for DefaultDetails {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        write_id(writer, self.id)?;
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
        let label = <Option<NameOrId>>::read_from(reader)?;
        let len = reader.read_u64()?;
        let mut map = HashMap::with_capacity(len as usize);
        for _i in 0..len {
            let k = <NameOrId>::read_from(reader)?;
            let v = <Object>::read_from(reader)?;
            map.insert(k, v);
        }
        Ok(DefaultDetails { id, label, inner: map })
    }
}
