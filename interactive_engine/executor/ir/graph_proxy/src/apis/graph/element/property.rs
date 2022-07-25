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
use pegasus_common::impl_as_any;

/// The three types of property to get
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PropKey {
    Id,
    Label,
    Len,
    All,
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
                Item::Len(_) => Ok(PropKey::Len),
                Item::All(_) => Ok(PropKey::All),
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
            PropKey::Len => {
                writer.write_u8(2)?;
            }
            PropKey::All => {
                writer.write_u8(3)?;
            }
            PropKey::Key(key) => {
                writer.write_u8(4)?;
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
            2 => Ok(PropKey::Len),
            3 => Ok(PropKey::All),
            4 => {
                let key = <NameOrId>::read_from(reader)?;
                Ok(PropKey::Key(key))
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
        }
    }
}

pub enum PropertyValue<'a> {
    Borrowed(BorrowObject<'a>),
    Owned(Object),
}

impl<'a> PropertyValue<'a> {
    pub fn try_to_owned(&self) -> Option<Object> {
        match self {
            PropertyValue::Borrowed(borrowed_obj) => borrowed_obj.try_to_owned(),
            PropertyValue::Owned(obj) => Some(obj.clone()),
        }
    }
}

pub trait Details: std::fmt::Debug + Send + Sync + AsAny {
    /// Get a property with given key
    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue>;

    /// get_all_properties returns all properties. None means that we failed in getting the properties.
    /// Specifically, it returns all properties of Vertex/Edge saved in RUNTIME rather than STORAGE.
    /// it may be used in two situations:
    /// (1) if no prop_keys are provided when querying the vertex/edge which indicates that all properties are necessary,
    /// then we can get all properties of the vertex/edge in storage; e.g., g.V().valueMap()
    /// (2) if some prop_keys are provided when querying the vertex/edge which indicates that only these properties are necessary,
    /// then we can only get all pre-specified properties of the vertex/edge.
    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>>;

    /// Insert a property with given property key and value
    fn insert_property(&mut self, key: NameOrId, value: Object) -> Option<Object>;
}

#[derive(Clone)]
pub struct DynDetails {
    inner: Arc<dyn Details>,
}

impl DynDetails {
    pub fn new<P: Details + 'static>(p: P) -> Self {
        DynDetails { inner: Arc::new(p) }
    }

    pub fn empty() -> Self {
        DynDetails { inner: Arc::new(EmptyDetails {}) }
    }
}

impl_as_any!(DynDetails);

impl Details for DynDetails {
    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        self.inner.get_property(key)
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        self.inner.get_all_properties()
    }

    fn insert_property(&mut self, key: NameOrId, value: Object) -> Option<Object> {
        Arc::get_mut(&mut self.inner)
            .map(|e| e.insert_property(key, value))
            .unwrap_or(None)
    }
}

impl fmt::Debug for DynDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynDetails")
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
        } else if let Some(empty) = self
            .inner
            .as_any_ref()
            .downcast_ref::<EmptyDetails>()
        {
            // hint to be as EmptyDetails
            writer.write_u8(2)?;
            empty.write_to(writer)?;
        } else {
            // TODO(yyy): handle other kinds of details
            // for Lazy details, we write id, label, and required properties
            writer.write_u8(3)?;
            let all_props = self.get_all_properties();
            if let Some(all_props) = all_props {
                writer.write_u64(all_props.len() as u64)?;
                for (k, v) in all_props {
                    k.write_to(writer)?;
                    v.write_to(writer)?;
                }
            } else {
                writer.write_u64(0)?;
            }
        }
        Ok(())
    }
}

impl Decode for DynDetails {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let kind = <u8>::read_from(reader)?;
        if kind == 1 || kind == 3 {
            // For either DefaultDetails or LazyDetails, we decoded as DefaultDetails
            let details = <DefaultDetails>::read_from(reader)?;
            Ok(DynDetails::new(details))
        } else if kind == 2 {
            let details = <EmptyDetails>::read_from(reader)?;
            Ok(DynDetails::new(details))
        } else {
            Err(io::Error::from(io::ErrorKind::Other))
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub struct DefaultDetails {
    inner: HashMap<NameOrId, Object>,
}

#[allow(dead_code)]
impl DefaultDetails {
    pub fn new(properties: HashMap<NameOrId, Object>) -> Self {
        DefaultDetails { inner: properties }
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
    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        self.inner
            .get(key)
            .map(|o| PropertyValue::Borrowed(o.as_borrow()))
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        // it's actually unreachable!()
        Some(self.inner.clone())
    }

    fn insert_property(&mut self, key: NameOrId, value: Object) -> Option<Object> {
        self.inner.insert(key, value)
    }
}

impl Encode for DefaultDetails {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
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
        let len = reader.read_u64()?;
        let mut map = HashMap::with_capacity(len as usize);
        for _i in 0..len {
            let k = <NameOrId>::read_from(reader)?;
            let v = <Object>::read_from(reader)?;
            map.insert(k, v);
        }
        Ok(DefaultDetails { inner: map })
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub struct EmptyDetails {}

impl_as_any!(EmptyDetails);

impl Details for EmptyDetails {
    fn get_property(&self, _key: &NameOrId) -> Option<PropertyValue> {
        None
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        None
    }

    fn insert_property(&mut self, _key: NameOrId, _value: Object) -> Option<Object> {
        // Won't insert_property() for EmptyDetails for now:
        // insert_property() only used to update a vertex which already have some properties, to append more props;
        // this won't happen on a vertex with EmptyDetails.
        // Still, be careful if insert_property() on EmptyDetails.
        warn!("Cannot insert property in EmptyDetails of ({:?},{:?})", _key, _value);
        None
    }
}

impl Encode for EmptyDetails {
    fn write_to<W: WriteExt>(&self, _writer: &mut W) -> io::Result<()> {
        Ok(())
    }
}

impl Decode for EmptyDetails {
    fn read_from<R: ReadExt>(_reader: &mut R) -> io::Result<Self> {
        Ok(EmptyDetails::default())
    }
}
