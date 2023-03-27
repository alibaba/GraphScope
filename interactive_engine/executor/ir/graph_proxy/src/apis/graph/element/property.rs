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

use std::convert::TryFrom;
use std::io;
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
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

#[derive(Debug)]
pub enum PropertyValue<'a> {
    Borrowed(BorrowObject<'a>),
    Owned(Object),
}

impl<'a> PropertyValue<'a> {
    pub fn try_to_owned(self) -> Option<Object> {
        match self {
            PropertyValue::Borrowed(borrowed_obj) => borrowed_obj.try_to_owned(),
            PropertyValue::Owned(obj) => Some(obj),
        }
    }
}

pub trait Details: std::fmt::Debug + Send + Sync + AsAny {
    /// Get a property with given key
    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue>;

    /// get_all_properties returns all properties. None means that we failed in getting the properties.
    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>>;

    /// get_property_keys returns the pre-cached prop_keys.
    /// Specifically, prop_keys of `Some(vec![])` indicates all properties,
    /// and prop_keys of `None` indicates no pre-cached property.
    // TODO: Compiler will give all the prop keys when need to get_all_properties().
    // After that, we can use a Vector to specify the cached_properties.
    fn get_property_keys(&self) -> Option<Vec<NameOrId>>;
}

/// Properties in Runtime, including:
/// Empty when no property required. e.g., for an id-only Vertex.
/// Default when properties required. Usually, these properties cannot be accessed locally, thus need to be carried with the GraphElement.
/// Lazy when properties required. Usually, these properties can be accessed locally, thus can be queried when used.
#[derive(Clone, Debug)]
pub enum DynDetails {
    Empty,
    Default(HashMap<NameOrId, Object>),
    Lazy(Arc<dyn Details>),
}

impl Default for DynDetails {
    fn default() -> Self {
        DynDetails::Empty
    }
}

impl DynDetails {
    pub fn new(default: HashMap<NameOrId, Object>) -> Self {
        DynDetails::Default(default)
    }

    pub fn lazy<P: Details + 'static>(p: P) -> Self {
        DynDetails::Lazy(Arc::new(p))
    }
}

impl_as_any!(DynDetails);

impl Details for DynDetails {
    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        match self {
            DynDetails::Empty => None,
            DynDetails::Default(default) => default
                .get(key)
                .map(|o| PropertyValue::Borrowed(o.as_borrow())),
            DynDetails::Lazy(lazy) => lazy.get_property(key),
        }
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        match self {
            DynDetails::Empty => None,
            DynDetails::Default(default) => Some(default.clone()),
            DynDetails::Lazy(lazy) => lazy.get_all_properties(),
        }
    }

    fn get_property_keys(&self) -> Option<Vec<NameOrId>> {
        match self {
            DynDetails::Empty => None,
            DynDetails::Default(_) => unreachable!(),
            DynDetails::Lazy(lazy) => lazy.get_property_keys(),
        }
    }
}

impl Encode for DynDetails {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            DynDetails::Empty => {
                writer.write_u8(0)?;
            }
            DynDetails::Default(default) => {
                writer.write_u8(1)?;
                writer.write_u64(default.len() as u64)?;
                for (k, v) in default {
                    k.write_to(writer)?;
                    v.write_to(writer)?;
                }
            }
            DynDetails::Lazy(lazy) => {
                if let Some(prop_keys) = lazy.get_property_keys() {
                    if prop_keys.is_empty() {
                        // If all properties required, try to get all properties
                        let all_props = self.get_all_properties().unwrap_or_default();
                        let len = all_props.len();
                        if len == 0 {
                            // If no properties fetched, hint as Empty DynDetails
                            writer.write_u8(0)?;
                        } else {
                            // Otherwise, hint as Default DynDetails
                            writer.write_u8(1)?;
                            writer.write_u64(len as u64)?;
                            for (k, v) in all_props {
                                k.write_to(writer)?;
                                v.write_to(writer)?;
                            }
                        }
                    } else {
                        // If some specific properties required, try to get these properties; and hint as Default DynDetails
                        writer.write_u8(1)?;
                        writer.write_u64(prop_keys.len() as u64)?;
                        for k in prop_keys {
                            let v = self
                                .get_property(&k)
                                .map(|prop| prop.try_to_owned())
                                .unwrap_or(None)
                                .unwrap_or(Object::None);
                            k.write_to(writer)?;
                            v.write_to(writer)?;
                        }
                    }
                } else {
                    // If no properties required, hint as Empty DynDetails
                    writer.write_u8(0)?;
                }
            }
        }
        Ok(())
    }
}

impl Decode for DynDetails {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let kind = <u8>::read_from(reader)?;
        if kind == 0 {
            // Empty DynDetails
            Ok(DynDetails::default())
        } else if kind == 1 {
            // For either DefaultDetails or LazyDetails(with required details), we decoded as DefaultDetails
            let len = reader.read_u64()?;
            let mut map = HashMap::with_capacity(len as usize);
            for _i in 0..len {
                let k = <NameOrId>::read_from(reader)?;
                let v = <Object>::read_from(reader)?;
                map.insert(k, v);
            }
            Ok(DynDetails::Default(map))
        } else {
            Err(io::Error::from(io::ErrorKind::Other))
        }
    }
}
