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

use super::data_type::DataType;
use super::PropId;
use std::cell::UnsafeCell;
use maxgraph_common::proto::schema::*;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct PropDef {
    prop_id: PropId,
    name: String,
    data_type: DataType,
    comment: String,
    default_value: Option<Vec<u8>>,
}

impl PropDef {
    pub fn build_def(prop_id: PropId,
                     name: String,
                     data_type: DataType,
                     comment: String,
                     default_value: Option<Vec<u8>>) -> Self {
        PropDef {
            prop_id,
            name,
            data_type,
            comment,
            default_value
        }
    }

    #[inline]
    pub fn get_name(&self) -> &str {
        self.name.as_str()
    }

    #[inline]
    pub fn get_data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    pub fn get_comment(&self) -> &str {
        self.comment.as_str()
    }

    #[inline]
    pub fn get_prop_id(&self) -> PropId {
        self.prop_id
    }

    #[inline]
    pub fn get_default_value(&self) -> Option<&Vec<u8>> {
        self.default_value.as_ref()
    }

    #[inline]
    pub fn to_proto(&self) -> PropertyDefProto {
        let mut proto = PropertyDefProto::new();
        proto.set_comment(self.comment.clone());
        proto.set_id(self.prop_id as i32);
        proto.set_name(self.name.clone());
        proto.set_dataType(self.data_type as i32);
        let expression = match self.data_type {
            DataType::ListInt => "INT",
            DataType::ListLong => "LONG",
            DataType::ListFloat => "FLOAT",
            DataType::ListDouble => "DOUBLE",
            DataType::ListString => "STRING",
            DataType::ListBytes => "BYTES",
            _ => "",
        };
        proto.set_typeExpression(expression.to_owned());
        if let Some(ref v) = self.default_value {
            proto.set_hasDefaultValue(true);
            proto.set_defaultValue(v.clone());
        } else {
            proto.set_hasDefaultValue(false);
        }
        proto
    }
}

#[derive(Default)]
pub struct PropDefBuilder {
    inner: UnsafeCell<PropDef>,
}

#[allow(unused_mut)]
impl PropDefBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    #[inline]
    pub fn prop_id(mut self, prop_id: PropId) -> Self {
        self.get_inner().prop_id = prop_id;
        self
    }

    #[inline]
    pub fn name(mut self, name: &str) -> Self {
        self.get_inner().name = name.to_owned();
        self
    }

    #[inline]
    pub fn data_type(mut self, data_type: DataType) -> Self {
        self.get_inner().data_type = data_type;
        self
    }

    #[inline]
    pub fn comment(mut self, comment: &str) -> Self {
        self.get_inner().comment = comment.to_owned();
        self
    }

    pub fn default_value(mut self, default_value: Option<Vec<u8>>) -> Self {
        self.get_inner().default_value = default_value;
        self
    }

    #[inline]
    pub fn build(self) -> PropDef {
        self.inner.into_inner()
    }

    #[inline]
    fn get_inner(&self) -> &mut PropDef {
        unsafe {
            &mut *self.inner.get()
        }
    }
}

impl<'a> From<&'a PropertyDefProto> for PropDef {
    fn from(proto: &'a PropertyDefProto) -> Self {
        let mut builder = PropDefBuilder::new()
            .prop_id(proto.id as u32)
            .name(proto.get_name())
            .data_type(DataType::new(proto.get_dataType() as u32, proto.get_typeExpression()))
            .comment(proto.get_comment());
        if proto.hasDefaultValue {
            builder = builder.default_value(Some(proto.get_defaultValue().to_vec()));
        }
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::test_util::*;

    #[test]
    fn test_prop_def_from_proto() {
        let proto = create_prop_def_proto(1, "aa", DataType::String, "cc");
        let prop_def = PropDef::from(&proto);
        assert_eq!(prop_def.get_prop_id(), 1);
        assert_eq!(prop_def.get_name(), "aa");
        assert_eq!(prop_def.get_data_type(), &DataType::String);
        assert_eq!(prop_def.get_comment(), "cc");
    }

    #[test]
    fn test_prop_def_build() {
        let builder = PropDefBuilder::new();
        let prop_def = builder
            .name("aa")
            .comment("cc")
            .prop_id(1)
            .data_type(DataType::String)
            .build();
        assert_eq!(prop_def.get_prop_id(), 1);
        assert_eq!(prop_def.get_name(), "aa");
        assert_eq!(prop_def.get_data_type(), &DataType::String);
        assert_eq!(prop_def.get_comment(), "cc");
    }
}
