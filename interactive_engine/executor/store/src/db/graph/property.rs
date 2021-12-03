use std::collections::HashMap;

use crate::db::api::*;

pub enum PropData<'a> {
    Owned(Vec<u8>),
    Ref(&'a [u8]),
}

impl<'a> PropData<'a> {
    #[allow(dead_code)]
    pub fn as_bytes(&self) -> &[u8] {
        match *self {
            PropData::Owned(ref v) => v.as_slice(),
            PropData::Ref(v) => v,
        }
    }
}

impl<'a> From<Vec<u8>> for PropData<'a> {
    fn from(data: Vec<u8>) -> Self {
        PropData::Owned(data)
    }
}

impl<'a> From<&'a [u8]> for PropData<'a> {
    fn from(data: &'a [u8]) -> Self {
        PropData::Ref(data)
    }
}

impl PropertyMap for HashMap<PropertyId, Value> {
    fn get(&self, prop_id: i32) -> Option<ValueRef> {
        self.get(&prop_id).map(|v| v.as_ref())
    }

    fn as_map(&self) -> HashMap<i32, ValueRef> {
        let mut map = HashMap::new();
        for (prop_id, v) in self {
            map.insert(*prop_id, v.as_ref());
        }
        map
    }
}

impl PropertyMap for HashMap<PropertyId, ValueRef<'_>> {
    fn get(&self, prop_id: i32) -> Option<ValueRef> {
        self.get(&prop_id).map(|v| *v)
    }

    fn as_map(&self) -> HashMap<i32, ValueRef> {
        self.clone()
    }
}

