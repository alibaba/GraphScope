//
//! Copyright 2021 Alibaba Group Holding Limited.
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

use std::fmt;
use std::sync::atomic::{AtomicPtr, Ordering};

use ahash::HashMap;
use dyn_type::Object;
use dyn_type::Primitives;
use global_query::store_api::prelude::Property;
use global_query::store_api::PropId;
use global_query::store_api::{Edge as StoreEdge, Vertex as StoreVertex};
use ir_common::{KeyId, NameOrId};
use pegasus_common::downcast::*;

use crate::apis::Details;
use crate::apis::PropertyValue;

#[inline]
fn encode_runtime_property(prop_id: PropId, prop_val: Property) -> (NameOrId, Object) {
    let prop_key = NameOrId::Id(prop_id as KeyId);
    let prop_val = encode_runtime_prop_val(prop_val);
    (prop_key, prop_val)
}

#[inline]
fn encode_runtime_prop_val(prop_val: Property) -> Object {
    match prop_val {
        Property::Bool(b) => b.into(),
        Property::Char(c) => {
            if c <= (i8::MAX as u8) {
                Object::Primitive(Primitives::Byte(c as i8))
            } else {
                Object::Primitive(Primitives::Integer(c as i32))
            }
        }
        Property::Short(s) => Object::Primitive(Primitives::Integer(s as i32)),
        Property::Int(i) => Object::Primitive(Primitives::Integer(i)),
        Property::Long(l) => Object::Primitive(Primitives::Long(l)),
        Property::Float(f) => Object::Primitive(Primitives::Float(f as f64)),
        Property::Double(d) => Object::Primitive(Primitives::Float(d)),
        Property::Bytes(v) => Object::Blob(v.into_boxed_slice()),
        Property::String(s) => Object::String(s),
        _ => unimplemented!(),
    }
}

/// LazyVertexDetails is used for local property fetching optimization.
/// That is, the required properties will not be materialized until LazyVertexDetails need to be shuffled.
#[allow(dead_code)]
pub struct LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    // prop_keys specify the properties we would save for later queries after shuffle,
    // excluding the ones used only when local property fetching.
    // Specifically, in graphscope store, None means we do not need any property,
    // and Some(vec![]) means we need all properties
    prop_keys: Option<Vec<NameOrId>>,
    inner: AtomicPtr<V>,
}

impl<V> LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    pub fn new(v: V, prop_keys: Option<Vec<NameOrId>>) -> Self {
        let ptr = Box::into_raw(Box::new(v));
        LazyVertexDetails { prop_keys, inner: AtomicPtr::new(ptr) }
    }

    fn get_vertex_ptr(&self) -> Option<*mut V> {
        let ptr = self.inner.load(Ordering::SeqCst);
        if ptr.is_null() {
            None
        } else {
            Some(ptr)
        }
    }
}

impl<V> fmt::Debug for LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("gs_store LazyVertexDetails")
            .field("properties", &self.prop_keys)
            .field("inner", &self.inner)
            .finish()
    }
}

impl<V> Details for LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        if let NameOrId::Id(key) = key {
            if let Some(ptr) = self.get_vertex_ptr() {
                unsafe {
                    (*ptr)
                        .get_property(*key as PropId)
                        .map(|prop| PropertyValue::Owned(encode_runtime_prop_val(prop)))
                }
            } else {
                None
            }
        } else {
            info!("Have not support getting property by prop_name in gs_store yet");
            None
        }
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        if let Some(ptr) = self.get_vertex_ptr() {
            unsafe {
                Some(
                    (*ptr)
                        .get_properties()
                        .map(|(prop_id, prop_val)| encode_runtime_property(prop_id, prop_val))
                        .collect(),
                )
            }
        } else {
            None
        }
    }

    fn get_property_keys(&self) -> Option<&Vec<NameOrId>> {
        self.prop_keys.as_ref()
    }
}

impl<V> AsAny for LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<V> Drop for LazyVertexDetails<V>
where
    V: StoreVertex + 'static,
{
    fn drop(&mut self) {
        let ptr = self.inner.load(Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                std::ptr::drop_in_place(ptr);
            }
        }
    }
}

/// LazyEdgeDetails is used for local property fetching optimization.
/// That is, the required properties will not be materialized until LazyEdgeDetails need to be shuffled.
#[allow(dead_code)]
pub struct LazyEdgeDetails<E>
where
    E: StoreEdge + 'static,
{
    // prop_keys specify the properties we would save for later queries after shuffle,
    // excluding the ones used only when local property fetching.
    // Specifically, in graphscope store, None means we do not need any property,
    // and Some(vec![]) means we need all properties
    prop_keys: Option<Vec<NameOrId>>,
    inner: AtomicPtr<E>,
}

impl<E> LazyEdgeDetails<E>
where
    E: StoreEdge + 'static,
{
    pub fn new(e: E, prop_keys: Option<Vec<NameOrId>>) -> Self {
        let ptr = Box::into_raw(Box::new(e));
        LazyEdgeDetails { prop_keys, inner: AtomicPtr::new(ptr) }
    }

    fn get_edge_ptr(&self) -> Option<*mut E> {
        let ptr = self.inner.load(Ordering::SeqCst);
        if ptr.is_null() {
            None
        } else {
            Some(ptr)
        }
    }
}

impl<E> fmt::Debug for LazyEdgeDetails<E>
where
    E: StoreEdge + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("gs_store LazyEdgeDetails")
            .field("properties", &self.prop_keys)
            .field("inner", &self.inner)
            .finish()
    }
}

impl<E> Details for LazyEdgeDetails<E>
where
    E: StoreEdge + 'static,
{
    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        if let NameOrId::Id(key) = key {
            if let Some(ptr) = self.get_edge_ptr() {
                unsafe {
                    (*ptr)
                        .get_property(*key as PropId)
                        .map(|prop| PropertyValue::Owned(encode_runtime_prop_val(prop)))
                }
            } else {
                None
            }
        } else {
            info!("Have not support getting property by prop_name in gs_store yet");
            None
        }
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        // the case of get_all_properties from vertex;
        if let Some(ptr) = self.get_edge_ptr() {
            unsafe {
                Some(
                    (*ptr)
                        .get_properties()
                        .map(|(prop_id, prop_val)| encode_runtime_property(prop_id, prop_val))
                        .collect(),
                )
            }
        } else {
            None
        }
    }

    fn get_property_keys(&self) -> Option<&Vec<NameOrId>> {
        self.prop_keys.as_ref()
    }
}

impl<E> AsAny for LazyEdgeDetails<E>
where
    E: StoreEdge + 'static,
{
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<E> Drop for LazyEdgeDetails<E>
where
    E: StoreEdge + 'static,
{
    fn drop(&mut self) {
        let ptr = self.inner.load(Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                std::ptr::drop_in_place(ptr);
            }
        }
    }
}
