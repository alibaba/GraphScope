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

use crate::structure::element::Label;
use crate::structure::filter::{BiPredicate, Predicate};
use crate::{Element, ID};
use std::cell::RefCell;
use std::collections::HashSet;

mod by_id;
mod by_label;
mod by_property;

use crate::structure::PropKey;
use by_id::*;
use by_label::*;
use by_property::*;
use dyn_type::{DynType, Object};

/// The 'type' of TLV, which indicates that the expected value is left or right
/// By default, we assume the expected value is the right value
#[derive(Debug, Clone)]
pub enum TlvType {
    LeftValue,
    RightValue,
}
pub enum ExpectValue<T: DynType> {
    Local(T),
    TLV,
}

impl<T: DynType + Clone> ExpectValue<T> {
    #[inline]
    pub fn test<P: BiPredicate<T, T>>(&self, cmp: &P, left: &T) -> Option<bool> {
        match self {
            ExpectValue::Local(right) => cmp.test(left, right),
            ExpectValue::TLV => compare_to_tlv(cmp, left),
        }
    }
}

impl<T: DynType + Clone> From<Option<T>> for ExpectValue<T> {
    fn from(raw: Option<T>) -> Self {
        if let Some(v) = raw {
            ExpectValue::Local(v)
        } else {
            ExpectValue::TLV
        }
    }
}

thread_local! {
    static RIGHT_VALUE : RefCell<Option<Object>> = RefCell::new(None);
    static TLV_TYPE: RefCell<TlvType> = RefCell::new(TlvType::RightValue);
}

pub fn reset_tlv_right_value<T: Into<Object>>(value: T) {
    let value = value.into();
    RIGHT_VALUE.with(|tlv| {
        *tlv.borrow_mut() = Some(value);
    })
}

pub fn reset_tlv_left_value<T: Into<Object>>(value: T) {
    let value = value.into();
    TLV_TYPE.with(|tlv_type| *tlv_type.borrow_mut() = TlvType::LeftValue);
    RIGHT_VALUE.with(|tlv| {
        *tlv.borrow_mut() = Some(value);
    })
}

pub fn get_tlv_type() -> TlvType {
    let mut tlv_type = TlvType::RightValue;
    TLV_TYPE.with(|t_type| {
        tlv_type = t_type.borrow().clone();
    });
    tlv_type
}

pub fn clear_tlv_right_value() {
    RIGHT_VALUE.with(|tlv| *tlv.borrow_mut() = None)
}

#[inline]
pub fn compare_to_tlv<T: DynType + Clone, P: BiPredicate<T, T>>(
    cmp: &P, value: &T,
) -> Option<bool> {
    RIGHT_VALUE.with(|tlv| {
        let right = tlv.borrow();
        if let Some(v) = right.as_ref() {
            match v.get() {
                Ok(t) => match get_tlv_type() {
                    TlvType::LeftValue => cmp.test(value, &*t),
                    TlvType::RightValue => cmp.test(&*t, value),
                },
                Err(e) => {
                    warn!("cast compare left value failure: {}", e);
                    None
                }
            }
        } else {
            Some(false)
        }
    })
}

#[inline]
pub fn compare_to_tlv_obj<P: BiPredicate<Object, Object>>(cmp: &P, value: &Object) -> Option<bool> {
    RIGHT_VALUE.with(|tlv| tlv.borrow().as_ref().map(|v| cmp.test(v, value)).unwrap_or(Some(false)))
}

#[inline]
pub fn with_tlv<R, P: Fn(&Object) -> R>(func: P) -> Option<R> {
    RIGHT_VALUE.with(|tlv| tlv.borrow().as_ref().map(|v| func(v)))
}

#[enum_dispatch]
pub trait Reverse {
    fn reverse(&mut self);
}

impl Reverse for bool {
    fn reverse(&mut self) {
        *self = !*self;
    }
}

#[enum_dispatch(Reverse)]
pub enum ElementFilter {
    PassBy(bool),
    HasId(HasId),
    ContainsId(ContainsId),
    HasLabel(HasLabel),
    ContainsLabel(ContainsLabel),
    HasProperty(HasProperty),
    ContainsProperty(ContainsProperty),
}

impl<E: Element> Predicate<E> for ElementFilter {
    fn test(&self, entry: &E) -> Option<bool> {
        match self {
            ElementFilter::HasId(f) => f.test(entry),
            ElementFilter::ContainsId(f) => f.test(entry),
            ElementFilter::HasLabel(f) => f.test(entry),
            ElementFilter::ContainsLabel(f) => f.test(entry),
            ElementFilter::HasProperty(f) => f.test(entry),
            ElementFilter::PassBy(v) => Some(*v),
            ElementFilter::ContainsProperty(f) => f.test(entry),
        }
    }
}

pub fn has_id(id: Option<ID>) -> ElementFilter {
    ElementFilter::HasId(HasId::eq(id))
}

pub fn contains_id(ids: HashSet<ID>) -> ElementFilter {
    ElementFilter::ContainsId(ContainsId::with_in(ids))
}

pub fn has_label(label: Option<Label>) -> ElementFilter {
    ElementFilter::HasLabel(HasLabel::eq(label))
}

pub fn contains_label(labels: HashSet<Label>) -> ElementFilter {
    ElementFilter::ContainsLabel(ContainsLabel::with_in(labels))
}

pub fn has_property<O: Into<Object>>(key: PropKey, value: O) -> ElementFilter {
    ElementFilter::HasProperty(HasProperty::eq(key, Some(value.into())))
}

pub fn has_property_lt<O: Into<Object>>(key: PropKey, value: O) -> ElementFilter {
    ElementFilter::HasProperty(HasProperty::lt(key, Some(value.into())))
}

pub fn has_property_le<O: Into<Object>>(key: PropKey, value: O) -> ElementFilter {
    ElementFilter::HasProperty(HasProperty::le(key, Some(value.into())))
}

pub fn has_property_gt<O: Into<Object>>(key: PropKey, value: O) -> ElementFilter {
    ElementFilter::HasProperty(HasProperty::gt(key, Some(value.into())))
}

pub fn has_property_ge<O: Into<Object>>(key: PropKey, value: O) -> ElementFilter {
    ElementFilter::HasProperty(HasProperty::ge(key, Some(value.into())))
}

pub fn contains_property(key: PropKey, value: HashSet<Object>) -> ElementFilter {
    ElementFilter::ContainsProperty(ContainsProperty::with_in(key, value))
}

pub fn by() -> ElementFilter {
    has_id(None)
}

pub fn by_label() -> ElementFilter {
    has_label(None)
}

pub fn by_property(key: PropKey) -> ElementFilter {
    ElementFilter::HasProperty(HasProperty::eq(key, None))
}

pub fn by_property_lt(key: PropKey) -> ElementFilter {
    ElementFilter::HasProperty(HasProperty::lt(key, None))
}

pub fn by_property_le(key: PropKey) -> ElementFilter {
    ElementFilter::HasProperty(HasProperty::le(key, None))
}
