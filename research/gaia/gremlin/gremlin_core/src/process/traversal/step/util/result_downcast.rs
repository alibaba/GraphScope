use crate::process::traversal::traverser::{ShadeSync, Traverser};
use crate::Object;
use pegasus_server::factory::HashKey;

// TODO: more result type downcast

/// downcast result of groupCount() or group().by().by(count()) where AccumKind is CNT
pub fn try_downcast_group_count_pair(
    obj: &Object,
) -> Option<&ShadeSync<(HashKey<Traverser>, u64)>> {
    if let Object::UnknownOwned(object) = obj {
        object.try_downcast_ref::<ShadeSync<(HashKey<Traverser>, u64)>>()
    } else {
        None
    }
}

/// downcast result of group().by() where AccumKind is TO_LIST
pub fn try_downcast_group_by_pair(
    obj: &Object,
) -> Option<&ShadeSync<(HashKey<Traverser>, Vec<Traverser>)>> {
    if let Object::UnknownOwned(object) = obj {
        object.try_downcast_ref::<ShadeSync<(HashKey<Traverser>, Vec<Traverser>)>>()
    } else {
        None
    }
}

/// downcast result of count()
pub fn try_downcast_count(obj: &Object) -> Option<&ShadeSync<u64>> {
    if let Object::UnknownOwned(object) = obj {
        object.try_downcast_ref::<ShadeSync<u64>>()
    } else {
        None
    }
}

/// downcast result of group().by() and get key where key is a Traverser
pub fn try_downcast_group_key(obj: &Object) -> Option<Traverser> {
    if let Some(group_count_pair) = try_downcast_group_count_pair(obj) {
        let hash_key = group_count_pair.clone().inner.0;
        let key = hash_key.take();
        Some(key)
    } else if let Some(group_by_pair) = try_downcast_group_by_pair(obj) {
        let hash_key = group_by_pair.clone().inner.0;
        let key = hash_key.take();
        Some(key)
    } else {
        None
    }
}

/// downcast result of groupCount() and get value where value is u64 (i.e., AccumKind is CNT)
pub fn try_downcast_group_count_value(obj: &Object) -> Option<u64> {
    if let Some(group_count_pair) = try_downcast_group_count_pair(obj) {
        let value = group_count_pair.inner.1;
        Some(value)
    } else {
        None
    }
}

/// downcast result of group().by() and get value where value is Vec<Traverser> (i.e., AccumKind is TO_LIST)
pub fn try_downcast_group_by_value(obj: &Object) -> Option<Vec<Traverser>> {
    if let Some(group_count_pair) = try_downcast_group_by_pair(obj) {
        let value = group_count_pair.clone().inner.1;
        Some(value)
    } else {
        None
    }
}
