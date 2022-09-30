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

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use dyn_type::{object, BorrowObject, Object, Primitives};

    #[test]
    fn test_as_primitive() {
        let obj = object!(10);
        let p = Primitives::Integer(10);
        assert_eq!(obj.as_primitive().unwrap(), p);

        let obj = Object::String("a".to_owned());
        assert!(obj.as_primitive().is_err());

        let bytes: Vec<u8> = vec![];
        let obj = Object::Blob(bytes.into_boxed_slice());
        assert!(obj.as_primitive().is_err());
    }

    #[test]
    fn test_object_as() {
        let obj = object!(10);
        let b = obj.as_borrow();
        assert_eq!(obj.as_i8().unwrap(), 10_i8);
        assert_eq!(b.as_i8().unwrap(), 10_i8);

        let obj = object!(10.0);
        let b = obj.as_borrow();
        assert_eq!(obj.as_f64().unwrap(), 10.0);
        assert_eq!(b.as_f64().unwrap(), 10.0);

        let obj = object!(true);
        assert!(obj.as_bool().unwrap());
        let obj = object!(false);
        assert!(!obj.as_bool().unwrap());
    }

    #[test]
    fn test_object_contains() {
        // vector of numbers
        let object_vec: Object = vec![1, 2, 3].into();
        assert!(object_vec.contains(&1.into()));
        assert!(!object_vec.contains(&4.into()));
        assert!(object_vec.contains(&vec![1, 3].into()));
        assert!(!object_vec.contains(&vec![1, 5].into()));
        assert!(!object_vec.contains(&vec![1, 2, 3, 4].into()));
        // An i32 array can contain a `u64` value
        assert!(object_vec.contains(&1_u64.into()));

        // vector of floats
        let object_vec: Object = vec![1.0, 2.0, 3.0].into();
        assert!(object_vec.contains(&1.0.into()));
        assert!(!object_vec.contains(&4.0.into()));
        assert!(object_vec.contains(&vec![1.0, 3.0].into()));
        assert!(!object_vec.contains(&vec![1.0, 5.0].into()));
        assert!(!object_vec.contains(&vec![1.0, 2.0, 3.0, 4.0].into()));
        // An float-number array can contain a `u64` value
        assert!(object_vec.contains(&1_u64.into()));
        let object_vec: Object = vec![1.1, 2.0, 3.0].into();
        // An float-number array can contain a `u64` value only their values are equal
        assert!(!object_vec.contains(&1_u64.into()));

        // vector of strings
        let object_vec: Object = vec!["a".to_string(), "b".to_string(), "c".to_string()].into();
        assert!(object_vec.contains(&"a".into()));
        assert!(!object_vec.contains(&"d".into()));
        assert!(object_vec.contains(&vec!["a".to_string()].into()));
        assert!(!object_vec.contains(&vec!["a".to_string(), "d".to_string()].into()));

        // string object
        let object_str: Object = "abcde".to_string().into();
        assert!(object_str.contains(&"a".to_string().into()));
        assert!(object_str.contains(&"abc".to_string().into()));
        assert!(!object_str.contains(&"ac".to_string().into()));

        // borrow object
        let object_vec_b: BorrowObject = object_vec.as_borrow();
        assert!(object_vec_b.contains(&object!("a".to_string()).as_borrow()));
        assert!(!object_vec_b.contains(&object!("d".to_string()).as_borrow()));
        assert!(object_vec_b.contains(&object!(vec!["a".to_string()]).as_borrow()));
        assert!(!object_vec_b.contains(&object!(vec!["a".to_string(), "d".to_string()]).as_borrow()));
    }

    #[test]
    fn test_object_compare() {
        // vector
        let object_vec1 = object!(vec![1, 2, 3]);
        let object_vec2 = object!(vec![1, 2, 3]);
        let object_vec3 = object!(vec![3, 2]);

        assert_eq!(object_vec1, object_vec2);
        assert_ne!(object_vec1, object_vec3);
        assert!(object_vec1 < object_vec3);
        assert!(object_vec3 > object_vec2);

        // kv
        let object_kv1 = object!(vec![("a".to_string(), 1_u64), ("b".to_string(), 2_u64)]);
        let object_kv2 = object!(vec![("a".to_string(), 1_u64), ("b".to_string(), 2_u64)]);
        let object_kv3 = object!(vec![("a".to_string(), 2_u64), ("b".to_string(), 3_u64)]);
        let object_kv4 = object!(vec![("c".to_string(), 1_u64), ("d".to_string(), 2_u64)]);

        assert_eq!(object_kv1, object_kv2);
        assert_ne!(object_kv1, object_kv3);
        assert!(object_kv1 < object_kv3);
        assert!(object_kv3 > object_kv2);
        assert!(object_kv1 < object_kv4);
        assert!(object_kv4 > object_kv2);
    }

    #[test]
    fn test_dyn_object() {
        let vec = vec![1_u32, 2, 3, 4, 5];
        let vec_obj = Object::DynOwned(Box::new(vec.clone()));
        let vec_recovered = vec_obj.get::<Vec<u32>>().unwrap();
        assert_eq!(vec_recovered, vec);

        let vec_borrow = vec_obj.as_borrow();
        let vec_borrow_to_owned = vec_borrow.try_to_owned().unwrap();
        assert_eq!(vec_borrow_to_owned.get::<Vec<u32>>().unwrap(), vec);
    }

    #[test]
    fn test_owned_or_ref() {
        let a = object!(8_u128);
        let left = 0u128;
        let right = a.get().unwrap();
        assert_eq!(left.partial_cmp(&*right), Some(Ordering::Less));
        assert_eq!(right.partial_cmp(&left), Some(Ordering::Greater));
        assert_eq!(*&*right, 8_u128);
    }
}
