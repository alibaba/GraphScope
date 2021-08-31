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
    extern crate itertools;

    use self::itertools::Itertools;
    use dyn_type::{object, Object, Primitives};
    use std::cmp::Ordering;
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::hash::Hash;

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

    fn is_map_eq<K: PartialEq + Ord + Debug + Hash, V: PartialEq + Ord + Debug>(
        map1: &HashMap<K, V>, map2: &HashMap<K, V>,
    ) -> bool {
        map1.iter().sorted().eq(map2.iter().sorted())
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

        /*
        let mut map = HashMap::new();
        // Review some books.
        map.insert("Adventures of Huckleberry Finn".to_string(), "My favorite book.".to_string());
        map.insert("Grimms' Fairy Tales".to_string(), "Masterpiece.".to_string());
        map.insert("Pride and Prejudice".to_string(), "Very enjoyable.".to_string());
        map.insert(
            "The Adventures of Sherlock Holmes".to_string(),
            "Eye lyked it alot.".to_string(),
        );

        let map_obj = Object::DynOwned(Box::new(map.clone()));
        let map_recovered = map_obj.get::<HashMap<String, String>>().unwrap();
        assert!(is_map_eq(&map, &(*map_recovered)));

        let map_borrow = map_obj.as_borrow();
        let map_borrow_to_owned = map_borrow.try_to_owned().unwrap();
        assert!(is_map_eq(&map, &(*map_borrow_to_owned.get::<HashMap<String, String>>().unwrap())));
         */
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
