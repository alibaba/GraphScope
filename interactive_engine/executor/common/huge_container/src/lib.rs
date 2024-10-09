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

#[cfg(target_os = "linux")]
mod linux_hugepages;
#[cfg(target_os = "linux")]
use linux_hugepages::hugepage_alloc;
#[cfg(target_os = "linux")]
use linux_hugepages::hugepage_dealloc;

#[cfg(not(target_os = "linux"))]
mod notlinux_hugepages;
#[cfg(not(target_os = "linux"))]
use notlinux_hugepages::hugepage_alloc;
#[cfg(not(target_os = "linux"))]
use notlinux_hugepages::hugepage_dealloc;

mod huge_vec;

pub use huge_vec::HugeVec;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);

        let mut vec = HugeVec::<i32>::new();
        vec.push(1);
        vec.push(2);
        vec.push(3);

        assert_eq!(vec.len(), 3);
        assert_eq!(vec[0], 1);
        assert_eq!(vec[1], 2);
        assert_eq!(vec[2], 3);
    }
}
