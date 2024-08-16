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
