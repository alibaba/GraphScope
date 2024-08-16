pub(crate) fn hugepage_alloc(size: usize) -> *mut u8 {
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };

    if ptr == libc::MAP_FAILED {
        panic!("hugepage allocation failed");
    }

    ptr as *mut u8
}

pub(crate) fn hugepage_dealloc(ptr: *mut u8, size: usize) {
    let ret = unsafe { libc::munmap(ptr as *mut libc::c_void, size) };
    if ret != 0 {
        panic!("hugepage deallocation failed, {}", ret);
    }
}
