use lazy_static::lazy_static;
use std::{
    fs::File,
    io::{self, BufRead, BufReader},
};

fn get_hugepage_size() -> io::Result<usize> {
    let file = File::open("/proc/meminfo")?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        if line.starts_with("Hugepagesize:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                if let Ok(size_kb) = parts[1].parse::<usize>() {
                    match parts[2] {
                        "kB" => return Ok(size_kb * 1024),
                        "MB" => return Ok(size_kb * 1024 * 1024),
                        "GB" => return Ok(size_kb * 1024 * 1024 * 1024),
                        _ => {}
                    }
                }
            }
        }
    }

    Err(io::Error::new(io::ErrorKind::NotFound, "Hugepagesize info not found"))
}

lazy_static! {
    static ref HUGE_PAGE_SIZE: usize = get_hugepage_size().unwrap();
}

fn align_to(size: usize, align: usize) -> usize {
    (size + align - 1) & !(align - 1)
}

pub(crate) fn hugepage_alloc(size: usize) -> *mut u8 {
    let len = align_to(size, *HUGE_PAGE_SIZE);
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_HUGETLB,
            -1,
            0,
        )
    };
    p as *mut u8
}

pub(crate) fn hugepage_dealloc(ptr: *mut u8, size: usize) {
    let len = align_to(size, *HUGE_PAGE_SIZE);
    let ret = unsafe { libc::munmap(ptr as *mut libc::c_void, len) };
    if ret != 0 {
        panic!("hugepage deallocation failed, {} - {} -> {}", ret, size, len);
    }
}
