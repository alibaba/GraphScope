use std::fmt;
use std::ops;

pub struct HugeVec<T> {
    ptr: *mut T,
    cap: usize,
    len: usize,
}

impl<T> HugeVec<T> {
    pub fn new() -> Self {
        Self { ptr: std::ptr::null_mut(), cap: 0, len: 0 }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let cap_in_bytes = capacity * std::mem::size_of::<T>();
        let ptr = crate::hugepage_alloc(cap_in_bytes) as *mut T;
        Self { ptr, cap: capacity, len: 0 }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    pub fn reserve(&mut self, additional: usize) {
        let new_cap = self.cap + additional;
        let new_cap_in_bytes = new_cap * std::mem::size_of::<T>();
        let new_ptr = crate::hugepage_alloc(new_cap_in_bytes) as *mut T;

        if self.len > 0 {
            unsafe {
                std::ptr::copy_nonoverlapping(self.ptr, new_ptr, self.len);
            }
        }
        if self.cap > 0 {
            crate::hugepage_dealloc(self.ptr as *mut u8, self.cap * std::mem::size_of::<T>());
        }

        self.ptr = new_ptr;
        self.cap = new_cap;
    }

    pub fn as_ptr(&self) -> *const T {
        self.ptr
    }

    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.ptr
    }

    pub fn push(&mut self, value: T) {
        if self.len == self.cap {
            self.reserve(1);
        }

        unsafe {
            self.ptr.add(self.len).write(value);
        }

        self.len += 1;
    }

    pub fn clear(&mut self) {
        unsafe { std::ptr::drop_in_place(std::slice::from_raw_parts_mut(self.ptr, self.len)) }
        self.len = 0;
    }

    pub fn resize(&mut self, new_len: usize, value: T)
    where
        T: Clone,
    {
        if new_len > self.len {
            if new_len > self.cap {
                self.reserve(new_len - self.len);
            }

            for i in self.len..new_len {
                unsafe {
                    self.ptr.add(i).write(value.clone());
                }
            }
        } else {
            unsafe {
                std::ptr::drop_in_place(std::slice::from_raw_parts_mut(
                    self.ptr.add(new_len),
                    self.len - new_len,
                ));
            }
        }

        self.len = new_len;
    }
}

impl<T> Drop for HugeVec<T> {
    fn drop(&mut self) {
        self.clear();
        if self.cap > 0 {
            crate::hugepage_dealloc(self.ptr as *mut u8, self.cap * std::mem::size_of::<T>());
        }
    }
}

impl<T> ops::Deref for HugeVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl<T> ops::DerefMut for HugeVec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl<T: fmt::Debug> fmt::Debug for HugeVec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

unsafe impl<T> Sync for HugeVec<T> {}
unsafe impl<T> Send for HugeVec<T> {}
