use std::cell::UnsafeCell;

#[derive(Clone)]
pub struct Volatile<T> {
    data: T,
}

impl<T> Volatile<T> {
    pub fn new(data: T) -> Self {
        Volatile { data }
    }
}

impl<T: Copy + Clone> Volatile<T> {
    pub fn get(&self) -> T {
        unsafe { std::ptr::read_volatile(&self.data as *const T) }
    }

    #[rustversion::before(1.74.0)]
    pub fn set(&self, data: T) {
        unsafe { std::ptr::write_volatile(&self.data as *const T as *mut T, data) }
    }

    #[rustversion::since(1.74.0)]
    #[rustversion::before(1.76.0)]
    pub fn set(&self, data: T) {
        unsafe {
            let ptr = &self.data as *const T;
            std::ptr::write_volatile(ptr as *mut T, data)
        }
    }

    #[rustversion::since(1.76.0)]
    pub fn set(&self, data: T) {
        let ptr = &self.data as *const T;
        unsafe { std::ptr::write_volatile((&*(ptr as *mut UnsafeCell<T>)).get(), data) }
    }
}
