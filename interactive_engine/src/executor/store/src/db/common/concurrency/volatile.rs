#[derive(Clone)]
pub struct Volatile<T> {
    data: T,
}

impl<T> Volatile<T> {
    pub fn new(data: T) -> Self {
        Volatile {
            data,
        }
    }
}

impl<T: Copy + Clone> Volatile<T> {
    pub fn get(&self) -> T {
        unsafe { std::ptr::read_volatile(&self.data as *const T) }
    }

    pub fn set(&self, data: T) {
        unsafe { std::ptr::write_volatile(&self.data as *const T as *mut T, data) }
    }
}
