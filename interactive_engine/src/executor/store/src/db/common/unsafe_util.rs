pub unsafe fn to_mut<T>(t: &T) -> &mut T {
    &mut *(t as *const T as *mut T)
}