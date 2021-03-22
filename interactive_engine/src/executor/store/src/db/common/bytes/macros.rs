macro_rules! unsafe_write_func {
    ($ob:ident, $ty:ty, $of:expr, $da:expr) => {
        {
            unsafe {
                let dst = $ob.buf.offset($of as isize) as *mut $ty;
                *dst = $da;
            }
        }
    };
}

macro_rules! unsafe_read_func {
    ($ob:ident, $ty:ty, $of:expr) => {
        {
            unsafe {
                *($ob.buf.offset($of as isize) as *const $ty)
            }
        }
    };
}

#[allow(unused_macros)]
macro_rules! write_func {
    ($ob:ident, $ty:ty, $of:expr, $da:ident) => {
        {
            let size = ::std::mem::size_of::<$ty>();
            if $of + size > $ob.capacity {
                Err(MemError::new(ErrCode::Overflow, format!("try to write a {}B obj at offset {}, but beyond the capacity: {}", size, $of, $ob.capacity)))
            } else {
                unsafe {
                    let src = &$da as *const $ty as *const u8;
                    let dst = $ob.buf.offset($of as isize) as *mut u8;
                    ::std::intrinsics::copy_nonoverlapping(src, dst, size);
                }
                Ok(())
            }
        }
    };
}

#[allow(unused_macros)]
macro_rules! read_func {
    ($obj:ident, $ty:ty, $of:expr) => {
        {
            let size = ::std::mem::size_of::<$ty>();
            if $of + size > $obj.capacity {
                Err(MemError::new(ErrCode::Overflow, format!("try to read a {}B obj at offset {}, but beyond the capacity: {}", size, $of, $obj.capacity)))
            } else {
                let ret = unsafe { *($obj.buf.offset($of as isize) as *const $ty) };
                Ok(ret)
            }
        }
    };
}

macro_rules! int_to_vec {
    ($x:ident, $ty:ty) => {
        {
            let size = ::std::mem::size_of::<$ty>();
            let mut data = Vec::with_capacity(size);
            unsafe {
                let src = &$x as *const $ty as *const u8;
                let dst = data.as_mut_ptr();
                ::std::intrinsics::copy_nonoverlapping(src, dst, size);
                data.set_len(size);
            }
            data
        }
    };
}
