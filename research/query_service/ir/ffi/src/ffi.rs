use dyn_type::object::Primitives;
use dyn_type::object::RawType;
use dyn_type::Object;
use ir_common::generated::common as common_pb;

pub type VertexId = u64;
pub type LabelId = i32;
pub type EdgeId = u64;
pub type GraphHandle = *const ::libc::c_void;
pub type PropertyId = i32;
pub type SchemaHandle = *const ::libc::c_void;

/// -1 means failed, 0 means success
pub type FFIState = i32;

pub const STATE_SUCCESS: i32 = 0;
pub const STATE_FAILED: i32 = -1;

#[repr(C)]
#[derive(Debug)]
pub struct EdgeHandle {
    src: i64,
    dst: i64,
    offset: i64,
}

impl EdgeHandle {
    pub fn new() -> Self {
        EdgeHandle { src: 0, dst: 0, offset: 0 }
    }
}

#[repr(C)]
#[derive(Debug)]
pub enum PropertyType {
    Bool = 1,
    Char = 2,
    Short = 3,
    Int = 4,
    Long = 5,
    Float = 6,
    Double = 7,
    String = 8,
    Bytes = 9,
    IntList = 10,
    LongList = 11,
    FloatList = 12,
    DoubleList = 13,
    StringList = 14,
}

#[repr(C)]
pub struct NativeProperty {
    id: i32,
    r#type: PropertyType,
    data: *const u8,
    len: i64,
}

#[repr(C)]
pub struct WriteNativeProperty {
    id: i32,
    r#type: PropertyType,
    data: *const u8,
    len: i64,
}

#[allow(dead_code)]
extern "C" {
    fn get_property_as_bool(property: *const NativeProperty, out: *mut bool) -> FFIState;
    fn get_property_as_char(property: *const NativeProperty, out: *mut u8) -> FFIState;
    fn get_property_as_short(property: *const NativeProperty, out: *mut i16) -> FFIState;
    fn get_property_as_int(property: *const NativeProperty, out: *mut i32) -> FFIState;
    fn get_property_as_long(property: *const NativeProperty, out: *mut i64) -> FFIState;
    fn get_property_as_float(property: *const NativeProperty, out: *mut f32) -> FFIState;
    fn get_property_as_double(property: *const NativeProperty, out: *mut f64) -> FFIState;
    fn get_property_as_string(
        property: *const NativeProperty, out: *mut *const u8, out_len: *mut i32,
    ) -> FFIState;
    fn get_property_as_bytes(
        property: *const NativeProperty, out: *mut *const u8, out_len: *mut i32,
    ) -> FFIState;
    fn get_property_as_int_list(
        property: *const NativeProperty, out: *mut *const i32, out_len: *mut i32,
    ) -> FFIState;
    fn get_property_as_long_list(
        property: *const NativeProperty, out: *mut *const i64, out_len: *mut i32,
    ) -> FFIState;
    fn get_property_as_float_list(
        property: *const NativeProperty, out: *mut *const f32, out_len: *mut i32,
    ) -> FFIState;
    fn get_property_as_double_list(
        property: *const NativeProperty, out: *mut *const f64, out_len: *mut i32,
    ) -> FFIState;
    fn get_property_as_string_list(
        property: *const NativeProperty, out: *mut *const *const u8, out_len: *mut *const i32,
        out_num: *mut i32,
    ) -> FFIState;

    fn free_property(p: *const NativeProperty);
}

#[allow(dead_code)]
unsafe fn get_list_from_c_ptr<T: Copy>(ptr: *const T, len: i32) -> Vec<T> {
    let mut ret = Vec::with_capacity(len as usize);
    for offset in 0..len {
        ret.push(*ptr.offset(offset as isize));
    }
    ret
}

#[allow(dead_code)]
unsafe fn get_string_list_from_c_ptr(
    ptr: *const *const u8, len: *const i32, count: i32,
) -> Option<Vec<String>> {
    let mut ret = Vec::with_capacity(count as usize);
    for offset in 0..count {
        let s = *ptr.offset(offset as isize);
        let l = *len.offset(offset as isize);
        let buf = std::slice::from_raw_parts(s, l as usize);
        let tmp = std::str::from_utf8(buf).ok()?;
        ret.push(tmp.to_owned());
    }
    Some(ret)
}

impl NativeProperty {
    pub fn default() -> Self {
        NativeProperty { id: 0, r#type: PropertyType::Int, data: std::ptr::null(), len: 0 }
    }

    pub fn to_object(&self) -> Option<Object> {
        let property = self.as_ptr();
        match self.r#type {
            PropertyType::Bool => {
                let mut v = false;
                let res = unsafe { get_property_as_bool(property, &mut v as *mut bool) };
                if res == STATE_SUCCESS {
                    if v {
                        return Some(Object::Primitive(Primitives::Byte(1)));
                    } else {
                        return Some(Object::Primitive(Primitives::Byte(0)));
                    }
                }
            }
            PropertyType::Char => {
                let mut v: u8 = 0;
                let res = unsafe { get_property_as_char(property, &mut v as *mut u8) };
                if res == STATE_SUCCESS {
                    return Some(Object::Primitive(Primitives::Byte(v as i8)));
                }
            }
            PropertyType::Short => {
                let mut v: i16 = 0;
                let res = unsafe { get_property_as_short(property, &mut v as *mut i16) };
                if res == STATE_SUCCESS {
                    return Some(Object::Primitive(Primitives::Integer(v as i32)));
                }
            }
            PropertyType::Int => {
                let mut v = 0;
                let res = unsafe { get_property_as_int(property, &mut v as *mut i32) };
                if res == STATE_SUCCESS {
                    return Some(Object::Primitive(Primitives::Integer(v)));
                }
            }
            PropertyType::Long => {
                let mut v = 0;
                let res = unsafe { get_property_as_long(property, &mut v as *mut i64) };
                if res == STATE_SUCCESS {
                    return Some(Object::Primitive(Primitives::Long(v)));
                }
            }
            PropertyType::Float => {
                let mut v: f32 = 0.0;
                let res = unsafe { get_property_as_float(property, &mut v as *mut f32) };
                if res == STATE_SUCCESS {
                    return Some(Object::Primitive(Primitives::Float(v as f64)));
                }
            }
            PropertyType::Double => {
                let mut v: f64 = 0.0;
                let res = unsafe { get_property_as_double(property, &mut v as *mut f64) };
                if res == STATE_SUCCESS {
                    return Some(Object::Primitive(Primitives::Float(v)));
                }
            }
            PropertyType::String => {
                let mut v: *const u8 = std::ptr::null();
                let mut len = 0;
                let res = unsafe { get_property_as_string(property, &mut v, &mut len) };
                if res == STATE_SUCCESS {
                    let s = unsafe {
                        let buf = std::slice::from_raw_parts(v, len as usize);
                        std::str::from_utf8(buf)
                            .ok()
                            .map(|s| s.to_owned())
                    };
                    return s.map(|s| Object::String(s));
                }
            }
            PropertyType::Bytes => {
                let mut v: *const u8 = std::ptr::null();
                let mut len = 0;
                let res = unsafe { get_property_as_bytes(property, &mut v, &mut len) };
                if res == STATE_SUCCESS {
                    let ret = unsafe { std::slice::from_raw_parts(v, len as usize) }.to_vec();
                    return Some(Object::Blob(ret.into_boxed_slice()));
                }
            }
            _ => (),
        }
        None
    }

    fn as_ptr(&self) -> *const Self {
        self as *const Self
    }
}

impl Drop for NativeProperty {
    fn drop(&mut self) {
        if !self.data.is_null() {
            unsafe {
                free_property(self.as_ptr());
            }
        }
    }
}

#[repr(C)]
pub union PropertyUnion {
    pub b: bool,
    pub c: u8,
    pub s: i16,
    pub i: i32,
    pub l: i64,
    pub f: f32,
    pub d: f64,
}

impl WriteNativeProperty {
    pub fn default() -> Self {
        WriteNativeProperty { id: 0, r#type: PropertyType::Int, data: std::ptr::null(), len: 0 }
    }

    pub fn new(id: i32, r#type: PropertyType, data: *const u8, len: i64) -> Self {
        WriteNativeProperty { id, r#type, data, len }
    }

    fn as_ptr(&self) -> *const Self {
        self as *const Self
    }
}

impl Drop for WriteNativeProperty {
    fn drop(&mut self) {
        if !self.data.is_null() {
            let data_len = self.len as usize;
            match self.r#type {
                PropertyType::Bool
                | PropertyType::Char
                | PropertyType::Short
                | PropertyType::Int
                | PropertyType::Long
                | PropertyType::Float
                | PropertyType::Double => unsafe {
                    drop(Vec::from_raw_parts(self.data as *mut u8, 0, 0));
                },
                _ => unsafe {
                    drop(Vec::from_raw_parts(self.data as *mut u8, data_len, data_len));
                },
            }
        }
    }
}

impl PropertyType {
    pub fn from_raw_type(raw_type: RawType) -> Self {
        match raw_type {
            RawType::Byte => PropertyType::Char,
            RawType::Integer => PropertyType::Int,
            RawType::Long => PropertyType::Long,
            RawType::ULLong => PropertyType::Long,
            RawType::Float => PropertyType::Double,
            RawType::String => PropertyType::String,
            RawType::Blob(_) => PropertyType::Bytes,
            _ => {
                unimplemented!("Unsupported data type {:?}", raw_type)
            }
        }
    }
    pub fn to_raw_type(&self) -> RawType {
        match *self {
            PropertyType::Bool => RawType::Byte,
            PropertyType::Char => RawType::Byte,
            PropertyType::Short => RawType::Integer,
            PropertyType::Int => RawType::Integer,
            PropertyType::Long => RawType::Long,
            PropertyType::Float => RawType::Float,
            PropertyType::Double => RawType::Float,
            PropertyType::String => RawType::String,
            _ => {
                unimplemented!("Unsupported data type {:?}", *self)
            }
        }
    }

    pub fn from_data_type(raw_type: common_pb::DataType) -> Self {
        match raw_type {
            common_pb::DataType::Boolean => PropertyType::Bool,
            common_pb::DataType::Int32 => PropertyType::Int,
            common_pb::DataType::Int64 => PropertyType::Long,
            common_pb::DataType::Double => PropertyType::Double,
            common_pb::DataType::String => PropertyType::String,
            common_pb::DataType::Bytes => PropertyType::Bytes,
            common_pb::DataType::Int32Array => PropertyType::IntList,
            common_pb::DataType::Int64Array => PropertyType::LongList,
            common_pb::DataType::DoubleArray => PropertyType::DoubleList,
            common_pb::DataType::StringArray => PropertyType::StringList,
            _ => {
                unimplemented!("Unsupported data type {:?}", raw_type)
            }
        }
    }
    pub fn to_data_type(&self) -> common_pb::DataType {
        match *self {
            PropertyType::Bool => common_pb::DataType::Boolean,
            PropertyType::Int => common_pb::DataType::Int32,
            PropertyType::Long => common_pb::DataType::Int64,
            PropertyType::Double => common_pb::DataType::Double,
            PropertyType::String => common_pb::DataType::String,
            PropertyType::Bytes => common_pb::DataType::Bytes,
            PropertyType::IntList => common_pb::DataType::Int32Array,
            PropertyType::LongList => common_pb::DataType::Int64Array,
            PropertyType::DoubleList => common_pb::DataType::DoubleArray,
            PropertyType::StringList => common_pb::DataType::StringArray,
            _ => {
                unimplemented!("Unsupported data type {:?}", *self)
            }
        }
    }
}
