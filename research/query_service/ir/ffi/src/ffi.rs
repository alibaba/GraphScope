use std::ffi::{CStr, CString};
use std::sync::Arc;

use dyn_type::object::Primitives;
use dyn_type::object::RawType;
use dyn_type::Object;

// TODO: a preclude type define in ir-runtime
type KeyId = i32;
type VertexId = u64;
// TODO: these are types define in store, for read-path;
type LabelId = i32;
type PartitionId = u16;
type EdgeId = i64;

pub type GraphHandle = *const ::libc::c_void;
pub type GraphId = i64;
type GetVertexIterator = *const ::libc::c_void;
type GetAllVerticesIterator = *const ::libc::c_void;
type VertexHandle = i64;
pub(crate) type PropertyId = i32;
pub(crate) type FFILabelId = i32;
pub(crate) type FFIPartitionId = i32;
type PropertiesIterator = *const ::libc::c_void;
type OutEdgeIterator = *const ::libc::c_void;
type InEdgeIterator = *const ::libc::c_void;
type GetAllEdgesIterator = *const ::libc::c_void;
pub type SchemaHandle = *const ::libc::c_void;

/// -1 means failed, 0 means success
type FFIState = i32;

const STATE_SUCCESS: i32 = 0;
const STATE_FAILED: i32 = -1;

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

extern "C" {
    fn get_graph_handle(graph_id: GraphId, channel_num: FFIPartitionId) -> GraphHandle;
    fn free_graph_handle(handle: GraphHandle);

    fn get_vertices(
        graph: GraphHandle, partition_id: FFIPartitionId, label: *const FFILabelId, ids: *const VertexId,
        count: i32,
    ) -> GetVertexIterator;
    fn free_get_vertex_iterator(iter: GetVertexIterator);
    fn get_vertices_next(iter: GetVertexIterator, v_out: *mut VertexHandle) -> FFIState;

    fn get_all_vertices(
        graph: GraphHandle, partition_id: FFIPartitionId, labels: *const FFILabelId, label_count: i32,
        limit: i64,
    ) -> GetAllVerticesIterator;
    fn free_get_all_vertices_iterator(iter: GetAllVerticesIterator);
    fn get_all_vertices_next(iter: GetAllVerticesIterator, v_out: *mut VertexHandle) -> FFIState;

    fn get_vertex_id(graph: GraphHandle, v: VertexHandle) -> VertexId;
    fn get_vertex_label(graph: GraphHandle, v: VertexHandle) -> LabelId;
    fn get_vertex_property(
        graph: GraphHandle, v: VertexHandle, id: PropertyId, p_out: *mut NativeProperty,
    ) -> FFIState;
    fn get_vertex_properties(graph: GraphHandle, v: VertexHandle) -> PropertiesIterator;

    fn free_properties_iterator(iter: PropertiesIterator);
    fn properties_next(iter: PropertiesIterator, p_out: *mut NativeProperty) -> FFIState;

    fn get_out_edges(
        graph: GraphHandle, partition_id: FFIPartitionId, src_id: VertexId, labels: *const FFILabelId,
        label_count: i32, limit: i64,
    ) -> OutEdgeIterator;
    fn free_out_edge_iterator(iter: OutEdgeIterator);
    fn out_edge_next(iter: OutEdgeIterator, e_out: *mut EdgeHandle) -> FFIState;

    fn get_in_edges(
        graph: GraphHandle, partition_id: FFIPartitionId, dst_id: VertexId, labels: *const FFILabelId,
        label_count: i32, limit: i64,
    ) -> InEdgeIterator;
    fn free_in_edge_iterator(iter: InEdgeIterator);
    fn in_edge_next(iter: InEdgeIterator, e_out: *mut EdgeHandle) -> FFIState;

    fn get_all_edges(
        graph: GraphHandle, partition_id: FFIPartitionId, labels: *const FFILabelId, label_count: i32,
        limit: i64,
    ) -> GetAllEdgesIterator;
    fn free_get_all_edges_iterator(iter: GetAllEdgesIterator);
    fn get_all_edges_next(iter: GetAllEdgesIterator, e_out: *mut EdgeHandle) -> FFIState;

    fn get_edge_src_id(graph: GraphHandle, e: *const EdgeHandle) -> VertexId;
    fn get_edge_dst_id(graph: GraphHandle, e: *const EdgeHandle) -> VertexId;
    fn get_edge_id(graph: GraphHandle, e: *const EdgeHandle) -> EdgeId;
    fn get_edge_src_label(graph: GraphHandle, e: *const EdgeHandle) -> LabelId;
    fn get_edge_dst_label(graph: GraphHandle, e: *const EdgeHandle) -> LabelId;
    fn get_edge_label(graph: GraphHandle, e: *const EdgeHandle) -> LabelId;
    fn get_edge_property(
        graph: GraphHandle, e: *const EdgeHandle, id: PropertyId, p_out: *mut NativeProperty,
    ) -> FFIState;
    fn get_edge_properties(graph: GraphHandle, e: *const EdgeHandle) -> PropertiesIterator;

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

    pub(crate) fn get_schema(graph: GraphHandle) -> SchemaHandle;
    fn get_property_id(schema: SchemaHandle, name: *const ::libc::c_char, out: *mut PropertyId)
        -> FFIState;
    fn get_property_type(
        schema: SchemaHandle, label: LabelId, id: PropertyId, out: *mut PropertyType,
    ) -> FFIState;
    fn get_property_name(
        schema: SchemaHandle, id: PropertyId, out: *const *const ::libc::c_char,
    ) -> FFIState;
    fn get_label_id(schema: SchemaHandle, name: *const ::libc::c_char, out: *mut LabelId) -> FFIState;
    fn get_label_name(schema: SchemaHandle, label: LabelId, out: *const *const ::libc::c_char) -> FFIState;
    fn free_schema(schema: SchemaHandle);

    fn free_string(s: *const ::libc::c_char);

    fn get_partition_id(graph: GraphHandle, vid: VertexId) -> i32;
    fn get_process_partition_list(
        graph: GraphHandle, partition_list: *const *const ::libc::c_int,
        partition_count: *const ::libc::c_int,
    );
    fn free_partition_list(partition_list: *const ::libc::c_int);

    fn get_vertex_id_from_primary_key(
        graph: GraphHandle, label_id: LabelId, key: *const ::libc::c_char, internal_id: &mut VertexId,
        partition_id: &mut PartitionId,
    ) -> FFIState;

    fn get_outer_id(graph: GraphHandle, vid: VertexId) -> VertexId;

    fn test1(test: NativeProperty);
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

unsafe fn get_list_from_c_ptr<T: Copy>(ptr: *const T, len: i32) -> Vec<T> {
    let mut ret = Vec::with_capacity(len as usize);
    for offset in 0..len {
        ret.push(*ptr.offset(offset as isize));
    }
    ret
}

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
union PropertyUnion {
    b: bool,
    c: u8,
    s: i16,
    i: i32,
    l: i64,
    f: f32,
    d: f64,
}

impl WriteNativeProperty {
    pub fn default() -> Self {
        WriteNativeProperty { id: 0, r#type: PropertyType::Int, data: std::ptr::null(), len: 0 }
    }

    pub fn from_object(prop_id: KeyId, property: Object) -> Self {
        let (prop_type, mut data, data_len) = {
            match property {
                Object::Primitive(Primitives::Byte(v)) => {
                    let u = PropertyUnion { c: v as u8 };
                    (PropertyType::Char, vec![], unsafe { u.l })
                }
                Object::Primitive(Primitives::Integer(v)) => {
                    let u = PropertyUnion { i: v };
                    (PropertyType::Int, vec![], unsafe { u.l })
                }
                Object::Primitive(Primitives::Long(v)) => {
                    let u = PropertyUnion { l: v };
                    (PropertyType::Long, vec![], unsafe { u.l })
                }
                Object::Primitive(Primitives::ULLong(v)) => {
                    let u = PropertyUnion { l: v as i64 };
                    (PropertyType::Long, vec![], unsafe { u.l })
                }
                Object::Primitive(Primitives::Float(v)) => {
                    let u = PropertyUnion { d: v };
                    (PropertyType::Double, vec![], unsafe { u.l })
                }
                Object::String(ref v) => {
                    let vecdata = v.to_owned().into_bytes();
                    let len = vecdata.len() as i64;
                    (PropertyType::String, vecdata, len)
                }
                Object::Blob(ref v) => {
                    let vecdata = v.to_vec();
                    let len = vecdata.len() as i64;
                    (PropertyType::Bytes, vecdata, len)
                }
                _ => {
                    panic!("Unsupported object type: {:?}", property)
                }
            }
        };
        data.shrink_to_fit();
        let data_ptr = data.as_ptr();
        ::std::mem::forget(data);
        WriteNativeProperty { id: prop_id as i32, r#type: prop_type, data: data_ptr, len: data_len as i64 }
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
}

// TODO: should preserve the schema apis
// //////////// schema apis //////////////
// struct FFISchema {
//     schema: SchemaHandle,
// }
//
// impl FFISchema {
//     pub fn new(schema: SchemaHandle) -> Self {
//         FFISchema { schema }
//     }
// }
//
// impl Schema for FFISchema {
//     fn get_prop_id(&self, name: &str) -> Option<u32> {
//         let c_name = CString::new(name).unwrap();
//         let mut ret = 0;
//         let state = unsafe { get_property_id(self.schema, c_name.as_ptr(), &mut ret) };
//         if state == STATE_SUCCESS {
//             return Some(ret as u32);
//         }
//         None
//     }
//
//     fn get_prop_type(&self, label: u32, prop_id: u32) -> Option<DataType> {
//         let mut t = PropertyType::Bool;
//         let state = unsafe { get_property_type(self.schema, label, prop_id as PropertyId, &mut t) };
//         if state == STATE_SUCCESS {
//             return Some(t.to_data_type());
//         }
//         None
//     }
//
//     fn get_prop_name(&self, prop_id: u32) -> Option<String> {
//         let name: *const ::libc::c_char = std::ptr::null();
//         let state = unsafe { get_property_name(self.schema, prop_id as PropertyId, &name) };
//         info!("get prop name: state = {:?}, expected: {:?}", state, STATE_SUCCESS);
//         if state == STATE_SUCCESS {
//             let c_name = unsafe { CStr::from_ptr(name) };
//             let ret = c_name.to_owned().into_string().ok();
//             info!("get prop name: c_name = {:?} -> {:?}", c_name, ret);
//             unsafe {
//                 free_string(name);
//             }
//             return ret;
//         }
//         None
//     }
//
//     fn get_label_id(&self, name: &str) -> Option<u32> {
//         let mut id = 0;
//         let c_name = CString::new(name).unwrap();
//         let state = unsafe { get_label_id(self.schema, c_name.as_ptr(), &mut id) };
//         if state == STATE_SUCCESS {
//             return Some(id);
//         }
//         None
//     }
//
//     fn get_label_name(&self, label: u32) -> Option<String> {
//         let name: *const ::libc::c_char = std::ptr::null();
//         let state = unsafe { get_label_name(self.schema, label, &name) };
//         info!("get label name: state = {:?}, expected: {:?}", state, STATE_SUCCESS);
//         if state == STATE_SUCCESS {
//             let c_name = unsafe { CStr::from_ptr(name) };
//             let ret = c_name.to_str().ok().map(|s| s.to_owned());
//             info!("get label name: c_name = {:?} -> {:?}", c_name, ret);
//             unsafe {
//                 free_string(name);
//             }
//             return ret;
//         }
//         None
//     }
//
//     fn to_proto(&self) -> Vec<u8> {
//         unimplemented!()
//     }
// }
//
// impl Drop for FFISchema {
//     fn drop(&mut self) {
//         unsafe {
//             free_schema(self.schema);
//         }
//     }
// }
//
// unsafe impl Send for FFISchema {}
//
// unsafe impl Sync for FFISchema {}
//
//
// impl VineyardPartitionManager {
//     pub fn new(graph: GraphHandle) -> Self {
//         VineyardPartitionManager { graph }
//     }
// }
//
// impl GraphPartitionManager for VineyardPartitionManager {
//     fn get_partition_id(&self, vid: i64) -> i32 {
//         unsafe { get_partition_id(self.graph, vid) }
//     }
//
//     // TODO(bingqing): check with vineyard
//     fn get_server_id(&self, _pid: u32) -> Option<u32> {
//         unimplemented!()
//     }
//
//     fn get_process_partition_list(&self) -> Vec<u32> {
//         let mut partition_id_list = Vec::new();
//         let mut partition_count: i32 = 0;
//         unsafe {
//             let pvalue_list: *const ::libc::c_int = std::ptr::null();
//             let partition_list: *const *const ::libc::c_int = &pvalue_list;
//             info!("start read partition list");
//             get_process_partition_list(self.graph, partition_list, &mut partition_count);
//             for i in 0..partition_count as usize {
//                 info!("receive partition: {:?}", pvalue_list.offset(i as isize) as PartitionId);
//                 partition_id_list.push(*pvalue_list.offset(i as isize) as PartitionId)
//             }
//             info!("partition_id_list: {:?}", partition_id_list);
//             free_partition_list(pvalue_list);
//         }
//
//         partition_id_list
//     }
//
//     fn get_vertex_id_by_primary_key(&self, label_id: u32, key: &String) -> Option<(u32, i64)> {
//         let mut partition_id = 0;
//         let mut vertex_id = 0;
//         let c_key = CString::new(key.as_str()).unwrap();
//         unsafe {
//             let ret = get_vertex_id_from_primary_key(
//                 self.graph,
//                 label_id,
//                 c_key.as_ptr(),
//                 &mut vertex_id,
//                 &mut partition_id,
//             );
//             if 0 != ret {
//                 error!("get vertex id from primary key {:?} with label id {:?} failed", key, label_id);
//                 return None;
//             }
//         }
//         return Some((partition_id as u32, vertex_id));
//     }
//
//     fn get_vertex_id_by_primary_keys(&self, label_id: LabelId, pks: &[Property]) -> Option<VertexId> {
//         if pks.len() != 1 {
//             warn!("multiple pks are not supported in Vineyard {:?}", pks);
//             return None;
//         } else {
//             let pk = pks.get(0).unwrap();
//             let key = match pk {
//                 // Vineyard only supports `id` as pk.
//                 Property::Char(i) => Some(i.to_string()),
//                 Property::Short(i) => Some(i.to_string()),
//                 Property::Int(i) => Some(i.to_string()),
//                 Property::Long(i) => Some(i.to_string()),
//                 Property::String(s) => Some(s.clone()),
//                 _ => None,
//             };
//             if let Some(key) = key {
//                 let mut partition_id = 0;
//                 let mut vertex_id = 0;
//                 let c_key = CString::new(key.as_str()).unwrap();
//                 unsafe {
//                     let ret = get_vertex_id_from_primary_key(
//                         self.graph,
//                         label_id,
//                         c_key.as_ptr(),
//                         &mut vertex_id,
//                         &mut partition_id,
//                     );
//                     if 0 != ret {
//                         error!(
//                             "get vertex id from primary key {:?} with label id {:?} failed",
//                             key, label_id
//                         );
//                         return None;
//                     }
//                 }
//                 Some(vertex_id)
//             } else {
//                 None
//             }
//         }
//     }
// }
//
// unsafe impl Send for VineyardPartitionManager {}
//
// unsafe impl Sync for VineyardPartitionManager {}
//
// #[test]
// fn zzzzzz() {
//     let pid_list = vec![1, 2, 3];
//     unsafe {
//         let graph_handle = get_graph_handle(1, 1);
//         let partition_manager = VineyardPartitionManager::new(graph_handle);
//         let partition_id_list = partition_manager.get_process_partition_list();
//         println!("partition id list {:?}", partition_id_list);
//
//         // get_graph_handle(1);
//         // free_graph_handle(std::ptr::null());
//
//         // let p = NativeProperty { data: std::ptr::null(), len: 10, r#type: PropertyType::Double};
//
//         // test1(p);
//     }
// }
