use ::crossbeam_epoch as epoch;
use ::crossbeam_epoch::{Atomic, Owned, Guard};

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::collections::HashMap;

use crate::db::api::*;
use crate::db::common::bytes::util::{UnsafeBytesReader, UnsafeBytesWriter};
use crate::db::util::lock::GraphMutexLock;
use super::version::*;
use std::fmt;


pub type CodecVersion = i32;

/// format:
///                null area: each bit represent whether the corresponding prop is null
/// +---------+---------------+---+---------+
/// | version |0|1|2|3|4|5|6|7|8|9| padding | null area's len = (prop_count + 7) / 8
/// +---------+-------+-------+---------+---+-+-----------------+
/// | fixed len prop1 | fixed len prop2 | ... | fixed len propN |
/// +----------+------+---+-----+-------+--+--+-----------------+
/// | end off1 | end off2 | ... | end offM |                        ← every off is 3B len
/// +----------+----+-----+-----+--------+-+---+---------------+
/// | var len prop1 | var len prop2 .... | ... | var len propM |
/// +---------------+--------------------+-----+---------------+
/// ↑ this is `var_len_prop_start_offset`
#[derive(Clone, Debug)]
pub struct Codec {
    version: CodecVersion,
    id_map: HashMap<PropertyId, usize>,
    // prop id to idx
    inner_id_map: HashMap<PropertyId, usize>,
    // internal id to idx
    props: Vec<PropInfo>,
    offsets: Vec<usize>,

    fixed_len_prop_count: usize,
    var_len_prop_start_offset: usize,
    null_bytes: Vec<u8>,
}

impl Codec {
    pub fn get_version(&self) -> CodecVersion {
        self.version
    }

    pub fn from(type_def: &TypeDef) -> Self {
        let mut prop_defs: Vec<&PropDef> = type_def.get_prop_defs().collect();
        prop_defs.sort_by(|a, b| {
            let f1 = a.r#type.has_fixed_length();
            let f2 = b.r#type.has_fixed_length();
            if !(f1 ^ f2) {
                a.id.cmp(&b.id)
            } else if f1 {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        });

        let mut offset = std::mem::size_of::<CodecVersion>();
        let version = type_def.get_version();
        let mut id_map = HashMap::new();
        let mut inner_id_map = HashMap::new();
        let mut props = Vec::new();
        let mut offsets = Vec::new();
        let null_bytes = vec![0; (prop_defs.len() + 7) / 8];
        offset += null_bytes.len();
        let mut fixed_len_prop_count = 0;
        for prop_def in &prop_defs {
            id_map.insert(prop_def.id, props.len());
            inner_id_map.insert(prop_def.inner_id, props.len());
            let prop_info = PropInfo::from(*prop_def);
            props.push(prop_info);
            offsets.push(offset);
            if prop_def.r#type.has_fixed_length() {
                fixed_len_prop_count += 1;
                offset += prop_def.r#type.len();
            } else {
                offset += 3;
            }
        }
        let var_len_prop_start_offset = offset;

        Codec {
            version,
            id_map,
            inner_id_map,
            props,
            offsets,
            fixed_len_prop_count,
            var_len_prop_start_offset,
            null_bytes,
        }
    }
}

/// The src codec is used for decoding the binary data, the target codec defines the property should
/// be return. For example, target codec contains prop#1, prop#2 and prop#3, src codec contains prop#1,
/// prop#3 and prop#4. So the binary data has prop#1, prop#3 and prop#4. User will get prop#1, prop#2,
/// and prop#3 because current schema user can see has these properties. When user gets prop#1 or prop#3
/// and it's in binary data, so just return it. When user get prop#2 but it's not in binary data,
/// so return None. And prop#4 in data will never be get because user don't know it.
pub struct Decoder {
    target: &'static Codec,
    src: &'static Codec,
    _guard: Guard,
}

impl fmt::Debug for Decoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Decoder")
            .field("src", &self.src)
            .field("target", &self.target)
            .finish()
    }
}

impl Decoder {
    fn new(target: &'static Codec, src: &'static Codec, guard: Guard) -> Self {
        Decoder {
            target,
            src,
            _guard: guard,
        }
    }

    pub fn decode_properties<'a>(&self, data: &'a [u8]) -> IterDecoder<'a> {
        IterDecoder::new(self.clone(), data)
    }

    pub fn decode_all<'a>(&self, data: &'a [u8]) -> HashMap<PropertyId, ValueRef<'a>> {
        let mut map = HashMap::new();
        let mut iter = self.decode_properties(data);
        while let Some((prop_id, v)) = iter.next() {
            map.insert(prop_id, v);
        }
        map
    }

    pub fn decode_property<'a>(&self, data: &'a [u8], prop_id: PropertyId) -> Option<ValueRef<'a>> {
        let reader = UnsafeBytesReader::new(data);
        let idx = *self.target.id_map.get(&prop_id)?;
        if self.fast_mode() {
            return self.decode_property_at(&reader, idx);
        }
        let internal_id = self.target.props[idx].inner_id;
        let idx = *self.src.inner_id_map.get(&internal_id)?;
        self.decode_property_at(&reader, idx)
    }

    fn decode_property_at<'a>(&self, reader: &UnsafeBytesReader<'a>, idx: usize) -> Option<ValueRef<'a>> {
        let null_byte_off = std::mem::size_of::<CodecVersion>() + idx / 8;
        let null_byte = reader.read_u8(null_byte_off);
        let null_mask = 1 << (7 - (idx % 8) as u8);
        if (null_byte & null_mask) == 0 {
            if idx < self.src.fixed_len_prop_count {
                self.decode_fixed_len_property_at(reader, idx)
            } else {
                self.decode_var_len_property_at(reader, idx)
            }
        } else {
            None
        }
    }

    fn decode_fixed_len_property_at<'a>(&self, reader: &UnsafeBytesReader<'a>, idx: usize) -> Option<ValueRef<'a>> {
        let info = &self.src.props[idx];
        let offset = self.src.offsets[idx];
        let bytes = match info.r#type {
            ValueType::Bool | ValueType::Char => reader.read_bytes(offset, 1),
            ValueType::Short => reader.read_bytes(offset, 2),
            ValueType::Int | ValueType::Float => reader.read_bytes(offset, 4),
            ValueType::Double | ValueType::Long => reader.read_bytes(offset, 8),
            _ => unreachable!(),
        };
        let ret = ValueRef::new(info.r#type, bytes);
        Some(ret)
    }

    fn decode_var_len_property_at<'a>(&self, reader: &UnsafeBytesReader<'a>, idx: usize) -> Option<ValueRef<'a>> {
        let info = &self.src.props[idx];
        let offset = self.src.offsets[idx];
        let end_off = bytes_to_len(reader.read_bytes(offset, 3));
        let mut start_off = 0;
        if idx > self.src.fixed_len_prop_count {
            let offset = self.src.offsets[idx - 1];
            start_off = bytes_to_len(reader.read_bytes(offset, 3));
        }
        let len = end_off - start_off;
        if len > bytes_to_len(reader.read_bytes(*self.src.offsets.last().unwrap(), 3)) {
            let msg = format!("fatal error! This codec cannot decode the bytes");
            let err = gen_graph_err!(GraphErrorCode::DecodeError, msg);
            error!("{:?}", err);
            return None;
        }
        let start_off = start_off + self.src.var_len_prop_start_offset;
        let bytes = reader.read_bytes(start_off, len);
        let ret = ValueRef::new(info.r#type, bytes);
        Some(ret)
    }

    fn fast_mode(&self) -> bool {
        self.target.version == self.src.version
    }
}

impl Clone for Decoder {
    fn clone(&self) -> Self {
        Decoder {
            target: self.target,
            src: self.src,
            _guard: epoch::pin(),
        }

    }
}

/// this structure can decode properties as an iterator, each time get one property until all
/// properties are decoded
pub struct IterDecoder<'a> {
    decoder: Decoder,
    reader: UnsafeBytesReader<'a>,
    cur: usize,
}

impl<'a> IterDecoder<'a> {
    pub fn new(decoder: Decoder, data: &'a [u8]) -> Self {
        let reader = UnsafeBytesReader::new(data);
        IterDecoder {
            decoder,
            reader,
            cur: 0,
        }
    }

    pub fn next(&mut self) -> Option<(PropertyId, ValueRef<'a>)> {
        while self.cur < self.decoder.target.props.len() {
            let ret = if self.decoder.fast_mode() {
                self.fast_path()
            } else {
                self.slow_path()
            };
            self.cur += 1;
            if ret.is_some() {
                return ret;
            }
        }
        None
    }

    fn fast_path(&self) -> Option<(PropertyId, ValueRef<'a>)> {
        let v = self.decoder.decode_property_at(&self.reader, self.cur)?;
        let prop_id = self.decoder.target.props[self.cur].prop_id;
        Some((prop_id, v))
    }

    fn slow_path(&self) -> Option<(PropertyId, ValueRef<'a>)> {
        let info = &self.decoder.target.props[self.cur];
        let prop_id = info.prop_id;
        let internal_id = info.inner_id;
        let idx = *self.decoder.src.inner_id_map.get(&internal_id)?;
        let v = self.decoder.decode_property_at(&self.reader, idx)?;
        Some((prop_id, v))
    }
}

pub struct Encoder {
    codec: &'static Codec,
    _guard: Guard,
}

impl fmt::Debug for Encoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Encoder")
            .field("codec", &self.codec)
            .finish()
    }
}

impl Encoder {
    pub fn new(codec: &'static Codec, guard: Guard) -> Self {
        Encoder {
            codec,
            _guard: guard,
        }
    }

    pub fn encode(&self, props: &dyn PropertyMap, buf: &mut Vec<u8>) -> GraphResult<()> {
        // the vector pass to encoder may be not filled with zeros, so encoder should make sure
        // every bit is written by itself and set the vector's len to real length.
        let size = res_unwrap!(self.check_and_cal_size(props), encode)?;
        if buf.capacity() < size {
            buf.reserve(size - buf.capacity());
        }
        unsafe { buf.set_len(size); }
        let mut writer = UnsafeBytesWriter::new(buf);
        writer.write_i32(0, self.codec.version.to_be());
        let mut null_byte = 0;
        // write fixed len property
        self.encode_fix_len_properties(&mut writer, props, &mut null_byte)?;
        self.encode_var_len_properties(&mut writer, props, &mut null_byte)?;
        Ok(())
    }

    fn encode_fix_len_properties(&self, writer: &mut UnsafeBytesWriter, props: &dyn PropertyMap, null_byte: &mut u8) -> GraphResult<()> {
        for idx in 0..self.codec.fixed_len_prop_count {
            let info = &self.codec.props[idx];
            if let Some(data) = props.get(info.prop_id) {
                self.write_fix_len_property(writer, idx, data)?;
            } else if let Some(ref v) = info.default_value {
                writer.write_bytes(self.codec.offsets[idx], v);
            } else {
                *null_byte = *null_byte | (1 << (7 - (idx % 8) as u8));
            }
            if idx % 8 == 7 {
                let null_offset = std::mem::size_of_val(&self.codec.version) + (idx / 8);
                writer.write_u8(null_offset, *null_byte);
                *null_byte = 0;
            }
        }
        Ok(())
    }

    fn encode_var_len_properties(&self, writer: &mut UnsafeBytesWriter, props: &dyn PropertyMap, null_byte: &mut u8) -> GraphResult<()> {
        let mut end_off = 0;
        let mut null_written = false;
        for idx in self.codec.fixed_len_prop_count..self.codec.props.len() {
            null_written = false;
            let info = &self.codec.props[idx];
            if let Some(data) = props.get(info.prop_id) {
                res_unwrap!(data.check_type_match(info.r#type), encode_var_len_properties)?;
                let bytes = data.as_bytes();
                writer.write_bytes(self.codec.var_len_prop_start_offset + end_off, bytes);
                end_off += bytes.len();
            } else if let Some(ref v) = info.default_value {
                writer.write_bytes(self.codec.var_len_prop_start_offset + end_off, v);
                end_off += v.len();
            } else {
                *null_byte = *null_byte | (1 << (7 - (idx % 8) as u8));
            }
            let offset = self.codec.offsets[idx];
            let end_off_bytes = len_to_bytes(end_off);
            writer.write_bytes(offset, &end_off_bytes);
            if idx % 8 == 7 {
                let null_offset = std::mem::size_of_val(&self.codec.version) + (idx / 8);
                writer.write_u8(null_offset, *null_byte);
                null_written = true;
                *null_byte = 0;
            }
        }
        if !null_written && self.codec.props.len() > 0 {
            let null_offset = std::mem::size_of_val(&self.codec.version) + ((self.codec.props.len() - 1) / 8);
            writer.write_u8(null_offset, *null_byte);
        }
        Ok(())
    }

    fn write_fix_len_property(&self, writer: &mut UnsafeBytesWriter, idx: usize, data: ValueRef) -> GraphResult<()> {
        let info = &self.codec.props[idx];
        let offset = self.codec.offsets[idx];

        res_unwrap!(data.check_type_match(info.r#type), write_fix_len_property)?;
        writer.write_bytes(offset, data.as_bytes());
        Ok(())
    }

    fn check_and_cal_size(&self, props: &dyn PropertyMap) -> GraphResult<usize> {
        // for idx in 0..self.codec.fixed_len_prop_count {
        //     let info = &self.codec.props[idx];
        //     if let Some(data) = props.get(info.prop_id) {
        //         if !check_fixed_prop_len(info.r#type, data) {
        //             let msg = format!("invalid {:?} data", info.r#type);
        //             let err = gen_graph_err!(GraphErrorCode::InvalidData, msg);
        //             return Err(err);
        //         }
        //     }
        // }

        let mut size = self.codec.var_len_prop_start_offset;
        for idx in self.codec.fixed_len_prop_count..self.codec.props.len() {
            let info = &self.codec.props[idx];
            if let Some(data) = props.get(info.prop_id) {
                // check_var_len_prop(info.r#type, data)?;
                size += data.as_bytes().len();
            } else if let Some(ref v) = info.default_value {
                size += v.len();
            }
        }
        Ok(size)
    }
}

pub fn get_codec_version(data: &[u8]) -> CodecVersion {
    let reader = UnsafeBytesReader::new(data);
    reader.read_i32(0).to_be()
}

#[derive(Clone, Debug, PartialEq)]
struct PropInfo {
    prop_id: PropertyId,
    inner_id: PropertyId,
    r#type: ValueType,
    default_value: Option<Vec<u8>>,
}

impl PropInfo {
    #[cfg(test)]
    fn new(prop_id: PropertyId, inner_id: PropertyId, r#type: ValueType, default_value: Option<Value>) -> Self {
        PropInfo {
            prop_id,
            inner_id,
            r#type,
            default_value: default_value.map(|v| v.into_vec()),
        }
    }
}

impl From<&'_ PropDef> for PropInfo {
    fn from(prop_def: &PropDef) -> Self {
        PropInfo {
            prop_id: prop_def.id,
            inner_id: prop_def.inner_id,
            r#type: prop_def.r#type,
            default_value: prop_def.default_value.clone().map(|v| v.into_vec()).clone(),
        }
    }
}

#[inline]
fn len_to_bytes(len: usize) -> [u8; 3] {
    if len >= (1 << 24) {
        panic!("len shouldn't be larger than {}", 1 << 24);
    }
    let mut ret = [0u8, 0u8, 0u8];
    ret[0] = (len & 255) as u8;
    ret[1] = (len >> 8 & 255) as u8;
    ret[2] = (len >> 16) as u8;
    ret
}

#[inline]
fn bytes_to_len(bytes: &[u8]) -> usize {
    ((bytes[2] as usize) << 16) | ((bytes[1] as usize) << 8) | (bytes[0] as usize)
}

#[allow(dead_code)]
#[inline]
fn check_fixed_prop_len(r#type: ValueType, data: &[u8]) -> bool {
    match r#type {
        ValueType::Bool | ValueType::Char => data.len() == 1,
        ValueType::Short => data.len() == 2,
        ValueType::Int | ValueType::Float => data.len() == 4,
        ValueType::Long | ValueType::Double => data.len() == 8,
        _ => {
            panic!("need fixed len type but meet {:?}", r#type)
        }
    }
}

#[allow(dead_code)]
fn check_var_len_prop(r#type: ValueType, data: &[u8]) -> GraphResult<()> {
    match r#type {
        ValueType::String => {
            if std::str::from_utf8(data).is_ok() {
                return Ok(());
            }
            let msg = format!("string is not utf-8");
            let err = gen_graph_err!(GraphErrorCode::InvalidData, msg);
            return Err(err);
        }
        ValueType::Bytes => {
            return Ok(());
        }
        ValueType::IntList | ValueType::FloatList => {
            let reader = UnsafeBytesReader::new(data);
            if reader.read_i32(0).to_be() * 4 == data.len() as i32 - 4 {
                return Ok(());
            }
        }
        ValueType::DoubleList | ValueType::LongList => {
            let reader = UnsafeBytesReader::new(data);
            if reader.read_i32(0).to_be() * 8 == data.len() as i32 - 4 {
                return Ok(());
            }
        }
        ValueType::StringList => {
            //todo
            return Ok(());
        }
        _ => unreachable!()
    }
    let msg = format!("invalid {:?} data, length is not right", r#type);
    let err = gen_graph_err!(GraphErrorCode::InvalidData, msg);
    Err(err)
}

type CodecMap = HashMap<CodecVersion, Arc<Codec>>;

pub struct CodecManager {
    versions: VersionManager,
    codec_map: Atomic<CodecMap>,
    lock: GraphMutexLock<CodecVersion>,
}

impl CodecManager {
    pub fn new() -> Self {
        CodecManager {
            versions: VersionManager::new(),
            codec_map: Atomic::new(CodecMap::new()),
            lock: GraphMutexLock::new(-1),
        }
    }

    pub fn add_codec(&self, si: SnapshotId, codec: Codec) -> GraphResult<()> {
        let mut max_version = res_unwrap!(self.lock.lock(), add_codec, si, codec)?;
        if self.versions.get_latest_version() >= si {
            let msg = format!("si#{} is too small, cannot add", si);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, add_codec, si, codec);
            return Err(err);
        }
        let version = codec.get_version();
        if version <= *max_version {
            let msg = format!("current max codec version is {}, you cannot add a older one", *max_version);
            let err = gen_graph_err!(GraphErrorCode::InvalidOperation, msg, add_codec, si, codec);
            return Err(err);
        }
        let codec = Arc::new(codec);
        let guard = epoch::pin();
        let map = self.get_map(&guard);
        let mut map_clone = map.clone();
        map_clone.insert(version, codec);
        self.codec_map.store(Owned::new(map_clone), Ordering::Relaxed);
        self.versions.add(si, version as i64).unwrap();
        *max_version = version;
        Ok(())
    }

    pub fn get_encoder(&self, si: SnapshotId) -> GraphResult<Encoder> {
        if let Some(v) = self.versions.get(si) {
            let version = v.data as CodecVersion;
            let guard = epoch::pin();
            let map = self.get_map(&guard);
            return get_codec(map, version).map(|codec| {
                Encoder::new(codec, guard)
            });
        }
        let msg = format!("codec not found at si#{}", si);
        let err = gen_graph_err!(GraphErrorCode::MetaNotFound, msg, get_encoder, si);
        Err(err)
    }

    pub fn get_decoder(&self, si: SnapshotId, version: CodecVersion) -> GraphResult<Decoder> {
        if let Some(v) = self.versions.get(si) {
            let target_version = v.data as CodecVersion;
            let guard = epoch::pin();
            let map = self.get_map(&guard);
            let src = res_unwrap!(get_codec(map, version), get_decoder, si, version)?;
            let target = res_unwrap!(get_codec(map, target_version), get_decoder, si, version)?;
            return Ok(Decoder::new(target, src, guard));
        }
        let msg = format!("codec not found at si#{}", si);
        let err = gen_graph_err!(GraphErrorCode::MetaNotFound, msg, get_decoder, si, version);
        Err(err)
    }

    #[allow(dead_code)]
    pub fn drop_codec(&self, version: CodecVersion) -> GraphResult<()> {
        let _lock = res_unwrap!(self.lock.lock(), drop_codec, version)?;
        let guard = epoch::pin();
        let map = self.get_map(&guard);
        let mut map_clone = map.clone();
        if map_clone.remove(&version).is_some() {
            self.codec_map.store(Owned::new(map_clone), Ordering::Relaxed);
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn gc(&self, si: SnapshotId) -> GraphResult<()> {
        res_unwrap!(self.versions.gc(si).map(|_| ()), gc, si)
    }

    fn get_map(&self, guard: &Guard) -> &'static CodecMap {
        unsafe { &*self.codec_map.load(Ordering::Relaxed, &guard).as_raw() }
    }
}

fn get_codec(map: &CodecMap, version: CodecVersion) -> GraphResult<&Codec> {
    map.get(&version).map(|codec| {
        codec.as_ref()
    }).ok_or_else(|| {
        let msg = format!("codec of version#{} not found", version);
        gen_graph_err!(GraphErrorCode::MetaNotFound, msg, get_encoder, version)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_len_transform() {
        for len in vec![1, 2, 1000, 32342, 65536, (1 << 24) - 1] {
            let bytes = len_to_bytes(len);
            assert_eq!(bytes_to_len(&bytes), len);
        }
    }

    #[test]
    fn test_create_test_codec() {
        let codec = create_test_codec();
        check_codec(&codec);
    }

    fn check_codec(codec: &Codec) {
        assert_eq!(codec.fixed_len_prop_count, 7);
        assert_eq!(codec.var_len_prop_start_offset, 4 + 2 + 49);
        assert_eq!(codec.props, vec![
            PropInfo::new(3, 3, ValueType::Char, None),
            PropInfo::new(4, 4, ValueType::Int, None),
            PropInfo::new(5, 5, ValueType::Short, None),
            PropInfo::new(6, 6, ValueType::Bool, None),
            PropInfo::new(7, 7, ValueType::Long, None),
            PropInfo::new(8, 8, ValueType::Float, None),
            PropInfo::new(9, 9, ValueType::Double, None),
            PropInfo::new(1, 1, ValueType::String, None),
            PropInfo::new(2, 2, ValueType::Bytes, None),
            PropInfo::new(10, 10, ValueType::IntList, None),
            PropInfo::new(11, 11, ValueType::DoubleList, None),
            PropInfo::new(12, 12, ValueType::FloatList, None),
            PropInfo::new(13, 13, ValueType::StringList, None),
            PropInfo::new(14, 14, ValueType::LongList, None),
        ]);
        assert_eq!(codec.inner_id_map.len(), 14);
        assert_eq!(*codec.inner_id_map.get(&3).unwrap(), 0);
        assert_eq!(*codec.inner_id_map.get(&4).unwrap(), 1);
        assert_eq!(*codec.inner_id_map.get(&5).unwrap(), 2);
        assert_eq!(*codec.inner_id_map.get(&6).unwrap(), 3);
        assert_eq!(*codec.inner_id_map.get(&7).unwrap(), 4);
        assert_eq!(*codec.inner_id_map.get(&8).unwrap(), 5);
        assert_eq!(*codec.inner_id_map.get(&9).unwrap(), 6);
        assert_eq!(*codec.inner_id_map.get(&1).unwrap(), 7);
        assert_eq!(*codec.inner_id_map.get(&2).unwrap(), 8);
        assert_eq!(*codec.inner_id_map.get(&10).unwrap(), 9);
        assert_eq!(*codec.inner_id_map.get(&11).unwrap(), 10);
        assert_eq!(*codec.inner_id_map.get(&12).unwrap(), 11);
        assert_eq!(*codec.inner_id_map.get(&13).unwrap(), 12);
        assert_eq!(*codec.inner_id_map.get(&14).unwrap(), 13);
    }

    #[test]
    fn test_codec() {
        let mut builder = TypeDefBuilder::new();
        builder.version(0);
        builder.add_property(18, 18, "18".to_string(), ValueType::Long, None, false, "cmt".to_string());
        let type_def = builder.build();
        let codec = Codec::from(&type_def);
        let guard = epoch::pin();
        let codec_ref = unsafe { std::mem::transmute(&codec) };
        let encoder = Encoder::new(codec_ref, guard);
        let mut buf = Vec::new();

        let mut properties = HashMap::new();
        properties.insert(18, Value::long(20120904101614543));
        encoder.encode(&properties, &mut buf).unwrap();

        let decoder = Decoder::new(codec_ref, codec_ref, epoch::pin());
        let mut decode_iter = decoder.decode_properties(buf.as_slice());
        let decode_item = decode_iter.next();
        assert_ne!(decode_item, None);
        if let Some((prop_id, v)) = decode_item {
            assert_eq!(prop_id, 18);
            assert_eq!(*v.get_type() as i32, ValueType::Long as i32);
            assert_eq!(v.get_long().unwrap(), 20120904101614543);
        }
        assert_eq!(decode_iter.next(), None);
    }

    #[test]
    fn test_encode_decode() {
        let codec = create_test_codec();
        let guard = epoch::pin();
        let codec_ref = unsafe { std::mem::transmute(&codec) };
        let encoder = Encoder::new(codec_ref, guard);
        let data = test_data();
        // pollute the buf to make sure the encoder can work in any event
        let mut buf = vec![255; 1000];
        encoder.encode(&data, &mut buf).unwrap();
        assert_eq!(get_codec_version(&buf), codec.get_version());
        let decoder = Decoder::new(codec_ref, codec_ref, epoch::pin());
        check_properties(decoder, &buf, test_data());
    }

    #[test]
    fn test_default_value() {
        let codec = create_default_value_codec();
        let _encoder = create_decoder(&codec);
        let _decoder = create_decoder(&codec);
        let _data: Vec<(PropertyId, Value)> = test_data().into_iter().collect();

        #[allow(dead_code)]
        fn dfs(prop_list: &Vec<(PropertyId, Value)>, cur: usize, map: &mut HashMap<PropertyId, Value>, encoder: &Encoder) {
            if cur == prop_list.len() {

                return;
            }

            dfs(prop_list, cur + 1, map, encoder);
            let (ref prop_id, ref value) = prop_list[cur];
            map.insert(*prop_id, value.clone());
            dfs(prop_list, cur + 1, map, encoder);
            map.remove(prop_id);
        }

        #[allow(dead_code)]
        fn check(encoder: &Encoder, _decoder: &Decoder, map: &HashMap<PropertyId, Value>) {
            let mut buf = Vec::new();
            encoder.encode(map, &mut buf).unwrap();

        }

    }

    #[test]
    fn test_null_support() {
        let codec = create_test_codec();
        let encoder = create_encoder(&codec);
        let decoder = create_decoder(&codec);
        let data = test_data();
        let mut buf = Vec::new();

        for (prop_id, _v) in &data {
            let mut real_data = data.clone();
            real_data.remove(prop_id);
            encoder.encode(&real_data, &mut buf).unwrap();
            check_properties(decoder.clone(), &buf, real_data);
        }
    }

    fn create_encoder(codec: &Codec) -> Encoder {
        let codec_ref = unsafe { std::mem::transmute(codec) };
        Encoder::new(codec_ref, epoch::pin())
    }

    fn create_decoder(codec: &Codec) -> Decoder {
        let codec_ref = unsafe { std::mem::transmute(codec) };
        Decoder::new(codec_ref, codec_ref, epoch::pin())
    }

    fn check_properties(decoder: Decoder, data: &[u8], mut ans: HashMap<PropertyId, Value>) {
        for (prop_id, val) in &ans {
            let v = decoder.decode_property(data, *prop_id).unwrap();
            assert_eq!(v, val.as_ref());
        }
        let mut iter = decoder.decode_properties(data);
        while let Some((prop_id, v)) = iter.next() {
            assert_eq!(ans.remove(&prop_id).unwrap().as_ref(), v);
        }
        assert!(ans.is_empty());
    }

    fn test_prop_list() -> Vec<(PropertyId, PropertyId, ValueType)> {
        vec![
            (1, 1, ValueType::String),
            (2, 2, ValueType::Bytes),
            (3, 3, ValueType::Char),
            (4, 4, ValueType::Int),
            (5, 5, ValueType::Short),
            (6, 6, ValueType::Bool),
            (7, 7, ValueType::Long),
            (8, 8, ValueType::Float),
            (9, 9, ValueType::Double),
            (10, 10, ValueType::IntList),
            (11, 11, ValueType::DoubleList),
            (12, 12, ValueType::FloatList),
            (13, 13, ValueType::StringList),
            (14, 14, ValueType::LongList),
        ]
    }

    fn default_value() -> HashMap<PropertyId, Value> {
        let mut map = HashMap::new();
        map.insert(1, Value::string("default-string"));
        map.insert(2, Value::bytes(b"default-bytes"));
        map.insert(3, Value::char(1));
        map.insert(4, Value::int(2));
        map.insert(5, Value::short(3));
        map.insert(6, Value::bool(true));
        map.insert(7, Value::long(4));
        map.insert(8, Value::float(5.0));
        map.insert(9, Value::double(6.0));
        map.insert(10, Value::int_list(&[7,1,1]));
        map.insert(11, Value::double_list(&[8.1, 1.1, 1.1]));
        map.insert(12, Value::float_list(&[9.1, 1.1, 1.1]));
        map.insert(13, Value::string_list(&["1".to_owned(), "2".to_owned(), "3".to_owned()]));
        map.insert(14, Value::long_list(&[10,1,1]));
        map
    }

    fn test_data() -> HashMap<PropertyId, Value> {
        let mut map = HashMap::new();
        map.insert(1, Value::from("aababasdasdas"));
        map.insert(2, Value::bytes(b"asdasdasda"));
        map.insert(3, Value::char(123));
        map.insert(4, Value::int(1234));
        map.insert(5, Value::short(55));
        map.insert(6, Value::bool(true));
        map.insert(7, Value::long(644588766664));
        map.insert(8, Value::float(1.23));
        map.insert(9, Value::double(3.56342));
        map.insert(10, Value::int_list(&[1,2,3,4,5]));
        map.insert(11, Value::double_list(&[1.1, 2.2, 3.3, 4.4]));
        map.insert(12, Value::float_list(&[1.2, 2.3, 3.4]));
        map.insert(13, Value::string_list(&["aaa".to_owned(), "bbb".to_owned(), "ccc".to_owned()]));
        map.insert(14, Value::long_list(&[5,6,67,7,8,8,8,34]));
        map
    }

    fn create_test_codec() -> Codec {
        let mut builder = TypeDefBuilder::new();
        builder.version(10);
        for (prop_id, inner_id, r#type) in test_prop_list() {
            builder.add_property(prop_id, inner_id, prop_id.to_string(), r#type, None, false, "cmt".to_string());
        }
        let type_def = builder.build();
        Codec::from(&type_def)
    }

    fn create_default_value_codec() -> Codec {
        let mut builder = TypeDefBuilder::new();
        builder.version(12);
        let default_value = default_value();
        for (prop_id, inner_id, r#type) in test_prop_list() {
            let value = default_value.get(&prop_id).unwrap().clone();
            builder.add_property(prop_id, inner_id, prop_id.to_string(), r#type, Some(value), false, "cmt".to_string());
        }
        let type_def = builder.build();
        Codec::from(&type_def)
    }
}