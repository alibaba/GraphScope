use std::cell::RefCell;
use std::io::Write;
use std::ops::Deref;

use byteorder::{BigEndian, WriteBytesExt};

use crate::db::api::{LabelId, VertexId};

#[cfg(test)]
mod bench;
pub mod bin;
pub mod codec;
pub mod entity;
pub mod iter;
mod meta;
mod property;
pub mod store;
mod table_manager;
#[cfg(test)]
mod tests;
pub mod types;
mod version;

thread_local! {
    static BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(64 << 10));
}

pub fn get_vertex_id_by_primary_keys<'a, T: Deref<Target = Vec<u8>>>(
    label_id: LabelId, pks: impl Iterator<Item = T>,
) -> VertexId {
    BUFFER.with(|bytes| {
        let mut bytes = bytes.borrow_mut();
        bytes.clear();
        bytes
            .write_i32::<BigEndian>(label_id as i32)
            .unwrap();
        for pk in pks {
            let pk = pk.as_slice();
            bytes
                .write_i32::<BigEndian>(pk.len() as i32)
                .unwrap();
            bytes.write(pk).unwrap();
        }
        hash64(bytes.as_slice(), bytes.len())
    })
}

pub fn hash64(data: &[u8], length: usize) -> i64 {
    let seed = 0xc70f6907;
    hash64_with_seed(data, length, seed)
}

pub fn hash64_with_seed(data: &[u8], length: usize, seed: u32) -> i64 {
    let m = 0xc6a4a7935bd1e995_u64;
    let r = 47;
    let mut h = (seed as u64 & 0xffffffff_u64) ^ (m.wrapping_mul(length as u64));
    let length8 = length / 8;
    for i in 0..length8 {
        let i8 = i * 8;
        let mut k = (data[i8] & 0xff) as u64
            + (((data[i8 + 1] & 0xff) as u64) << 8)
            + (((data[i8 + 2] & 0xff) as u64) << 16)
            + (((data[i8 + 3] & 0xff) as u64) << 24)
            + (((data[i8 + 4] & 0xff) as u64) << 32)
            + (((data[i8 + 5] & 0xff) as u64) << 40)
            + (((data[i8 + 6] & 0xff) as u64) << 48)
            + (((data[i8 + 7] & 0xff) as u64) << 56);
        k = k.wrapping_mul(m);
        k ^= k >> r;
        k = k.wrapping_mul(m);
        h ^= k;
        h = h.wrapping_mul(m);
    }
    let tmp = length % 8;
    if tmp > 0 {
        for i in (1..=tmp).rev() {
            let o = i - 1;
            let s = o * 8;
            h ^= ((data[(length & !7) + o] & 0xff) as u64) << s;
        }
        h = h.wrapping_mul(m);
    }
    h ^= h >> r;
    h = h.wrapping_mul(m);
    h ^= h >> r;
    h as i64
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use std::io::Write;
    use std::ops::Sub;
    use std::time::Instant;

    use byteorder::{BigEndian, WriteBytesExt};

    use crate::db::graph::get_vertex_id_by_primary_keys;

    thread_local! {
        static FIELD_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(64 << 10));
    }

    #[allow(dead_code)]
    #[derive(Clone, PartialEq, Debug, PartialOrd)]
    pub enum Property {
        Bool(bool),
        Char(u8),
        Short(i16),
        Int(i32),
        Long(i64),
        Float(f32),
        Double(f64),
        Bytes(Vec<u8>),
        String(String),
        Date(String),
        ListInt(Vec<i32>),
        ListLong(Vec<i64>),
        ListFloat(Vec<f32>),
        ListDouble(Vec<f64>),
        ListString(Vec<String>),
        ListBytes(Vec<Vec<u8>>),
        Null,
        Unknown,
    }

    #[test]
    fn test_pk_hash() {
        let p = [Property::Bool(true)];
        assert_eq!(-5831416668854475863_i64, get_vertex_id_by_pk_property(1, &p));

        let p = [Property::Bool(false)];
        assert_eq!(4016402799948355554_i64, get_vertex_id_by_pk_property(1, &p));

        let p = [Property::Short(5)];
        assert_eq!(-6461270943182640449_i64, get_vertex_id_by_pk_property(1, &p));

        let p = [Property::Int(6)];
        assert_eq!(-5566731246168985051_i64, get_vertex_id_by_pk_property(1, &p));

        let p = [Property::Long(7)];
        assert_eq!(-2037727154783756963_i64, get_vertex_id_by_pk_property(1, &p));

        let p = [Property::Float(5.5)];
        assert_eq!(3718470844984468536_i64, get_vertex_id_by_pk_property(1, &p));

        let p = [Property::Double(11.5)];
        assert_eq!(-6473588278280513549_i64, get_vertex_id_by_pk_property(1, &p));

        let p = [Property::Bytes(Vec::from([1_u8, 2_u8, 3_u8]))];
        assert_eq!(-5358885630460755339_i64, get_vertex_id_by_pk_property(1, &p));

        let p = [Property::String("abc".to_string())];
        assert_eq!(-1681001599945530356_i64, get_vertex_id_by_pk_property(1, &p));

        let p = [Property::ListInt(Vec::from([400, 500, 600, 700]))];
        assert_eq!(-6843607735995935492_i64, get_vertex_id_by_pk_property(1, &p));

        let p = [Property::ListLong(Vec::from([
            111111111111_i64,
            222222222222_i64,
            333333333333_i64,
            444444444444_i64,
        ]))];
        assert_eq!(7595853299324856776_i64, get_vertex_id_by_pk_property(1, &p));

        let p = [Property::ListFloat(Vec::from([1.234567_f32, 12.34567_f32, 123.4567_f32, 1234.567_f32]))];
        assert_eq!(-866958979581072036_i64, get_vertex_id_by_pk_property(1, &p));

        let p = [Property::ListDouble(Vec::from([987654.3, 98765.43, 9876.543, 987.6543]))];
        assert_eq!(-1870641235154602931_i64, get_vertex_id_by_pk_property(1, &p));

        let p = [Property::ListString(Vec::from(["English".to_string(), "中文".to_string()]))];
        assert_eq!(-5965060437586883158_i64, get_vertex_id_by_pk_property(1, &p));

        let p = [Property::String("aaa".to_string()), Property::Long(999999999999_i64)];
        assert_eq!(7757033342887554736_i64, get_vertex_id_by_pk_property(1, &p));
    }

    fn get_vertex_id_by_pk_property(label_id: u32, pks: &[Property]) -> i64 {
        FIELD_BUF.with(|data| {
            let pks_bytes = pks.iter().map(|pk| {
                let mut data = data.borrow_mut();
                data.clear();
                match pk {
                    Property::Bool(v) => {
                        data.write_u8(*v as u8).unwrap();
                    }
                    Property::Char(v) => {
                        data.write_u8(*v as u8).unwrap();
                    }
                    Property::Short(v) => {
                        data.write_i16::<BigEndian>(*v).unwrap();
                    }
                    Property::Int(v) => {
                        data.write_i32::<BigEndian>(*v).unwrap();
                    }
                    Property::Long(v) => {
                        data.write_i64::<BigEndian>(*v).unwrap();
                    }
                    Property::Float(v) => {
                        data.write_f32::<BigEndian>(*v).unwrap();
                    }
                    Property::Double(v) => {
                        data.write_f64::<BigEndian>(*v).unwrap();
                    }
                    Property::Bytes(v) => {
                        data.extend_from_slice(v);
                    }
                    Property::String(v) => {
                        data.extend(v.as_bytes());
                    }
                    Property::Date(v) => {
                        data.extend(v.as_bytes());
                    }
                    Property::ListInt(v) => {
                        data.write_i32::<BigEndian>(v.len() as i32)
                            .unwrap();
                        for i in v {
                            data.write_i32::<BigEndian>(*i).unwrap();
                        }
                    }
                    Property::ListLong(v) => {
                        data.write_i32::<BigEndian>(v.len() as i32)
                            .unwrap();
                        for i in v {
                            data.write_i64::<BigEndian>(*i).unwrap();
                        }
                    }
                    Property::ListFloat(v) => {
                        data.write_i32::<BigEndian>(v.len() as i32)
                            .unwrap();
                        for i in v {
                            data.write_f32::<BigEndian>(*i).unwrap();
                        }
                    }
                    Property::ListDouble(v) => {
                        data.write_i32::<BigEndian>(v.len() as i32)
                            .unwrap();
                        for i in v {
                            data.write_f64::<BigEndian>(*i).unwrap();
                        }
                    }
                    Property::ListString(v) => {
                        data.write_i32::<BigEndian>(v.len() as i32)
                            .unwrap();
                        let mut offset = 0;
                        let mut bytes_vec = Vec::with_capacity(v.len());
                        for i in v {
                            let b = i.as_bytes();
                            bytes_vec.push(b);
                            offset += b.len();
                            data.write_i32::<BigEndian>(offset as i32)
                                .unwrap();
                        }
                        for b in bytes_vec {
                            data.write(b).unwrap();
                        }
                    }
                    Property::ListBytes(v) => {
                        data.write_i32::<BigEndian>(v.len() as i32)
                            .unwrap();
                        let mut offset = 0;
                        for i in v {
                            offset += i.len();
                            data.write_i32::<BigEndian>(offset as i32)
                                .unwrap();
                        }
                        for i in v {
                            data.write(i.as_slice()).unwrap();
                        }
                    }
                    Property::Null => {
                        unimplemented!()
                    }
                    Property::Unknown => {
                        unimplemented!()
                    }
                }
                data
            });
            get_vertex_id_by_primary_keys(label_id as i32, pks_bytes)
        })
    }

    #[test]
    fn bench_hash() {
        let mut i = 0_i64;
        let interval = 10000000;
        let t = Instant::now();
        let mut c = t.elapsed();
        loop {
            i += 1;
            let s = format!("test_key_{}", i);
            let p = [Property::String(s)];
            get_vertex_id_by_pk_property(1, &p);
            if i % interval == 0 {
                let n = t.elapsed();
                let cost = n.sub(c).as_nanos();
                c = n;
                println!(
                    "total {}, cost {}, interval_speed {}",
                    i,
                    cost / 1000000,
                    1000000000 * interval as u128 / cost
                );
            }
        }
    }
}
