//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::io::{self, Read, Write};
use std::ops::Deref;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, BufMut, Bytes};

use crate::bytes::BytesSlab;

pub trait WriteExt: Write {
    fn write_u8(&mut self, v: u8) -> io::Result<()> {
        WriteBytesExt::write_u8(self, v)
    }

    fn write_u16(&mut self, v: u16) -> io::Result<()> {
        WriteBytesExt::write_u16::<LittleEndian>(self, v)
    }

    fn write_u32(&mut self, v: u32) -> io::Result<()> {
        WriteBytesExt::write_u32::<LittleEndian>(self, v)
    }

    fn write_u64(&mut self, v: u64) -> io::Result<()> {
        WriteBytesExt::write_u64::<LittleEndian>(self, v)
    }

    fn write_u128(&mut self, v: u128) -> io::Result<()> {
        WriteBytesExt::write_u128::<LittleEndian>(self, v)
    }

    fn write_i8(&mut self, v: i8) -> io::Result<()> {
        WriteBytesExt::write_i8(self, v)
    }

    fn write_i16(&mut self, v: i16) -> io::Result<()> {
        WriteBytesExt::write_i16::<LittleEndian>(self, v)
    }

    fn write_i32(&mut self, v: i32) -> io::Result<()> {
        WriteBytesExt::write_i32::<LittleEndian>(self, v)
    }

    fn write_i64(&mut self, v: i64) -> io::Result<()> {
        WriteBytesExt::write_i64::<LittleEndian>(self, v)
    }

    fn write_i128(&mut self, v: i128) -> io::Result<()> {
        WriteBytesExt::write_i128::<LittleEndian>(self, v)
    }

    fn write_f32(&mut self, v: f32) -> io::Result<()> {
        WriteBytesExt::write_f32::<LittleEndian>(self, v)
    }

    fn write_f64(&mut self, v: f64) -> io::Result<()> {
        WriteBytesExt::write_f64::<LittleEndian>(self, v)
    }
}

pub enum ByteRef<'a> {
    Slice(&'a [u8]),
    Vec(Box<[u8]>),
    Bytes(Bytes),
}

impl<'a> AsRef<[u8]> for ByteRef<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            ByteRef::Slice(ref slice) => slice,
            ByteRef::Vec(vec) => vec.as_ref(),
            ByteRef::Bytes(b) => b.as_ref(),
        }
    }
}

impl<'a> Deref for ByteRef<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

pub trait ReadExt: Read {
    fn read_u8(&mut self) -> io::Result<u8> {
        ReadBytesExt::read_u8(self)
    }

    fn read_u16(&mut self) -> io::Result<u16> {
        ReadBytesExt::read_u16::<LittleEndian>(self)
    }

    fn read_u32(&mut self) -> io::Result<u32> {
        ReadBytesExt::read_u32::<LittleEndian>(self)
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        ReadBytesExt::read_u64::<LittleEndian>(self)
    }

    fn read_u128(&mut self) -> io::Result<u128> {
        ReadBytesExt::read_u128::<LittleEndian>(self)
    }

    fn read_i8(&mut self) -> io::Result<i8> {
        ReadBytesExt::read_i8(self)
    }

    fn read_i16(&mut self) -> io::Result<i16> {
        ReadBytesExt::read_i16::<LittleEndian>(self)
    }

    fn read_i32(&mut self) -> io::Result<i32> {
        ReadBytesExt::read_i32::<LittleEndian>(self)
    }

    fn read_i64(&mut self) -> io::Result<i64> {
        ReadBytesExt::read_i64::<LittleEndian>(self)
    }

    fn read_i128(&mut self) -> io::Result<i128> {
        ReadBytesExt::read_i128::<LittleEndian>(self)
    }

    fn read_f32(&mut self) -> io::Result<f32> {
        ReadBytesExt::read_f32::<LittleEndian>(self)
    }

    fn read_f64(&mut self) -> io::Result<f64> {
        ReadBytesExt::read_f64::<LittleEndian>(self)
    }

    fn read_to(&mut self, len: usize) -> io::Result<ByteRef> {
        let mut buf = vec![0u8; len];
        self.read_exact(&mut buf)?;
        Ok(ByteRef::Vec(buf.into_boxed_slice()))
    }
}

impl WriteExt for BytesSlab {
    fn write_u8(&mut self, v: u8) -> io::Result<()> {
        self.put_u8(v);
        Ok(())
    }

    fn write_u16(&mut self, v: u16) -> io::Result<()> {
        self.put_u16_le(v);
        Ok(())
    }

    fn write_u32(&mut self, v: u32) -> io::Result<()> {
        self.put_u32_le(v);
        Ok(())
    }

    fn write_u64(&mut self, v: u64) -> io::Result<()> {
        self.put_u64_le(v);
        Ok(())
    }

    fn write_u128(&mut self, v: u128) -> io::Result<()> {
        self.put_u128_le(v);
        Ok(())
    }

    fn write_i8(&mut self, v: i8) -> io::Result<()> {
        self.put_i8(v);
        Ok(())
    }

    fn write_i16(&mut self, v: i16) -> io::Result<()> {
        self.put_i16_le(v);
        Ok(())
    }

    fn write_i32(&mut self, v: i32) -> io::Result<()> {
        self.put_i32_le(v);
        Ok(())
    }

    fn write_i64(&mut self, v: i64) -> io::Result<()> {
        self.put_i64_le(v);
        Ok(())
    }

    fn write_i128(&mut self, v: i128) -> io::Result<()> {
        self.put_i128_le(v);
        Ok(())
    }

    fn write_f32(&mut self, v: f32) -> io::Result<()> {
        self.put_f32_le(v);
        Ok(())
    }

    fn write_f64(&mut self, v: f64) -> io::Result<()> {
        self.put_f64_le(v);
        Ok(())
    }
}

impl WriteExt for &mut [u8] {}
impl WriteExt for std::fs::File {}
impl WriteExt for &std::fs::File {}
impl WriteExt for std::io::Cursor<Box<[u8]>> {}
impl WriteExt for std::io::Cursor<Vec<u8>> {}
impl WriteExt for std::io::Sink {}
impl WriteExt for std::net::TcpStream {}
impl WriteExt for &std::net::TcpStream {}
impl WriteExt for Vec<u8> {}

impl ReadExt for &[u8] {}
impl ReadExt for std::fs::File {}
impl ReadExt for &std::fs::File {}
impl ReadExt for std::io::Empty {}
impl ReadExt for std::net::TcpStream {}
impl ReadExt for &std::net::TcpStream {}
impl<T: AsRef<[u8]>> ReadExt for std::io::Cursor<T> {}

pub struct BytesRead {
    buf: Bytes,
}

impl Read for BytesRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let can_read = std::cmp::min(self.buf.remaining(), buf.len());
        if can_read > 0 {
            self.buf.copy_to_slice(&mut buf[0..can_read]);
        }
        Ok(can_read)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let remaining = self.buf.remaining();
        if remaining < buf.len() {
            Err(std::io::ErrorKind::UnexpectedEof.into())
        } else {
            self.buf.copy_to_slice(&mut buf[0..]);
            Ok(())
        }
    }
}

impl ReadExt for BytesRead {
    fn read_u8(&mut self) -> io::Result<u8> {
        if self.buf.remaining() == 0 {
            Err(std::io::ErrorKind::UnexpectedEof.into())
        } else {
            Ok(self.buf.get_u8())
        }
    }

    fn read_u16(&mut self) -> io::Result<u16> {
        if self.buf.remaining() < std::mem::size_of::<u16>() {
            Err(std::io::ErrorKind::UnexpectedEof.into())
        } else {
            Ok(self.buf.get_u16_le())
        }
    }

    fn read_u32(&mut self) -> io::Result<u32> {
        if self.buf.remaining() < std::mem::size_of::<u32>() {
            Err(std::io::ErrorKind::UnexpectedEof.into())
        } else {
            Ok(self.buf.get_u32_le())
        }
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        if self.buf.remaining() < std::mem::size_of::<u64>() {
            Err(std::io::ErrorKind::UnexpectedEof.into())
        } else {
            Ok(self.buf.get_u64_le())
        }
    }

    fn read_u128(&mut self) -> io::Result<u128> {
        if self.buf.remaining() < std::mem::size_of::<u128>() {
            Err(std::io::ErrorKind::UnexpectedEof.into())
        } else {
            Ok(self.buf.get_u128_le())
        }
    }

    fn read_i8(&mut self) -> io::Result<i8> {
        if self.buf.remaining() == 0 {
            Err(std::io::ErrorKind::UnexpectedEof.into())
        } else {
            Ok(self.buf.get_i8())
        }
    }

    fn read_i16(&mut self) -> io::Result<i16> {
        if self.buf.remaining() < std::mem::size_of::<i16>() {
            Err(std::io::ErrorKind::UnexpectedEof.into())
        } else {
            Ok(self.buf.get_i16_le())
        }
    }

    fn read_i32(&mut self) -> io::Result<i32> {
        if self.buf.remaining() < std::mem::size_of::<i32>() {
            Err(std::io::ErrorKind::UnexpectedEof.into())
        } else {
            Ok(self.buf.get_i32_le())
        }
    }

    fn read_i64(&mut self) -> io::Result<i64> {
        if self.buf.remaining() < std::mem::size_of::<i64>() {
            Err(std::io::ErrorKind::UnexpectedEof.into())
        } else {
            Ok(self.buf.get_i64_le())
        }
    }

    fn read_i128(&mut self) -> io::Result<i128> {
        if self.buf.remaining() < std::mem::size_of::<i128>() {
            Err(std::io::ErrorKind::UnexpectedEof.into())
        } else {
            Ok(self.buf.get_i128_le())
        }
    }

    fn read_f32(&mut self) -> io::Result<f32> {
        if self.buf.remaining() < std::mem::size_of::<f32>() {
            Err(std::io::ErrorKind::UnexpectedEof.into())
        } else {
            Ok(self.buf.get_f32_le())
        }
    }

    fn read_f64(&mut self) -> io::Result<f64> {
        if self.buf.remaining() < std::mem::size_of::<f64>() {
            Err(std::io::ErrorKind::UnexpectedEof.into())
        } else {
            Ok(self.buf.get_f64_le())
        }
    }

    fn read_to(&mut self, len: usize) -> io::Result<ByteRef> {
        if self.buf.remaining() < len {
            Err(std::io::ErrorKind::UnexpectedEof.into())
        } else {
            let bytes = self.buf.split_to(len);
            Ok(ByteRef::Bytes(bytes))
        }
    }
}
