use std::fmt::{Debug, Display, Formatter};

use chrono::{Datelike, Duration, NaiveDate};
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::error::GDBResult;

#[derive(Copy, Clone)]
pub struct Date {
    inner: u32,
}

impl Date {
    pub fn empty() -> Self {
        Date { inner: 0 }
    }

    pub fn from_u32(inner: u32) -> Self {
        Self { inner }
    }

    pub fn new(year: i32, month: u32, day: u32) -> Self {
        let mut ret = year as u32;
        ret = (ret << 4) | month;
        ret = (ret << 5) | day;
        Date { inner: ret }
    }

    pub fn year(&self) -> i32 {
        ((self.inner >> 9) & 0b11_1111_1111_1111) as i32
    }

    pub fn month(&self) -> u32 {
        (self.inner >> 5) & 0b1111
    }

    pub fn day(&self) -> u32 {
        self.inner & 0b1_1111
    }

    pub fn to_u32(&self) -> u32 {
        self.inner
    }

    pub fn datetime_to_u64(&self) -> u64 {
        let v = self.to_u32() as u64;
        v << 27
    }

    pub fn add_days(&self, days: u32) -> Self {
        let din = NaiveDate::from_ymd_opt(self.year(), self.month(), self.day()).unwrap();
        let duration = Duration::days(days as i64);
        let dout = din + duration;
        Self::new(dout.year(), dout.month(), dout.day())
    }
}

impl Display for Date {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year(), self.month(), self.day())
    }
}

impl Encode for Date {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u32(self.inner).unwrap();
        Ok(())
    }
}

impl Decode for Date {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let ret = reader.read_u32().unwrap();
        Ok(Self { inner: ret })
    }
}

impl Debug for Date {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

pub fn parse_date(val: &str) -> GDBResult<Date> {
    let year = val[0..4].parse::<i32>()?;
    let month = val[5..7].parse::<u32>()?;
    let day = val[8..10].parse::<u32>()?;
    Ok(Date::new(year, month, day))
}
