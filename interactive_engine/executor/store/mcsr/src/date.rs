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

use std::fmt::{Debug, Display, Formatter};

use chrono::{Datelike, Duration, NaiveDate};
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::date_time::DateTime;
use crate::error::GDBResult;

#[derive(Copy, Clone)]
pub struct Date {
    inner: i32,
}

impl Date {
    pub fn empty() -> Self {
        Date { inner: 0 }
    }

    pub fn from_i32(inner: i32) -> Self {
        Self { inner }
    }

    pub fn new(year: i32, month: u32, day: u32) -> Self {
        Date {
            inner: chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
                .unwrap()
                .num_days_from_ce(),
        }
    }

    pub fn year(&self) -> i32 {
        chrono::NaiveDate::from_num_days_from_ce_opt(self.inner)
            .unwrap()
            .year()
    }

    pub fn month(&self) -> u32 {
        chrono::NaiveDate::from_num_days_from_ce_opt(self.inner)
            .unwrap()
            .month()
    }

    pub fn day(&self) -> u32 {
        chrono::NaiveDate::from_num_days_from_ce_opt(self.inner)
            .unwrap()
            .day()
    }

    pub fn to_i32(&self) -> i32 {
        self.inner
    }

    pub fn add_days(&self, days: i32) -> Self {
        let din = NaiveDate::from_num_days_from_ce_opt(self.inner).unwrap();
        let duration = Duration::days(days as i64);
        let dout = din + duration;
        Self::from_i32(dout.num_days_from_ce())
    }
}

impl Display for Date {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year(), self.month(), self.day())
    }
}

impl Encode for Date {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_i32(self.inner).unwrap();
        Ok(())
    }
}

impl Decode for Date {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let ret = reader.read_i32().unwrap();
        Ok(Self { inner: ret })
    }
}

impl Debug for Date {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

pub fn parse_date(val: &str) -> GDBResult<Date> {
    if let Ok(timestamp) = val.parse::<i64>() {
        let datetime = DateTime::new(timestamp);
        let year = datetime.year();
        let month = datetime.month() as u32;
        let day = datetime.day() as u32;
        Ok(Date::new(year, month, day))
    } else {
        let year = val[0..4].parse::<i32>()?;
        let month = val[5..7].parse::<u32>()?;
        let day = val[8..10].parse::<u32>()?;
        Ok(Date::new(year, month, day))
    }
}
