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

use chrono::DateTime as CDateTime;
use chrono::{Datelike, Duration, FixedOffset, SecondsFormat, TimeZone, Timelike, Utc};
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};

#[derive(Clone, Copy)]
pub struct DateTime {
    inner: u64,
}

impl DateTime {
    pub fn empty() -> Self {
        DateTime { inner: 0 }
    }

    pub fn new(
        year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32, millisecond: u32,
        tz_flag: bool, tz_hour: u32, tz_minute: u32,
    ) -> Self {
        let mut ret = if tz_flag { 1_u64 } else { 0_u64 };
        ret = (ret << 5) | tz_hour as u64;
        ret = (ret << 6) | tz_minute as u64;
        ret = (ret << 14) | year as u64;
        ret = (ret << 4) | month as u64;
        ret = (ret << 5) | day as u64;
        ret = (ret << 5) | hour as u64;
        ret = (ret << 6) | minute as u64;
        ret = (ret << 6) | second as u64;
        ret = (ret << 10) | millisecond as u64;

        Self { inner: ret }
    }

    pub fn year(&self) -> i32 {
        ((self.inner >> 36) & 0b11_1111_1111_1111) as i32
    }

    pub fn month(&self) -> u32 {
        ((self.inner >> 32) & 0b1111) as u32
    }

    pub fn day(&self) -> u32 {
        ((self.inner >> 27) & 0b1_1111) as u32
    }

    pub fn hour(&self) -> u32 {
        ((self.inner >> 22) & 0b1_1111) as u32
    }

    pub fn minute(&self) -> u32 {
        ((self.inner >> 16) & 0b11_1111) as u32
    }

    pub fn second(&self) -> u32 {
        ((self.inner >> 10) & 0b11_1111) as u32
    }

    pub fn millisecond(&self) -> u32 {
        (self.inner & 0b11_1111_1111) as u32
    }

    pub fn tz_hour(&self) -> u32 {
        ((self.inner >> 56) & 0b1_1111) as u32
    }

    pub fn tz_minute(&self) -> u32 {
        ((self.inner >> 50) & 0b11_1111) as u32
    }

    pub fn tz_flag(&self) -> bool {
        ((self.inner >> 61) & 0b1) != 0
    }

    pub fn to_u64(&self) -> u64 {
        self.inner & 0b11_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111
    }

    pub fn date_to_u32(&self) -> u32 {
        ((self.inner >> 27) & 0b111_1111_1111_1111_1111_1111) as u32
    }

    pub fn to_chrono_date_utc(&self) -> CDateTime<Utc> {
        let utc_str = format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}+00:00",
            self.year(),
            self.month(),
            self.day(),
            self.hour(),
            self.minute(),
            self.second(),
            self.millisecond()
        );
        utc_str.parse::<CDateTime<Utc>>().unwrap()
    }

    pub fn from_chrono_date_utc(dt: CDateTime<Utc>, tz_flag: bool, tz_hour: u32, tz_minute: u32) -> Self {
        Self::new(
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
            dt.nanosecond() / 1000000,
            tz_flag,
            tz_hour,
            tz_minute,
        )
    }

    pub fn add_days(&self, days: u32) -> Self {
        let utc_dt = self.to_chrono_date_utc();
        let duration = Duration::days(days as i64);
        let ret = utc_dt + duration;
        Self::from_chrono_date_utc(ret, true, 0, 0)
    }

    pub fn minus_hours(&self, hours: u32) -> Self {
        let utc_dt = self.to_chrono_date_utc();
        let duration = Duration::hours(hours as i64);
        let ret = utc_dt - duration;
        Self::from_chrono_date_utc(ret, true, 0, 0)
    }
}

impl Display for DateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.tz_hour() == 0 && self.tz_minute() == 0 {
            write!(
                f,
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}+00:00",
                self.year(),
                self.month(),
                self.day(),
                self.hour(),
                self.minute(),
                self.second(),
                self.millisecond(),
            )
        } else {
            let dt = self.to_chrono_date_utc();
            let offset = (self.tz_hour() * 3600 + self.tz_minute() * 60) as i32;
            let dt2 = if self.tz_flag() {
                FixedOffset::east_opt(offset)
                    .unwrap()
                    .timestamp_millis_opt(dt.timestamp_millis())
                    .unwrap()
            } else {
                FixedOffset::west_opt(offset)
                    .unwrap()
                    .timestamp_millis_opt(dt.timestamp_millis())
                    .unwrap()
            };
            write!(f, "{}", dt2.to_rfc3339_opts(SecondsFormat::Millis, false))
        }
    }
}

impl Debug for DateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

pub fn parse_datetime(val: &str) -> DateTime {
    let dt = val.parse::<CDateTime<Utc>>().unwrap();
    let flag = if &val[23..24] == "+" { true } else { false };
    let tz_hour = val[24..26].parse::<u32>().unwrap();
    let tz_minute = val[27..29].parse::<u32>().unwrap();
    DateTime::from_chrono_date_utc(dt, flag, tz_hour, tz_minute)
}

impl Encode for DateTime {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64(self.inner).unwrap();
        Ok(())
    }
}

impl Decode for DateTime {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let inner = reader.read_u64().unwrap();
        Ok(Self { inner })
    }
}
