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

use chrono::{DateTime as CDateTime, TimeZone};
use chrono::{Datelike, Duration, Timelike, Utc};

#[derive(Clone, Copy)]
pub struct DateTime {
    inner: i64,
}

impl DateTime {
    pub fn empty() -> Self {
        DateTime { inner: 0 }
    }

    pub fn new(timestamp: i64) -> Self {
        DateTime { inner: timestamp }
    }

    pub fn from_datetime(
        year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32, millisecond: u32,
    ) -> Self {
        let date_dt = CDateTime::<Utc>::from_utc(
            chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
                .unwrap()
                .and_hms_milli_opt(hour as u32, minute as u32, second as u32, millisecond as u32)
                .unwrap(),
            Utc,
        )
        .timestamp_millis();
        Self { inner: date_dt }
    }

    pub fn year(&self) -> i32 {
        chrono::NaiveDateTime::from_timestamp_millis(self.inner)
            .unwrap()
            .year()
    }

    pub fn month(&self) -> i32 {
        chrono::NaiveDateTime::from_timestamp_millis(self.inner)
            .unwrap()
            .month() as i32
    }

    pub fn day(&self) -> i32 {
        chrono::NaiveDateTime::from_timestamp_millis(self.inner)
            .unwrap()
            .day() as i32
    }

    pub fn hour(&self) -> i32 {
        chrono::NaiveDateTime::from_timestamp_millis(self.inner)
            .unwrap()
            .hour() as i32
    }

    pub fn minute(&self) -> i32 {
        chrono::NaiveDateTime::from_timestamp_millis(self.inner)
            .unwrap()
            .minute() as i32
    }

    pub fn second(&self) -> i32 {
        chrono::NaiveDateTime::from_timestamp_millis(self.inner)
            .unwrap()
            .second() as i32
    }

    pub fn millisecond(&self) -> i32 {
        (chrono::NaiveDateTime::from_timestamp_millis(self.inner)
            .unwrap()
            .timestamp_subsec_nanos()
            / 1000000) as i32
    }

    pub fn to_i64(&self) -> i64 {
        self.inner
    }

    pub fn to_days_i64(&self) -> i64 {
        Utc.timestamp_millis_opt(self.inner).unwrap().date().and_hms(0, 0, 0).timestamp_millis()
    }

    pub fn to_chrono_date_utc(&self) -> CDateTime<Utc> {
        CDateTime::<Utc>::from_utc(chrono::NaiveDateTime::from_timestamp_millis(self.inner).unwrap(), Utc)
    }

    pub fn from_chrono_date_utc(dt: CDateTime<Utc>) -> Self {
        Self::new(dt.timestamp_millis())
    }

    pub fn add_days(&self, days: u32) -> Self {
        let utc_dt = self.to_chrono_date_utc();
        let duration = Duration::days(days as i64);
        let ret = utc_dt + duration;
        Self::from_chrono_date_utc(ret)
    }

    pub fn minus_hours(&self, hours: u32) -> Self {
        let utc_dt = self.to_chrono_date_utc();
        let duration = Duration::hours(hours as i64);
        let ret = utc_dt - duration;
        Self::from_chrono_date_utc(ret)
    }
}

impl PartialEq for DateTime {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl Eq for DateTime {}

impl Display for DateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
    }
}

impl Debug for DateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

pub fn parse_datetime(val: &str) -> DateTime {
    let datetime = if let Ok(utc_dt) = val.parse::<CDateTime<Utc>>() {
        let tz_hour = val[24..26].parse::<u32>().unwrap();
        let tz_minute = val[27..29].parse::<u32>().unwrap();
        let duration_hour = Duration::hours(tz_hour as i64);
        let duration_minute = Duration::minutes(tz_minute as i64);
        let dt = utc_dt + duration_hour + duration_minute;
        DateTime::from_chrono_date_utc(dt)
    } else if let Ok(timestamp) = val.parse::<i64>() {
        DateTime::new(timestamp)
    } else {
        panic!("Failed to parse datetime {}", val);
    };
    datetime
}
