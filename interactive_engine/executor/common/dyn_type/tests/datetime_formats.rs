#[cfg(test)]
mod tests {
    use std::vec;

    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
    use dyn_type::DateTimeFormats;
    use pegasus_common::codec::{Decode, Encode};

    #[test]
    fn test_dateformat_as() {
        // date as date
        let date = NaiveDate::from_ymd_opt(2020, 10, 10).unwrap();
        let date_format = DateTimeFormats::Date(date);
        let date_recovered = date_format.as_date().unwrap();
        assert_eq!(date_recovered, date);

        // time as time
        let time = NaiveTime::from_hms_milli_opt(10, 10, 10, 100).unwrap();
        let time_format = DateTimeFormats::Time(time);
        let time_recovered = time_format.as_time().unwrap();
        assert_eq!(time_recovered, time);

        // datetime as datetime
        let date_time =
            NaiveDateTime::parse_from_str("2020-10-10 10:10:10.100", "%Y-%m-%d %H:%M:%S%.f").unwrap();
        let date_time_format = DateTimeFormats::DateTime(date_time);
        let date_time_recovered = date_time_format.as_date_time().unwrap();
        assert_eq!(date_time_recovered, date_time);

        // datetime_with_tz as datetime_with_tz
        let date_time_with_tz = DateTime::parse_from_rfc3339("2020-10-09T23:10:10.100-11:00").unwrap();
        let date_time_with_tz_format = DateTimeFormats::DateTimeWithTz(date_time_with_tz);
        let date_time_with_tz_recovered = date_time_with_tz_format
            .as_date_time_with_tz()
            .unwrap();
        assert_eq!(date_time_with_tz_recovered, date_time_with_tz);

        // date_time as date
        let date_time_to_date = date_time_format.as_date().unwrap();
        assert_eq!(date_time_to_date, date);

        // date_time as time
        let date_time_to_time = date_time_format.as_time().unwrap();
        assert_eq!(date_time_to_time, time);

        // date_time_with_tz as date, notice the returned date is in local timezone
        // 2020-10-09T23:10:10.100-11:00
        let date_time_with_tz_to_date = date_time_with_tz_format.as_date().unwrap();
        assert_eq!(date_time_with_tz_to_date, NaiveDate::from_ymd_opt(2020, 10, 9).unwrap());

        // date_time_with_tz as time, notice that the returned time is in local timezone
        let date_time_with_tz_to_time = date_time_with_tz_format.as_time().unwrap();
        assert_eq!(date_time_with_tz_to_time, NaiveTime::from_hms_milli_opt(23, 10, 10, 100).unwrap());

        // date_time_with_tz as date_time, notice that the returned date_time is in local timezone
        let date_time_with_tz_to_date_time = date_time_with_tz_format.as_date_time().unwrap();
        assert_eq!(
            date_time_with_tz_to_date_time,
            NaiveDateTime::parse_from_str("2020-10-09 23:10:10.100", "%Y-%m-%d %H:%M:%S%.f").unwrap()
        );

        //  date_time as date_time_with_tz
        let date_time_to_date_time_with_tz = date_time_format.as_date_time_with_tz().unwrap();
        assert_eq!(date_time_to_date_time_with_tz, date_time_with_tz);
    }

    #[test]
    fn test_dateformat_extract() {
        // Date: 2020-10-10
        let date = NaiveDate::from_ymd_opt(2020, 10, 10).unwrap();
        let date_format = DateTimeFormats::Date(date);
        let year = date_format.year().unwrap();
        assert_eq!(year, 2020);
        let month = date_format.month().unwrap();
        assert_eq!(month, 10);
        let day = date_format.day().unwrap();
        assert_eq!(day, 10);
        let timestamp = date_format.timestamp().unwrap();
        // 2020-10-10 00:00:00
        assert_eq!(timestamp, 1602288000);

        // Time: 10:10:10.100
        let time = NaiveTime::from_hms_milli_opt(10, 10, 10, 100).unwrap();
        let time_format = DateTimeFormats::Time(time);
        let hour = time_format.hour().unwrap();
        assert_eq!(hour, 10);
        let minute = time_format.minute().unwrap();
        assert_eq!(minute, 10);
        let second = time_format.second().unwrap();
        assert_eq!(second, 10);
        let millisecond = time_format.millisecond().unwrap();
        assert_eq!(millisecond, 100);

        // DateTime: 2020-10-10 10:10:10.100
        let date_time =
            NaiveDateTime::parse_from_str("2020-10-10 10:10:10.100", "%Y-%m-%d %H:%M:%S%.f").unwrap();
        let date_time_format = DateTimeFormats::DateTime(date_time);
        let year = date_time_format.year().unwrap();
        assert_eq!(year, 2020);
        let month = date_time_format.month().unwrap();
        assert_eq!(month, 10);
        let day = date_time_format.day().unwrap();
        assert_eq!(day, 10);
        let hour = date_time_format.hour().unwrap();
        assert_eq!(hour, 10);
        let minute = date_time_format.minute().unwrap();
        assert_eq!(minute, 10);
        let second = date_time_format.second().unwrap();
        assert_eq!(second, 10);
        let millisecond = date_time_format.millisecond().unwrap();
        assert_eq!(millisecond, 100);
        let timestamp = date_time_format.timestamp().unwrap();
        assert_eq!(timestamp, 1602324610);
        let timestamp_millis = date_time_format.timestamp_millis().unwrap();
        assert_eq!(timestamp_millis, 1602324610100);

        // DateTimeWithTz: 2020-10-09T23:10:10.100-11:00
        let date_time_with_tz = DateTime::parse_from_rfc3339("2020-10-09T23:10:10.100-11:00").unwrap();
        let date_time_with_tz_format = DateTimeFormats::DateTimeWithTz(date_time_with_tz);
        let year = date_time_with_tz_format.year().unwrap();
        assert_eq!(year, 2020);
        let month = date_time_with_tz_format.month().unwrap();
        assert_eq!(month, 10);
        let day = date_time_with_tz_format.day().unwrap();
        assert_eq!(day, 9);
        let hour = date_time_with_tz_format.hour().unwrap();
        assert_eq!(hour, 23);
        let minute = date_time_with_tz_format.minute().unwrap();
        assert_eq!(minute, 10);
        let second = date_time_with_tz_format.second().unwrap();
        assert_eq!(second, 10);
        let millisecond = date_time_with_tz_format.millisecond().unwrap();
        assert_eq!(millisecond, 100);
        let timestamp = date_time_with_tz_format.timestamp().unwrap();
        assert_eq!(timestamp, 1602324610);
        let timestamp_millis = date_time_with_tz_format
            .timestamp_millis()
            .unwrap();
        assert_eq!(timestamp_millis, 1602324610100);
    }

    #[test]
    fn test_dateformat_cmp() {
        // Date comparison
        // 2020-10-10
        let date = NaiveDate::from_ymd_opt(2020, 10, 10).unwrap();
        let date_format = DateTimeFormats::Date(date);
        // 2020-10-11
        let date2 = NaiveDate::from_ymd_opt(2020, 10, 11).unwrap();
        let date_format2 = DateTimeFormats::Date(date2);
        assert!(date_format < date_format2);

        // Time comparison
        // 10:10:10
        let time = NaiveTime::from_hms_opt(10, 10, 10).unwrap();
        let time_format = DateTimeFormats::Time(time);
        // 10:10:11
        let time2 = NaiveTime::from_hms_opt(10, 10, 11).unwrap();
        let time_format2 = DateTimeFormats::Time(time2);
        assert!(time_format < time_format2);

        // DateTime comparison
        // 2020-10-10 10:10:10.100
        let date_time =
            NaiveDateTime::parse_from_str("2020-10-10 10:10:10.100", "%Y-%m-%d %H:%M:%S%.f").unwrap();
        let date_time_format = DateTimeFormats::DateTime(date_time);
        // 2020-10-10 10:10:11.100
        let date_time2 =
            NaiveDateTime::parse_from_str("2020-10-10 10:10:11.100", "%Y-%m-%d %H:%M:%S%.f").unwrap();
        let date_time_format2 = DateTimeFormats::DateTime(date_time2);
        assert!(date_time_format < date_time_format2);

        // DateTimeWithTz comparison
        // 2020-10-09T23:10:10.100-11:00
        let date_time_with_tz = DateTime::parse_from_rfc3339("2020-10-09T23:10:10.100-11:00").unwrap();
        let date_time_with_tz_format = DateTimeFormats::DateTimeWithTz(date_time_with_tz);
        // 2020-10-09T23:10:11.100-11:00
        let date_time_with_tz2 = DateTime::parse_from_rfc3339("2020-10-09T23:10:11.100-11:00").unwrap();
        let date_time_with_tz_format2 = DateTimeFormats::DateTimeWithTz(date_time_with_tz2);
        assert!(date_time_with_tz_format < date_time_with_tz_format2);

        // DateTime and DateTimeWithTz comparison
        assert!(date_time_with_tz_format < date_time_format2);
        assert!(date_time_format < date_time_with_tz_format2);

        // uncomparable cases
        assert_eq!(date_format < date_time_format || date_format > date_time_format, false);
        assert_eq!(date_format < date_time_with_tz_format || date_format > date_time_with_tz_format, false);
        assert_eq!(time_format < date_time_format || time_format > date_time_format, false);
        assert_eq!(time_format < date_time_with_tz_format || time_format > date_time_with_tz_format, false);
    }

    #[test]
    fn test_dateformat_eq() {
        // Date equality
        // 2020-10-10
        let date = NaiveDate::from_ymd_opt(2020, 10, 10).unwrap();
        let date_format = DateTimeFormats::Date(date);
        // 2020-10-10
        let date2 = NaiveDate::from_yo_opt(2020, 284).unwrap();
        let date_format2 = DateTimeFormats::Date(date2);
        assert_eq!(date_format, date_format2);

        // Time equality
        // 10:10:10
        let time = NaiveTime::from_hms_opt(10, 10, 10).unwrap();
        let time_format = DateTimeFormats::Time(time);
        // 10:10:10.000
        let time2 = NaiveTime::from_hms_milli_opt(10, 10, 10, 0).unwrap();
        let time_format2 = DateTimeFormats::Time(time2);
        assert_eq!(time_format, time_format2);

        // DateTime equality
        // 2020-10-10 10:10:10.100
        let date_time =
            NaiveDateTime::parse_from_str("2020-10-10 10:10:10.100", "%Y-%m-%d %H:%M:%S%.f").unwrap();
        let date_time_format = DateTimeFormats::DateTime(date_time);
        // 2020-10-10 10:10:10.100
        let date_time2 = NaiveDateTime::from_timestamp_millis(1602324610100).unwrap();
        let date_time_format2 = DateTimeFormats::DateTime(date_time2);
        assert_eq!(date_time_format, date_time_format2);

        // DateTimeWithTz equality
        // 2020-10-09T23:10:10.100-11:00
        let date_time_with_tz = DateTime::parse_from_rfc3339("2020-10-09T23:10:10.100-11:00").unwrap();
        let date_time_with_tz_format = DateTimeFormats::DateTimeWithTz(date_time_with_tz);
        // 2020-10-10T18:10:10.100+08:00
        let date_time_with_tz2 = DateTime::parse_from_rfc3339("2020-10-10T18:10:10.100+08:00").unwrap();
        let date_time_with_tz_format2 = DateTimeFormats::DateTimeWithTz(date_time_with_tz2);
        assert_eq!(date_time_with_tz_format, date_time_with_tz_format2);

        // DateTime v.s.  DateTimeWithTz
        assert_eq!(date_time_format, date_time_with_tz_format);
    }

    #[test]
    fn test_dateformat_ser_de() {
        let date: NaiveDate = NaiveDate::from_ymd_opt(2020, 10, 10).unwrap();
        let date_format = DateTimeFormats::Date(date);
        let mut bytes = vec![];
        date_format.write_to(&mut bytes).unwrap();
        let date_format_de = <DateTimeFormats>::read_from(&mut bytes.as_slice()).unwrap();
        assert_eq!(date_format, date_format_de);

        let time = NaiveTime::from_hms_milli_opt(10, 10, 10, 100).unwrap();
        let time_format = DateTimeFormats::Time(time);
        let mut bytes = vec![];
        time_format.write_to(&mut bytes).unwrap();
        let time_format_de = <DateTimeFormats>::read_from(&mut bytes.as_slice()).unwrap();
        assert_eq!(time_format, time_format_de);

        let date_time =
            NaiveDateTime::parse_from_str("2020-10-10 10:10:10.100", "%Y-%m-%d %H:%M:%S%.f").unwrap();
        let date_time_format = DateTimeFormats::DateTime(date_time);
        let mut bytes = vec![];
        date_time_format.write_to(&mut bytes).unwrap();
        let date_time_format_de = <DateTimeFormats>::read_from(&mut bytes.as_slice()).unwrap();
        assert_eq!(date_time_format, date_time_format_de);

        let date_time_with_tz = DateTime::parse_from_rfc3339("2020-10-09T23:10:10.100-11:00").unwrap();
        let date_time_with_tz_format = DateTimeFormats::DateTimeWithTz(date_time_with_tz);
        let mut bytes = vec![];
        date_time_with_tz_format
            .write_to(&mut bytes)
            .unwrap();
        let date_time_with_tz_format_de = <DateTimeFormats>::read_from(&mut bytes.as_slice()).unwrap();
        assert_eq!(date_time_with_tz_format, date_time_with_tz_format_de);
    }
}
