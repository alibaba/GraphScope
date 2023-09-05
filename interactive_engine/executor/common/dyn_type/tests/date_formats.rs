#[cfg(test)]
mod tests {
    use std::vec;

    use dyn_type::DateFormats;
    use pegasus_common::codec::{Decode, Encode};

    #[test]
    fn test_dateformat_as() {
        // 2020-10-10
        let date = chrono::NaiveDate::from_ymd_opt(2020, 10, 10).unwrap();
        let date_format = DateFormats::Date(date);
        let date_recovered = date_format.as_date().unwrap();
        assert_eq!(date_recovered, date);
        // convert as 2020-10-10 00:00:00
        let date_to_datetime = date_format.as_date_time().unwrap();
        assert_eq!(date_to_datetime.timestamp(), 1602288000);

        // 10:10:10.100
        let time = chrono::NaiveTime::from_hms_milli_opt(10, 10, 10, 100).unwrap();
        let time_format = DateFormats::Time(time);
        let time_recovered = time_format.as_time().unwrap();
        assert_eq!(time_recovered, time);

        // 2020-10-10 10:10:10.100
        let date_time = chrono::NaiveDateTime::from_timestamp_millis(1602324610100).unwrap();
        let date_time_format = DateFormats::DateTime(date_time);
        let date_time_recovered = date_time_format.as_date_time().unwrap();
        assert_eq!(date_time_recovered, date_time);
    }

    #[test]
    fn test_dateformat_extract() {
        // 2020-10-10
        let date = chrono::NaiveDate::from_ymd_opt(2020, 10, 10).unwrap();
        let date_format = DateFormats::Date(date);
        let year = date_format.year().unwrap();
        assert_eq!(year, 2020);
        let month = date_format.month().unwrap();
        assert_eq!(month, 10);
        let day = date_format.day().unwrap();
        assert_eq!(day, 10);
        let timestamp = date_format.timestamp().unwrap();
        // 2020-10-10 00:00:00
        assert_eq!(timestamp, 1602288000);

        // 10:10:10.100
        let time = chrono::NaiveTime::from_hms_milli_opt(10, 10, 10, 100).unwrap();
        let time_format = DateFormats::Time(time);
        let hour = time_format.hour().unwrap();
        assert_eq!(hour, 10);
        let minute = time_format.minute().unwrap();
        assert_eq!(minute, 10);
        let second = time_format.second().unwrap();
        assert_eq!(second, 10);
        let millisecond = time_format.millisecond().unwrap();
        assert_eq!(millisecond, 100);

        // 2020-10-10 10:10:10.100
        let date_time = chrono::NaiveDateTime::from_timestamp_millis(1602324610100).unwrap();
        let date_time_format = DateFormats::DateTime(date_time);
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
    }

    #[test]
    fn test_dateformat_cmp() {
        // 2020-10-10
        let date = chrono::NaiveDate::from_ymd_opt(2020, 10, 10).unwrap();
        let date_format = DateFormats::Date(date);
        // 2020-10-11
        let date2 = chrono::NaiveDate::from_ymd_opt(2020, 10, 11).unwrap();
        let date_format2 = DateFormats::Date(date2);
        assert!(date_format < date_format2);

        // 10:10:10
        let time = chrono::NaiveTime::from_hms_opt(10, 10, 10).unwrap();
        let time_format = DateFormats::Time(time);
        // 10:10:11
        let time2 = chrono::NaiveTime::from_hms_opt(10, 10, 11).unwrap();
        let time_format2 = DateFormats::Time(time2);
        assert!(time_format < time_format2);

        // 2020-10-10 10:10:10.100
        let date_time = chrono::NaiveDateTime::from_timestamp_millis(1602324610100).unwrap();
        let date_time_format = DateFormats::DateTime(date_time);
        // 2020-10-10 10:10:11.100
        let date_time2 = chrono::NaiveDateTime::from_timestamp_millis(1602324611100).unwrap();
        let date_time_format2 = DateFormats::DateTime(date_time2);
        assert!(date_time_format < date_time_format2);

        assert!(date_format < date_time_format);
    }

    #[test]
    fn test_dateformat_eq() {
        // 2020-10-10
        let date = chrono::NaiveDate::from_ymd_opt(2020, 10, 10).unwrap();
        let date_format = DateFormats::Date(date);
        // 2020-10-10
        let date2 = chrono::NaiveDate::from_yo_opt(2020, 284).unwrap();
        let date_format2 = DateFormats::Date(date2);
        assert_eq!(date_format, date_format2);

        // 10:10:10
        let time = chrono::NaiveTime::from_hms_opt(10, 10, 10).unwrap();
        let time_format = DateFormats::Time(time);
        // 10:10:10.000
        let time2 = chrono::NaiveTime::from_hms_milli_opt(10, 10, 10, 0).unwrap();
        let time_format2 = DateFormats::Time(time2);
        assert_eq!(time_format, time_format2);

        // 2020-10-10 10:10:10.100
        let date_time = chrono::NaiveDateTime::from_timestamp_millis(1602324610100).unwrap();
        let date_time_format = DateFormats::DateTime(date_time);
        // 2020-10-10 10:10:10.100
        let date_time2 = chrono::NaiveDateTime::from_timestamp_opt(1602324610, 100000000).unwrap();
        let date_time_format2 = DateFormats::DateTime(date_time2);
        assert_eq!(date_time_format, date_time_format2);

        // 2020-10-10 00:00:00
        let date_time3 = chrono::NaiveDateTime::from_timestamp_millis(1602288000000).unwrap();
        let date_time_format3 = DateFormats::DateTime(date_time3);
        assert_eq!(date_format, date_time_format3);
    }

    #[test]
    fn test_dateformat_ser_de() {
        let date: chrono::NaiveDate = chrono::NaiveDate::from_ymd_opt(2020, 10, 10).unwrap();
        let date_format = DateFormats::Date(date);
        let mut bytes = vec![];
        date_format.write_to(&mut bytes).unwrap();
        let date_format_de = <DateFormats>::read_from(&mut bytes.as_slice()).unwrap();
        assert_eq!(date_format, date_format_de);

        let time = chrono::NaiveTime::from_hms_milli_opt(10, 10, 10, 100).unwrap();
        let time_format = DateFormats::Time(time);
        let mut bytes = vec![];
        time_format.write_to(&mut bytes).unwrap();
        let time_format_de = <DateFormats>::read_from(&mut bytes.as_slice()).unwrap();
        assert_eq!(time_format, time_format_de);

        let date_time = chrono::NaiveDateTime::from_timestamp_opt(1602324610, 0).unwrap();
        let date_time_format = DateFormats::DateTime(date_time);
        let mut bytes = vec![];
        date_time_format.write_to(&mut bytes).unwrap();
        let date_time_format_de = <DateFormats>::read_from(&mut bytes.as_slice()).unwrap();
        assert_eq!(date_time_format, date_time_format_de);
    }
}
