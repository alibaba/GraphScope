use chrono::{Datelike, TimeZone, Utc};

fn is_time_splitter(c: char) -> bool {
    c == '-' || c == ':' || c == ' ' || c == 'T' || c == 'Z' || c == '.'
}

fn parse_datetime(val: &str) -> u64 {
    let mut dt_str = val;
    #[allow(unused_assignments)]
    let mut s = String::new();
    let mut is_millis = false;
    if let Ok(millis) = dt_str.parse::<i64>() {
        if let Some(dt) = Utc.timestamp_millis_opt(millis).single() {
            if dt.year() > 1970 && dt.year() < 2030 {
                println!("here");
                s = dt.to_rfc3339();
                dt_str = s.as_ref();
                is_millis = true;
            }
        }
    }
    let mut _time = String::with_capacity(dt_str.len());
    for c in dt_str.chars() {
        if c == '+' {
            // "2012-07-21T07:59:14.322+000", skip the content after "."
            break;
        } else if is_time_splitter(c) {
            continue; // replace any time splitter with void
        } else {
            _time.push(c);
        }
    }

    if is_millis {
        // pad '0' if not 'yyyyMMddHHmmssSSS'
        while _time.len() < 17 {
            // push the SSS to fill the datetime as the required format
            _time.push('0');
        }
    }
    _time.parse::<u64>().unwrap()
}

fn get_millis(t: u64) -> u64 {
    let mut dt = t;
    let ss = dt % 1000;
    dt = dt / 1000;
    let s = dt % 100;
    dt = dt / 100;
    let m = dt % 100;
    dt = dt / 100;
    let h = dt % 100;
    dt = dt / 100;
    let d = dt % 100;
    dt = dt / 100;
    let mm = dt % 100;
    dt = dt / 100;
    let y = dt % 10000;

    Utc.ymd(y as i32, mm as u32, d as u32)
        .and_hms_milli(h as u32, m as u32, s as u32, ss as u32)
        .timestamp_millis() as u64
}

fn main() {
    // let from = "20100226184952236";
    let to = "1267210192236";

    let ret = parse_datetime(to);

    println!("{}", ret);

    let a = 1285022511268_u64;
    println!(
        "{}, {}",
        a,
        get_millis(parse_datetime(a.to_string().as_str()))
    );
}
