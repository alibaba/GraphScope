use crate::error::GDBResult;
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use std::fmt::{Debug, Display, Formatter};

#[derive(Copy, Clone)]
pub enum Browser {
    IE,
    FF,
    OP,
    CH,
    SA,
    UNKNOWN,
}

impl Display for Browser {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Browser::IE => "Internet Explorer",
                Browser::OP => "Opera",
                Browser::FF => "Firefox",
                Browser::UNKNOWN => "Unknown",
                Browser::CH => "Chrome",
                Browser::SA => "Safari",
            }
        )
    }
}

impl Encode for Browser {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u8(match self {
            Browser::IE => 0_u8,
            Browser::FF => 1_u8,
            Browser::OP => 2_u8,
            Browser::UNKNOWN => 3_u8,
            Browser::CH => 4_u8,
            Browser::SA => 5_u8,
        })
    }
}

impl Decode for Browser {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let v = reader.read_u8()?;
        if v == 0 {
            Ok(Browser::IE)
        } else if v == 1 {
            Ok(Browser::FF)
        } else if v == 2 {
            Ok(Browser::OP)
        } else if v == 4 {
            Ok(Browser::CH)
        } else if v == 5 {
            Ok(Browser::SA)
        } else {
            Ok(Browser::UNKNOWN)
        }
    }
}

impl Debug for Browser {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

pub fn parse_browser(val: &str) -> GDBResult<Browser> {
    if val == "Internet Explorer" {
        Ok(Browser::IE)
    } else if val == "Opera" {
        Ok(Browser::OP)
    } else if val == "Firefox" {
        Ok(Browser::FF)
    } else if val == "Chrome" {
        Ok(Browser::CH)
    } else if val == "Safari" {
        Ok(Browser::SA)
    } else {
        println!("unrecognized browser: {}", val);
        Ok(Browser::UNKNOWN)
    }
}
