use crate::error::GDBResult;
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use std::fmt::{Debug, Display, Formatter};

#[derive(Copy, Clone)]
pub struct IpAddr {
    pub(crate) a: u8,
    pub(crate) b: u8,
    pub(crate) c: u8,
    pub(crate) d: u8,
}

impl IpAddr {
    pub fn new() -> Self {
        Self {
            a: 0_u8,
            b: 0_u8,
            c: 0_u8,
            d: 0_u8,
        }
    }
}

impl Display for IpAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}.{}", self.a, self.b, self.c, self.d)
    }
}

impl Encode for IpAddr {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u8(self.a)?;
        writer.write_u8(self.b)?;
        writer.write_u8(self.c)?;
        writer.write_u8(self.d)?;
        Ok(())
    }
}

impl Decode for IpAddr {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let mut ret = IpAddr::new();
        ret.a = reader.read_u8()?;
        ret.b = reader.read_u8()?;
        ret.c = reader.read_u8()?;
        ret.d = reader.read_u8()?;
        Ok(ret)
    }
}

impl Debug for IpAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

pub fn parse_ip_addr(val: &str) -> GDBResult<IpAddr> {
    let mut ret = IpAddr::new();
    let mut loc = [0; 3];
    let mut cur = 0;
    for (i, c) in val.char_indices() {
        if c == '.' {
            loc[cur] = i;
            cur += 1;
        }
    }
    ret.a = val[0..loc[0]].parse::<u8>()?;
    ret.b = val[loc[0] + 1..loc[1]].parse::<u8>()?;
    ret.c = val[loc[1] + 1..loc[2]].parse::<u8>()?;
    ret.d = val[loc[2] + 1..].parse::<u8>()?;

    Ok(ret)
}
