pub trait ToBigEndian: Copy {
    fn to_big_endian(&self) -> Self;
}

impl ToBigEndian for i16 {
    fn to_big_endian(&self) -> Self {
        self.to_be()
    }
}

impl ToBigEndian for u16 {
    fn to_big_endian(&self) -> Self {
        self.to_be()
    }
}

impl ToBigEndian for i32 {
    fn to_big_endian(&self) -> Self {
        self.to_be()
    }
}

impl ToBigEndian for u32 {
    fn to_big_endian(&self) -> Self {
        self.to_be()
    }
}

impl ToBigEndian for i64 {
    fn to_big_endian(&self) -> Self {
        self.to_be()
    }
}

impl ToBigEndian for u64 {
    fn to_big_endian(&self) -> Self {
        self.to_be()
    }
}

impl ToBigEndian for f32 {
    fn to_big_endian(&self) -> Self {
        f32::from_bits(self.to_bits().to_be())
    }
}

impl ToBigEndian for f64 {
    fn to_big_endian(&self) -> Self {
        f64::from_bits(self.to_bits().to_be())
    }
}

//impl Numeric for i8 {}
//impl Numeric for u8 {}
//impl Numeric for i16 {}
//impl Numeric for u16 {}
//impl Numeric for i32 {}
//impl Numeric for u32 {}
//impl Numeric for i64 {}
//impl Numeric for u64 {}
//impl Numeric for f32 {}
//impl Numeric for f64 {}