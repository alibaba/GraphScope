#[macro_use]
extern crate log;

pub mod read_ffi;
pub mod write_ffi;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
