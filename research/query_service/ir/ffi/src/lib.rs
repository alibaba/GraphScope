#[macro_use]
extern crate log;

pub mod ffi;
pub mod graph_builder_ffi;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
