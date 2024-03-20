extern crate dlopen;
#[macro_use]
extern crate dlopen_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;

extern crate core;
extern crate rand;

pub mod queries;

#[cfg(not(feature = "gcip"))]
mod generated {
    pub mod protocol {
        tonic::include_proto!("protocol");
    }
}

#[rustfmt::skip]
#[cfg(feature = "gcip")]
mod generated {
    #[path = "protocol.rs"]
    pub mod protocol;
}

pub use generated::protocol as pb;
