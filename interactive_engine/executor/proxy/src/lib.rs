pub mod request;

#[macro_use]
extern crate lazy_static;

#[cfg(not(feature = "gcip"))]
mod generated {
    pub mod common {
        tonic::include_proto!("common");
    }

    pub mod protocol {
        tonic::include_proto!("protocol");
    }

    pub mod procedure {
        tonic::include_proto!("procedure");
    }
}

#[rustfmt::skip]
#[cfg(feature = "gcip")]
mod generated {
    #[path = "common.rs"]
    pub mod common;

    #[path = "protocol.rs"]
    pub mod protocol;

    #[path = "procedure.rs"]
    pub mod procedure;
}
