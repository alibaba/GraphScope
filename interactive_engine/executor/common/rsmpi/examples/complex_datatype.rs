#![deny(warnings)]
#![allow(clippy::forget_copy)]
extern crate mpi;

#[macro_use]
extern crate memoffset;

use mpi::{
    datatype::{UncommittedUserDatatype, UserDatatype},
    traits::*,
    Address,
};

#[derive(Default)]
struct TupleType([f32; 2], u8);

#[derive(Default)]
struct ComplexDatatype {
    b: bool,
    ints: [i32; 4],
    tuple: TupleType,
}

unsafe impl Equivalence for ComplexDatatype {
    type Out = UserDatatype;
    fn equivalent_datatype() -> Self::Out {
        UserDatatype::structured(
            &[1, 1, 1],
            &[
                offset_of!(ComplexDatatype, b) as Address,
                offset_of!(ComplexDatatype, ints) as Address,
                offset_of!(ComplexDatatype, tuple) as Address,
            ],
            &[
                bool::equivalent_datatype().into(),
                UncommittedUserDatatype::contiguous(4, &i32::equivalent_datatype()).as_ref(),
                UncommittedUserDatatype::structured(
                    &[2, 1],
                    &[
                        offset_of!(TupleType, 0) as Address,
                        offset_of!(TupleType, 1) as Address,
                    ],
                    &[f32::equivalent_datatype(), u8::equivalent_datatype()],
                )
                .as_ref(),
            ],
        )
    }
}

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();

    let root_process = world.process_at_rank(0);

    let mut data = if world.rank() == 0 {
        ComplexDatatype {
            b: true,
            ints: [1, -2, 3, -4],
            tuple: TupleType([-0.1, 0.1], 7),
        }
    } else {
        ComplexDatatype::default()
    };

    root_process.broadcast_into(&mut data);

    assert_eq!(true, data.b);
    assert_eq!([1, -2, 3, -4], data.ints);
    assert_eq!([-0.1, 0.1], data.tuple.0);
    assert_eq!(7, data.tuple.1);
}
