use std::fmt::Debug;

use mpi::{
    topology::{Communicator, SystemCommunicator},
    traits::*,
};

fn assert_equivalence<A, B>(comm: &SystemCommunicator, a: &A, b: &B)
where
    A: Buffer,
    B: BufferMut + PartialEq + Debug + Default,
{
    let packed = comm.pack(a);

    let mut new_b = B::default();
    unsafe {
        comm.unpack_into(&packed, &mut new_b, 0);
    }

    assert_eq!(b, &new_b);
}

fn main() {
    let universe = mpi::initialize().unwrap();

    let world = universe.world();

    #[derive(Equivalence)]
    struct MyProgramOpts {
        name: [u8; 100],
        num_cycles: u32,
        material_properties: [f64; 20],
    }

    #[derive(Equivalence, Default, PartialEq, Debug)]
    struct MyDataRust {
        b: bool,
        f: f64,
        i: u16,
    }

    assert_equivalence(
        &world,
        &MyDataRust {
            b: true,
            f: 3.4,
            i: 7,
        },
        &MyDataRust {
            b: true,
            f: 3.4,
            i: 7,
        },
    );

    #[derive(Equivalence, Default, PartialEq, Debug)]
    #[repr(C)]
    struct MyDataC {
        b: bool,
        f: f64,
        i: u16,
    }

    assert_equivalence(
        &world,
        &MyDataRust {
            b: true,
            f: 3.4,
            i: 7,
        },
        &MyDataC {
            b: true,
            f: 3.4,
            i: 7,
        },
    );

    #[derive(Equivalence, Default, PartialEq, Debug)]
    struct MyDataOrdered {
        bf: (bool, f64),
        i: u16,
    }

    assert_equivalence(
        &world,
        &MyDataRust {
            b: true,
            f: 3.4,
            i: 7,
        },
        &MyDataOrdered {
            bf: (true, 3.4),
            i: 7,
        },
    );

    #[derive(Equivalence, Default, PartialEq, Debug)]
    struct MyDataNestedTuple {
        bfi: (bool, (f64, u16)),
    }

    assert_equivalence(
        &world,
        &MyDataRust {
            b: true,
            f: 3.4,
            i: 7,
        },
        &MyDataNestedTuple {
            bfi: (true, (3.4, 7)),
        },
    );

    #[derive(Equivalence, Default, PartialEq, Debug)]
    struct MyDataUnnamed(bool, f64, u16);

    assert_equivalence(
        &world,
        &MyDataRust {
            b: true,
            f: 3.4,
            i: 7,
        },
        &MyDataUnnamed(true, 3.4, 7),
    );

    #[derive(Equivalence, PartialEq, Debug)]
    struct BoolBoolBool(bool, bool, bool);

    #[derive(Equivalence, Default, PartialEq, Debug)]
    struct ThreeBool([bool; 3]);

    assert_equivalence(
        &world,
        &BoolBoolBool(true, false, true),
        &ThreeBool([true, false, true]),
    );

    #[derive(Equivalence, PartialEq, Debug)]
    struct ComplexComplexComplex((i8, bool, i8), (i8, bool, i8), (i8, bool, i8));

    #[derive(Equivalence, Default, PartialEq, Debug)]
    struct ThreeComplex([(i8, bool, i8); 3]);

    assert_equivalence(
        &world,
        &ComplexComplexComplex((1, true, 1), (2, false, 2), (3, true, 3)),
        &ThreeComplex([(1, true, 1), (2, false, 2), (3, true, 3)]),
    );

    #[derive(Equivalence, Default, PartialEq, Debug)]
    struct Empty;

    #[derive(Equivalence, PartialEq, Debug)]
    struct ZeroArray([i32; 0]);

    assert_equivalence(&world, &ZeroArray([]), &Empty);

    #[derive(Equivalence, Default, PartialEq, Debug)]
    struct Parent {
        b: bool,
        child: Child,
    }

    #[derive(Equivalence, Default, PartialEq, Debug)]
    struct Child(f64, u16);

    assert_equivalence(
        &world,
        &MyDataRust {
            b: true,
            f: 3.4,
            i: 7,
        },
        &Parent {
            b: true,
            child: Child(3.4, 7),
        },
    );
}
