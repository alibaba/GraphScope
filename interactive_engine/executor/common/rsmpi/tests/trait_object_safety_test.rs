use mpi::traits::*;

/// This tests to see if rust will treat the `Communicator` and `Group`
/// traits as objects (i.e., you can use `&dyn`). All that matters is
/// that this file compiles.
#[test]
fn object_safety_test() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let gp = world.group();
    foo(&world, &world.get_name());
    bar(&gp, gp.size())
}

/// All that matters is that this compiles
fn foo(comm: &dyn Communicator, name: &str) {
    assert_eq!(comm.get_name(), name);
}

/// All that matters is that this compiles
fn bar(gp: &dyn Group, size: i32) {
    assert_eq!(gp.size(), size);
}
