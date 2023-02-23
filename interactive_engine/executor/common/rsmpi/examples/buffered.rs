#![deny(warnings)]
extern crate mpi;

use mpi::request::WaitGuard;
use mpi::traits::*;

const BUFFER_SIZE: usize = 10 * 1024 * 1024;

fn main() {
    let mut universe = mpi::initialize().unwrap();
    // Try to attach a buffer.
    universe.set_buffer_size(BUFFER_SIZE);
    // Check buffer size matches.
    assert_eq!(universe.buffer_size(), BUFFER_SIZE);
    // Try to detach the buffer.
    universe.detach_buffer();
    // Attach another buffer.
    universe.set_buffer_size(BUFFER_SIZE);

    let world = universe.world();

    let x = vec![std::f32::consts::PI; 1024];
    let mut y = vec![0.0; 1024];
    mpi::request::scope(|scope| {
        let _rreq = WaitGuard::from(
            world
                .any_process()
                .immediate_receive_into(scope, &mut y[..]),
        );
        world.this_process().buffered_send(&x[..]);
    });
    assert_eq!(x, y);
}
