#![deny(warnings)]
extern crate mpi;

#[cfg(msmpi)]
fn main() {
    // There appears to be a bug with MPI_Ibarrier on MS-MPI. Its state machine is not advanced
    // while other I/O is blocking.
}

#[cfg(not(msmpi))]
fn main() {
    use mpi::traits::*;
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let size = world.size();
    let receiver_rank = 0;

    if world.rank() == receiver_rank {
        // receiver process
        let n = (size - 1) as usize;
        let mut buf = vec![0u64; 3 * n];
        // receive first 2 * n messages
        for x in buf[0..2 * n].iter_mut() {
            world.any_process().receive_into(x);
        }
        // signal the waiting senders that 2 * n messages have been received
        let breq = world.immediate_barrier();
        // receive remaining n messages
        for x in buf[2 * n..3 * n].iter_mut() {
            world.any_process().receive_into(x);
        }
        println!("{:?}", buf);
        // messages "1" and "2" may be interleaved, but all have to be contained within the first
        // 2 * n slots of the buffer
        assert_eq!(buf[0..2 * n].iter().filter(|&&x| x == 1).count(), n);
        assert_eq!(buf[0..2 * n].iter().filter(|&&x| x == 2).count(), n);
        // the last n slots in the buffer may only contain message "3"
        assert!(buf[2 * n..3 * n].iter().all(|&x| x == 3));
        // clean up the barrier request
        breq.wait();
    } else {
        // sender processes
        // send message "1"
        world.process_at_rank(0).send(&1u64);
        // join barrier, but do not block
        let breq = world.immediate_barrier();
        // send message "2"
        world.process_at_rank(0).send(&2u64);
        // wait for receiver process to receive the first 2 * n messages
        breq.wait();
        // send message "3"
        world.process_at_rank(0).send(&3u64);
    }
}
