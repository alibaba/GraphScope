#![deny(warnings)]
#![allow(clippy::cognitive_complexity)]
extern crate mpi;

use mpi::traits::*;

fn main() {
    let universe = mpi::initialize().unwrap();

    let comm = universe.world();

    if comm.size() < 4 {
        return;
    }

    let cart_comm = {
        let dims = [2, 2];
        let periodic = [false, true];
        let reorder = true;
        if let Some(cart_comm) = comm.create_cartesian_communicator(&dims, &periodic, reorder) {
            cart_comm
        } else {
            assert!(comm.rank() >= 4);
            return;
        }
    };

    assert_eq!(2, cart_comm.num_dimensions());

    let mpi::topology::CartesianLayout {
        dims,
        periods,
        coords,
    } = cart_comm.get_layout();

    assert_eq!([2 as mpi::Count, 2], &dims[..]);
    assert_eq!([false, true], &periods[..]);

    let xrank = coords[0];
    let yrank = coords[1];

    assert!(0 <= xrank && xrank < 2);
    assert!(0 <= yrank && yrank < 2);

    let xcomm = cart_comm.subgroup(&[true, false]);
    let ycomm = cart_comm.subgroup(&[false, true]);

    assert_eq!(2, xcomm.size());
    assert_eq!(xrank, xcomm.rank());

    assert_eq!(2, ycomm.size());
    assert_eq!(yrank, ycomm.rank());

    // the first dimension is non-periodic
    let (x_src, x_dest) = cart_comm.shift(0, 1);
    if xrank == 0 {
        assert!(x_src.is_none());
        assert!(x_dest.is_some());

        let coords = cart_comm.rank_to_coordinates(x_dest.unwrap());
        assert_eq!(1, coords[0]);
    } else {
        assert_eq!(1, xrank);

        assert!(x_src.is_some());
        assert!(x_dest.is_none());

        let coords = cart_comm.rank_to_coordinates(x_src.unwrap());
        assert_eq!(0, coords[0]);
    }

    // the second dimension is periodic
    {
        let (y_src, y_dest) = cart_comm.shift(1, 1);
        assert!(y_src.is_some());
        assert!(y_dest.is_some());

        let y_src_coords = cart_comm.rank_to_coordinates(y_src.unwrap());
        assert_eq!((yrank - 1) & 0b1, y_src_coords[1]);

        let y_dest_coords = cart_comm.rank_to_coordinates(y_dest.unwrap());
        assert_eq!((yrank + 1) & 0b1, y_dest_coords[1]);
    }

    // second dimension shift by 2 should be identity
    {
        let (y_src, y_dest) = cart_comm.shift(1, 2);
        assert_eq!(comm.rank(), y_src.unwrap());
        assert_eq!(comm.rank(), y_dest.unwrap());
    }
}
