#![deny(warnings)]
extern crate mpi;

use mpi::topology::{Color, GroupRelation, SystemGroup};
use mpi::traits::*;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();

    let odd = (0..world.size()).filter(|x| x % 2 != 0).collect::<Vec<_>>();
    let odd_group = world.group().include(&odd[..]);
    let even_group = world.group().difference(&odd_group);
    assert!(
        (world.rank() % 2 == 0 && even_group.rank().is_some() && odd_group.rank().is_none())
            || (even_group.rank().is_none() && odd_group.rank().is_some())
    );
    let my_group = if odd_group.rank().is_some() {
        &odd_group
    } else {
        &even_group
    };
    let empty_group = SystemGroup::empty();

    let oddness_comm = world.split_by_subgroup_collective(my_group);
    assert!(oddness_comm.is_some());
    let oddness_comm = oddness_comm.unwrap();
    assert_eq!(
        GroupRelation::Identical,
        oddness_comm.group().compare(my_group)
    );

    let odd_comm = if odd_group.rank().is_some() {
        world.split_by_subgroup_collective(&odd_group)
    } else {
        world.split_by_subgroup_collective(&empty_group)
    };
    if odd_group.rank().is_some() {
        assert!(odd_comm.is_some());
        let odd_comm = odd_comm.unwrap();
        assert_eq!(
            GroupRelation::Identical,
            odd_comm.group().compare(&odd_group)
        );
    } else {
        assert!(odd_comm.is_none());
    }

    #[cfg(not(msmpi))]
    {
        if even_group.rank().is_some() {
            let even_comm = world.split_by_subgroup(&even_group);
            assert!(even_comm.is_some());
            let even_comm = even_comm.unwrap();
            assert_eq!(
                GroupRelation::Identical,
                even_comm.group().compare(&even_group)
            );

            let no_comm = world.split_by_subgroup(&odd_group);
            assert!(no_comm.is_none());
        }
    }

    let oddness_comm = world.split_by_color(Color::with_value(world.rank() % 2));
    assert!(oddness_comm.is_some());
    let oddness_comm = oddness_comm.unwrap();
    assert_eq!(
        GroupRelation::Identical,
        oddness_comm.group().compare(my_group)
    );

    let odd_comm = world.split_by_color(if world.rank() % 2 != 0 {
        Color::with_value(0)
    } else {
        Color::undefined()
    });
    if world.rank() % 2 != 0 {
        assert!(odd_comm.is_some());
        let odd_comm = odd_comm.unwrap();
        assert_eq!(
            GroupRelation::Identical,
            odd_comm.group().compare(&odd_group)
        );
    } else {
        assert!(odd_comm.is_none());
    }
}
