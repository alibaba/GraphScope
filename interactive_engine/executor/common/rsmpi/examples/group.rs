#![deny(warnings)]
#![allow(clippy::many_single_char_names)]
extern crate mpi;

use mpi::topology::{GroupRelation, Rank, SystemGroup};
use mpi::traits::*;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();

    let g = world.group();
    // Group accessors and Communicator accessors agree
    assert_eq!(world.size(), g.size());
    assert_eq!(world.rank(), g.rank().unwrap());

    // g == g
    assert_eq!(GroupRelation::Identical, g.compare(&g));

    let h = world.group();
    // h == g
    assert_eq!(GroupRelation::Identical, g.compare(&h));

    let i = g.union(&h);
    // g union h == g union g == g
    assert_eq!(GroupRelation::Identical, g.compare(&i));

    let empty = g.difference(&h);
    // g difference h == g difference g = empty Group
    assert_eq!(
        GroupRelation::Identical,
        SystemGroup::empty().compare(&empty)
    );
    assert_eq!(0, empty.size());

    // g intersection empty == empty Group
    assert_eq!(0, g.intersection(&empty).size());

    let first_half: Vec<Rank> = (0..g.size() / 2).collect();

    // f and s are first and second half of g
    let f = g.include(&first_half[..]);
    let s = g.exclude(&first_half[..]);
    // f != s
    assert_eq!(GroupRelation::Unequal, f.compare(&s));

    // g intersection f == f
    let f_ = g.intersection(&f);
    assert_eq!(GroupRelation::Identical, f.compare(&f_));
    // g intersection s == s
    let s_ = g.intersection(&s);
    assert_eq!(GroupRelation::Identical, s.compare(&s_));

    // g difference s == f
    let f__ = g.difference(&s);
    assert_eq!(GroupRelation::Identical, f.compare(&f__));
    // g difference f == s
    let s__ = g.difference(&f);
    assert_eq!(GroupRelation::Identical, s.compare(&s__));

    // f union s == g
    let fs = f.union(&s);
    assert_eq!(GroupRelation::Identical, g.compare(&fs));

    // f intersection s == empty Group
    let fs = f.intersection(&s);
    assert_eq!(GroupRelation::Identical, empty.compare(&fs));

    // rank is either in f or in s
    assert!(
        (f.rank().is_some() && s.rank().is_none()) ^ (f.rank().is_none() && s.rank().is_some())
    );

    // inverting rank mappings
    let rev: Vec<Rank> = (0..g.size()).rev().collect();
    let r = g.include(&rev[..]);
    assert_eq!(
        Some(rev[g.rank().unwrap() as usize]),
        r.translate_rank(g.rank().unwrap(), &g)
    );
}
