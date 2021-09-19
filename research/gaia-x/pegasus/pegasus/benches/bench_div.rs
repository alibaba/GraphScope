#![feature(test)]
extern crate test;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use rand::prelude::*;

#[bench]
fn bench_add(b: &mut test::Bencher) {
    let mut rng = rand::thread_rng();
    let mut d: u32 = rng.gen();
    let mut x = 0u32;
    b.iter(|| {
        for i in 1..1001 {
            if d > 1000 {
                x += i;
            }
        }
        //d = d.wrapping_add(1);
    });
    println!("{}", x);
}

#[bench]
fn bench_div(b: &mut test::Bencher) {
    let p = 4u32;
    let mut x = 0;
    b.iter(|| {
        for i in 1..1001 {
            x += i % p;
        }
    });
    println!("{}", x);
}

#[bench]
fn bench_and(b: &mut test::Bencher) {
    let p = 3u32;
    let mut x = 0;
    b.iter(|| {
        for i in 1..1001 {
            x += i & p;
        }
    });
    println!("{}", x);
}

#[bench]
fn bench_hash(b: &mut test::Bencher) {
    let mut x = 0;
    b.iter(|| {
        for i in 1..1001 {
            let mut h = DefaultHasher::new();
            h.write_u32(i);
            x += h.finish();
        }
    });
    println!("{}", x);
}

enum Mix {
    Div(u32),
    And(u32),
}

#[bench]
fn bench_mix_div(b: &mut test::Bencher) {
    let p = Mix::Div(4);
    let mut x = 0;
    b.iter(|| {
        for i in 1..1001 {
            match p {
                Mix::Div(d) => x += i % d,
                Mix::And(a) => x += i & a,
            }
        }
    });
    println!("{}", x);
}

#[bench]
fn bench_mix_and(b: &mut test::Bencher) {
    let p = Mix::And(3);
    let mut x = 0;
    b.iter(|| {
        for i in 1..1001 {
            match p {
                Mix::Div(d) => x += i % d,
                Mix::And(a) => x += i & a,
            }
        }
    });
    println!("{}", x);
}
