#![feature(test)]
extern crate test;
use std::collections::{LinkedList, VecDeque};
use std::time::Duration;

use crossbeam_queue::ArrayQueue;
use test::Bencher;

#[derive(Copy, Clone)]
struct FlatPtr(u64, u64);

#[bench]
fn write_vec_deque(b: &mut Bencher) {
    let mut queue = VecDeque::new();
    b.iter(|| queue.push_back(FlatPtr(!0, 65536)));
    println!("after write queue.size {}", queue.len());
}

#[bench]
fn read_vec_deque(b: &mut Bencher) {
    let mut queue = VecDeque::new();
    //let item = "bench".to_owned();
    for _ in 0..572726701 {
        queue.push_back(FlatPtr(!0, 65536));
    }

    b.iter(|| queue.pop_front());
    println!("after read queue.size {}", queue.len());
}

#[bench]
fn write_linked_list(b: &mut Bencher) {
    let mut queue = LinkedList::new();
    b.iter(|| queue.push_back(FlatPtr(!0, 65536)));
    println!("after write linked size {}", queue.len());
}

#[bench]
fn write_array_queue(b: &mut Bencher) {
    let queue = ArrayQueue::new(100_000_000);
    b.iter(|| {
        for i in 0..100_000_000 {
            queue.push(FlatPtr(i, i)).ok();
        }

        while let Ok(_) = queue.pop() {}
    })
}

#[bench]
fn read_array_queue(b: &mut Bencher) {
    let queue = ArrayQueue::new(100_000_000);
    //let item = "bench".to_owned();
    for _ in 0..100_000_000 {
        queue.push(FlatPtr(!0, 65536)).ok();
    }

    b.iter(|| queue.pop());
    println!("after read array queue.size {}", queue.len());
}
