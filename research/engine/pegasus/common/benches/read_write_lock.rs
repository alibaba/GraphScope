//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

#![feature(test)]
extern crate test;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use test::Bencher;

#[bench]
fn std_rwlock_write(bench: &mut Bencher) {
    let lock = Arc::new(RwLock::new(1u64));
    bench.iter(|| {
        let mut lock = lock.write().unwrap();
        *lock += 1;
    })
}

#[bench]
fn std_rwlock_2_write(bench: &mut Bencher) {
    let lock = Arc::new(RwLock::new(1u64));
    let signal = Arc::new(AtomicBool::new(true));
    let _guard = {
        let lock = lock.clone();
        let signal = signal.clone();
        ::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                let mut lock = lock.write().unwrap();
                *lock += 1;
            }
        })
    };

    ::std::thread::sleep(Duration::from_millis(4));
    bench.iter(|| {
        let mut lock = lock.write().unwrap();
        *lock += 1;
    });
    signal.store(false, Ordering::SeqCst);
}

#[bench]
fn std_rwlock_1_write_1_read(bench: &mut Bencher) {
    let lock = Arc::new(RwLock::new(1u64));
    let signal = Arc::new(AtomicBool::new(true));
    let _guard = {
        let lock = lock.clone();
        let signal = signal.clone();
        ::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                let mut lock = lock.write().unwrap();
                *lock += 1;
            }
        })
    };

    ::std::thread::sleep(Duration::from_millis(4));
    bench.iter(|| {
        let lock = lock.read().unwrap();
        assert!(*lock >= 1);
    });
    signal.store(false, Ordering::SeqCst);
}

#[bench]
fn std_rwlock_read(bench: &mut Bencher) {
    let lock = Arc::new(RwLock::new(1u64));

    bench.iter(|| {
        let lock = lock.read().unwrap();
        assert_eq!(*lock, 1);
    });
}

#[bench]
fn std_rwlock_2_read(bench: &mut Bencher) {
    let lock = Arc::new(RwLock::new(1u64));
    let signal = Arc::new(AtomicBool::new(true));
    let _guard = {
        let lock = lock.clone();
        let signal = signal.clone();
        ::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                let lock = lock.read().unwrap();
                assert_eq!(*lock, 1)
            }
        })
    };

    ::std::thread::sleep(Duration::from_millis(4));

    bench.iter(|| {
        let lock = lock.read().unwrap();
        assert_eq!(*lock, 1)
    });
    signal.store(false, Ordering::SeqCst);
}

#[bench]
fn std_rwlock_4_read(bench: &mut Bencher) {
    let lock = Arc::new(RwLock::new(1u64));
    let signal = Arc::new(AtomicBool::new(true));
    let mut guards = Vec::with_capacity(4);
    for _ in 0..4 {
        let lock = lock.clone();
        let signal = signal.clone();
        guards.push(::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                let lock = lock.read().unwrap();
                assert_eq!(*lock, 1)
            }
        }));
    }

    ::std::thread::sleep(Duration::from_millis(4));
    bench.iter(|| {
        let lock = lock.read().unwrap();
        assert_eq!(*lock, 1)
    });
    signal.store(false, Ordering::SeqCst);
}

#[bench]
fn std_rwlock_read_with_little_write(bench: &mut Bencher) {
    let lock = Arc::new(RwLock::new(1u64));
    let signal = Arc::new(AtomicBool::new(true));
    let count = Arc::new(AtomicUsize::new(0));

    let _guard = {
        let lock = lock.clone();
        let signal = signal.clone();
        let count = count.clone();
        ::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                if count.load(Ordering::SeqCst) % 1000 == 0 {
                    let mut lock = lock.write().unwrap();
                    *lock += 1;
                }
                //::std::thread::sleep(Duration::from_millis(1));
            }
        })
    };

    bench.iter(|| {
        count.fetch_add(1, Ordering::SeqCst);
        let lock = lock.read().unwrap();
        assert!(*lock >= 1);
    });
    signal.store(false, Ordering::SeqCst);
}

use crossbeam_utils::sync::ShardedLock;

#[bench]
fn cb_rwlock_write(bench: &mut Bencher) {
    let lock = Arc::new(ShardedLock::new(1u64));
    bench.iter(|| {
        let mut lock = lock.write().unwrap();
        *lock += 1;
    })
}

#[bench]
fn cb_rwlock_2_write(bench: &mut Bencher) {
    let lock = Arc::new(ShardedLock::new(1u64));
    let signal = Arc::new(AtomicBool::new(true));
    let _guard = {
        let lock = lock.clone();
        let signal = signal.clone();
        ::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                let mut lock = lock.write().unwrap();
                *lock += 1;
            }
        })
    };

    ::std::thread::sleep(Duration::from_millis(4));
    bench.iter(|| {
        let mut lock = lock.write().unwrap();
        *lock += 1;
    });
    signal.store(false, Ordering::SeqCst);
}

#[bench]
fn cb_rwlock_1_write_1_read(bench: &mut Bencher) {
    let lock = Arc::new(ShardedLock::new(1u64));
    let signal = Arc::new(AtomicBool::new(true));
    let _guard = {
        let lock = lock.clone();
        let signal = signal.clone();
        ::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                let mut lock = lock.write().unwrap();
                *lock += 1;
            }
        })
    };

    ::std::thread::sleep(Duration::from_millis(4));
    bench.iter(|| {
        let lock = lock.read().unwrap();
        assert!(*lock >= 1);
    });
    signal.store(false, Ordering::SeqCst);
}

#[bench]
fn cb_rwlock_read(bench: &mut Bencher) {
    let lock = Arc::new(ShardedLock::new(1u64));

    bench.iter(|| {
        let lock = lock.read().unwrap();
        assert_eq!(*lock, 1);
    });
}

#[bench]
fn cb_rwlock_2_read(bench: &mut Bencher) {
    let lock = Arc::new(ShardedLock::new(1u64));
    let signal = Arc::new(AtomicBool::new(true));
    let _guard = {
        let lock = lock.clone();
        let signal = signal.clone();
        ::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                let lock = lock.read().unwrap();
                assert_eq!(*lock, 1)
            }
        })
    };

    ::std::thread::sleep(Duration::from_millis(4));

    bench.iter(|| {
        let lock = lock.read().unwrap();
        assert_eq!(*lock, 1)
    });
    signal.store(false, Ordering::SeqCst);
}

#[bench]
fn cb_rwlock_4_read(bench: &mut Bencher) {
    let lock = Arc::new(ShardedLock::new(1u64));
    let signal = Arc::new(AtomicBool::new(true));
    let mut guards = Vec::with_capacity(4);
    for _ in 0..4 {
        let lock = lock.clone();
        let signal = signal.clone();
        guards.push(::std::thread::spawn(move || {
            while signal.load(Ordering::SeqCst) {
                let lock = lock.read().unwrap();
                assert_eq!(*lock, 1)
            }
        }));
    }

    ::std::thread::sleep(Duration::from_millis(4));
    bench.iter(|| {
        let lock = lock.read().unwrap();
        assert_eq!(*lock, 1)
    });
    signal.store(false, Ordering::SeqCst);
}
