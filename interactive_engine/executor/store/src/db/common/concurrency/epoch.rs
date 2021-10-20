#![allow(dead_code)]
use ::crossbeam_epoch::Guard;
use std::ops::{Deref};

pub struct EpochGuard<T> {
    guard: Guard,
    data: T,
}

impl<T> EpochGuard<T> {
    pub fn new(guard: Guard, data: T) -> Self {
        EpochGuard {
            guard,
            data,
        }
    }
}

impl<T> Deref for EpochGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

pub struct EpochGuardIter<I, T: Iterator<Item=I>> {
    guard: Guard,
    iter: T,
}

impl<I, T: Iterator<Item=I>> EpochGuardIter<I, T> {
    pub fn new(guard: Guard, iter: T) -> Self {
        EpochGuardIter {
            guard,
            iter,
        }
    }
}

impl<I, T: Iterator<Item=I>> Iterator for EpochGuardIter<I, T> {
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}
