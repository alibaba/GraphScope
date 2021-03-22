#![allow(dead_code)]
use std::time::Instant;

pub struct Timer {
    timer: Instant,
}

impl Timer {
    pub fn new() -> Self {
        Timer {
            timer: Instant::now(),
        }
    }

    pub fn elapsed_ms(&self) -> f64 {
        let t = self.timer.elapsed();
        t.as_secs() as f64 * 1000.0 + t.subsec_nanos() as f64 / 1000000.0
    }

    pub fn elapsed_secs(&self) -> f64 {
        let t = self.timer.elapsed();
        t.as_secs() as f64 + t.subsec_nanos() as f64 / 1000000000.0
    }
}