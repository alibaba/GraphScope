use std::thread;
use std::time::Duration;

#[allow(dead_code)]
pub fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}

