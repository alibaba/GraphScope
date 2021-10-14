use std::sync::{Mutex, MutexGuard};
use crate::db::api::*;
use std::ops::{Deref, DerefMut};

pub struct GraphMutexLock<T> {
    inner: Mutex<T>,
}

impl<T> GraphMutexLock<T> {
    pub fn new(t: T) -> Self {
        GraphMutexLock {
            inner: Mutex::new(t),
        }
    }

    pub fn lock(&self) -> GraphResult<GraphMutexLockGuard<T>> {
        self.inner.lock()
            .map(|guard| GraphMutexLockGuard::new(guard))
            .map_err(|e| {
                let msg = format!("{:?}", e);
                gen_graph_err!(GraphErrorCode::LockFailed, msg, lock)
            })
    }
}

pub struct GraphMutexLockGuard<'a, T> {
    inner: MutexGuard<'a, T>,
}

impl<'a, T> GraphMutexLockGuard<'a, T> {
    fn new(guard: MutexGuard<'a, T>) -> Self {
        GraphMutexLockGuard {
            inner: guard,
        }
    }
}

impl<T> Deref for GraphMutexLockGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for GraphMutexLockGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_graph_mutex_lock() {
        let count = Arc::new(GraphMutexLock::new(0));
        let mut threads = Vec::new();
        let thread_count = 4;
        let test_count = 10000;
        for _ in 0..thread_count {
            let count_clone = count.clone();
            let t = thread::spawn(move || {
                for _ in 0..test_count {
                    *count_clone.lock().unwrap() += 1;
                }
            });
            threads.push(t);
        }

        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(*count.lock().unwrap(), thread_count * test_count);
    }
}