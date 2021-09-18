use std::cell::Cell;
use std::time::Instant;

pub trait Factory {
    type Target;

    fn create(&self) -> Self::Target;
}

pub struct DefaultFactory<T: Default> {
    _ph: std::marker::PhantomData<T>,
}

impl<T: Default> DefaultFactory<T> {
    pub fn new() -> Self {
        DefaultFactory { _ph: std::marker::PhantomData }
    }
}

impl<T: Default> Factory for DefaultFactory<T> {
    type Target = T;

    fn create(&self) -> Self::Target {
        Default::default()
    }
}

impl<T: Factory + ?Sized> Factory for Box<T> {
    type Target = T::Target;

    fn create(&self) -> Self::Target {
        (**self).create()
    }
}

pub struct ExecuteTimeMetric {
    times: usize,
    st: Cell<u128>,
}

impl ExecuteTimeMetric {
    pub fn new() -> Self {
        ExecuteTimeMetric { times: 0, st: Cell::new(0) }
    }

    pub fn metric(&mut self) -> TimeMonitor {
        self.times += 1;
        TimeMonitor::new(&self.st)
    }

    pub fn get_total(&self) -> u128 {
        self.st.get()
    }

    pub fn get_avg(&self) -> f64 {
        if self.times == 0 {
            0.0
        } else {
            self.st.get() as f64 / self.times as f64
        }
    }
}

pub struct TimeMonitor<'a> {
    exec_st: &'a Cell<u128>,
    start: Instant,
}

impl<'a> TimeMonitor<'a> {
    pub fn new(exec_st: &'a Cell<u128>) -> Self {
        TimeMonitor { exec_st, start: Instant::now() }
    }
}

impl<'a> Drop for TimeMonitor<'a> {
    fn drop(&mut self) {
        let s = self.exec_st.get() + self.start.elapsed().as_micros();
        self.exec_st.set(s);
    }
}
