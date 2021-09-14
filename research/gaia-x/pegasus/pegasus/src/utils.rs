use std::cell::RefCell;
use std::time::{Duration, Instant};

struct StackTimeline {
    start: Instant,
    name: &'static str,
    events: Vec<(&'static str, Duration)>,
}

impl StackTimeline {
    pub fn new(name: &'static str) -> Self {
        StackTimeline { start: Instant::now(), name, events: Vec::with_capacity(1024) }
    }

    pub fn reset(&mut self, name: &'static str) {
        self.name = name;
        self.events.clear();
        self.start = Instant::now();
        self.events
            .push(("====== start trace ======", self.start.elapsed()));
    }

    #[inline]
    pub fn push(&mut self, name: &'static str) {
        let dur = self.start.elapsed();
        self.events.push((name, dur));
    }

    pub fn print(&mut self) {
        for (x, e) in self.events.drain(..) {
            info_worker!("\t{:?}\t\t{}", e, x);
        }
    }
}

thread_local! {
    static STACK_TIMELINE: RefCell<StackTimeline> = RefCell::new(StackTimeline::new(""));
}

#[inline]
pub fn new_stack_trace(name: &'static str) {
    STACK_TIMELINE.with(|st| st.borrow_mut().reset(name));
}

#[inline]
pub fn add_point(name: &'static str) {
    STACK_TIMELINE.with(|st| st.borrow_mut().push(name));
}

#[inline]
pub fn drain_time_line() {
    STACK_TIMELINE.with(|st| st.borrow_mut().print());
}
