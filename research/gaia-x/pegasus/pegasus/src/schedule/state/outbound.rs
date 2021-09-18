use nohash_hasher::{IntMap, IntSet};

use crate::communication::cancel::CancelListener;
use crate::graph::Port;
use crate::tag::tools::map::TidyTagMap;
use crate::Tag;

struct CancelSingle {
    ch: u32,
    listener: Box<dyn CancelListener>,
}

impl CancelSingle {
    fn cancel(&mut self, ch: u32, to: u32, tag: &Tag) -> Vec<Tag> {
        assert_eq!(self.ch, ch);
        if let Some(res) = self.listener.cancel(tag, to) {
            vec![res]
        } else {
            vec![]
        }
    }
}

struct CancelTee {
    // the number of channels mounted to this port;
    channels: usize,
    // scope level of data which will enter this port;
    scope_level: u32,
    // channel's index -> channel's skip listener;
    tee: IntMap<u32, Box<dyn CancelListener>>,
    // trace canceled scope in each mounted channel;
    cancel_trace: Vec<TidyTagMap<IntSet<u32>>>,
}

impl CancelTee {
    fn cancel(&mut self, ch: u32, to: u32, tag: &Tag) -> Vec<Tag> {
        let res = {
            if let Some(listener) = self.tee.get_mut(&ch) {
                listener.cancel(tag, to)
            } else {
                None
            }
        };

        if let Some(res) = res {
            let level = res.len() as u32;
            let guard = self.channels;
            let mut vec = vec![];

            if level < self.scope_level {
                for i in (level + 1..self.scope_level + 1).rev() {
                    for (tag, st) in self.cancel_trace[i as usize].iter_mut() {
                        if res.is_parent_of(&*tag) {
                            st.insert(ch);
                            if st.len() == guard {
                                vec.push((&*tag).clone());
                            }
                        }
                    }
                }
            }

            if level <= self.scope_level {
                let set = self.cancel_trace[level as usize].get_mut_or_insert(&res);
                set.insert(ch);
                if set.len() == guard {
                    vec.push(res)
                }
            } else {
                warn_worker!("unexpected cancel tag {:?} expected level <= {}", tag, self.scope_level);
            }
            vec
        } else {
            vec![]
        }
    }
}

enum HandleKind {
    Single(CancelSingle),
    Tee(CancelTee),
}

impl HandleKind {
    fn cancel(&mut self, ch: u32, to: u32, tag: &Tag) -> Vec<Tag> {
        match self {
            HandleKind::Single(x) => x.cancel(ch, to, tag),
            HandleKind::Tee(x) => x.cancel(ch, to, tag),
        }
    }
}

#[allow(dead_code)]
pub struct OutputCancelState {
    port: Port,
    handle: HandleKind,
}

impl OutputCancelState {
    pub fn single(port: Port, ch: u32, listener: Box<dyn CancelListener>) -> Self {
        OutputCancelState { port, handle: HandleKind::Single(CancelSingle { ch, listener }) }
    }

    pub fn tee(port: Port, scope_level: u32, tee: Vec<(u32, Box<dyn CancelListener>)>) -> Self {
        let channels = tee.len();
        let mut map = IntMap::default();
        for (ch, lis) in tee {
            map.insert(ch, lis);
        }
        let mut cancel_trace = Vec::with_capacity(scope_level as usize);
        for i in 0..scope_level + 1 {
            cancel_trace.push(TidyTagMap::new(i));
        }
        let handle = CancelTee { channels, scope_level, tee: map, cancel_trace };
        OutputCancelState { port, handle: HandleKind::Tee(handle) }
    }

    pub fn on_cancel(&mut self, ch: u32, to: u32, tag: &Tag) -> Vec<Tag> {
        self.handle.cancel(ch, to, tag)
    }
}
