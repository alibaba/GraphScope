use std::cell::RefCell;

use ahash::AHashMap;
use pegasus_common::rc::UnsafeRcPtr;

use crate::communication::Magic;
use crate::tag::tools::map::TidyTagMap;
use crate::Tag;

pub trait CancelListener: Send + 'static {
    /// invoked by the consumer who won't consume any data of the scope=[`Tag`], and notify the
    /// producer don't produce data of the scope to it;
    ///
    /// this listener is owned by the producer to listen notification from it's consumers;
    fn cancel(&mut self, tag: &Tag, to: u32) -> Option<Tag>;
}

/// to local static single consumer cancel;
/// SingleConsumerCancel;
#[derive(Copy, Clone)]
pub(crate) struct SingleConsCancel {
    target: u32,
}

impl SingleConsCancel {
    pub fn new(target: u32) -> Self {
        SingleConsCancel { target }
    }
}

impl CancelListener for SingleConsCancel {
    fn cancel(&mut self, tag: &Tag, to: u32) -> Option<Tag> {
        if to == self.target {
            Some(tag.clone())
        } else {
            None
        }
    }
}

/// multi-consumers cancel;
pub(crate) struct MultiConsCancel {
    scope_level: u32,
    targets: usize,
    current_level: TidyTagMap<Vec<bool>>,
    parent: AHashMap<Tag, Vec<bool>>,
}

impl MultiConsCancel {
    fn new(scope_level: u32, targets: usize) -> Self {
        MultiConsCancel {
            scope_level,
            targets,
            current_level: TidyTagMap::new(scope_level),
            parent: AHashMap::new(),
        }
    }

    fn is_canceled(&self, target: usize, tag: &Tag) -> bool {
        if target >= self.targets {
            return false;
        }

        let level = tag.len() as u32;
        if level == self.scope_level {
            if !self.current_level.is_empty() {
                if let Some(ce) = self.current_level.get(tag) {
                    if ce[target] {
                        return true;
                    }
                }
            }

            // no cancel checked in current level;
            if *crate::config::ENABLE_CANCEL_CHILD && !self.parent.is_empty() {
                let p = tag.to_parent_uncheck();
                self.check_parent(target, p)
            } else {
                false
            }
        } else if level < self.scope_level {
            self.check_parent(target, tag.clone())
        } else {
            false
        }
    }

    fn check_parent(&self, target: usize, mut p: Tag) -> bool {
        loop {
            if let Some(ce) = self.parent.get(&p) {
                if ce[target] {
                    return true;
                }
            }

            if p.is_root() {
                break;
            } else {
                p = p.to_parent_uncheck();
            }
        }
        false
    }
}

impl CancelListener for MultiConsCancel {
    fn cancel(&mut self, tag: &Tag, to: u32) -> Option<Tag> {
        let level = tag.len() as u32;
        let targets = self.targets;
        let to = to as usize;
        if to < targets {
            if level == self.scope_level {
                let x = self
                    .current_level
                    .get_mut_or_else(tag, || vec![false; targets]);
                x[to] = true;
                trace_worker!("cancel output data of {:?} to {};", tag, to);
                if x.iter().all(|f| *f) {
                    Some(tag.clone())
                } else {
                    None
                }
            } else if level < self.scope_level {
                let x = self
                    .parent
                    .entry(tag.clone())
                    .or_insert_with(|| vec![false; targets]);
                x[to] = true;
                if x.iter().all(|f| *f) {
                    Some(tag.clone())
                } else {
                    None
                }
            } else {
                warn_worker!("invalid cancel notify of {:?}  in scope_level {}", tag, self.scope_level);
                None
            }
        } else {
            None
        }
    }
}

#[derive(Clone)]
pub(crate) struct MultiConsCancelPtr {
    inner: UnsafeRcPtr<RefCell<MultiConsCancel>>,
}

impl MultiConsCancelPtr {
    pub(crate) fn new(scope_level: u32, targets: usize) -> Self {
        let inner = MultiConsCancel::new(scope_level, targets);
        MultiConsCancelPtr { inner: UnsafeRcPtr::new(RefCell::new(inner)) }
    }

    pub fn cancel(&self, tag: &Tag, to: u32) -> Option<Tag> {
        self.inner.borrow_mut().cancel(tag, to)
    }

    pub fn is_canceled(&self, tag: &Tag, target: usize) -> bool {
        self.inner.borrow().is_canceled(target, tag)
    }
}

pub(crate) struct DynSingleConsCancel {
    scope_level: u32,
    targets: usize,
    route: Magic,
    current_level: TidyTagMap<()>,
    parent: AHashMap<Tag, Vec<bool>>,
}

impl CancelListener for DynSingleConsCancel {
    fn cancel(&mut self, tag: &Tag, to: u32) -> Option<Tag> {
        let level = tag.len() as u32;
        if level == self.scope_level {
            assert!(level > 0);
            let cur = tag.current_uncheck() as u64;
            let target = self.route.exec(cur) as u32;
            if to == target {
                self.current_level.insert(tag.clone(), ());
                Some(tag.clone())
            } else {
                None
            }
        } else if level < self.scope_level {
            let targets = self.targets;
            let x = self
                .parent
                .entry(tag.clone())
                .or_insert_with(|| vec![false; targets]);
            x[to as usize] = true;
            if x.iter().all(|f| *f) {
                Some(tag.clone())
            } else {
                None
            }
        } else {
            warn_worker!("invalid cancel notify of {:?}  in scope_level {}", tag, self.scope_level);
            None
        }
    }
}

impl DynSingleConsCancel {
    pub(crate) fn is_canceled(&self, tag: &Tag, offset: usize) -> bool {
        let level = tag.len() as u32;
        if level == self.scope_level {
            if !self.current_level.is_empty() && self.current_level.contains_key(tag) {
                return true;
            }

            if *crate::config::ENABLE_CANCEL_CHILD && !self.parent.is_empty() {
                let p = tag.to_parent_uncheck();
                assert!(offset < self.targets);
                self.check_parent(p, offset)
            } else {
                false
            }
        } else if level < self.scope_level {
            assert!(offset < self.targets);
            self.check_parent(tag.clone(), offset)
        } else {
            false
        }
    }

    fn check_parent(&self, mut p: Tag, target: usize) -> bool {
        loop {
            if let Some(cal) = self.parent.get(&p) {
                if cal[target] {
                    return true;
                }
            }
            if p.is_root() {
                break;
            } else {
                p = p.to_parent_uncheck();
            }
        }
        false
    }
}

#[derive(Clone)]
pub(crate) struct DynSingleConsCancelPtr {
    inner: UnsafeRcPtr<RefCell<DynSingleConsCancel>>,
}

impl CancelListener for DynSingleConsCancelPtr {
    fn cancel(&mut self, tag: &Tag, to: u32) -> Option<Tag> {
        self.inner.borrow_mut().cancel(tag, to)
    }
}

impl DynSingleConsCancelPtr {
    pub(crate) fn new(scope_level: u32, targets: usize) -> Self {
        let inner = DynSingleConsCancel {
            scope_level,
            targets,
            route: Magic::new(targets),
            current_level: TidyTagMap::new(scope_level),
            parent: AHashMap::new(),
        };

        DynSingleConsCancelPtr { inner: UnsafeRcPtr::new(RefCell::new(inner)) }
    }

    pub(crate) fn is_canceled(&self, tag: &Tag, target: usize) -> bool {
        self.inner.borrow().is_canceled(tag, target)
    }
}

#[derive(Clone)]
pub(crate) enum CancelHandle {
    SC(SingleConsCancel),
    MC(MultiConsCancelPtr),
    DSC(DynSingleConsCancelPtr),
}

impl CancelHandle {
    pub(crate) fn is_canceled(&self, tag: &Tag, to: usize) -> bool {
        match self {
            CancelHandle::SC(_) => false,
            CancelHandle::MC(x) => x.is_canceled(tag, to),
            CancelHandle::DSC(x) => x.is_canceled(tag, to),
        }
    }
}

impl CancelListener for CancelHandle {
    fn cancel(&mut self, tag: &Tag, to: u32) -> Option<Tag> {
        match self {
            CancelHandle::SC(x) => x.cancel(tag, to),
            CancelHandle::MC(x) => x.cancel(tag, to),
            CancelHandle::DSC(x) => x.cancel(tag, to),
        }
    }
}
