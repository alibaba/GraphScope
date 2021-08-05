use crate::queue::BoundLinkQueue;
use crate::rc::RcPointer;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

type Buf<D> = VecDeque<D>;

pub struct Batch<D> {
    // TODO: optimize batch implementation instead of VecDeque;
    inner: Option<Buf<D>>,
    // TODO: consider use small vec instead;
    recycle: Vec<BatchRecycleHook<D>>,
}

impl<D> Batch<D> {
    pub fn new() -> Self {
        Batch { inner: None, recycle: vec![] }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Batch { inner: Some(Buf::with_capacity(capacity)), recycle: vec![] }
    }

    pub fn push(&mut self, data: D) {
        if let Some(ref mut buf) = self.inner {
            buf.push_back(data);
        } else {
            let mut buf = Buf::new();
            buf.push_back(data);
            self.inner = Some(buf);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inner
            .as_ref()
            .map(|x| x.is_empty())
            .unwrap_or(true)
    }

    pub fn is_full(&self) -> bool {
        self.inner
            .as_ref()
            .map(|x| x.len() > 0 && x.capacity() == x.len())
            .unwrap_or(false)
    }

    pub fn len(&self) -> usize {
        self.inner
            .as_ref()
            .map(|x| x.len())
            .unwrap_or(0)
    }

    pub fn capacity(&self) -> usize {
        self.inner
            .as_ref()
            .map(|x| x.capacity())
            .unwrap_or(0)
    }

    pub fn iter(&self) -> Option<impl Iterator<Item = &D>> {
        self.inner.as_ref().map(|v| v.iter())
    }

    pub fn clear(&mut self) {
        self.inner.as_mut().map(|x| x.clear());
    }

    fn insert_recycle_hook(&mut self, hook: BatchRecycleHook<D>) {
        self.recycle.push(hook);
    }

    fn recycle(&mut self) {
        if !self.recycle.is_empty() {
            let mut batch = std::mem::replace(self, Batch::new());
            while let Some(hook) = batch.recycle.pop() {
                if let Some(b) = hook.recycle(batch) {
                    batch = b;
                } else {
                    return;
                }
            }
            batch.inner.take();
        }
    }
}

impl<D: Clone> Clone for Batch<D> {
    fn clone(&self) -> Self {
        Batch { inner: self.inner.clone(), recycle: vec![] }
    }

    fn clone_from(&mut self, source: &Self) {
        if let Some(buf) = source.inner.as_ref() {
            if let Some(ref mut b) = self.inner {
                b.clone_from(buf);
            } else {
                self.inner = Some(buf.clone());
            }
        }
    }
}

impl<D> Drop for Batch<D> {
    fn drop(&mut self) {
        if self.capacity() > 0 {
            self.recycle();
        }
    }
}

impl<D> Iterator for Batch<D> {
    type Item = D;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut buf) = self.inner {
            buf.pop_front()
        } else {
            None
        }
    }
}

pub trait BatchFactory<D> {
    fn create(&mut self, batch_size: usize) -> Option<Batch<D>>;

    fn try_reuse(&mut self) -> Option<Batch<D>>;

    fn release(&mut self, batch: Batch<D>);
}

pub struct BatchPool<D, F: BatchFactory<D>> {
    pub batch_size: usize,
    pub capacity: usize,
    alloc: usize,
    recycle: Arc<BoundLinkQueue<Batch<D>>>,
    dropped: Arc<AtomicBool>,
    factory: F,
}

impl<D, F: BatchFactory<D>> BatchPool<D, F> {
    pub fn new(batch_size: usize, capacity: usize, factory: F) -> Self {
        BatchPool {
            batch_size,
            capacity,
            alloc: 0,
            recycle: Arc::new(BoundLinkQueue::new(capacity)),
            dropped: Arc::new(AtomicBool::new(false)),
            factory,
        }
    }

    pub fn fetch(&mut self) -> Option<Batch<D>> {
        if let Ok(mut batch) = self.recycle.pop() {
            batch.insert_recycle_hook(self.get_hook());
            return Some(batch);
        } else if self.alloc < self.capacity {
            if let Some(mut batch) = self.factory.create(self.batch_size) {
                self.alloc += 1;
                batch.insert_recycle_hook(self.get_hook());
                return Some(batch);
            }
        } else {
            return self.factory.try_reuse();
        }
        None
    }

    pub fn in_use_size(&self) -> usize {
        if self.alloc == 0 {
            0
        } else {
            assert!(self.alloc >= self.recycle.len());
            self.alloc - self.recycle.len()
        }
    }

    pub fn release(&mut self) {
        if !self.recycle.is_empty() {
            while let Ok(batch) = self.recycle.pop() {
                self.factory.release(batch);
                self.alloc = self.alloc.wrapping_sub(1);
            }
        }
    }

    pub fn has_available(&self) -> bool {
        self.alloc < self.capacity || !self.recycle.is_empty()
    }

    #[inline]
    pub fn is_idle(&self) -> bool {
        self.alloc == 0 || self.alloc == self.recycle.len()
    }

    fn get_hook(&self) -> BatchRecycleHook<D> {
        BatchRecycleHook {
            batch_size: self.batch_size,
            proxy: self.recycle.clone(),
            dropped: self.dropped.clone(),
        }
    }
}

impl<D, F: BatchFactory<D>> Drop for BatchPool<D, F> {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
        self.release();
    }
}

struct BatchRecycleHook<D> {
    batch_size: usize,
    proxy: Arc<BoundLinkQueue<Batch<D>>>,
    dropped: Arc<AtomicBool>,
}

impl<D> BatchRecycleHook<D> {
    pub fn recycle(&self, mut buf: Batch<D>) -> Option<Batch<D>> {
        if buf.capacity() > 0 {
            assert!(buf.capacity() >= self.batch_size);
            if !self.dropped.load(Ordering::SeqCst) {
                //debug!("try to recycle batch;");
                buf.clear();
                return if let Err(e) = self.proxy.push(buf) { Some(e.0) } else { None };
            }
        }
        Some(buf)
    }
}

pub struct MemoryAlloc<D> {
    alloc: usize,
    _ph: std::marker::PhantomData<D>,
}

impl<D> MemoryAlloc<D> {
    pub fn new() -> Self {
        MemoryAlloc { alloc: 0, _ph: std::marker::PhantomData }
    }
}

impl<D> BatchFactory<D> for MemoryAlloc<D> {
    fn create(&mut self, batch_size: usize) -> Option<Batch<D>> {
        self.alloc += 1;
        //debug!("alloc new batch, already allocated {}", self.alloc);
        Some(Batch::with_capacity(batch_size))
    }

    #[inline]
    fn try_reuse(&mut self) -> Option<Batch<D>> {
        None
    }

    fn release(&mut self, _: Batch<D>) {
        self.alloc -= 1;
    }
}

// impl<D> Drop for MemoryAlloc<D> {
//     fn drop(&mut self) {
//         if self.alloc > 0 {
//             debug!("has {} batches not release;", self.alloc);
//         }
//     }
// }

impl<D: Send, F: BatchFactory<D>> BatchFactory<D> for BatchPool<D, F> {
    fn create(&mut self, batch_size: usize) -> Option<Batch<D>> {
        assert_eq!(batch_size, self.batch_size);
        self.fetch()
    }

    fn try_reuse(&mut self) -> Option<Batch<D>> {
        if let Ok(mut batch) = self.recycle.pop() {
            batch.insert_recycle_hook(self.get_hook());
            return Some(batch);
        } else {
            None
        }
    }

    fn release(&mut self, _: Batch<D>) {
        // wait batch auto recycle;
    }
}

impl<D: Send, F: BatchFactory<D>> BatchFactory<D> for RcPointer<RefCell<F>> {
    fn create(&mut self, batch_size: usize) -> Option<Batch<D>> {
        self.borrow_mut().create(batch_size)
    }

    fn try_reuse(&mut self) -> Option<Batch<D>> {
        self.borrow_mut().try_reuse()
    }

    fn release(&mut self, batch: Batch<D>) {
        self.borrow_mut().release(batch)
    }
}

pub type MemBatchPool<D> = BatchPool<D, MemoryAlloc<D>>;
