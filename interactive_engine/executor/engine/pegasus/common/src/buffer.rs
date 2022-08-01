use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::queue::BoundLinkQueue;

struct BufferRecycleHook<D> {
    proxy: Arc<BoundLinkQueue<Buffer<D>>>,
    dropped: Arc<AtomicBool>,
}

impl<D> BufferRecycleHook<D> {
    pub fn recycle(&self, mut buf: Buffer<D>) -> Option<Buffer<D>> {
        let cap = buf.capacity();
        if cap > 0 {
            //assert!(cap >= self.batch_size);
            if !self.dropped.load(Ordering::SeqCst) {
                buf.clear();
                return if let Err(e) = self.proxy.push(buf) {
                    Some(e.0)
                } else {
                    // trace!("try to recycle buf with capacity={}", cap);
                    None
                };
            }
        }
        Some(buf)
    }
}

pub struct Buffer<D> {
    inner: Vec<Option<D>>,
    head: usize,
    tail: usize,
    recycle_hooks: Vec<BufferRecycleHook<D>>,
}

impl<D> Buffer<D> {
    pub fn new() -> Self {
        Buffer { inner: vec![], head: 0, tail: 0, recycle_hooks: vec![] }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Buffer { inner: Vec::with_capacity(cap), head: 0, tail: 0, recycle_hooks: vec![] }
    }

    pub fn from(vec: Vec<Option<D>>) -> Self {
        Buffer { inner: vec, head: 0, tail: 0, recycle_hooks: vec![] }
    }

    pub fn push(&mut self, item: D) {
        if self.tail >= self.inner.len() {
            self.inner.push(Some(item));
            self.tail = self.inner.len();
        } else {
            let cursor = self.tail;
            self.tail += 1;
            self.inner[cursor] = Some(item);
        }
    }

    pub fn pop(&mut self) -> Option<D> {
        if self.head >= self.tail || self.head >= self.inner.len() {
            None
        } else {
            let cursor = self.head;
            self.head += 1;
            self.inner[cursor].take()
        }
    }

    pub fn get(&self, offset: usize) -> Option<&D> {
        let offset = self.head + offset;
        if offset >= self.tail || offset >= self.inner.len() {
            None
        } else {
            self.inner[offset].as_ref()
        }
    }

    pub fn len(&self) -> usize {
        self.tail.checked_sub(self.head).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[inline]
    pub fn clear(&mut self) {
        self.head = 0;
        self.tail = 0;
    }

    fn insert_recycle_hook(&mut self, hook: BufferRecycleHook<D>) {
        self.recycle_hooks.push(hook);
    }

    fn recycle(&mut self) {
        if !self.recycle_hooks.is_empty() {
            // trace!("try to recycle buf with {} hooks;", self.recycle_hooks.len());
            let mut batch = std::mem::replace(self, Buffer::new());
            while let Some(hook) = batch.recycle_hooks.pop() {
                if let Some(b) = hook.recycle(batch) {
                    batch = b;
                } else {
                    return;
                }
            }
            batch.inner = vec![];
        } else {
            // trace!("no recycle hook found;")
        }
    }
}

impl<D: Clone> Clone for Buffer<D> {
    fn clone(&self) -> Self {
        Buffer { inner: self.inner.clone(), head: self.head, tail: self.tail, recycle_hooks: vec![] }
    }
}

impl<D> Drop for Buffer<D> {
    fn drop(&mut self) {
        if self.inner.capacity() > 0 {
            self.recycle();
        }
    }
}

impl<D> Iterator for Buffer<D> {
    type Item = D;

    fn next(&mut self) -> Option<Self::Item> {
        self.pop()
    }
}

pub struct SharedReadBuffer<D> {
    inner: Arc<Buffer<D>>,
    cursor: usize,
    length: usize,
}

impl<D: Clone> SharedReadBuffer<D> {
    pub fn pop(&mut self) -> Option<D> {
        let offset = self.cursor;
        self.cursor += 1;
        self.inner.get(offset).map(|v| v.clone())
    }
}

impl<D> Clone for SharedReadBuffer<D> {
    fn clone(&self) -> Self {
        SharedReadBuffer { inner: self.inner.clone(), cursor: self.cursor, length: self.length }
    }
}

impl<D> SharedReadBuffer<D> {
    pub fn new(buf: Buffer<D>) -> Self {
        let length = buf.len();
        SharedReadBuffer { inner: Arc::new(buf), cursor: 0, length }
    }

    pub fn get(&self, offset: usize) -> Option<&D> {
        let offset = self.cursor + offset;
        self.inner.get(offset)
    }

    pub fn len(&self) -> usize {
        self.length
            .checked_sub(self.cursor)
            .unwrap_or(0)
    }
}

#[derive(Clone)]
pub enum ReadBuffer<D> {
    Exclusive(Buffer<D>),
    Shared(SharedReadBuffer<D>),
}

impl<D: Clone> ReadBuffer<D> {
    pub fn pop(&mut self) -> Option<D> {
        match self {
            ReadBuffer::Exclusive(b) => b.pop(),
            ReadBuffer::Shared(b) => b.pop(),
        }
    }
}

impl<D> ReadBuffer<D> {
    pub fn new() -> Self {
        let buf = Buffer::new();
        ReadBuffer::Exclusive(buf)
    }

    pub fn get(&self, offset: usize) -> Option<&D> {
        match self {
            ReadBuffer::Exclusive(b) => b.get(offset),
            ReadBuffer::Shared(b) => b.get(offset),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ReadBuffer::Exclusive(b) => b.len(),
            ReadBuffer::Shared(b) => b.len(),
        }
    }

    pub fn iter(&self) -> BufferIter<D> {
        BufferIter { inner: self, cursor: 0 }
    }

    pub fn make_share(&mut self) -> ReadBuffer<D> {
        let shared = match self {
            ReadBuffer::Exclusive(b) => {
                let buf = std::mem::replace(b, Buffer::new());
                SharedReadBuffer::new(buf)
            }
            ReadBuffer::Shared(b) => b.clone(),
        };

        match self {
            ReadBuffer::Exclusive(_) => {
                let clone = shared.clone();
                *self = ReadBuffer::Shared(shared);
                ReadBuffer::Shared(clone)
            }
            ReadBuffer::Shared(_) => ReadBuffer::Shared(shared),
        }
    }
}

impl<D> Buffer<D> {
    pub fn into_read_only(self) -> ReadBuffer<D> {
        ReadBuffer::Exclusive(self)
    }
}

pub struct BufferIter<'a, D> {
    inner: &'a ReadBuffer<D>,
    cursor: usize,
}

impl<'a, D> Iterator for BufferIter<'a, D> {
    type Item = &'a D;

    fn next(&mut self) -> Option<Self::Item> {
        let cursor = self.cursor;
        self.cursor += 1;
        self.inner.get(cursor)
    }
}

impl<D: Clone> Iterator for ReadBuffer<D> {
    type Item = D;

    fn next(&mut self) -> Option<Self::Item> {
        self.pop()
    }
}

pub trait BufferFactory<D> {
    fn create(&mut self, batch_size: usize) -> Option<Buffer<D>>;

    fn try_reuse(&mut self) -> Option<Buffer<D>>;

    fn release(&mut self, batch: Buffer<D>);
}

pub struct MemBufAlloc<D> {
    alloc: usize,
    _ph: std::marker::PhantomData<D>,
}

impl<D> MemBufAlloc<D> {
    pub fn new() -> Self {
        MemBufAlloc { alloc: 0, _ph: std::marker::PhantomData }
    }
}

impl<D> BufferFactory<D> for MemBufAlloc<D> {
    fn create(&mut self, batch_size: usize) -> Option<Buffer<D>> {
        self.alloc += 1;
        //debug!("alloc new batch, already allocated {}", self.alloc);
        Some(Buffer::with_capacity(batch_size))
    }

    #[inline]
    fn try_reuse(&mut self) -> Option<Buffer<D>> {
        None
    }

    fn release(&mut self, mut b: Buffer<D>) {
        b.inner = vec![];
        self.alloc -= 1;
    }
}

pub struct BufferPool<D, F: BufferFactory<D>> {
    pub batch_size: usize,
    pub capacity: usize,
    alloc: usize,
    recycle: Arc<BoundLinkQueue<Buffer<D>>>,
    dropped: Arc<AtomicBool>,
    factory: F,
}

impl<D, F: BufferFactory<D>> BufferPool<D, F> {
    pub fn new(batch_size: usize, capacity: usize, factory: F) -> Self {
        BufferPool {
            batch_size,
            capacity,
            alloc: 0,
            recycle: Arc::new(BoundLinkQueue::new(capacity)),
            dropped: Arc::new(AtomicBool::new(false)),
            factory,
        }
    }

    pub fn fetch(&mut self) -> Option<Buffer<D>> {
        if let Ok(mut buf) = self.recycle.pop() {
            // self reuse;
            buf.clear();
            buf.insert_recycle_hook(self.get_hook());
            trace!("reuse idle buf;");
            return Some(buf);
        } else if self.alloc < self.capacity {
            // create new and use;
            if let Some(mut buf) = self.factory.create(self.batch_size) {
                self.alloc += 1;
                buf.insert_recycle_hook(self.get_hook());
                return Some(buf);
            }
        }
        // try steal from factory;
        self.factory.try_reuse()
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
        self.alloc == 0 || self.alloc <= self.recycle.len()
    }

    fn get_hook(&self) -> BufferRecycleHook<D> {
        BufferRecycleHook { proxy: self.recycle.clone(), dropped: self.dropped.clone() }
    }
}

impl<D, F: BufferFactory<D>> Drop for BufferPool<D, F> {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
        self.release();
    }
}

impl<D, F: BufferFactory<D>> BufferFactory<D> for BufferPool<D, F> {
    fn create(&mut self, batch_size: usize) -> Option<Buffer<D>> {
        assert_eq!(batch_size, self.batch_size);
        if let Some(inner) = self.fetch() {
            Some(inner)
        } else {
            None
        }
    }

    fn try_reuse(&mut self) -> Option<Buffer<D>> {
        if let Ok(mut batch) = self.recycle.pop() {
            batch.insert_recycle_hook(self.get_hook());
            return Some(batch);
        } else {
            None
        }
    }

    fn release(&mut self, _: Buffer<D>) {
        // wait batch auto recycle;
    }
}
