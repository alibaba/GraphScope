use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

use pegasus_common::buffer::{Buffer, BufferFactory, BufferPool, MemBufAlloc, ReadBuffer};

use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};

struct MemoryBufferPool<D> {
    pool: NonNull<BufferPool<D, MemBufAlloc<D>>>,
    need_drop: bool,
}

impl<D> MemoryBufferPool<D> {
    fn new(pool: BufferPool<D, MemBufAlloc<D>>) -> Self {
        let ptr = Box::new(pool);
        let pool = unsafe { NonNull::new_unchecked(Box::into_raw(ptr)) };
        MemoryBufferPool { pool, need_drop: true }
    }

    fn destroy(&mut self) {
        if self.need_drop {
            unsafe {
                debug!("drop memory buffer pool");
                let ptr = self.pool;
                Box::from_raw(ptr.as_ptr());
            }
        }
    }
}

impl<D> Clone for MemoryBufferPool<D> {
    fn clone(&self) -> Self {
        MemoryBufferPool { pool: self.pool, need_drop: self.need_drop }
    }
}

impl<D> BufferFactory<D> for MemoryBufferPool<D> {
    fn create(&mut self, batch_size: usize) -> Option<Buffer<D>> {
        unsafe { self.pool.as_mut().create(batch_size) }
    }

    fn try_reuse(&mut self) -> Option<Buffer<D>> {
        unsafe { self.pool.as_mut().try_reuse() }
    }

    fn release(&mut self, _buf: Buffer<D>) {
        //
    }
}

pub struct WouldBlock<D>(pub Option<D>);

pub(crate) struct BufSlot<D> {
    batch_size: usize,
    discard: bool,
    buf: Option<Buffer<D>>,
    pool: BufferPool<D, MemoryBufferPool<D>>,
}

impl<D> BufSlot<D> {
    fn new(batch_size: usize, buf: Option<Buffer<D>>, pool: BufferPool<D, MemoryBufferPool<D>>) -> Self {
        BufSlot { batch_size, discard: false, buf, pool }
    }

    pub(crate) fn push(&mut self, entry: D) -> Result<Option<ReadBuffer<D>>, WouldBlock<D>> {
        if self.discard {
            // trace_worker!("discard data");
            return Ok(None);
        }
        if self.batch_size == 1 {
            return if let Some(mut b) = self.pool.fetch() {
                b.push(entry);
                Ok(Some(b.into_read_only()))
            } else {
                Err(WouldBlock(Some(entry)))
            };
        }

        if let Some(mut buf) = self.buf.take() {
            buf.push(entry);
            if buf.len() == self.batch_size {
                return Ok(Some(buf.into_read_only()));
            }
            self.buf = Some(buf);
            Ok(None)
        } else {
            if let Some(mut b) = self.pool.fetch() {
                b.push(entry);
                self.buf = Some(b);
                Ok(None)
            } else {
                Err(WouldBlock(Some(entry)))
            }
        }
    }

    fn is_idle(&self) -> bool {
        self.discard && self.pool.is_idle()
    }
}

struct NonNullBufSlotPtr<D> {
    ptr: NonNull<BufSlot<D>>,
}

impl<D> NonNullBufSlotPtr<D> {
    fn new(slot: BufSlot<D>) -> Self {
        let ptr = Box::new(slot);
        let ptr = unsafe { NonNull::new_unchecked(Box::into_raw(ptr)) };
        NonNullBufSlotPtr { ptr }
    }

    fn destroy(&mut self) {
        unsafe {
            let ptr = self.ptr;
            Box::from_raw(ptr.as_ptr());
        }
    }
}

impl<D> Deref for NonNullBufSlotPtr<D> {
    type Target = BufSlot<D>;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

impl<D> DerefMut for NonNullBufSlotPtr<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.ptr.as_mut() }
    }
}

impl<D> Clone for NonNullBufSlotPtr<D> {
    fn clone(&self) -> Self {
        NonNullBufSlotPtr { ptr: self.ptr }
    }
}

pub(crate) struct ScopeBufferPool<D: Data> {
    batch_size: usize,
    batch_capacity: usize,
    scope_capacity: usize,
    global_pool: MemoryBufferPool<D>,
    buf_slots: TidyTagMap<NonNullBufSlotPtr<D>>,
    pinned: Option<(Tag, NonNullBufSlotPtr<D>)>,
}

unsafe impl<D: Data> Send for ScopeBufferPool<D> {}

#[allow(dead_code)]
impl<D: Data> ScopeBufferPool<D> {
    pub(crate) fn new(
        batch_size: usize, batch_capacity: usize, scope_capacity: usize, scope_level: u32,
    ) -> Self {
        let global_batch_capacity = scope_capacity * batch_capacity;
        let pool = BufferPool::new(batch_size, global_batch_capacity, MemBufAlloc::new());
        let global_pool = MemoryBufferPool::new(pool);
        // let enable = std::env::var("BATCH_POOL_METRIC")
        //     .map(|v| v.parse::<bool>().unwrap_or(false))
        //     .unwrap_or(false);
        //let exe_metric = if enable { Some(ExecuteTimeMetric::new()) } else { None };
        ScopeBufferPool {
            batch_size,
            batch_capacity,
            scope_capacity,
            global_pool,
            buf_slots: TidyTagMap::new(scope_level),
            pinned: None,
        }
    }

    pub fn unpin(&mut self) {
        self.pinned.take();
    }

    pub fn pin(&mut self, tag: &Tag) -> bool {
        if let Some((p, buf)) = self.pinned.take() {
            if &p == tag {
                self.pinned = Some((p, buf));
                true
            } else {
                // self.buf_slots.insert(p, buf);
                if let Some(buf) = self.buf_slots.get(tag) {
                    trace_worker!("update pinned buffers to scope {:?};", tag);
                    self.pinned = Some((tag.clone(), buf.clone()));
                    true
                } else {
                    // trace_worker!("can't pin buffer for scope {:?} as slot not created;", tag);
                    false
                }
            }
        } else {
            if let Some(buf) = self.buf_slots.get(tag) {
                trace_worker!("pinned buffers to scope {:?};", tag);
                self.pinned = Some((tag.clone(), buf.clone()));
                true
            } else {
                // trace_worker!("can't pin buffer for scope {:?} as slot not created;", tag);
                false
            }
        }
    }

    pub fn push(&mut self, tag: &Tag, item: D) -> Result<Option<ReadBuffer<D>>, WouldBlock<D>> {
        if let Some((p, buf)) = self.pinned.as_mut() {
            if p == tag {
                return buf.push(item);
            }
        }

        if let Some(mut slot) = self.get_slot_mut(tag) {
            slot.push(item)
        } else {
            Err(WouldBlock(Some(item)))
        }
    }

    pub fn push_iter(
        &mut self, tag: &Tag, iter: &mut impl Iterator<Item = D>,
    ) -> Result<Option<ReadBuffer<D>>, WouldBlock<D>> {
        if let Some((p, buf)) = self.pinned.as_mut() {
            if p == tag {
                while let Some(next) = iter.next() {
                    if let Some(batch) = buf.push(next)? {
                        return Ok(Some(batch));
                    }
                }
                return Ok(None);
            }
        }

        if let Some(mut buf) = self.get_slot_mut(tag) {
            while let Some(next) = iter.next() {
                if let Some(batch) = buf.push(next)? {
                    return Ok(Some(batch));
                }
            }
            Ok(None)
        } else {
            Err(WouldBlock(None))
        }
    }

    pub fn take_buf(&mut self, tag: &Tag, is_last: bool) -> Option<Buffer<D>> {
        let b = self.buf_slots.get_mut(tag)?;
        if !b.discard {
            trace_worker!("discard buffer of {:?} because of last;", tag);
        }
        b.discard = is_last;
        b.buf.take()
    }

    pub fn skip_buf(&mut self, tag: &Tag) {
        let level = tag.len() as u32;
        if level == self.buf_slots.scope_level {
            self.take_buf(tag, true);
        } else if level < self.buf_slots.scope_level {
            for (k, v) in self.buf_slots.iter_mut() {
                if tag.is_parent_of(&*k) {
                    trace_worker!("discard buffer of {:?};", k);
                    v.discard = true;
                    v.buf.take();
                }
            }
        } else {
            // ignore;
        }
    }

    pub fn buffers(&mut self) -> impl Iterator<Item = (Tag, Buffer<D>)> + '_ {
        self.buf_slots
            .iter_mut()
            .filter(|(_t, b)| b.buf.is_some())
            .map(|(tag, b)| ((&*tag).clone(), b.buf.take().unwrap()))
    }

    pub fn child_buffers_of(
        &mut self, parent: &Tag, is_last: bool,
    ) -> impl Iterator<Item = (Tag, Buffer<D>)> + '_ {
        let p = parent.clone();
        self.buf_slots
            .iter_mut()
            .filter_map(move |(t, b)| {
                if p.is_parent_of(&*t) {
                    b.discard = is_last;
                    b.buf.take().map(|b| ((&*t).clone(), b))
                } else {
                    None
                }
            })
    }

    fn get_slot_mut(&mut self, tag: &Tag) -> Option<NonNullBufSlotPtr<D>> {
        if let Some(slot) = self.buf_slots.get(tag) {
            Some(slot.clone())
        } else {
            if self.buf_slots.len() < self.scope_capacity {
                let pool = BufferPool::new(self.batch_size, self.batch_capacity, self.global_pool.clone());
                let buf = BufSlot::new(self.batch_size, None, pool);
                let buf_slot = NonNullBufSlotPtr::new(buf);
                trace_worker!("create new buffer slot for scope {:?};", tag);
                self.buf_slots
                    .insert(tag.clone(), buf_slot.clone());
                Some(buf_slot)
            } else {
                let mut find = None;
                for (t, b) in self.buf_slots.iter() {
                    if b.is_idle() {
                        find = Some((&*t).clone());
                        break;
                    } else {
                        //trace_worker!("slot of {:?} is in use: is_end = {}, in use ={}", t, b.end, b.pool.in_use_size());
                    }
                }

                if let Some(f) = find {
                    trace_worker!("reuse idle buffer slot for scope {:?};", tag);
                    let mut slot = self.buf_slots.remove(&f).expect("find lost");
                    slot.discard = false;
                    assert!(slot.buf.is_none());
                    self.buf_slots.insert(tag.clone(), slot.clone());
                    Some(slot)
                } else {
                    trace_worker!("no buffer slot available for scope {:?};", tag);
                    None
                }
            }
        }
    }
}

impl<D: Data> Default for ScopeBufferPool<D> {
    fn default() -> Self {
        let global_pool = MemoryBufferPool { pool: NonNull::dangling(), need_drop: false };
        ScopeBufferPool {
            batch_size: 1,
            batch_capacity: 1,
            scope_capacity: 1,
            global_pool,
            buf_slots: Default::default(),
            pinned: None,
        }
    }
}

impl<D: Data> Drop for ScopeBufferPool<D> {
    fn drop(&mut self) {
        self.pinned.take();
        for (_, x) in self.buf_slots.iter_mut() {
            x.destroy();
        }
        self.global_pool.destroy();
    }
}
