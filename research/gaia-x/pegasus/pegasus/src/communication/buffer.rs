use pegasus_common::buffer::{Buffer, BufferPool, MemBufAlloc, BufferFactory, BufferReader};
use std::ptr::NonNull;
use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};
use std::ops::{Deref, DerefMut};

struct MemoryBufferPool<D> {
    pool: NonNull<BufferPool<D, MemBufAlloc<D>>>
}

impl<D> MemoryBufferPool<D> {
    fn new(pool: BufferPool<D, MemBufAlloc<D>>) -> Self {
        let ptr = Box::new(pool);
        let pool = unsafe {
            NonNull::new_unchecked(Box::into_raw(ptr))
        };
        MemoryBufferPool {
            pool
        }
    }
}

impl<D> Clone for MemoryBufferPool<D> {
    fn clone(&self) -> Self {
        MemoryBufferPool {
            pool: self.pool
        }
    }
}

impl<D> BufferFactory<D> for MemoryBufferPool<D> {
    fn create(&mut self, batch_size: usize) -> Option<Buffer<D>> {
        unsafe {
            self.pool.as_mut().create(batch_size)
        }
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
    end: bool,
    buf : Option<Buffer<D>>,
    pool: BufferPool<D, MemoryBufferPool<D>>
}

impl<D> BufSlot<D> {

    fn new(batch_size: usize, buf: Option<Buffer<D>>, pool: BufferPool<D, MemoryBufferPool<D>>) -> Self {
        BufSlot {
            batch_size,
            end: false,
            buf,
            pool
        }
    }

    pub(crate) fn push(&mut self, entry: D) -> Result<Option<BufferReader<D>>, WouldBlock<D>> {
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
        self.end && self.pool.is_idle()
    }
}


struct NonNullBufSlotPtr<D> {
    ptr: NonNull<BufSlot<D>>,
}

impl<D> NonNullBufSlotPtr<D> {
    fn new(slot: BufSlot<D>) -> Self {
        let ptr = Box::new(slot);
        let ptr = unsafe {
            NonNull::new_unchecked(Box::into_raw(ptr))
        };
        NonNullBufSlotPtr { ptr }
    }
}

impl<D> Deref for NonNullBufSlotPtr<D> {
    type Target = BufSlot<D>;

    fn deref(&self) -> &Self::Target {
        unsafe {
            self.ptr.as_ref()
        }
    }
}

impl<D> DerefMut for NonNullBufSlotPtr<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            self.ptr.as_mut()
        }
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
        batch_size: usize, batch_capacity: usize, scope_capacity: usize, scope_level: usize,
    ) -> Self {
        let global_batch_capacity = scope_capacity * batch_capacity;
        let pool = BufferPool::new(batch_size, global_batch_capacity, MemBufAlloc::new());
        let global_pool = MemoryBufferPool::new(pool);
        // let enable = std::env::var("BATCH_POOL_METRIC")
        //     .map(|v| v.parse::<bool>().unwrap_or(false))
        //     .unwrap_or(false);
        //let exe_metric = if enable { Some(ExecuteTimeMetric::new()) } else { None };
        ScopeBufferPool { batch_size, batch_capacity, scope_capacity, global_pool, buf_slots: TidyTagMap::new(scope_level), pinned: None }
    }

    pub fn pin(&mut self, tag: &Tag) {
        if let Some((p, buf)) = self.pinned.take() {
            if &p == tag {
                self.pinned = Some((p, buf));
                return;
            } else {
                self.buf_slots.insert(p, buf);
                if let Some(buf) = self.buf_slots.get(tag) {
                    self.pinned = Some((tag.clone(), buf.clone()));
                }
            }
        }
    }

    pub fn push(&mut self, tag: &Tag, item: D) -> Result<Option<BufferReader<D>>, WouldBlock<D>> {
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

    pub fn push_iter(&mut self, tag: &Tag, iter: &mut impl Iterator<Item = D>) -> Result<Option<BufferReader<D>>, WouldBlock<D>> {
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

    pub fn take_last_buf(&mut self, tag: &Tag) -> Option<Buffer<D>> {
        let b = self.buf_slots.get_mut(tag)?;
        b.end = true;
        b.buf.take()
    }

    pub fn buffers(&mut self) -> impl Iterator<Item = (Tag, Buffer<D>)> + '_ {
        self.buf_slots.iter_mut()
            .filter(|(_t, b)| b.buf.is_some())
            .map(|(tag, b)| {
                ((&*tag).clone(), b.buf.take().unwrap())
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
                self.buf_slots.insert(tag.clone(), buf_slot.clone());
                Some(buf_slot)
            } else {
                let mut find = None;
                for (t, b) in self.buf_slots.iter() {
                    if b.is_idle() {
                        find = Some((&*t).clone());
                        break;
                    }
                }


                if let Some(f) = find {
                    let slot = self.buf_slots.remove(&f).expect("find lost");
                    self.buf_slots.insert(tag.clone(), slot.clone());
                    Some(slot)
                } else {
                    None
                }
            }
        }
    }

}
