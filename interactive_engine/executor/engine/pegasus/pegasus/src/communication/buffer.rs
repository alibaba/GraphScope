use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

use pegasus_common::buffer::{Buffer, BufferPool, MemBufAlloc, ReadBuffer};

use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};

pub struct WouldBlock<D>(pub Option<D>);

pub(crate) struct BufSlot<D> {
    batch_size: usize,
    discard: bool,
    exhaust: bool,
    tag: Tag,
    buf: Option<Buffer<D>>,
    pool: BufferPool<D, MemBufAlloc<D>>,
}

impl<D> BufSlot<D> {
    fn new(
        batch_size: usize, tag: Tag, buf: Option<Buffer<D>>, pool: BufferPool<D, MemBufAlloc<D>>,
    ) -> Self {
        BufSlot { batch_size, tag, discard: false, exhaust: false, buf, pool }
    }

    pub(crate) fn push(&mut self, entry: D) -> Result<Option<ReadBuffer<D>>, WouldBlock<D>> {
        if self.exhaust {
            error_worker!("push entry of {:?} after set exhaust;", self.tag)
        }
        assert!(!self.exhaust, "still push after set exhaust");
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

    pub(crate) fn push_last(&mut self, entry: D) -> Result<Option<ReadBuffer<D>>, WouldBlock<D>> {
        if self.exhaust {
            error_worker!("push entry of {:?} after set exhaust;", self.tag)
        }
        assert!(!self.exhaust, "still push after set exhaust");
        self.exhaust = true;
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
            Ok(Some(buf.into_read_only()))
        } else {
            if let Some(mut b) = self.pool.fetch() {
                b.push(entry);
                Ok(Some(b.into_read_only()))
            } else {
                Err(WouldBlock(Some(entry)))
            }
        }
    }

    fn reuse(&mut self) {
        self.exhaust = false;
        self.discard = false;
    }

    fn is_idle(&self) -> bool {
        (self.exhaust || self.discard) && self.pool.is_idle()
    }
}

struct BufSlotPtr<D: Data> {
    ptr: NonNull<BufSlot<D>>,
}

impl<D: Data> BufSlotPtr<D> {
    fn new(slot: BufSlot<D>) -> Self {
        let ptr = Box::new(slot);
        let ptr = unsafe { NonNull::new_unchecked(Box::into_raw(ptr)) };
        BufSlotPtr { ptr }
    }

    fn destroy(&mut self) {
        unsafe {
            let ptr = self.ptr;
            Box::from_raw(ptr.as_ptr());
        }
    }
}

impl<D: Data> Clone for BufSlotPtr<D> {
    fn clone(&self) -> Self {
        BufSlotPtr { ptr: self.ptr }
    }
}

impl<D: Data> Deref for BufSlotPtr<D> {
    type Target = BufSlot<D>;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

impl<D: Data> DerefMut for BufSlotPtr<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.ptr.as_mut() }
    }
}

pub(crate) struct ScopeBufferPool<D: Data> {
    batch_size: usize,
    batch_capacity: usize,
    /// buffer slots for each scope;
    buf_slots: TidyTagMap<BufSlotPtr<D>>,
    pinned: Option<(Tag, BufSlotPtr<D>)>,
}

unsafe impl<D: Data> Send for ScopeBufferPool<D> {}

impl<D: Data> ScopeBufferPool<D> {
    pub(crate) fn new(batch_size: usize, batch_capacity: usize, scope_level: u32) -> Self {
        ScopeBufferPool {
            batch_size,
            batch_capacity,
            buf_slots: TidyTagMap::new(scope_level),
            pinned: None,
        }
    }

    #[allow(dead_code)]
    pub fn unpin(&mut self) {
        self.pinned.take();
    }

    pub fn pin(&mut self, tag: &Tag) {
        if let Some((pin, _)) = self.pinned.as_mut() {
            if pin == tag {
                return;
            }
        }

        let ptr = self.fetch_slot_ptr(tag);
        self.pinned = Some((tag.clone(), ptr));
    }

    pub fn push(&mut self, tag: &Tag, item: D) -> Result<Option<ReadBuffer<D>>, WouldBlock<D>> {
        if let Some((p, buf)) = self.pinned.as_mut() {
            if p == tag {
                return buf.push(item);
            }
        }

        self.fetch_slot_ptr(tag).push(item)
    }

    pub fn push_last(&mut self, tag: &Tag, item: D) -> Result<Option<ReadBuffer<D>>, WouldBlock<D>> {
        if let Some((p, buf)) = self.pinned.as_mut() {
            if p == tag {
                return buf.push_last(item);
            }
        }

        self.fetch_slot_ptr(tag).push_last(item)
    }

    pub fn push_iter(
        &mut self, tag: &Tag, iter: &mut impl Iterator<Item = D>,
    ) -> Result<Option<ReadBuffer<D>>, WouldBlock<D>> {
        let mut slot = if let Some((pin, slot)) = self.pinned.as_mut() {
            if pin == tag {
                slot.clone()
            } else {
                self.fetch_slot_ptr(tag)
            }
        } else {
            self.fetch_slot_ptr(tag)
        };

        while let Some(next) = iter.next() {
            if let Some(batch) = slot.push(next)? {
                return Ok(Some(batch));
            }
        }
        Ok(None)
    }

    #[inline]
    pub fn take_last_buf(&mut self, tag: &Tag) -> Option<Buffer<D>> {
        let b = self.buf_slots.get_mut(tag)?;
        // if !b.discard {
        //     trace_worker!("discard buffer of {:?} because of last;", tag);
        // }
        b.exhaust = true;
        b.discard = false;
        b.buf.take()
    }

    #[inline]
    pub fn discard_buf(&mut self, tag: &Tag) -> Option<Buffer<D>> {
        let b = self.buf_slots.get_mut(tag)?;
        // if !b.discard {
        //     trace_worker!("discard buffer of {:?} because of last;", tag);
        // }
        b.exhaust = false;
        b.discard = true;
        b.buf.take()
    }

    pub fn skip_buf(&mut self, tag: &Tag) {
        let level = tag.len() as u32;
        if level == self.buf_slots.scope_level {
            self.discard_buf(tag);
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
                    b.exhaust = is_last;
                    b.buf.take().map(|b| ((&*t).clone(), b))
                } else {
                    None
                }
            })
    }

    fn fetch_slot_ptr(&mut self, tag: &Tag) -> BufSlotPtr<D> {
        if let Some(slot) = self.buf_slots.get(tag) {
            slot.clone()
        } else {
            if self.buf_slots.len() == 0 {
                self.create_new_buffer_slot(tag)
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
                    let mut slot = self.buf_slots.remove(&f).expect("find lost");
                    // trace_worker!("reuse idle buffer slot for scope {:?};", tag);
                    slot.reuse();
                    slot.tag = tag.clone();
                    assert!(slot.buf.is_none());
                    let ptr = slot.clone();
                    self.buf_slots.insert(tag.clone(), slot);
                    ptr
                } else {
                    self.create_new_buffer_slot(tag)
                }
            }
        }
    }

    fn create_new_buffer_slot(&mut self, tag: &Tag) -> BufSlotPtr<D> {
        let pool = BufferPool::new(self.batch_size, self.batch_capacity, MemBufAlloc::new());
        let slot = BufSlotPtr::new(BufSlot::new(self.batch_size, tag.clone(), None, pool));
        let ptr = slot.clone();
        // trace_worker!("create new buffer slot for scope {:?};", tag);
        self.buf_slots.insert(tag.clone(), slot);
        ptr
    }
}

impl<D: Data> Default for ScopeBufferPool<D> {
    fn default() -> Self {
        ScopeBufferPool { batch_size: 1, batch_capacity: 1, buf_slots: Default::default(), pinned: None }
    }
}

impl<D: Data> Drop for ScopeBufferPool<D> {
    fn drop(&mut self) {
        self.pinned.take();
        for (_, x) in self.buf_slots.iter_mut() {
            x.destroy();
        }
    }
}
