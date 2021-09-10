pub use rob::*;

#[cfg(not(feature = "rob"))]
mod rob {
    use std::cell::RefCell;

    use pegasus_common::buffer::{Batch, BatchPool, MemBatchPool, MemBufAlloc};
    use pegasus_common::rc::RcPointer;
    use pegasus_common::utils::ExecuteTimeMetric;

    use crate::communication::decorator::{ScopeStreamBuffer, ScopeStreamPush};
    use crate::communication::IOResult;
    use crate::data::{DataSetPool, MicroBatch};
    use crate::errors::IOError;
    use crate::graph::Port;
    use crate::progress::EndSignal;
    use crate::tag::tools::map::TidyTagMap;
    use crate::{Data, Tag};

    type MemBatchPoolRef<D> = RcPointer<RefCell<MemBatchPool<D>>>;

    struct ScopeBatchPool<D: Data> {
        src: u32,
        scope_capacity: usize,
        batch_capacity: usize,
        top_pool: MemBatchPoolRef<D>,
        sec_pool: TidyTagMap<DataSetPool<D, MemBatchPoolRef<D>>>,
        exe_metric: Option<ExecuteTimeMetric>,
    }

    impl<D: Data> ScopeBatchPool<D> {
        fn new(
            src: u32, batch_size: usize, batch_capacity: usize, scope_capacity: usize, scope_level: u32,
        ) -> Self {
            let global_batch_capacity = scope_capacity * batch_capacity;
            let pool = BatchPool::new(batch_size, global_batch_capacity, MemBufAlloc::new());
            let top_pool = RcPointer::new(RefCell::new(pool));
            let enable = std::env::var("BATCH_POOL_METRIC")
                .map(|v| v.parse::<bool>().unwrap_or(false))
                .unwrap_or(false);
            let exe_metric = if enable { Some(ExecuteTimeMetric::new()) } else { None };

            ScopeBatchPool {
                src,
                scope_capacity,
                batch_capacity,
                top_pool,
                sec_pool: TidyTagMap::new(scope_level),
                exe_metric,
            }
        }

        fn get_pool_mut(&mut self, tag: &Tag) -> Option<&mut DataSetPool<D, MemBatchPoolRef<D>>> {
            let _x = self.exe_metric.as_mut().map(|x| x.metric());
            if self.sec_pool.contains_key(tag) {
                return self.sec_pool.get_mut(tag);
            }

            if self.sec_pool.len() >= self.scope_capacity {
                self.sec_pool.retain(|_, v| !v.is_idle())
            }

            if self.sec_pool.len() >= self.scope_capacity {
                return None;
            }

            let p = self.top_pool.clone();
            let br = p.borrow();
            let batch_size = br.batch_size;
            let capacity = self.batch_capacity;
            std::mem::drop(br);
            let src = self.src;

            let pool = self.sec_pool.get_mut_or_else(tag, || {
                let pool = BatchPool::new(batch_size, capacity, p);
                DataSetPool::new(tag.clone(), src, pool)
            });

            Some(pool)
        }

        fn get_batch_mut(&mut self, tag: &Tag) -> Option<&mut MicroBatch<D>> {
            if let Some(pool) = self.get_pool_mut(tag) {
                pool.get_batch_mut()
            } else {
                None
            }
        }

        fn scope_size(&self) -> usize {
            self.sec_pool.len()
        }

        fn is_empty(&self) -> bool {
            self.sec_pool.is_empty()
        }

        fn clean(&mut self) {
            self.sec_pool.retain(|_, v| !v.is_idle());
        }

        #[inline]
        fn contains_scope(&self, tag: &Tag) -> bool {
            self.sec_pool.contains_key(tag)
        }
    }

    impl<D: Data> Drop for ScopeBatchPool<D> {
        fn drop(&mut self) {
            if let Some(metric) = self.exe_metric.take() {
                debug_worker!("get pool total cost {}us, avg {:.2}us", metric.get_total(), metric.get_avg())
            }
        }
    }

    pub struct BufferedPush<D: Data, P: ScopeStreamPush<MicroBatch<D>>> {
        pub src: u32,
        pub scope_level: u32,
        pub batch_capacity: usize,
        batch_size: usize,
        scope_capacity: usize,
        push: P,
        pool: ScopeBatchPool<D>,
        single_pool: MemBatchPool<D>,
    }

    impl<D: Data, P: ScopeStreamPush<MicroBatch<D>>> BufferedPush<D, P> {
        pub fn new(
            scope_level: u32, batch_size: usize, scope_capacity: usize, batch_capacity: usize, push: P,
        ) -> Self {
            assert!(batch_size > 0);
            let src = crate::worker_id::get_current_worker().index;
            let pool = ScopeBatchPool::new(src, batch_size, batch_capacity, scope_capacity, scope_level);
            BufferedPush {
                src: crate::worker_id::get_current_worker().index,
                scope_level,
                batch_capacity,
                scope_capacity,
                batch_size,
                push,
                pool,
                single_pool: BatchPool::new(1, scope_capacity, MemBufAlloc::new()),
            }
        }

        /// inner usage only for pipeline channel;
        pub fn forward_buffer(&mut self, data: MicroBatch<D>) -> IOResult<()> {
            self.push.push(&data.tag.clone(), data)
        }

        #[inline]
        pub fn has_buffer(&self, tag: &Tag) -> bool {
            self.pool.contains_scope(tag)
        }
    }

    impl<D: Data, P: ScopeStreamPush<MicroBatch<D>>> ScopeStreamBuffer for BufferedPush<D, P> {
        fn scope_size(&self) -> usize {
            self.pool.scope_size() + self.single_pool.in_use_size()
        }

        fn ensure_capacity(&mut self, tag: &Tag) -> IOResult<usize> {
            if let Some(pool) = self.pool.sec_pool.get_mut(tag) {
                if let Some(batch) = pool.get_batch_mut() {
                    assert!(batch.capacity() > 0);
                    let cap = batch.capacity() - batch.len();
                    assert!(cap > 0);
                    Ok(cap)
                } else {
                    would_block!("no buffer available")
                }
            } else {
                if self.pool.scope_size() >= self.scope_capacity {
                    self.pool.clean();
                }
                // debug_worker!("scope size {}", self.sec_pool.len());
                if self.pool.scope_size() < self.scope_capacity {
                    Ok(self.batch_size)
                } else {
                    debug_worker!(
                        "output[{:?}] interrupted as scope capacity bound to {};",
                        self.port(),
                        self.scope_capacity
                    );

                    interrupt!("scope size bounded;")
                }
            }
        }

        fn flush_scope(&mut self, tag: &Tag) -> IOResult<()> {
            if let Some(batch) = self.pool.get_batch_mut(tag) {
                if !batch.is_empty() {
                    let force = std::mem::replace(batch, MicroBatch::empty());
                    self.push.push(tag, force)?;
                }
            }
            Ok(())
        }
    }

    impl<D: Data, P: ScopeStreamPush<MicroBatch<D>>> ScopeStreamPush<D> for BufferedPush<D, P> {
        fn port(&self) -> Port {
            self.push.port()
        }

        // Push a message into buffer, flush the buffer if it is full;
        // The message will be pushed successfully anyway even if an would block error maybe returned, this error is
        // a signal to hint the caller to stop pushing more messages;
        fn push(&mut self, tag: &Tag, msg: D) -> IOResult<()> {
            if let Some(p) = self.pool.get_pool_mut(tag) {
                if let Some(batch) = p.get_batch_mut() {
                    batch.push(msg);
                    if batch.is_full() {
                        let full = std::mem::replace(batch, MicroBatch::empty());
                        self.push.push(tag, full)?;
                    }
                    Ok(())
                } else {
                    let batch = p.tmp(msg);
                    match self.push.push(tag, batch) {
                        Ok(_) => would_block!("no buffer available"),
                        Err(e) => Err(e),
                    }
                }
            } else {
                let mut batch = MicroBatch::new(tag.clone(), self.src, 0, Batch::with_capacity(1));
                batch.push(msg);
                match self.push.push(tag, batch) {
                    Ok(_) => {
                        if self.pool.scope_size() >= self.scope_capacity {
                            self.pool.clean();
                        }

                        if self.pool.scope_size() < self.scope_capacity {
                            // hint the caller don't to push more data;
                            would_block!("no buffer available")
                        } else {
                            interrupt!("scope up-bound")
                        }
                    }
                    Err(e) => Err(e),
                }
            }
        }

        fn push_last(&mut self, msg: D, end: EndSignal) -> IOResult<()> {
            if let Some(pool) = self.pool.sec_pool.get_mut(&end.tag) {
                if let Some(batch) = pool.get_batch_mut() {
                    batch.push(msg);
                    let last = std::mem::replace(batch, MicroBatch::empty());
                    self.push.push_last(last, end)
                } else {
                    let batch = pool.tmp(msg);
                    self.push.push_last(batch, end)
                }
            } else {
                if let Some(batch) = self.single_pool.fetch() {
                    let mut batch = MicroBatch::new(end.tag.clone(), self.src, 0, batch);
                    batch.push(msg);
                    self.push.push_last(batch, end)
                } else {
                    let mut tmp = MicroBatch::new(end.tag.clone(), self.src, 0, Batch::with_capacity(1));
                    tmp.push(msg);
                    match self.push.push_last(tmp, end) {
                        Ok(_) => interrupt!("scope up-bound"),
                        Err(e) => Err(e),
                    }
                }
            }
        }

        fn try_push_iter<I: Iterator<Item = D>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
            if let Some(pool) = self.pool.get_pool_mut(tag) {
                'a: loop {
                    if let Some(batch) = pool.get_batch_mut() {
                        while let Some(item) = iter.next() {
                            batch.push(item);
                            if batch.is_full() {
                                let full = std::mem::replace(batch, MicroBatch::empty());
                                self.push.push(tag, full)?;
                                continue 'a;
                            }
                        }
                        return Ok(());
                    } else {
                        return would_block!("no buffer available");
                    }
                }
            } else {
                if self.pool.scope_size() >= self.scope_capacity {
                    self.pool.clean();
                }

                if self.pool.scope_size() < self.scope_capacity {
                    // hint the caller don't to push more data;
                    would_block!("no buffer available")
                } else {
                    interrupt!("scope up-bound")
                }
            }
        }

        fn notify_end(&mut self, mut end: EndSignal) -> Result<(), IOError> {
            if let Some(pool) = self.pool.sec_pool.get_mut(&end.tag) {
                let batch = pool.take_current();
                if !batch.is_empty() {
                    self.push.push_last(batch, end)
                } else {
                    end.seq = pool.get_seq();
                    self.push.notify_end(end)
                }
            } else {
                self.push.notify_end(end)
            }
        }

        fn flush(&mut self) -> IOResult<()> {
            if !self.pool.is_empty() {
                self.pool.clean();
                for (tag, pool) in self.pool.sec_pool.iter_mut() {
                    let buf = pool.take_current();
                    if !buf.is_empty() {
                        self.push.push(tag.as_ref(), buf)?;
                    }
                }
            }
            self.push.flush()?;
            Ok(())
        }

        fn close(&mut self) -> Result<(), IOError> {
            self.pool.clean();
            self.push.flush()?;
            self.push.close()
        }
    }
}

#[cfg(feature = "rob")]
mod rob {
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
        end: bool,
        buf: Option<Buffer<D>>,
        pool: BufferPool<D, MemoryBufferPool<D>>,
    }

    impl<D> BufSlot<D> {
        fn new(
            batch_size: usize, buf: Option<Buffer<D>>, pool: BufferPool<D, MemoryBufferPool<D>>,
        ) -> Self {
            BufSlot { batch_size, end: false, buf, pool }
        }

        pub(crate) fn push(&mut self, entry: D) -> Result<Option<ReadBuffer<D>>, WouldBlock<D>> {
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
            b.end = is_last;
            b.buf.take()
        }

        pub fn skip_buf(&mut self, tag: &Tag) {
            let level = tag.len() as u32;
            if level == self.buf_slots.scope_level {
                self.take_buf(tag, true);
            } else if level < self.buf_slots.scope_level {
                for (k, v) in self.buf_slots.iter_mut() {
                    if tag.is_parent_of(&k) {
                        v.end = true;
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

        pub fn buffers_of(
            &mut self, parent: &Tag, is_last: bool,
        ) -> impl Iterator<Item = (Tag, Buffer<D>)> + '_ {
            let p = parent.clone();
            self.buf_slots
                .iter_mut()
                .filter_map(move |(t, b)| {
                    if p.is_parent_of(&*t) {
                        b.end = is_last;
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
                    let pool =
                        BufferPool::new(self.batch_size, self.batch_capacity, self.global_pool.clone());
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
                        let slot = self.buf_slots.remove(&f).expect("find lost");
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
}
