//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use crate::communication::decorator::{ScopeStreamBuffer, ScopeStreamPush};
use crate::communication::IOResult;
use crate::data::{DataSetPool, MicroBatch};
use crate::errors::IOError;
use crate::graph::Port;
use crate::progress::EndSignal;
use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};
use pegasus_common::buffer::{Batch, BatchPool, MemBatchPool, MemBufAlloc};
use pegasus_common::rc::RcPointer;
use pegasus_common::utils::ExecuteTimeMetric;
use std::cell::RefCell;

type MemBatchPoolRef<D> = RcPointer<RefCell<MemBatchPool<D>>>;

struct ScopeBatchPool<D: Data> {
    src: u32,
    scope_capacity: usize,
    top_pool: MemBatchPoolRef<D>,
    sec_pool: TidyTagMap<DataSetPool<D, MemBatchPoolRef<D>>>,
    exe_metric: Option<ExecuteTimeMetric>,
}

impl<D: Data> ScopeBatchPool<D> {
    fn new(
        src: u32, batch_size: usize, batch_capacity: usize, scope_capacity: usize, scope_level: usize,
    ) -> Self {
        let global_batch_capacity = scope_capacity * batch_capacity;
        let pool = BatchPool::new(batch_size, global_batch_capacity, MemBufAlloc::new());
        let top_pool = RcPointer::new(RefCell::new(pool));
        let enable = std::env::var("BATCH_POOL_METRIC")
            .map(|v| v.parse::<bool>().unwrap_or(false))
            .unwrap_or(false);
        let exe_metric = if enable { Some(ExecuteTimeMetric::new()) } else { None };

        ScopeBatchPool { src, scope_capacity, top_pool, sec_pool: TidyTagMap::new(scope_level), exe_metric }
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
        let capacity = self.scope_capacity;
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
    pub scope_level: usize,
    pub batch_capacity: usize,
    batch_size: usize,
    scope_capacity: usize,
    push: P,
    pool: ScopeBatchPool<D>,
    single_pool: MemBatchPool<D>,
}

impl<D: Data, P: ScopeStreamPush<MicroBatch<D>>> BufferedPush<D, P> {
    pub fn new(
        scope_level: usize, batch_size: usize, scope_capacity: usize, batch_capacity: usize, push: P,
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

    fn push_iter<I: Iterator<Item = D>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
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
