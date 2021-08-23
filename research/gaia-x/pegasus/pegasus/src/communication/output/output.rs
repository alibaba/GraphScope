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

pub use rob::{OutputHandle, OutputSession};

#[cfg(not(feature = "rob"))]
mod rob {
    use std::cell::RefMut;
    use std::collections::{HashSet, VecDeque};

    use crate::communication::decorator::ScopeStreamPush;
    use crate::communication::output::builder::OutputMeta;
    use crate::communication::output::tee::Tee;
    use crate::config::CANCEL_DESC;
    use crate::data::MicroBatch;
    use crate::errors::IOResult;
    use crate::graph::Port;
    use crate::progress::EndSignal;
    use crate::tag::tools::map::TidyTagMap;
    use crate::{Data, Tag};

    pub struct OutputHandle<D: Data> {
        pub port: Port,
        pub scope_level: u32,
        pub src: u32,
        tee: Tee<D>,
        in_block: TidyTagMap<Box<dyn Iterator<Item = D> + Send + 'static>>,
        blocks: VecDeque<Tag>,
        is_closed: bool,
        skips: HashSet<Tag>,
    }

    impl<D: Data> OutputHandle<D> {
        pub(crate) fn new(meta: OutputMeta, output: Tee<D>) -> Self {
            let scope_level = meta.scope_level;
            let port = meta.port;
            let src = crate::worker_id::get_current_worker().index;
            OutputHandle {
                port,
                scope_level,
                src,
                tee: output,
                in_block: TidyTagMap::new(scope_level),
                blocks: VecDeque::new(),
                is_closed: false,
                skips: HashSet::new(),
            }
        }

        pub fn push_batch(&mut self, dataset: MicroBatch<D>) -> IOResult<()> {
            if self.skips.is_empty() || !self.skips.contains(&dataset.tag) {
                self.tee.forward(dataset)
            } else {
                Ok(())
            }
        }

        pub fn push_batch_mut(&mut self, batch: &mut MicroBatch<D>) -> IOResult<()> {
            if self.skips.is_empty() || !self.skips.contains(&batch.tag) {
                let seq = batch.seq;
                let data = MicroBatch::new(batch.tag.clone(), self.src, seq, batch.take_batch());
                self.push_batch(data)
            } else {
                Ok(())
            }
        }

        pub fn push_iter<I: Iterator<Item = D> + Send + 'static>(
            &mut self, tag: &Tag, mut iter: I,
        ) -> IOResult<()> {
            match self.try_push_iter(tag, &mut iter) {
                Err(e) => {
                    let iter = Box::new(iter);
                    if e.is_would_block() || e.is_interrupted() {
                        self.in_block.insert(tag.clone(), iter);
                        self.blocks.push_back(tag.clone());
                    }
                    Err(e)
                }
                _ => Ok(()),
            }
        }

        pub(crate) fn try_unblock(&mut self, unblocked: &mut Vec<Tag>) -> IOResult<()> {
            let len = self.blocks.len();
            if len > 0 {
                for _ in 0..len {
                    if let Some(tag) = self.blocks.pop_front() {
                        if let Some(mut iter) = self.in_block.remove(&tag) {
                            match self.try_push_iter(&tag, &mut iter) {
                                Err(e) => {
                                    if e.is_would_block() || e.is_interrupted() {
                                        self.in_block.insert(tag.clone(), iter);
                                        self.blocks.push_back(tag);
                                    } else {
                                        return Err(e);
                                    }
                                }
                                _ => {
                                    trace_worker!("data in scope {:?} unblocked", tag);
                                    unblocked.push(tag);
                                }
                            }
                        } else {
                            unblocked.push(tag);
                        }
                    }
                }
            }
            Ok(())
        }

        pub fn get_blocks(&self) -> &VecDeque<Tag> {
            &self.blocks
        }

        #[inline]
        pub fn skip(&mut self, tag: &Tag) {
            self.skips.insert(tag.clone());
            let len = self.blocks.len();
            for _ in 0..len {
                if let Some(t) = self.blocks.pop_front() {
                    if *CANCEL_DESC {
                        if !t.eq(tag) && !tag.is_parent_of(&t) {
                            self.blocks.push_back(t);
                        }
                    } else {
                        if !t.eq(tag) {
                            self.blocks.push_back(t);
                        }
                    }
                }
            }
            if *CANCEL_DESC && self.scope_level as usize > tag.len() {
                self.in_block
                    .retain(|t, _| !tag.is_parent_of(t));
            } else {
                self.in_block.remove(tag);
            }
        }

        #[inline]
        fn is_skipped(&self, tag: &Tag) -> bool {
            if *CANCEL_DESC && self.scope_level as usize > tag.len() {
                for skip_tag in self.skips.iter() {
                    if skip_tag.is_parent_of(tag) {
                        return true;
                    }
                }
                false
            } else {
                self.skips.contains(tag)
            }
        }

        #[inline]
        pub fn is_closed(&self) -> bool {
            self.is_closed
        }
    }

    impl<D: Data> ScopeStreamPush<D> for OutputHandle<D> {
        #[inline]
        fn port(&self) -> Port {
            self.port
        }

        #[inline]
        fn push(&mut self, tag: &Tag, msg: D) -> IOResult<()> {
            if !self.is_skipped(tag) {
                self.tee.push(tag, msg)?;
            }
            Ok(())
        }

        #[inline]
        fn push_last(&mut self, msg: D, end: EndSignal) -> IOResult<()> {
            self.tee.push_last(msg, end)
        }

        #[inline]
        fn try_push_iter<I: Iterator<Item = D>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
            if !self.is_skipped(tag) {
                self.tee.try_push_iter(tag, iter)?;
            }
            Ok(())
        }

        #[inline]
        fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
            self.tee.notify_end(end)
        }

        #[inline]
        fn flush(&mut self) -> IOResult<()> {
            self.tee.flush()
        }

        fn close(&mut self) -> IOResult<()> {
            if !self.is_closed {
                debug_worker!("close output on port ({:?});", self.port);
                if let Err(err) = self.tee.close() {
                    error_worker!("close output on port {:?} failure, caused by: {}", self.port, err);
                }
                self.is_closed = true;
            }
            Ok(())
        }
    }

    pub struct OutputSession<'a, D: Data> {
        pub tag: Tag,
        output: RefMut<'a, OutputHandle<D>>,
    }

    impl<'a, D: Data> OutputSession<'a, D> {
        pub(crate) fn new(tag: Tag, output: RefMut<'a, OutputHandle<D>>) -> Self {
            OutputSession { tag, output }
        }

        pub fn give(&mut self, msg: D) -> IOResult<()> {
            self.output.push(&self.tag, msg)
        }

        pub fn give_last(&mut self, msg: D, end: EndSignal) -> IOResult<()> {
            assert_eq!(self.tag, end.tag);
            self.output.push_last(msg, end)
        }

        pub fn give_iterator<I>(&mut self, iter: I) -> IOResult<()>
        where
            I: Iterator<Item = D> + Send + 'static,
        {
            self.output.push_iter(&self.tag, iter)
        }

        pub fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
            assert_eq!(self.tag, end.tag);
            self.output.notify_end(end)
        }

        pub fn flush(&mut self) -> IOResult<()> {
            self.output.flush()
        }
    }
}

///////////////////////////////////////////////////
#[cfg(feature = "rob")]
mod rob {
    use std::cell::RefMut;
    use std::collections::{HashSet, VecDeque};

    use pegasus_common::buffer::ReadBuffer;

    use crate::communication::buffer::ScopeBufferPool;
    use crate::communication::decorator::{BlockPush, ScopeStreamPush};
    use crate::communication::output::builder::OutputMeta;
    use crate::communication::output::tee::Tee;
    use crate::communication::IOResult;
    use crate::data::MicroBatch;
    use crate::data_plane::Push;
    use crate::errors::IOError;
    use crate::graph::Port;
    use crate::progress::EndSignal;
    use crate::tag::tools::map::TidyTagMap;
    use crate::{Data, Tag};

    enum BlockEntry<D: Data> {
        Single(D),
        LastSingle(D, EndSignal),
        DynIter(Option<D>, Box<dyn Iterator<Item = D> + Send + 'static>),
    }

    pub struct OutputHandle<D: Data> {
        pub port: Port,
        pub scope_level: u32,
        pub src: u32,
        tee: Tee<D>,
        buf_pool: ScopeBufferPool<D>,
        in_block: TidyTagMap<BlockEntry<D>>,
        blocks: VecDeque<Tag>,
        seq_emit: TidyTagMap<u64>,
        is_closed: bool,
        skips: HashSet<Tag>,
    }

    impl<D: Data> OutputHandle<D> {
        pub(crate) fn new(meta: OutputMeta, output: Tee<D>) -> Self {
            let batch_capacity = meta.batch_capacity as usize;
            let scope_capacity = meta.scope_capacity as usize;
            let scope_level = meta.scope_level;
            let buf_pool =
                ScopeBufferPool::new(meta.batch_size, batch_capacity, scope_capacity, scope_level);
            let src = crate::worker_id::get_current_worker().index;
            OutputHandle {
                port: meta.port,
                scope_level,
                src,
                tee: output,
                buf_pool,
                in_block: TidyTagMap::new(scope_level),
                blocks: VecDeque::new(),
                seq_emit: TidyTagMap::new(scope_level),
                is_closed: false,
                skips: HashSet::new(),
            }
        }

        pub fn push_iter<I: Iterator<Item = D> + Send + 'static>(
            &mut self, tag: &Tag, mut iter: I,
        ) -> IOResult<()> {
            if let Err(e) = self.try_push_iter(tag, &mut iter) {
                if e.is_would_block() {
                    if let Some(b) = self.in_block.remove(tag) {
                        match b {
                            BlockEntry::Single(x) => {
                                self.in_block
                                    .insert(tag.clone(), BlockEntry::DynIter(Some(x), Box::new(iter)));
                            }
                            BlockEntry::LastSingle(_, _) => {
                                unreachable!("unexpected blocking: LastSingle;");
                            }
                            BlockEntry::DynIter(_, _) => {
                                unreachable!("unexpected blocking: DynIter")
                            }
                        }
                    } else {
                        self.blocks.push_back(tag.clone());
                        self.in_block
                            .insert(tag.clone(), BlockEntry::DynIter(None, Box::new(iter)));
                    }
                }
                Err(e)
            } else {
                Ok(())
            }
        }

        pub fn push_batch(&mut self, batch: MicroBatch<D>) -> IOResult<()> {
            self.tee.push(batch)
        }

        pub fn push_batch_mut(&mut self, batch: &mut MicroBatch<D>) -> IOResult<()> {
            let mut new_batch = MicroBatch::new(batch.tag.clone(), self.src, batch.take_data());
            new_batch.set_seq(batch.get_seq());
            self.push_batch(new_batch)
        }

        pub fn is_closed(&self) -> bool {
            self.is_closed
        }

        pub(crate) fn try_unblock(&mut self, unblocked: &mut Vec<Tag>) -> IOResult<()> {
            let len = self.blocks.len();
            if len > 0 {
                for _ in 0..len {
                    if let Some(tag) = self.blocks.pop_front() {
                        if self.tee.try_unblock(&tag)? {
                            if let Some(mut iter) = self.in_block.remove(&tag) {
                                match iter {
                                    BlockEntry::Single(x) => {
                                        self.push(&tag, x)?;
                                    }
                                    BlockEntry::LastSingle(x, e) => {
                                        self.push_last(x, e)?;
                                    }
                                    BlockEntry::DynIter(head, iter) => {
                                        if let Some(x) = head {
                                            if let Err(e) = self.push(&tag, x) {
                                                if e.is_would_block() {
                                                    if let Some(blocked) = self.in_block.remove(&tag) {
                                                        match blocked {
                                                            BlockEntry::Single(x) => {
                                                                self.in_block.insert(
                                                                    tag.clone(),
                                                                    BlockEntry::DynIter(Some(x), iter),
                                                                );
                                                            }
                                                            _ => unreachable!("unexpected blocking;"),
                                                        }
                                                    } else {
                                                        self.in_block.insert(
                                                            tag.clone(),
                                                            BlockEntry::DynIter(None, iter),
                                                        );
                                                        self.blocks.push_back(tag.clone());
                                                    }
                                                }
                                                return Err(e);
                                            }
                                        }

                                        self.push_box_iter(&tag, iter)?;
                                    }
                                }
                                trace_worker!("data in scope {:?} unblocked", tag);
                                unblocked.push(tag);
                            } else {
                                unblocked.push(tag);
                            }
                        } else {
                            self.blocks.push_back(tag);
                        }
                    }
                }
            }
            Ok(())
        }

        pub(crate) fn get_blocks(&self) -> &VecDeque<Tag> {
            &self.blocks
        }

        #[inline]
        pub(crate) fn skip(&mut self, tag: &Tag) {
            todo!()
        }

        #[inline]
        fn is_skipped(&self, tag: &Tag) -> bool {
            todo!()
        }

        #[inline]
        fn push_box_iter(
            &mut self, tag: &Tag, mut iter: Box<dyn Iterator<Item = D> + Send + 'static>,
        ) -> IOResult<()> {
            if let Err(e) = self.try_push_iter(tag, &mut iter) {
                if e.is_would_block() {
                    if let Some(e) = self.in_block.remove(&tag) {
                        match e {
                            BlockEntry::Single(x) => {
                                self.in_block
                                    .insert(tag.clone(), BlockEntry::DynIter(Some(x), iter));
                            }
                            _ => unreachable!("unexpected blocking"),
                        }
                    } else {
                        self.blocks.push_back(tag.clone());
                        self.in_block
                            .insert(tag.clone(), BlockEntry::DynIter(None, iter));
                    }
                }
                Err(e)
            } else {
                Ok(())
            }
        }

        #[inline]
        fn flush_batch(&mut self, mut batch: MicroBatch<D>) -> IOResult<()> {
            let seq = self.seq_emit.get_mut_or_insert(&batch.tag);
            batch.set_seq(*seq);
            *seq += 1;
            let tag = batch.tag();
            match self.tee.push(batch) {
                Err(e) => {
                    if e.is_would_block() {
                        self.blocks.push_back(tag);
                    }
                    Err(e)
                }
                _ => Ok(()),
            }
        }

        fn flush_inner(&mut self, buffers: &mut ScopeBufferPool<D>) -> IOResult<()> {
            for (tag, buf) in buffers.buffers() {
                let batch = MicroBatch::new(tag, self.src, buf.into_read_only());
                self.flush_batch(batch)?;
            }
            self.tee.flush()
        }
    }

    pub struct OutputSession<'a, D: Data> {
        pub tag: Tag,
        output: RefMut<'a, OutputHandle<D>>,
    }

    impl<'a, D: Data> OutputSession<'a, D> {
        pub(crate) fn new(tag: Tag, mut output: RefMut<'a, OutputHandle<D>>) -> Self {
            output.buf_pool.pin(&tag);
            OutputSession { tag, output }
        }

        pub fn give(&mut self, msg: D) -> IOResult<()> {
            self.output.push(&self.tag, msg)
        }

        pub fn give_last(&mut self, msg: D, end: EndSignal) -> IOResult<()> {
            assert_eq!(self.tag, end.tag);
            self.output.push_last(msg, end)
        }

        pub fn give_iterator<I>(&mut self, iter: I) -> IOResult<()>
        where
            I: Iterator<Item = D> + Send + 'static,
        {
            self.output.push_iter(&self.tag, iter)
        }

        pub fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
            assert_eq!(self.tag, end.tag);
            self.output.notify_end(end)
        }

        pub fn flush(&mut self) -> IOResult<()> {
            self.output.flush()
        }
    }

    impl<D: Data> ScopeStreamPush<D> for OutputHandle<D> {
        fn port(&self) -> Port {
            self.port
        }

        fn push(&mut self, tag: &Tag, msg: D) -> IOResult<()> {
            match self.buf_pool.push(tag, msg) {
                Ok(Some(buf)) => {
                    let batch = MicroBatch::new(tag.clone(), self.src, buf);
                    self.flush_batch(batch)
                }
                Err(e) => {
                    if let Some(item) = e.0 {
                        self.in_block
                            .insert(tag.clone(), BlockEntry::Single(item));
                    }
                    self.blocks.push_back(tag.clone());
                    would_block!("no buffer available")
                }
                Ok(_) => Ok(()),
            }
        }

        fn push_last(&mut self, msg: D, end: EndSignal) -> IOResult<()> {
            match self.buf_pool.push(&end.tag, msg) {
                Ok(Some(buf)) => {
                    let mut batch = MicroBatch::new(end.tag.clone(), self.src, buf);
                    batch.set_end(end);
                    self.flush_batch(batch)
                }
                Ok(None) => {
                    let buf = self
                        .buf_pool
                        .take_buf(&end.tag, true)
                        .expect("buf can't be none");
                    let mut batch = MicroBatch::new(end.tag.clone(), self.src, buf.into_read_only());
                    batch.set_end(end);
                    self.flush_batch(batch)
                }
                Err(e) => {
                    if let Some(item) = e.0 {
                        self.blocks.push_back(end.tag.clone());
                        self.in_block
                            .insert(end.tag.clone(), BlockEntry::LastSingle(item, end));
                    } else {
                        unreachable!("data may lost;")
                    }
                    would_block!("no buffer available")
                }
            }
        }

        fn try_push_iter<I: Iterator<Item = D>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
            self.buf_pool.pin(tag);
            loop {
                match self.buf_pool.push_iter(tag, iter) {
                    Ok(Some(buf)) => {
                        let batch = MicroBatch::new(tag.clone(), self.src, buf);
                        self.flush_batch(batch)?;
                    }
                    Ok(None) => {
                        // all data in iter should be send;
                        assert!(iter.next().is_none());
                        break;
                    }
                    Err(e) => {
                        if let Some(item) = e.0 {
                            self.blocks.push_back(tag.clone());
                            self.in_block
                                .insert(tag.clone(), BlockEntry::Single(item));
                        }
                        return would_block!("");
                    }
                }
            }
            Ok(())
        }

        fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
            let mut batch = if let Some(buf) = self.buf_pool.take_buf(&end.tag, true) {
                MicroBatch::new(end.tag.clone(), self.src, buf.into_read_only())
            } else {
                MicroBatch::new(end.tag.clone(), self.src, ReadBuffer::new())
            };
            batch.set_end(end);
            self.flush_batch(batch)
        }

        fn flush(&mut self) -> IOResult<()> {
            let mut buffers = std::mem::replace(&mut self.buf_pool, Default::default());
            let result = self.flush_inner(&mut buffers);
            self.buf_pool = buffers;
            result
        }

        fn close(&mut self) -> IOResult<()> {
            self.flush()?;
            self.is_closed = true;
            self.tee.close()
        }
    }
}
