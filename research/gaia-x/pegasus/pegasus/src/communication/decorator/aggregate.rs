pub(crate) use rob::*;

#[cfg(not(feature = "rob"))]
mod rob {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use crate::channel_id::ChannelInfo;
    use crate::communication::decorator::evented::EventEmitPush;
    use crate::communication::decorator::ScopeStreamPush;
    use crate::communication::IOResult;
    use crate::data::MicroBatch;
    use crate::graph::Port;
    use crate::progress::EndSignal;
    use crate::{Data, Tag};

    pub struct AggregateBatchPush<D: Data> {
        pub ch_info: ChannelInfo,
        data_push: EventEmitPush<D>,
        event_push: Vec<EventEmitPush<D>>,
        has_cycles: Arc<AtomicBool>,
    }

    impl<D: Data> AggregateBatchPush<D> {
        pub fn new(
            target: u32, info: ChannelInfo, mut pushes: Vec<EventEmitPush<D>>, cyclic: &Arc<AtomicBool>,
        ) -> Self {
            let data_push = pushes.swap_remove(target as usize);
            AggregateBatchPush { ch_info: info, data_push, event_push: pushes, has_cycles: cyclic.clone() }
        }
    }

    impl<T: Data> ScopeStreamPush<MicroBatch<T>> for AggregateBatchPush<T> {
        fn port(&self) -> Port {
            self.ch_info.source_port
        }

        fn push(&mut self, tag: &Tag, msg: MicroBatch<T>) -> IOResult<()> {
            self.data_push.push(tag, msg)
        }

        fn push_last(&mut self, msg: MicroBatch<T>, end: EndSignal) -> IOResult<()> {
            if msg.tag.is_root() {
                for p in self.event_push.iter_mut() {
                    p.notify_end(end.clone())?;
                }
            }
            self.data_push.push_last(msg, end)
        }

        fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
            if end.tag.is_root()
                || end.tag.len() < self.ch_info.scope_level as usize
                || self.has_cycles.load(Ordering::SeqCst)
            {
                for p in self.event_push.iter_mut() {
                    p.notify_end(end.clone())?;
                }
            }
            self.data_push.notify_end(end)
        }

        fn flush(&mut self) -> IOResult<()> {
            self.data_push.flush()
        }

        fn close(&mut self) -> IOResult<()> {
            for p in self.event_push.iter_mut() {
                p.close()?;
            }
            self.data_push.close()
        }
    }
}

///////////////////////////////////////////////////

#[cfg(feature = "rob")]
mod rob {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use crate::channel_id::ChannelInfo;
    use crate::communication::decorator::evented::EventEmitPush;
    use crate::communication::IOResult;
    use crate::data::MicroBatch;
    use crate::data_plane::Push;
    use crate::errors::IOError;
    use crate::progress::EndSignal;
    use crate::Data;

    pub struct AggregateBatchPush<D: Data> {
        pub ch_info: ChannelInfo,
        data_push: EventEmitPush<D>,
        event_push: Vec<EventEmitPush<D>>,
        has_cycles: Arc<AtomicBool>,
    }

    impl<D: Data> AggregateBatchPush<D> {
        pub fn new(
            target: u32, info: ChannelInfo, mut pushes: Vec<EventEmitPush<D>>, cyclic: &Arc<AtomicBool>,
        ) -> Self {
            let data_push = pushes.swap_remove(target as usize);
            AggregateBatchPush { ch_info: info, data_push, event_push: pushes, has_cycles: cyclic.clone() }
        }

        fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
            if end.tag.is_root()
                || end.tag.len() < self.ch_info.scope_level as usize
                || self.has_cycles.load(Ordering::SeqCst)
            {
                for p in self.event_push.iter_mut() {
                    p.notify_end(end.clone())?;
                }
            }
            self.data_push.notify_end(end)
        }
    }

    impl<D: Data> Push<MicroBatch<D>> for AggregateBatchPush<D> {
        fn push(&mut self, mut batch: MicroBatch<D>) -> Result<(), IOError> {
            let end = batch.take_end();
            if !batch.is_empty() {
                self.data_push.push(batch)?;
            }
            if let Some(end) = end {
                self.notify_end(end)
            } else {
                Ok(())
            }
        }

        fn flush(&mut self) -> Result<(), IOError> {
            self.data_push.flush()?;
            for p in self.event_push.iter_mut() {
                p.flush()?;
            }
            Ok(())
        }

        fn close(&mut self) -> Result<(), IOError> {
            self.data_push.close()?;
            for p in self.event_push.iter_mut() {
                p.close()?;
            }
            Ok(())
        }
    }
}
