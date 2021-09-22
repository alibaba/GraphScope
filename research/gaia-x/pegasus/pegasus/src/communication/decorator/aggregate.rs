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
    use crate::channel_id::ChannelInfo;
    use crate::communication::decorator::evented::EventEmitPush;
    use crate::data::MicroBatch;
    use crate::data_plane::Push;
    use crate::errors::IOError;
    use crate::progress::{EndSignal, Weight};
    use crate::Data;

    pub struct AggregateBatchPush<D: Data> {
        pub ch_info: ChannelInfo,
        data_push: EventEmitPush<D>,
        event_push: Option<EventEmitPush<D>>,
    }

    impl<D: Data> AggregateBatchPush<D> {
        pub fn new(target: u32, info: ChannelInfo, pushes: Vec<EventEmitPush<D>>) -> Self {
            assert_eq!(info.scope_level, 0);
            let source = crate::worker_id::get_current_worker().index;
            let mut vec = vec![];
            for mut p in pushes {
                if p.target_worker == source || p.target_worker == target {
                    vec.push(Some(p));
                } else {
                    p.close().ok();
                    vec.push(None);
                }
            }
            let data_push = vec[target as usize]
                .take()
                .expect("data push lost");
            let event_push = vec[source as usize].take();
            AggregateBatchPush { ch_info: info, data_push, event_push }
        }
    }

    impl<D: Data> Push<MicroBatch<D>> for AggregateBatchPush<D> {
        fn push(&mut self, mut batch: MicroBatch<D>) -> Result<(), IOError> {
            let end = batch.take_end();
            let src = batch.src;
            if !batch.is_empty() {
                self.data_push.push(batch)?;
            }

            if let Some(end) = end {
                if let Some(ref mut p) = self.event_push {
                    p.push(MicroBatch::last(src, end.clone()))?;
                }
                let end = EndSignal::new(end, Weight::all());
                self.data_push.notify_end(end)
            } else {
                Ok(())
            }
        }

        fn flush(&mut self) -> Result<(), IOError> {
            self.data_push.flush()?;
            if let Some(ref mut p) = self.event_push {
                p.flush()?;
            }
            Ok(())
        }

        fn close(&mut self) -> Result<(), IOError> {
            self.data_push.close()?;
            if let Some(ref mut p) = self.event_push {
                p.close()?;
            }
            Ok(())
        }
    }
}
