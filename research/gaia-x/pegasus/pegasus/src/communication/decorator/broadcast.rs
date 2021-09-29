pub(crate) use rob::*;

#[cfg(not(feature = "rob"))]
mod rob {
    use crate::channel_id::ChannelInfo;
    use crate::communication::cancel::{CancelHandle, MultiConsCancelPtr};
    use crate::communication::decorator::evented::EventEmitPush;
    use crate::communication::decorator::ScopeStreamPush;
    use crate::communication::IOResult;
    use crate::data::MicroBatch;
    use crate::graph::Port;
    use crate::progress::{EndSignal, Weight};
    use crate::{Data, Tag};

    pub struct BroadcastBatchPush<D: Data> {
        pub ch_info: ChannelInfo,
        //pool: MemBatchPool<D>,
        pushes: Vec<EventEmitPush<D>>,
        cancel_handle: MultiConsCancelPtr,
    }

    impl<D: Data> BroadcastBatchPush<D> {
        pub fn new(ch_info: ChannelInfo, pushes: Vec<EventEmitPush<D>>) -> Self {
            let cancel_handle = MultiConsCancelPtr::new(ch_info.scope_level, pushes.len());
            BroadcastBatchPush { ch_info, pushes, cancel_handle }
        }

        pub(crate) fn get_cancel_handle(&self) -> CancelHandle {
            CancelHandle::MC(self.cancel_handle.clone())
        }
    }

    impl<T: Data> ScopeStreamPush<MicroBatch<T>> for BroadcastBatchPush<T> {
        fn port(&self) -> Port {
            self.ch_info.source_port
        }

        fn push(&mut self, tag: &Tag, msg: MicroBatch<T>) -> IOResult<()> {
            for i in 1..self.pushes.len() {
                // TODO: avoid clone msg;
                if !self.cancel_handle.is_canceled(&msg.tag, i) {
                    self.pushes[i].push(tag, msg.clone())?;
                }
            }
            if !self.cancel_handle.is_canceled(&msg.tag, 0) {
                self.pushes[0].push(tag, msg)
            } else {
                Ok(())
            }
        }

        fn push_last(&mut self, msg: MicroBatch<T>, mut end: EndSignal) -> IOResult<()> {
            end.update_to(Weight::all());
            for i in 1..self.pushes.len() {
                // TODO: avoid clone msg;
                self.pushes[i].push_last(msg.clone(), end.clone())?;
            }
            self.pushes[0].push_last(msg, end)
        }

        fn notify_end(&mut self, mut end: EndSignal) -> IOResult<()> {
            end.update_to(Weight::all());
            for i in 1..self.pushes.len() {
                self.pushes[i].notify_end(end.clone())?;
            }
            self.pushes[0].notify_end(end)
        }

        fn flush(&mut self) -> IOResult<()> {
            for p in self.pushes.iter_mut() {
                p.flush()?;
            }
            Ok(())
        }

        fn close(&mut self) -> IOResult<()> {
            for p in self.pushes.iter_mut() {
                p.close()?;
            }
            Ok(())
        }
    }
}

#[cfg(feature = "rob")]
mod rob {
    use crate::channel_id::ChannelInfo;
    use crate::communication::cancel::{CancelHandle, MultiConsCancelPtr};
    use crate::communication::decorator::evented::EventEmitPush;
    use crate::data::MicroBatch;
    use crate::data_plane::Push;
    use crate::errors::IOError;
    use crate::progress::{EndSignal, Weight};
    use crate::Data;

    pub struct BroadcastBatchPush<D: Data> {
        pub ch_info: ChannelInfo,
        pushes: Vec<EventEmitPush<D>>,
        cancel_handle: MultiConsCancelPtr,
    }

    impl<D: Data> BroadcastBatchPush<D> {
        pub fn new(ch_info: ChannelInfo, pushes: Vec<EventEmitPush<D>>) -> Self {
            let cancel_handle = MultiConsCancelPtr::new(ch_info.scope_level, pushes.len());
            BroadcastBatchPush { ch_info, pushes, cancel_handle }
        }

        pub(crate) fn get_cancel_handle(&self) -> CancelHandle {
            CancelHandle::MC(self.cancel_handle.clone())
        }

        fn push_to(&mut self, target: usize, mut batch: MicroBatch<D>) -> Result<(), IOError> {
            let level = batch.tag.len() as u32;
            if level == self.ch_info.scope_level
                && self
                    .cancel_handle
                    .is_canceled(&batch.tag, target)
            {
                batch.clear();
                if !batch.is_last() {
                    return Ok(());
                }
            }

            if let Some(mut end) = batch.take_end() {
                if end.source.value() == 1 {
                    end.source = Weight::all();
                    batch.set_end(end);
                    self.pushes[target].push(batch)?;
                } else {
                    if !batch.is_empty() {
                        self.pushes[target].push(batch)?;
                    }
                    let end = EndSignal::new(end, Weight::all());
                    self.pushes[target].notify_end(end)?;
                }
            } else {
                self.pushes[target].push(batch)?;
            }

            Ok(())
        }
    }

    impl<D: Data> Push<MicroBatch<D>> for BroadcastBatchPush<D> {
        fn push(&mut self, mut batch: MicroBatch<D>) -> Result<(), IOError> {
            let level = batch.tag.len() as u32;
            if self.pushes.len() == 1 {
                // corner case;
                if level == self.ch_info.scope_level && self.cancel_handle.is_canceled(&batch.tag, 0) {
                    batch.clear();
                    if !batch.is_last() {
                        return Ok(());
                    }
                }
                self.pushes[0].push(batch)
            } else {
                for i in 1..self.pushes.len() {
                    self.push_to(i, batch.share())?;
                }

                self.push_to(0, batch)
            }
        }

        fn flush(&mut self) -> Result<(), IOError> {
            for p in self.pushes.iter_mut() {
                p.flush()?;
            }
            Ok(())
        }

        fn close(&mut self) -> Result<(), IOError> {
            for p in self.pushes.iter_mut() {
                p.close()?;
            }
            Ok(())
        }
    }
}
