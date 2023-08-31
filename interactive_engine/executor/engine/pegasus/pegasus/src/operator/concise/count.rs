use crate::api::{Count, Unary};
use crate::communication::output::OutputProxy;
use crate::stream::{Single, SingleItem, Stream};
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data};

impl<D: Data> Count<D> for Stream<D> {
    fn count(self) -> Result<SingleItem<u64>, BuildJobError> {
        let worker_id = self.get_worker_id().index;
        let total_peers = self.get_worker_id().total_peers();
        if self.get_partitions() > 1 {
            let mut stream = self.unary("count_local", |info| {
                let mut table = TidyTagMap::<u64>::new(info.scope_level);
                move |input, output| {
                    input.for_each_batch(|batch| {
                        if !batch.is_empty() {
                            let mut cnt = table.remove(&batch.tag).unwrap_or(0);
                            cnt += batch.len() as u64;
                            batch.clear();

                            if let Some(end) = batch.take_end() {
                                let mut session = output.new_session(&batch.tag)?;
                                trace_worker!("local count {} of {:?}", cnt, batch.tag);
                                if end.tag.len() > 0 {
                                    let mut new_end = end.clone();
                                    let mut new_peers = end.peers().clone();
                                    let owner_index = batch.tag.current_uncheck()
                                        % crate::worker_id::get_current_worker().total_peers();
                                    new_peers.add_source(owner_index);
                                    new_end.update_peers(new_peers);
                                    session.give_last(cnt, new_end)?;
                                } else {
                                    session.give_last(cnt, end)?;
                                }
                            } else {
                                table.insert(batch.tag.clone(), cnt);
                            }
                            return Ok(());
                        }

                        if let Some(end) = batch.take_end() {
                            if let Some(cnt) = table.remove(&batch.tag) {
                                let mut session = output.new_session(&batch.tag)?;
                                trace_worker!("local count {} of {:?}", cnt, batch.tag);
                                if end.tag.len() > 0 {
                                    let mut new_end = end.clone();
                                    let mut new_peers = end.peers().clone();
                                    let owner_index = batch.tag.current_uncheck()
                                        % crate::worker_id::get_current_worker().total_peers();
                                    new_peers.add_source(owner_index);
                                    new_end.update_peers(new_peers, end);
                                    session.give_last(cnt, new_end)?;
                                } else {
                                    session.give_last(cnt, end)?;
                                }
                            } else {
                                let worker = worker_id;
                                let new_end = if end.tag.len() > 0 {
                                    let mut new_end = end.clone();
                                    let mut new_peers = end.peers().clone();
                                    let owner_index = batch.tag.current_uncheck() % total_peers;
                                    new_peers.add_source(owner_index);
                                    new_end.update_peers(new_peers, total_peers);
                                    new_end
                                } else {
                                    end
                                };
                                if new_end.contains_source(worker) {
                                    let mut session = output.new_session(&batch.tag)?;
                                    trace_worker!("local count {} of {:?}", 0, batch.tag);
                                    session.give_last(0, new_end)?;
                                } else {
                                    output.notify_end(new_end)?;
                                }
                            }
                        }
                        Ok(())
                    })
                }
            })?;
            stream
                .set_upstream_batch_size(1)
                .set_upstream_batch_capacity(1);
            let x = stream
                .aggregate()
                .unary("count_global", |info| {
                    let mut table = TidyTagMap::<u64>::new(info.scope_level);
                    move |input, output| {
                        input.for_each_batch(|batch| {
                            if !batch.is_empty() {
                                let mut sum = table.remove(&batch.tag).unwrap_or(0);
                                for d in batch.drain() {
                                    sum += d;
                                }
                                if let Some(end) = batch.take_end() {
                                    let mut session = output.new_session(&batch.tag)?;
                                    trace_worker!("emit global count = {} of {:?};", sum, end.tag);
                                    session.give_last(Single(sum), end)?;
                                } else {
                                    table.insert(batch.tag.clone(), sum);
                                }
                                return Ok(());
                            }

                            if let Some(end) = batch.take_end() {
                                if let Some(sum) = table.remove(&end.tag) {
                                    let mut session = output.new_session(&batch.tag)?;
                                    trace_worker!("emit global count = {} of {:?};", sum, end.tag);
                                    session.give_last(Single(sum), end)?;
                                } else {
                                    let index = worker_id;
                                    if end.contains_source(index) {
                                        let mut session = output.new_session(&batch.tag)?;
                                        trace_worker!("emit global count = {} of {:?};", 0, end.tag);
                                        session.give_last(Single(0), end)?;
                                    } else {
                                        output.notify_end(end)?;
                                    }
                                }
                            }
                            Ok(())
                        })
                    }
                })?;

            Ok(SingleItem::new(x))
        } else {
            let stream = self.unary("count_global", |info| {
                let mut table = TidyTagMap::<u64>::new(info.scope_level);
                move |input, output| {
                    input.for_each_batch(|batch| {
                        if !batch.is_empty() {
                            let mut cnt = table.remove(&batch.tag).unwrap_or(0);
                            cnt += batch.len() as u64;
                            batch.clear();

                            if let Some(end) = batch.take_end() {
                                let mut session = output.new_session(&batch.tag)?;
                                trace_worker!("global count {} of {:?}", cnt, batch.tag);
                                session.give_last(Single(cnt), end)?;
                            } else {
                                table.insert(batch.tag.clone(), cnt);
                            }
                            return Ok(());
                        }

                        if let Some(end) = batch.take_end() {
                            if let Some(cnt) = table.remove(&batch.tag) {
                                let mut session = output.new_session(&batch.tag)?;
                                trace_worker!("global count {} of {:?}", cnt, batch.tag);
                                session.give_last(Single(cnt), end)?;
                            } else {
                                let worker = worker_id;
                                if end.contains_source(worker) {
                                    let mut session = output.new_session(&batch.tag)?;
                                    trace_worker!("global count {} of {:?}", 0, batch.tag);
                                    session.give_last(Single(0), end)?;
                                } else {
                                    output.notify_end(end)?;
                                }
                            }
                        }
                        Ok(())
                    })
                }
            })?;
            Ok(SingleItem::new(stream))
        }
    }
}
