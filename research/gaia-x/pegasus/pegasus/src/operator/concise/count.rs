use crate::api::{Count, Unary};
use crate::stream::{Single, SingleItem, Stream};
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data};

impl<D: Data> Count<D> for Stream<D> {
    fn count(self) -> Result<SingleItem<u64>, BuildJobError> {
        let stream = self
            .unary("count_local", |info| {
                let mut table = TidyTagMap::<u64>::new(info.scope_level);
                move |input, output| {
                    input.for_each_batch(|dataset| {
                        let mut cnt = table.remove(&dataset.tag).unwrap_or(0);
                        cnt += dataset.len() as u64;
                        dataset.clear();
                        if let Some(end) = dataset.take_end() {
                            let mut session = output.new_session(&dataset.tag)?;
                            trace_worker!("local count {} of {:?}", cnt, dataset.tag);
                            session.give_last(cnt, end)?;
                        } else {
                            table.insert(dataset.tag.clone(), cnt);
                        }
                        Ok(())
                    })
                }
            })?
            .aggregate()
            .unary("count_global", |info| {
                let mut table = TidyTagMap::<u64>::new(info.scope_level);
                move |input, output| {
                    input.for_each_batch(|dataset| {
                        if !dataset.is_empty() {
                            let mut sum = table.remove(&dataset.tag).unwrap_or(0);
                            for d in dataset.drain() {
                                sum += d;
                            }
                            if dataset.is_last() {
                                let mut session = output.new_session(&dataset.tag)?;
                                let end = dataset.take_end().expect("unreachable");
                                trace_worker!("emit global count = {} of {:?};", sum, end.tag);
                                session.give_last(Single(sum), end)?;
                            } else {
                                table.insert(dataset.tag.clone(), sum);
                            }
                        }

                        if dataset.is_last() {
                            let mut session = output.new_session(&dataset.tag)?;
                            let end = dataset.take_end().expect("unreachable");
                            if let Some(sum) = table.remove(&end.tag) {
                                trace_worker!("emit global count = {} of {:?};", sum, end.tag);
                                session.give_last(Single(sum), end)?;
                            } else {
                                session.notify_end(end)?;
                            }
                        }
                        Ok(())
                    })
                }
            })?;

        Ok(SingleItem::new(stream))
    }
}
