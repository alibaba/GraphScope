use crate::api::{HasAny, Unary};
use crate::stream::{Single, SingleItem, Stream};
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data};

impl<D: Data> HasAny<D> for Stream<D> {
    fn any(mut self) -> Result<SingleItem<bool>, BuildJobError> {
        self.set_upstream_batch_capacity(1);
        let mut stream = self.unary("any", |info| {
            let mut any_map = TidyTagMap::<()>::new(info.scope_level);
            move |input, output| {
                input.for_each_batch(|batch| {
                    if !batch.is_empty() {
                        if !any_map.contains_key(batch.tag()) {
                            any_map.insert(batch.tag().clone(), ());
                            output.new_session(batch.tag())?.give(())?;
                        }
                        batch.discard();
                    } else {
                        // ignore
                    }
                    if batch.is_last() {
                        any_map.remove(batch.tag());
                    }
                    Ok(())
                })
            }
        })?;
        stream
            .set_upstream_batch_capacity(1)
            .set_upstream_batch_size(1);
        let x = stream.aggregate().unary("any_global", |info| {
            let mut any_map = TidyTagMap::<()>::new(info.scope_level);
            move |input, output| {
                input.for_each_batch(|batch| {
                    if batch.is_last() {
                        if !any_map.contains_key(batch.tag()) {
                            any_map.insert(batch.tag().clone(), ());
                            output
                                .new_session(batch.tag())?
                                .give(Single(!batch.is_empty()))?;
                        }
                        batch.clear();
                        any_map.remove(batch.tag());
                    } else if !batch.is_empty() {
                        batch.clear();
                        if !any_map.contains_key(batch.tag()) {
                            any_map.insert(batch.tag().clone(), ());
                            output
                                .new_session(batch.tag())?
                                .give(Single(true))?;
                        }
                    }
                    Ok(())
                })
            }
        })?;

        Ok(SingleItem::new(x))
    }
}
