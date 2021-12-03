use crate::api::{Binary, Either, Merge};
use crate::stream::Stream;
use crate::{BuildJobError, Data};

impl<D: Data> Merge<D> for Stream<D> {
    fn merge(self, other: Stream<D>) -> Result<Stream<D>, BuildJobError> {
        self.binary("merge", other, |_| {
            |left, right, output| {
                left.for_each_batch(|batch| {
                    let end = batch.take_end();
                    let res = if !batch.is_empty() {
                        output.push_batch_mut(batch)
                    } else {
                        Ok(())
                    };

                    if let Some(end) = end {
                        batch.set_end(end);
                    }
                    Ok(res?)
                })?;
                right.for_each_batch(|batch| {
                    let end = batch.take_end();
                    let res = if !batch.is_empty() {
                        output.push_batch_mut(batch)
                    } else {
                        Ok(())
                    };

                    if let Some(end) = end {
                        batch.set_end(end);
                    }
                    Ok(res?)
                })
            }
        })
    }

    fn merge_isomer<T: Data>(self, other: Stream<T>) -> Result<Stream<Either<D, T>>, BuildJobError> {
        self.binary("merge_isomer", other, |_| {
            |left, right, output| {
                left.for_each_batch(|batch| {
                    if !batch.is_empty() {
                        let mut session = output.new_session(&batch.tag)?;
                        for out in batch.drain().map(|d| Either::A(d)) {
                            session.give(out)?;
                        }
                    }
                    Ok(())
                })?;
                right.for_each_batch(|batch| {
                    if !batch.is_empty() {
                        let mut session = output.new_session(&batch.tag)?;
                        for out in batch.drain().map(|d| Either::B(d)) {
                            session.give(out)?;
                        }
                    }
                    Ok(())
                })
            }
        })
    }
}
