use crate::api::{Binary, Either, Merge};
use crate::stream::Stream;
use crate::{BuildJobError, Data};

impl<D: Data> Merge<D> for Stream<D> {
    fn merge(self, other: Stream<D>) -> Result<Stream<D>, BuildJobError> {
        self.binary("merge", other, |_| {
            |left, right, output| {
                left.for_each_batch(|dataset| {
                    if !dataset.is_empty() {
                        output.push_batch_mut(dataset)?;
                    }
                    Ok(())
                })?;
                right.for_each_batch(|dataset| {
                    if !dataset.is_empty() {
                        output.push_batch_mut(dataset)?;
                    }
                    Ok(())
                })
            }
        })
    }

    fn merge_isomer<T: Data>(self, other: Stream<T>) -> Result<Stream<Either<D, T>>, BuildJobError> {
        self.binary("merge_isomer", other, |_| {
            |left, right, output| {
                left.for_each_batch(|dataset| {
                    if !dataset.is_empty() {
                        let mut session = output.new_session(&dataset.tag)?;
                        for out in dataset.drain().map(|d| Either::A(d)) {
                            session.give(out)?;
                        }
                    }
                    Ok(())
                })?;
                right.for_each_batch(|dataset| {
                    if !dataset.is_empty() {
                        let mut session = output.new_session(&dataset.tag)?;
                        for out in dataset.drain().map(|d| Either::B(d)) {
                            session.give(out)?;
                        }
                    }
                    Ok(())
                })
            }
        })
    }
}
