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

use std::cell::Cell;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, SendError, Sender, TryRecvError};
use rand::Rng;

/// The send abstraction of a communication_old used for intra-process communication_old;
pub trait MPMCSender<T>: Send + Clone {
    /// Push message into the underlying communication_old;
    ///
    /// Return `Result::Err(T)` if the message can't be pushed successfully, you can get the message
    /// back from the return value;
    fn send(&self, message: T) -> Result<(), T>;

    /// Close this sender, after invoking this function, this sender can't be used to send any message,
    /// All `send` function invocations on this sender should fail;
    fn close(&self);
}

/// The receive side abstraction of communication_old used for intra-process communication_old;
pub trait MPMCReceiver<T>: Send + Clone {
    /// Pull messages from communication_old, block current thread until any message is available;
    ///
    /// Return `Result::Err(std::io::Error)` if any exception occurred;
    fn recv(&self) -> io::Result<T>;

    /// Try to pop messages from communication_old in non-blocking manner;
    ///
    /// # Return value
    ///
    /// - `Result::Ok(Some(val))` if one message was pulled successfully;
    /// - `Result::Ok(None)` if no message is available at the moment;
    /// - `Result::Err(std::io::Error)` if any exceptions occurred;
    fn try_recv(&self) -> io::Result<Option<T>>;

    /// Try to pop messages, waiting message available until timeout;
    ///
    /// # Return value
    ///
    /// - `Result::Ok(val)` if one message was pulled successfully before timeout;
    /// - `Result::Err(std::io::Error)` if any exceptions occurred, check [`std::io::ErrorKind`] for
    /// error details, for example, `std::io::ErrorKind::TimedOut` means this invoke was timeout;
    ///
    /// [`std::io::ErrorKind`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html
    fn recv_timeout(&self, time: Duration) -> io::Result<T>;
}

enum Message<T> {
    Data(T),
    Close,
}

const NORMAL: usize = 0;
const POISONED: usize = 0b00000010;
const EXHAUSTED: usize = 0b00000001;

/// The `MessageSender` will promise that :
/// - it will always be closed by invoking the `close` function after it has finished all works;
/// - if it didn't been closed until it will be dropped, it will notify all the receivers this abnormal
/// behavior.
pub struct MessageSender<T: Send> {
    id : u64,
    inner: Sender<Message<T>>,
    peers: Arc<AtomicUsize>,
    state: Arc<AtomicUsize>,
    is_closed: Cell<bool>,
}

/// The `MessageReceiver` will promise that :
///
/// - it always return [`std::io::Error`] if any exception occurred;
/// - the error with kind equals `std::io::ErrorKind::BrokenPipe` means all sending parts were closed
/// successfully, and all data send by them had been consumed; This is a friendly error which only give
/// a hint of the state of the communication_old;
/// - the error with kind equals `std::io::ErrorKind::ConnectionAborted` means that some sending parts
/// was disconnected because of some unknown exception, as these sending parts didn't closed successfully,
/// the receiving parts may lost messages;
/// - other error kinds should be unreachable;
///
/// [`std::io::Error`]: https://doc.rust-lang.org/std/io/struct.Error.html
///
pub struct MessageReceiver<T> {
    inner: Receiver<Message<T>>,
    state: Arc<AtomicUsize>,
}

impl<T: Send> MessageSender<T> {
    fn new(tx: Sender<Message<T>>, state: &Arc<AtomicUsize>) -> Self {
        let id: u64 = rand::thread_rng().gen();
        trace!("create sender with id {}", id);
        MessageSender {
            id,
            inner: tx,
            peers: Arc::new(AtomicUsize::new(1)),
            state: state.clone(),
            is_closed: Cell::new(false),
        }
    }

    fn poison(&self) {
        self.state.fetch_or(POISONED, Ordering::SeqCst);
    }
}

impl<T: Send> Clone for MessageSender<T> {
    fn clone(&self) -> Self {
        self.peers.fetch_add(1, Ordering::Relaxed);
        MessageSender {
            id: self.id,
            inner: self.inner.clone(),
            peers: self.peers.clone(),
            state: self.state.clone(),
            is_closed: Cell::new(false),
        }
    }
}

impl<T: Send> Drop for MessageSender<T> {
    fn drop(&mut self) {
        if !self.is_closed.get() {
            warn!("dropping an unclosed 'MessageSender' id = {}", self.id);
            self.poison();
            self.close();
        }
    }
}

impl<T> MessageReceiver<T> {
    fn new(rx: Receiver<Message<T>>, state: &Arc<AtomicUsize>) -> Self {
        MessageReceiver { inner: rx, state: state.clone() }
    }

    #[inline]
    fn exhausted(&self) {
        self.state.fetch_or(EXHAUSTED, Ordering::SeqCst);
    }

    #[inline]
    fn check(&self) -> io::Result<()> {
        let state: usize = self.state.load(Ordering::SeqCst);
        if state == NORMAL {
            Ok(())
        } else if state & POISONED > 0 {
            Err(io::Error::from(io::ErrorKind::ConnectionAborted))
        } else if state & EXHAUSTED > 0 {
            Err(io::Error::from(io::ErrorKind::BrokenPipe))
        } else {
            Err(io::Error::from(io::ErrorKind::Other))
        }
    }
}

impl<T> Clone for MessageReceiver<T> {
    fn clone(&self) -> Self {
        MessageReceiver { inner: self.inner.clone(), state: self.state.clone() }
    }
}

/// A pair of error aware sender and receiver used for the intra-process communication_old;
///
/// There may be multi-senders produce messages into communication_old, and multi-receivers want to consume the
/// messages through the communication_old;
///
/// After a sender had finished send all the messages, it must be closed before been dropped, otherwise,
/// an exception signal will be generated and be delivered to the receivers;
///
/// After all senders were closed(or dropped), the receive side will get a exhaust signal indicate that the
/// communication_old was exhausted;
///
pub fn unbound<T: Send>() -> (MessageSender<T>, MessageReceiver<T>) {
    let (tx, rx) = crossbeam_channel::unbounded::<Message<T>>();
    let state = Arc::new(AtomicUsize::new(NORMAL));
    (MessageSender::new(tx, &state), MessageReceiver::new(rx, &state))
}

impl<T: Send> MPMCSender<T> for Sender<T> {
    #[inline]
    fn send(&self, message: T) -> Result<(), T> {
        self.send(message).map_err(|SendError(d)| d)
    }

    fn close(&self) {}
}

impl<T: Send> MPMCReceiver<T> for Receiver<T> {
    #[inline]
    fn recv(&self) -> io::Result<T> {
        self.recv()
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))
    }

    #[inline]
    fn try_recv(&self) -> io::Result<Option<T>> {
        match self.try_recv() {
            Ok(d) => Ok(Some(d)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(io::Error::from(io::ErrorKind::BrokenPipe)),
        }
    }

    #[inline]
    fn recv_timeout(&self, time: Duration) -> io::Result<T> {
        match self.recv_timeout(time) {
            Ok(d) => Ok(d),
            Err(RecvTimeoutError::Timeout) => Err(io::Error::from(io::ErrorKind::TimedOut)),
            Err(RecvTimeoutError::Disconnected) => Err(io::Error::from(io::ErrorKind::BrokenPipe)),
        }
    }
}

impl<T: Send> MPMCSender<T> for MessageSender<T> {
    fn send(&self, message: T) -> Result<(), T> {
        self.inner
            .send(Message::Data(message))
            .map_err(|SendError(err)| match err {
                Message::Data(d) => d,
                _ => unreachable!(""),
            })
    }

    fn close(&self) {
        if !self.is_closed.get() {
            self.is_closed.replace(true);
            if self.peers.fetch_sub(1, Ordering::Relaxed) == 1 {
                self.inner.send(Message::Close).ok();
            }
        }
    }
}

impl<T: Send> MPMCReceiver<T> for MessageReceiver<T> {
    fn recv(&self) -> io::Result<T> {
        self.check()?;
        match self.inner.recv() {
            Ok(Message::Data(d)) => Ok(d),
            Ok(Message::Close) => {
                self.exhausted();
                Err(io::Error::from(io::ErrorKind::BrokenPipe))
            }
            Err(_err) => {
                error!("MessageReceiver#recv disconnected: {:?}", backtrace::Backtrace::new());
                Err(io::Error::from(io::ErrorKind::UnexpectedEof))
            }
        }
    }

    fn try_recv(&self) -> io::Result<Option<T>> {
        self.check()?;
        match self.inner.try_recv() {
            Ok(Message::Data(d)) => Ok(Some(d)),
            Ok(Message::Close) => {
                self.exhausted();
                Err(io::Error::from(io::ErrorKind::BrokenPipe))
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => {
                error!("MessageReceiver#try_recv disconnected: {:?}", backtrace::Backtrace::new());
                Err(io::Error::from(io::ErrorKind::UnexpectedEof))
            }
        }
    }

    fn recv_timeout(&self, timeout: Duration) -> io::Result<T> {
        self.check()?;
        match self.inner.recv_timeout(timeout) {
            Ok(Message::Data(d)) => Ok(d),
            Ok(Message::Close) => {
                self.exhausted();
                Err(io::Error::from(io::ErrorKind::BrokenPipe))
            }
            Err(RecvTimeoutError::Timeout) => Err(io::Error::from(io::ErrorKind::TimedOut)),
            Err(RecvTimeoutError::Disconnected) => {
                error!("MessageReceiver#recv_timeout disconnected: {:?}", backtrace::Backtrace::new());
                Err(io::Error::from(io::ErrorKind::UnexpectedEof))
            }
        }
    }
}

pub struct InterruptRecv<T, R: MPMCReceiver<T>> {
    rx: R,
    interrupt: Arc<AtomicBool>,
    _ph: std::marker::PhantomData<T>,
}

pub struct InterruptSend<T, S: MPMCSender<T>> {
    tx: S,
    interrupt: Arc<AtomicBool>,
    _ph: std::marker::PhantomData<T>,
}

///// Only the marker `_ph` is not `Sync`, but it can be ignore;
//unsafe impl<T: Send, S: MPMCSender<T>> Sync for InterruptSend<T, S> {}

impl<T: Send, S: MPMCSender<T>> MPMCSender<T> for InterruptSend<T, S> {
    fn send(&self, entry: T) -> Result<(), T> {
        if self.interrupt.load(Ordering::Relaxed) {
            Err(entry)
        } else {
            self.tx.send(entry)
        }
    }

    fn close(&self) {
        self.tx.close()
    }
}

impl<T: Send, S: MPMCSender<T>> Clone for InterruptSend<T, S> {
    fn clone(&self) -> Self {
        InterruptSend {
            tx: self.tx.clone(),
            interrupt: self.interrupt.clone(),
            _ph: std::marker::PhantomData,
        }
    }
}

impl<T, R: MPMCReceiver<T>> InterruptRecv<T, R> {
    /// Prevent the sending parts from sending any new messages into communication_old;
    ///
    /// The consumer of the communication_old may be in fail over;
    /// After invoking this function, and `send` function invoked by the sending parts will fail and
    /// get a `Result::Err` with the sending message;
    pub fn interrupt(&self) {
        self.interrupt.store(true, Ordering::Relaxed);
    }

    /// Remove the prevent mechanism;
    ///
    /// The consumer of communication_old may be ready to serving after fail over;
    pub fn recover(&self) {
        self.interrupt.store(false, Ordering::Relaxed);
    }
}

impl<T, R: MPMCReceiver<T>> Clone for InterruptRecv<T, R> {
    fn clone(&self) -> Self {
        InterruptRecv {
            rx: self.rx.clone(),
            interrupt: self.interrupt.clone(),
            _ph: std::marker::PhantomData,
        }
    }
}

impl<T: Send, R: MPMCReceiver<T>> MPMCReceiver<T> for InterruptRecv<T, R> {
    #[inline(always)]
    fn recv(&self) -> io::Result<T> {
        self.rx.recv()
    }

    #[inline(always)]
    fn try_recv(&self) -> io::Result<Option<T>> {
        self.rx.try_recv()
    }

    #[inline(always)]
    fn recv_timeout(&self, time: Duration) -> io::Result<T> {
        self.rx.recv_timeout(time)
    }
}

/// Create a communication_old with a pair of sending and receiving parts;
///
/// Be different from the typical mpmc communication_old, this communication_old introduces a new feature that the receiving
/// part can prevent the sending part from sending new messages into communication_old by invoking the `interrupt`
/// function, and invoking the `recover` function to cancel the ban;
pub fn interrupt_channel<T: Send>() -> (InterruptSend<T, Sender<T>>, InterruptRecv<T, Receiver<T>>) {
    let (tx, rx) = crossbeam_channel::unbounded();
    let interrupt = Arc::new(AtomicBool::new(false));
    let tx = InterruptSend { tx, interrupt: interrupt.clone(), _ph: std::marker::PhantomData };
    let rx = InterruptRecv { rx, interrupt, _ph: std::marker::PhantomData };
    (tx, rx)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn interrupt_channel_test() {
        let (tx, rx) = interrupt_channel();
        tx.send(0u32).unwrap();
        tx.send(1u32).unwrap();
        rx.interrupt();
        assert_eq!(0, rx.recv().unwrap());
        assert_eq!(1, rx.recv().unwrap());
        let r = tx.send(2u32);
        assert_eq!(r, Err(2));
        rx.recover();
        tx.send(3u32).unwrap();
        assert_eq!(3, rx.recv().unwrap());
    }

    #[test]
    fn message_channel() {
        let (tx, rx) = unbound();
        let tx_1 = tx.clone();
        tx.send(1).unwrap();
        tx.send(1).unwrap();
        tx_1.send(1).unwrap();
        tx_1.send(1).unwrap();
        let mut count = 0;
        while let Ok(Some(m)) = rx.try_recv() {
            count += 1;
            assert_eq!(1, m);
        }
        assert_eq!(4, count);
        let tx_2 = tx.clone();
        tx.close();
        tx_1.close();
        tx_2.send(2).unwrap();
        assert_eq!(2, rx.recv().unwrap());
        tx_2.send(3).unwrap();
        assert_eq!(3, rx.recv_timeout(Duration::from_secs(1)).unwrap());
        tx_2.close();

        match rx.recv() {
            Err(err) => {
                assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
            }
            _ => panic!("broken pipe not detected"),
        }
    }

    #[test]
    fn message_channel_error() {
        let (tx, rx) = unbound();
        let tx_1 = tx.clone();
        tx.send(0).unwrap();
        tx_1.poison();
        tx_1.close();
        match rx.try_recv() {
            Err(err) => {
                assert_eq!(err.kind(), io::ErrorKind::ConnectionAborted)
            }
            _ => panic!("send poison undetected"),
        }
    }

    #[test]
    fn message_channel_error_drop() {
        let (tx, rx) = unbound();
        let tx_1 = tx.clone();
        tx.send(0).unwrap();
        std::mem::drop(tx);
        tx_1.send(0).unwrap();
        tx_1.close();
        match rx.try_recv() {
            Err(err) => {
                assert_eq!(err.kind(), io::ErrorKind::ConnectionAborted)
            }
            _ => panic!("send poison undetected"),
        }
    }
}
