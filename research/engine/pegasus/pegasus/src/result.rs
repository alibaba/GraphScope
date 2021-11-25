use std::error::Error;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender, TryRecvError};
use dyn_clonable::*;

use crate::api::function::FnResult;
use crate::api::FromStream;

#[clonable]
pub trait FromStreamExt<T>: FromStream<T> + Clone {
    fn on_error(&mut self, error: Box<dyn Error + Send>);
}

pub struct ResultSink<T> {
    cancel: Arc<AtomicBool>,
    kind: ResultSinkKind<T>,
}

pub enum ResultSinkKind<T> {
    Default(DefaultResultSink<T>),
    Customized(Box<dyn FromStreamExt<T>>),
}

impl<T> ResultSink<T> {
    pub fn new(tx: Sender<Result<T, Box<dyn Error + Send>>>) -> Self {
        ResultSink {
            cancel: Arc::new(AtomicBool::new(false)),
            kind: ResultSinkKind::Default(DefaultResultSink::new(tx)),
        }
    }

    pub fn with<F>(sink: F) -> Self
    where
        F: FromStreamExt<T>,
    {
        ResultSink {
            cancel: Arc::new(AtomicBool::new(false)),
            kind: ResultSinkKind::Customized(Box::new(sink)),
        }
    }

    pub fn get_cancel_hook(&self) -> &Arc<AtomicBool> {
        &self.cancel
    }

    pub fn on_error<E: std::error::Error + Send + 'static>(&mut self, error: E) {
        match &mut self.kind {
            ResultSinkKind::Default(tx) => {
                tx.tx.send(Err(Box::new(error))).ok();
            }
            ResultSinkKind::Customized(tx) => {
                tx.on_error(Box::new(error));
            }
        }
    }
}

impl<T: Send + Debug + 'static> FromStream<T> for ResultSink<T> {
    fn on_next(&mut self, next: T) -> FnResult<()> {
        match &mut self.kind {
            ResultSinkKind::Default(tx) => tx.on_next(next),
            ResultSinkKind::Customized(tx) => tx.on_next(next),
        }
    }
}

impl<T> Clone for ResultSink<T> {
    fn clone(&self) -> Self {
        let kind = match &self.kind {
            ResultSinkKind::Default(tx) => ResultSinkKind::Default(tx.clone()),
            ResultSinkKind::Customized(tx) => ResultSinkKind::Customized(tx.clone()),
        };
        ResultSink { cancel: self.cancel.clone(), kind }
    }
}

pub struct DefaultResultSink<T> {
    tx: Sender<Result<T, Box<dyn Error + Send>>>,
}

pub struct ResultStream<T> {
    pub job_id: u64,
    is_exhaust: AtomicBool,
    is_poison: AtomicBool,
    cancel_hook: Arc<AtomicBool>,
    rx: Receiver<Result<T, Box<dyn Error + Send>>>,
}

impl<T> DefaultResultSink<T> {
    pub fn new(tx: Sender<Result<T, Box<dyn Error + Send>>>) -> Self {
        DefaultResultSink { tx }
    }
}

impl<T> Clone for DefaultResultSink<T> {
    fn clone(&self) -> Self {
        DefaultResultSink { tx: self.tx.clone() }
    }
}

impl<T: Send + Debug + 'static> FromStream<T> for DefaultResultSink<T> {
    fn on_next(&mut self, next: T) -> FnResult<()> {
        self.tx.send(Ok(next)).ok();
        Ok(())
    }
}

impl<T> ResultStream<T> {
    pub fn new(
        job_id: u64, cancel_hook: Arc<AtomicBool>, rx: Receiver<Result<T, Box<dyn Error + Send>>>,
    ) -> Self {
        ResultStream {
            job_id,
            is_exhaust: AtomicBool::new(false),
            is_poison: AtomicBool::new(false),
            cancel_hook,
            rx,
        }
    }

    #[inline]
    pub fn is_exhaust(&self) -> bool {
        self.is_exhaust.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn is_poison(&self) -> bool {
        self.is_poison.load(Ordering::SeqCst)
    }

    fn pull_next(&mut self) -> Option<Result<T, Box<dyn Error + Send>>> {
        if self.is_exhaust.load(Ordering::SeqCst) {
            return None;
        }

        if self.is_poison.load(Ordering::SeqCst) {
            let err_msg = "ResultSteam is poison because error already occurred;".to_owned();
            let err: Box<dyn Error + Send + Sync> = err_msg.into();
            return Some(Err(err as Box<dyn Error + Send>));
        }

        // block receive until new message available;
        match self.rx.recv() {
            Ok(Ok(res)) => Some(Ok(res)),
            Ok(Err(e)) => {
                self.is_poison.store(true, Ordering::SeqCst);
                Some(Err(e))
            }
            Err(_) => {
                self.is_exhaust.store(true, Ordering::SeqCst);
                None
            }
        }
    }

    pub fn try_next(&self) -> Option<Result<Option<T>, Box<dyn Error + Send>>> {
        if self.is_exhaust.load(Ordering::SeqCst) {
            return None;
        }

        if self.is_poison.load(Ordering::SeqCst) {
            let err_msg = "ResultSteam is poison because error already occurred;".to_owned();
            let err: Box<dyn Error + Send + Sync> = err_msg.into();
            return Some(Err(err as Box<dyn Error + Send>));
        }

        match self.rx.try_recv() {
            Ok(Ok(res)) => Some(Ok(Some(res))),
            Ok(Err(e)) => {
                self.is_poison.store(true, Ordering::SeqCst);
                Some(Err(e))
            }
            Err(TryRecvError::Empty) => Some(Ok(None)),
            Err(TryRecvError::Disconnected) => {
                self.is_exhaust.store(true, Ordering::SeqCst);
                None
            }
        }
    }

    pub fn cancel(&self) {
        self.cancel_hook.store(true, Ordering::SeqCst)
    }
}

/// blocking iterator which will block on `next` if message is not available;
impl<T> Iterator for ResultStream<T> {
    type Item = Result<T, Box<dyn Error + Send>>;

    fn next(&mut self) -> Option<Self::Item> {
        ResultStream::pull_next(self)
    }
}

impl<T> Drop for ResultStream<T> {
    fn drop(&mut self) {
        if !self.is_exhaust.load(Ordering::SeqCst) {
            self.cancel_hook.store(true, Ordering::SeqCst);
        }
    }
}
