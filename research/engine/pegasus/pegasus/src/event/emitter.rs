use std::cell::RefCell;
use std::collections::VecDeque;

use pegasus_common::rc::RcPointer;

use crate::communication::IOResult;
use crate::data_plane::{GeneralPull, GeneralPush, Pull, Push};
use crate::errors::IOError;
use crate::event::Event;

#[derive(Clone)]
pub struct EventEmitter {
    tx: RcPointer<RefCell<Vec<GeneralPush<Event>>>>,
}

impl EventEmitter {
    pub fn new(tx: Vec<GeneralPush<Event>>) -> Self {
        EventEmitter { tx: RcPointer::new(RefCell::new(tx)) }
    }

    pub fn peers(&self) -> usize {
        self.tx.borrow().len()
    }

    pub fn send(&mut self, target: u32, event: Event) -> IOResult<()> {
        let offset = target as usize;
        let mut borrow = self.tx.borrow_mut();
        trace_worker!("EventBus: send {:?} to {} port {:?};", event.kind, target, event.target_port);
        borrow[offset].push(event)
    }

    pub fn broadcast(&mut self, event: Event) -> IOResult<()> {
        trace_worker!("EventBus: broadcast {:?} to port {:?}", event.kind, event.target_port);
        let mut borrow = self.tx.borrow_mut();
        for i in 1..borrow.len() {
            borrow[i].push(event.clone())?;
        }
        borrow[0].push(event)?;
        Ok(())
    }

    pub fn flush(&mut self) -> IOResult<()> {
        let mut borrow = self.tx.borrow_mut();
        for p in borrow.iter_mut() {
            p.flush()?;
        }
        Ok(())
    }

    pub fn close(&mut self) -> IOResult<()> {
        let mut borrow = self.tx.borrow_mut();
        for p in borrow.iter_mut() {
            p.close()?;
        }
        Ok(())
    }
}

pub struct EventCollector {
    rx: GeneralPull<Event>,
    received: VecDeque<Event>,
}

impl EventCollector {
    pub fn new(rx: GeneralPull<Event>) -> Self {
        EventCollector { rx, received: VecDeque::new() }
    }
}

impl EventCollector {
    pub fn collect(&mut self) -> Result<bool, IOError> {
        while let Some(event) = self.rx.next()? {
            self.received.push_back(event);
        }
        Ok(!self.received.is_empty())
    }

    #[allow(dead_code)]
    #[inline]
    pub fn has_updates(&self) -> bool {
        !self.received.is_empty()
    }

    pub fn get_updates(&mut self) -> &mut VecDeque<Event> {
        &mut self.received
    }
}
