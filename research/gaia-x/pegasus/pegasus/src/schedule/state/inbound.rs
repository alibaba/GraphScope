use crate::communication::IOResult;
use crate::data::MicroBatch;
use crate::data_plane::{GeneralPush, Push};
use crate::graph::Port;
use crate::progress::{EndSignal, Weight};
use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};

#[allow(dead_code)]
struct ScopeEndPanel {
    tag: Tag,
    expect_weight: Option<Weight>,
    update_weight: Option<Weight>,
    current_weight: Weight,
    is_exhaust: bool,
}

impl ScopeEndPanel {
    fn new(tag: Tag) -> Self {
        ScopeEndPanel {
            tag,
            expect_weight: None,
            update_weight: None,
            current_weight: Weight::partial_empty(),
            is_exhaust: false,
        }
    }

    fn update_end(&mut self, src: u32, end: EndSignal) -> Option<EndSignal> {
        let (_, weight, update) = end.take();
        if let Some(ref pre) = self.expect_weight {
            assert_eq!(pre, &weight, "conflict weight;");
        } else {
            self.expect_weight = Some(weight);
            self.update_weight = update;
            self.add_end_source(src);
            return None;
        }

        if let Some(update) = update {
            if let Some(ref mut pre_update) = self.update_weight {
                pre_update.merge(update);
            } else {
                self.update_weight = Some(update);
            }
        }

        self.add_end_source(src)
    }

    fn add_end_source(&mut self, src: u32) -> Option<EndSignal> {
        self.current_weight.add_source(src);
        // debug_worker!("current weight {:?}", self.current_weight);
        if let Some(expect) = self.expect_weight.take() {
            if self.current_weight >= expect {
                self.is_exhaust = true;
                let mut end = EndSignal::new(self.tag.clone(), expect);
                if let Some(update) = self.update_weight.take() {
                    end.update_to(update);
                }
                Some(end)
            } else {
                self.expect_weight = Some(expect);
                None
            }
        } else {
            None
        }
    }
}

pub trait InputEndNotify: Send + 'static {
    fn notify(&mut self, end: EndSignal) -> IOResult<()>;
}

impl<T: Data> InputEndNotify for GeneralPush<MicroBatch<T>> {
    fn notify(&mut self, mut end: EndSignal) -> IOResult<()> {
        end.update();
        assert_eq!(end.update_weight, None);
        let mut d = MicroBatch::empty();
        d.tag = end.tag.clone();
        d.end = Some(end);
        if d.tag.is_root() {
            self.push(d)?;
            self.close()
            // Ok(())
        } else {
            self.push(d)
        }
    }
}

pub struct InboundStreamState {
    port: Port,
    scope_level: u32,
    notify_guards: Vec<TidyTagMap<ScopeEndPanel>>,
    notify: Box<dyn InputEndNotify>,
}

impl InboundStreamState {
    pub fn new(port: Port, scope_level: u32, notify: Box<dyn InputEndNotify>) -> Self {
        let mut notify_guards = Vec::new();
        for i in 0..scope_level + 1 {
            notify_guards.push(TidyTagMap::new(i));
        }
        InboundStreamState { port, scope_level, notify_guards, notify }
    }

    pub fn on_end(&mut self, src: u32, end: EndSignal) -> IOResult<()> {
        //debug_worker!("accept eos {:?} from {}", end, src);
        assert!(end.source_weight.value() > 1, "source weight = 1 should be passed with data;");
        let idx = end.tag.len();
        assert!(idx <= self.scope_level as usize);
        let tag = end.tag.clone();
        if idx < self.scope_level as usize {
            // this is an end of parent scope;
            let mut notify_guards = std::mem::replace(&mut self.notify_guards, vec![]);
            for i in idx + 1..self.scope_level as usize + 1 {
                for (t, p) in notify_guards[i].iter_mut() {
                    if tag.is_parent_of(&*t) {
                        if let Some(end) = p.add_end_source(src) {
                            self.notify.notify(end)?;
                        }
                    }
                }
            }
            // TODO: clean notify guards; (only retain not exhaust;)
            self.notify_guards = notify_guards;
        }

        if let Some(mut p) = self.notify_guards[idx].remove(&end.tag) {
            if let Some(e) = p.update_end(src, end) {
                trace_worker!("in port {:?} get end of {:?}", self.port, e.tag);
                self.notify.notify(e)?;
            } else {
                trace_worker!(
                    "in port {:?} partial end of {:?}, expect {:?}, current {:?};",
                    self.port,
                    p.tag,
                    p.expect_weight,
                    p.current_weight
                );
                self.notify_guards[idx].insert(tag, p);
            }
        } else {
            let mut p = ScopeEndPanel::new(tag.clone());
            p.update_end(src, end);
            self.notify_guards[idx].insert(tag, p);
        }
        Ok(())
    }
}
