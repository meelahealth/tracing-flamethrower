use std::{
    collections::HashMap,
    io::{self},
    sync::{Arc, Mutex},
    time::Instant,
};

use tracing::{Metadata, Subscriber, span};
use tracing_subscriber::{
    Layer,
    layer::Context,
    registry::{LookupSpan, SpanRef},
};

#[derive(Default)]
pub struct StackLineWriteOptions {
    module_path: bool,
    file_and_line: bool,
}

#[derive(Clone)]
pub struct StackIter<'entry> {
    frames: &'entry [SpanFrame<OpenFrame>],
}

impl<'entry> StackIter<'entry> {
    fn new(parent_id: &Option<span::Id>, frames: &'entry [SpanFrame<OpenFrame>]) -> Self {
        let start_idx = parent_id
            .as_ref()
            .and_then(|parent_id| frames.iter().position(|f| f.id == *parent_id));
        let frames = if let Some(start_idx) = start_idx {
            &frames[..=start_idx]
        } else {
            &[]
        };
        Self { frames }
    }
}

impl<'entry> Iterator for StackIter<'entry> {
    type Item = &'entry SpanFrame<OpenFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        let top = self.frames.last()?;
        if let Some(parent_id) = &top.parent {
            let idx = self
                .frames
                .iter()
                .position(|f| f.id == *parent_id)
                .unwrap_or_default();
            self.frames = &self.frames[..=idx];
            Some(top)
        } else {
            self.frames = &[];
            Some(top)
        }
    }
}

impl<'entry> DoubleEndedIterator for StackIter<'entry> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let top = self.frames.last()?;
        for (i, frame) in self.frames.iter().enumerate() {
            if frame.id == top.id {
                self.frames = &[];
                return Some(frame);
            }
            // Search frame via relation from top of stack
            let mut find_id = top.parent.as_ref()?;
            for f in self.frames[i..].iter().rev() {
                if f.id == *find_id {
                    if f.id == frame.id {
                        self.frames = &self.frames[(i + 1)..];
                        return Some(frame);
                    } else if let Some(parent_id) = f.parent.as_ref() {
                        find_id = parent_id;
                    } else {
                        break;
                    }
                }
            }
        }
        None
    }
}

#[derive(Debug)]
enum StackLinesResult {
    Start {
        root_id: span::Id,
        metadata: &'static Metadata<'static>,
    },
    Frame {
        root_id: span::Id,
        frame: SpanFrame<TimedFrame>,
        closed: bool,
    },
    LastFrame {
        root_id: span::Id,
        frame: SpanFrame<TimedFrame>,
    },
}

pub trait FlamethrowerSink {
    fn begin(&self, root_id: &span::Id, metadata: &'static Metadata<'static>);
    fn frame(
        &self,
        root_id: &span::Id,
        stack: StackIter<'_>,
        frame: SpanFrame<TimedFrame>,
        closed: bool,
    );
    fn end(&self, root_id: &span::Id);
}

#[derive(Debug, Clone)]
pub struct OpenFrame;
#[derive(Debug, Clone)]
pub struct TimedFrame {
    pub tip_index: u32,
    pub samples: u32,
}

#[derive(Debug, Clone)]
pub struct SpanFrame<State = OpenFrame> {
    pub id: span::Id,
    pub parent: Option<span::Id>,
    pub metadata: &'static Metadata<'static>,
    pub state: State,
}

impl SpanFrame<OpenFrame> {
    fn new<'span, S: Subscriber + LookupSpan<'span>>(span: &SpanRef<'span, S>) -> SpanFrame {
        SpanFrame {
            id: span.id(),
            parent: span.parent().map(|p| p.id()),
            metadata: span.metadata(),
            state: OpenFrame,
        }
    }

    fn time(self, tip_index: u32, samples: u32) -> SpanFrame<TimedFrame> {
        SpanFrame {
            id: self.id,
            parent: self.parent,
            metadata: self.metadata,
            state: TimedFrame { tip_index, samples },
        }
    }

    fn write<W: io::Write>(&self, w: &mut W, opts: &StackLineWriteOptions) -> io::Result<()> {
        if opts.module_path
            && let Some(module_path) = self.metadata.module_path()
        {
            write!(w, "{}::", module_path)?;
        }
        write!(w, "{}", self.metadata.name())?;
        if opts.file_and_line {
            if let Some(file) = self.metadata.file() {
                write!(w, ":{}", file)?;
            }
            if let Some(line) = self.metadata.line() {
                write!(w, ":{}", line)?;
            }
        }
        write!(w, ";")?;
        Ok(())
    }
}

impl SpanFrame<TimedFrame> {
    pub fn write<W: io::Write>(
        &self,
        w: &mut W,
        stack: StackIter<'_>,
        opts: &StackLineWriteOptions,
    ) -> io::Result<()> {
        write!(w, "thread-{};", self.state.tip_index)?;
        for f in stack.rev() {
            f.write(w, opts)?;
        }
        if opts.module_path
            && let Some(module_path) = self.metadata.module_path()
        {
            write!(w, "{}::", module_path)?;
        }
        write!(w, "{}", self.metadata.name())?;
        if opts.file_and_line {
            if let Some(file) = self.metadata.file() {
                write!(w, ":{}", file)?;
            }
            if let Some(line) = self.metadata.line() {
                write!(w, ":{}", line)?;
            }
        }
        write!(w, " {}", self.state.samples)?;
        Ok(())
    }
}

pub struct StackLines {
    root_id: span::Id,
    open: Vec<SpanFrame<OpenFrame>>,
    tips: HashMap<span::Id, (u32, Instant)>,
}

impl StackLines {
    fn clear(&mut self) {
        self.open.clear();
        self.tips.clear();
    }
}

#[derive(Default)]
struct StackLinesStorage {
    entries: Vec<StackLines>,
    free: Vec<StackLines>,
}

pub struct FlamethrowerLayer<FS: FlamethrowerSink> {
    flame_starter_field: &'static str,
    sink: FS,
    storage: Arc<Mutex<StackLinesStorage>>,
}

impl<FS: FlamethrowerSink + 'static> FlamethrowerLayer<FS> {
    pub fn new(flame_starter_field: &'static str, sink: FS) -> Self {
        Self {
            flame_starter_field,
            sink,
            storage: Arc::new(Mutex::new(StackLinesStorage::default())),
        }
    }

    fn handle(&self, storage: &StackLinesStorage, result: StackLinesResult) {
        match result {
            StackLinesResult::Start { root_id, metadata } => self.sink.begin(&root_id, metadata),
            StackLinesResult::Frame {
                root_id,
                frame,
                closed,
            } => {
                if let Some(stack) = storage
                    .entries
                    .iter()
                    .find(|e| e.root_id == root_id)
                    .map(|e| StackIter::new(&frame.parent, &e.open))
                {
                    self.sink.frame(&root_id, stack, frame, closed);
                }
            }
            StackLinesResult::LastFrame { root_id, frame } => {
                if let Some(stack) = storage
                    .entries
                    .iter()
                    .find(|e| e.root_id == root_id)
                    .map(|e| StackIter::new(&frame.parent, &e.open))
                {
                    self.sink.frame(&root_id, stack, frame, true);
                }
                self.sink.end(&root_id)
            }
        }
    }
}

impl<S: Subscriber + for<'span> LookupSpan<'span>, FS: FlamethrowerSink + 'static> Layer<S>
    for FlamethrowerLayer<FS>
{
    fn on_enter(&self, id: &span::Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let mut storage = self.storage.lock().unwrap();
            let entry = span.parent().and_then(|parent| {
                storage.entries.iter_mut().find_map(|l| {
                    l.open
                        .iter_mut()
                        .position(|s| s.id == parent.id())
                        .map(|i| (l, i))
                })
            });
            if let Some((entry, frame_index)) = entry {
                let now = Instant::now();
                let frame = &entry.open[frame_index];
                if let Some((tip_index, instant)) = entry.tips.remove(&frame.id) {
                    // Launched from tip, replace
                    let samples = (now - instant).as_micros() as u32;
                    let frame = frame.clone().time(tip_index, samples);
                    entry.tips.insert(span.id(), (tip_index, now));
                    entry.open.push(SpanFrame::new(&span));
                    let root_id = entry.root_id.clone();
                    self.handle(
                        &storage,
                        StackLinesResult::Frame {
                            root_id,
                            frame,
                            closed: false,
                        },
                    );
                } else {
                    // New tip
                    let tip_index = entry
                        .tips
                        .values()
                        .map(|(idx, _)| *idx)
                        .max()
                        .unwrap_or_default()
                        + 1;
                    entry.tips.insert(span.id(), (tip_index, now));
                    entry.open.push(SpanFrame::new(&span));
                }
            } else if span
                .metadata()
                .fields()
                .field(self.flame_starter_field)
                .is_some()
            {
                let mut entry = if let Some(mut entry) = storage.free.pop() {
                    entry.root_id = span.id();
                    entry
                } else {
                    StackLines {
                        root_id: span.id(),
                        open: Vec::new(),
                        tips: HashMap::new(),
                    }
                };
                let now = Instant::now();
                let tip_index = entry
                    .tips
                    .values()
                    .map(|(idx, _)| *idx)
                    .max()
                    .unwrap_or_default()
                    + 1;
                entry.tips.insert(span.id(), (tip_index, now));
                entry.open.push(SpanFrame::new(&span));
                storage.entries.push(entry);

                self.handle(
                    &storage,
                    StackLinesResult::Start {
                        root_id: span.id(),
                        metadata: span.metadata(),
                    },
                );
            } else {
                // Untracked event
            }
        }
    }

    fn on_exit(&self, id: &span::Id, _ctx: Context<'_, S>) {
        let mut storage = self.storage.lock().unwrap();
        let entry = storage
            .entries
            .iter_mut()
            .enumerate()
            .find_map(|(index, l)| {
                l.open
                    .iter_mut()
                    .position(|s| &s.id == id)
                    .map(|i| (index, l, i))
            });
        if let Some((index, entry, frame_index)) = entry {
            if let Some((tip_index, instant)) = entry.tips.remove(id) {
                let now = Instant::now();
                let samples = now - instant;
                let frame = entry
                    .open
                    .remove(frame_index)
                    .time(tip_index, samples.as_micros() as u32);
                let parent = frame
                    .parent
                    .as_ref()
                    .and_then(|id| entry.open.iter_mut().find(|f| f.id == *id));
                if let Some(parent) = parent {
                    if !entry.tips.contains_key(&parent.id) {
                        entry.tips.insert(parent.id.clone(), (tip_index, now));
                    } else {
                        // Parent already running, merge
                    }
                }

                let root_id = entry.root_id.clone();
                if entry.tips.is_empty() {
                    // No parent, no tips, stacklines completed
                    self.handle(&storage, StackLinesResult::LastFrame { root_id, frame });
                    let mut entry = storage.entries.remove(index);
                    entry.clear();
                    storage.free.push(entry);
                } else {
                    self.handle(
                        &storage,
                        StackLinesResult::Frame {
                            root_id,
                            frame,
                            closed: true,
                        },
                    );
                }
            } else {
                unreachable!("Tip not running, yet still open? {:?}", id);
            }
        } else {
            // Untracked event
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Write, sync::Mutex};

    use tracing::{instrument, span};
    use tracing_subscriber::{Registry, layer::SubscriberExt};

    use super::*;

    struct DummyFlameSink {
        bufs: Mutex<HashMap<span::Id, Vec<u8>>>,
    }

    impl DummyFlameSink {
        fn new() -> Self {
            Self {
                bufs: Mutex::new(HashMap::new()),
            }
        }
    }

    impl FlamethrowerSink for DummyFlameSink {
        fn begin(&self, root_id: &span::Id, _metadata: &'static tracing::Metadata<'static>) {
            let exists = self
                .bufs
                .lock()
                .unwrap()
                .insert(root_id.clone(), Vec::new())
                .is_some();
            debug_assert!(!exists, "Accidental double begin or missing cleanup");
        }

        fn frame(
            &self,
            root_id: &tracing::span::Id,
            stack: StackIter<'_>,
            frame: SpanFrame<TimedFrame>,
            closed: bool,
        ) {
            let mut bufs = self.bufs.lock().unwrap();
            let mut buf = bufs
                .get_mut(root_id)
                .expect("Missing entry, either no begin or end already happened");
            frame
                .write(&mut buf, stack, &StackLineWriteOptions::default())
                .expect("Failed write");
            writeln!(&mut buf, " closed:{closed}").expect("Failed write");
        }

        fn end(&self, root_id: &tracing::span::Id) {
            let buf = self
                .bufs
                .lock()
                .unwrap()
                .remove(root_id)
                .expect("Missing entry, either no begin or end already happened");
            let str = str::from_utf8(&buf).expect("Failed utf8 convert");
            eprintln!("{}", str);
        }
    }

    #[instrument()]
    fn test_a() {}

    #[instrument()]
    fn test_b() {
        test_a()
    }

    #[instrument(fields(trace_flame = true))]
    fn flame_traced() {
        test_a();
        test_b();
    }

    #[instrument(fields(trace_flame = true))]
    fn also_flame_traced() {}

    #[test]
    pub fn test_trace_basic() -> Result<(), Box<dyn std::error::Error>> {
        let flamethrower = FlamethrowerLayer::new("trace_flame", DummyFlameSink::new());
        tracing::subscriber::set_global_default(Registry::default().with(flamethrower))
            .expect("Setting default subscriber failed");

        flame_traced();
        also_flame_traced();

        Ok(())
    }
}
