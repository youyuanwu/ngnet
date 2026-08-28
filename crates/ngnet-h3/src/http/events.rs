//! What the state machine's handlers record, and how the driver reads it back.
//!
//! nghttp3 reports everything through callbacks, and this crate's core turns those into
//! closures that receive a caller-chosen context. The driver's context is [`Events`]. The
//! handlers do as little as possible — they accumulate — and the driver dispatches
//! afterwards, because a handler runs inside an FFI call where the backend is either
//! mutably borrowed or out of scope, and because a panic in one would cross a C frame and
//! abort the process.

use bytes::Bytes;
use core::ops::Range;

use crate::handlers::{FieldSection, PeerSettings, Shutdown, StreamClosed};
use crate::stream::StreamId;

/// Something the state machine reported.
#[derive(Debug)]
pub(crate) enum Observation {
    /// A complete field section arrived.
    Head {
        stream: StreamId,
        section: FieldSection,
        fields: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// Body bytes arrived.
    Data {
        stream: StreamId,
        bytes: InboundData,
    },
    /// The peer will send nothing further on this stream.
    End { stream: StreamId },
    /// The state machine has finished with this stream.
    Closed {
        stream: StreamId,
        closed: StreamClosed,
    },
    /// The peer began a graceful shutdown.
    Shutdown(Shutdown),
    /// The peer's settings arrived.
    Settings(PeerSettings),
}

/// Body storage while an nghttp3 callback is being turned into an observation.
#[derive(Debug)]
pub(crate) enum InboundData {
    /// Storage independent of the transport input.
    Ready(Bytes),
    /// A checked range of the transport input, resolved after the FFI call returns.
    Range(Range<usize>),
}

impl InboundData {
    /// Takes storage after the driver has resolved every input range.
    pub(crate) fn into_ready(self) -> Bytes {
        match self {
            Self::Ready(bytes) => bytes,
            Self::Range(_) => {
                unreachable!("the driver resolves callback ranges before dispatching them")
            }
        }
    }
}

/// The input address range callbacks may refer to during one state-machine read.
#[derive(Clone, Copy, Debug)]
struct Inbound {
    start: usize,
    end: usize,
    observed: usize,
}

/// A field section being accumulated.
#[derive(Default)]
struct Partial {
    section: Option<FieldSection>,
    fields: Vec<(Vec<u8>, Vec<u8>)>,
}

/// The driver's context, and the only thing the state machine's handlers can reach.
#[derive(Default)]
pub(crate) struct Events {
    /// Field sections in progress, at most one per stream at a time.
    partial: Vec<(StreamId, Partial)>,
    /// What has been observed, in the order it happened.
    pub(crate) observed: Vec<Observation>,
    /// The transport buffer the current read is being fed from.
    ///
    /// Set by the driver immediately before `read_stream` and cleared after. It is what
    /// makes copy-free delivery possible — and what makes the containment check necessary,
    /// because the bytes a handler receives are not always inside it.
    inbound: Option<Inbound>,
}

impl Events {
    /// Begins one read whose callbacks may refer to `bytes`.
    pub(crate) fn begin_inbound(&mut self, bytes: &Bytes) {
        debug_assert!(self.inbound.is_none());
        self.inbound = Some(Inbound {
            start: bytes.as_ptr() as usize,
            end: bytes.as_ptr() as usize + bytes.len(),
            observed: self.observed.len(),
        });
    }

    /// Resolves checked callback ranges after the state-machine read has returned.
    pub(crate) fn finish_inbound(&mut self, bytes: Bytes) {
        let Some(inbound) = self.inbound.take() else {
            return;
        };
        let ranges = self.observed[inbound.observed..]
            .iter()
            .filter(|observation| {
                matches!(
                    observation,
                    Observation::Data {
                        bytes: InboundData::Range(_),
                        ..
                    }
                )
            })
            .count();
        let mut parent = Some(bytes);

        for observation in &mut self.observed[inbound.observed..] {
            let Observation::Data { bytes: data, .. } = observation else {
                continue;
            };
            if !matches!(data, InboundData::Range(_)) {
                continue;
            }
            let InboundData::Range(range) =
                core::mem::replace(data, InboundData::Ready(Bytes::new()))
            else {
                unreachable!("the range was checked immediately before replacement");
            };
            *data = InboundData::Ready(if ranges == 1 {
                let parent = parent.take().expect("the one range has one parent");
                if range.start == 0 && range.end == parent.len() {
                    parent
                } else {
                    parent.slice(range)
                }
            } else {
                parent
                    .as_ref()
                    .expect("multiple ranges retain their parent")
                    .slice(range)
            });
        }
    }

    /// Begins a field section.
    pub(crate) fn begin_section(&mut self, stream: StreamId, section: FieldSection) {
        let slot = self.slot(stream);
        slot.section = Some(section);
        slot.fields.clear();
    }

    /// Records one field of the section in progress.
    pub(crate) fn push_field(&mut self, stream: StreamId, name: &[u8], value: &[u8]) {
        let slot = self.slot(stream);
        slot.fields.push((name.to_vec(), value.to_vec()));
    }

    /// Completes a field section.
    pub(crate) fn end_section(&mut self, stream: StreamId, section: FieldSection) {
        let Some(index) = self.partial.iter().position(|(s, _)| *s == stream) else {
            return;
        };
        let (_, partial) = self.partial.remove(index);
        self.observed.push(Observation::Head {
            stream,
            section,
            fields: partial.fields,
        });
    }

    /// Records body bytes.
    ///
    /// # Why this cannot simply take a view
    ///
    /// The obvious implementation is `Bytes::slice_ref`, taking a refcounted view into the
    /// buffer the transport produced. It is also a process-ending bug. `slice_ref` panics
    /// when the subslice is not inside the parent allocation, and the core promises only
    /// that the bytes are readable for the duration of the call — not that they came from
    /// what was just handed in. They demonstrably do not: when a stream's QPACK decoding is
    /// blocked, nghttp3 buffers the input and replays it later from its own memory, during a
    /// call that is feeding *a different stream entirely*. A panic there unwinds into a C
    /// frame, which aborts.
    ///
    /// So containment is checked rather than assumed, and a replayed chunk is copied. The
    /// common case — bytes arriving and being delivered straight away — still costs nothing.
    pub(crate) fn push_data(&mut self, stream: StreamId, chunk: &[u8]) {
        let bytes = if chunk.is_empty() {
            InboundData::Ready(Bytes::new())
        } else {
            match self.inbound.and_then(|parent| range_of(parent, chunk)) {
                Some(range) => InboundData::Range(range),
                None => InboundData::Ready(Bytes::copy_from_slice(chunk)),
            }
        };
        self.observed.push(Observation::Data { stream, bytes });
    }

    /// Records the end of a message.
    pub(crate) fn push_end(&mut self, stream: StreamId) {
        self.observed.push(Observation::End { stream });
    }

    /// Records that the state machine has finished with a stream.
    pub(crate) fn push_closed(&mut self, stream: StreamId, closed: StreamClosed) {
        self.partial.retain(|(s, _)| *s != stream);
        self.observed.push(Observation::Closed { stream, closed });
    }

    /// Discards close observations produced by one driver-initiated state-machine close.
    ///
    /// The driver has already applied that close and its side effects. Leaving the callback's
    /// observation queued would replay the same close later, after a sufficiently large batch
    /// may already have evicted its bounded late-release tombstone.
    pub(crate) fn discard_closed_since(&mut self, checkpoint: usize, stream: StreamId) {
        let mut index = checkpoint;
        while index < self.observed.len() {
            if matches!(
                self.observed[index],
                Observation::Closed {
                    stream: observed,
                    ..
                } if observed == stream
            ) {
                self.observed.remove(index);
            } else {
                index += 1;
            }
        }
    }

    /// Records a graceful shutdown.
    pub(crate) fn push_shutdown(&mut self, shutdown: Shutdown) {
        self.observed.push(Observation::Shutdown(shutdown));
    }

    /// Records the peer's settings.
    pub(crate) fn push_settings(&mut self, settings: PeerSettings) {
        self.observed.push(Observation::Settings(settings));
    }

    /// Takes everything observed so far.
    pub(crate) fn drain(&mut self) -> Vec<Observation> {
        core::mem::take(&mut self.observed)
    }

    /// Whether anything is waiting to be dispatched.
    pub(crate) fn is_empty(&self) -> bool {
        self.observed.is_empty()
    }

    fn slot(&mut self, stream: StreamId) -> &mut Partial {
        if let Some(index) = self.partial.iter().position(|(s, _)| *s == stream) {
            return &mut self.partial[index].1;
        }
        self.partial.push((stream, Partial::default()));
        let last = self.partial.len() - 1;
        &mut self.partial[last].1
    }
}

/// A refcounted view of `chunk` into `parent`, or a copy when it does not lie inside.
///
/// Address arithmetic only, no dereferencing: both slices are live for the duration of the
/// call, and all this decides is whether one is a subrange of the other.
fn range_of(parent: Inbound, chunk: &[u8]) -> Option<Range<usize>> {
    let chunk_start = chunk.as_ptr() as usize;
    let chunk_end = chunk_start + chunk.len();

    if chunk_start >= parent.start && chunk_end <= parent.end {
        Some((chunk_start - parent.start)..(chunk_end - parent.start))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(id: i64) -> StreamId {
        StreamId::new(id).expect("a stream")
    }

    #[test]
    fn one_whole_callback_moves_the_unique_parent() {
        let parent = Bytes::from(b"hello world".to_vec());
        let ptr = parent.as_ptr();
        let mut events = Events::default();
        events.begin_inbound(&parent);
        events.push_data(stream(0), &parent);
        events.finish_inbound(parent);
        let Observation::Data { bytes, .. } = events.observed.pop().expect("an observation") else {
            panic!("body data");
        };
        let bytes = bytes.into_ready();
        assert_eq!(&bytes[..], b"hello world");
        assert!(std::ptr::eq(bytes.as_ptr(), ptr));
    }

    #[test]
    fn a_proper_range_is_resolved_after_the_read() {
        let parent = Bytes::from_static(b"hello world");
        let mut events = Events::default();
        events.begin_inbound(&parent);
        events.push_data(stream(0), &parent[6..]);
        events.finish_inbound(parent);
        let Observation::Data { bytes, .. } = events.observed.pop().expect("an observation") else {
            panic!("body data");
        };
        assert_eq!(&bytes.into_ready()[..], b"world");
    }

    #[test]
    fn multiple_ranges_keep_order_and_offsets() {
        let parent = Bytes::from_static(b"hello world");
        let mut events = Events::default();
        events.begin_inbound(&parent);
        events.push_data(stream(0), &parent[..5]);
        events.push_data(stream(0), &parent[6..]);
        events.finish_inbound(parent);
        let mut observed = events.observed.into_iter();
        let Observation::Data { bytes: first, .. } = observed.next().expect("first") else {
            panic!("body data");
        };
        let Observation::Data { bytes: second, .. } = observed.next().expect("second") else {
            panic!("body data");
        };
        assert_eq!(&first.into_ready()[..], b"hello");
        assert_eq!(&second.into_ready()[..], b"world");
    }

    #[test]
    fn a_chunk_from_elsewhere_is_copied_rather_than_panicking() {
        // The QPACK replay case. `Bytes::slice_ref` would panic here, and a panic reached
        // from a state-machine handler crosses a C frame and aborts the process.
        let parent = Bytes::from_static(b"hello world");
        let foreign = b"entirely elsewhere".to_vec();
        let mut events = Events::default();
        events.begin_inbound(&parent);
        events.push_data(stream(0), &foreign);
        events.finish_inbound(parent);
        let Observation::Data { bytes, .. } = events.observed.pop().expect("an observation") else {
            panic!("body data");
        };
        assert_eq!(&bytes.into_ready()[..], b"entirely elsewhere");
    }

    #[test]
    fn an_empty_chunk_is_handled_without_arithmetic_on_a_dangling_pointer() {
        let parent = Bytes::from_static(b"hello");
        let mut events = Events::default();
        events.begin_inbound(&parent);
        events.push_data(stream(0), b"");
        events.finish_inbound(parent);
        let Observation::Data { bytes, .. } = events.observed.pop().expect("an observation") else {
            panic!("body data");
        };
        assert!(bytes.into_ready().is_empty());
    }

    #[test]
    fn fields_accumulate_per_stream_and_complete_in_order() {
        let mut events = Events::default();
        let a = StreamId::new(0).expect("a stream");
        let b = StreamId::new(4).expect("a stream");

        events.begin_section(a, FieldSection::Headers);
        events.begin_section(b, FieldSection::Headers);
        events.push_field(a, b":status", b"200");
        events.push_field(b, b":status", b"404");
        events.end_section(b, FieldSection::Headers);
        events.end_section(a, FieldSection::Headers);

        let observed = events.drain();
        assert_eq!(observed.len(), 2);
        match &observed[0] {
            Observation::Head { stream, fields, .. } => {
                assert_eq!(*stream, b);
                assert_eq!(fields[0].1, b"404");
            }
            other => panic!("expected b's head first, got {other:?}"),
        }
        match &observed[1] {
            Observation::Head { stream, fields, .. } => {
                assert_eq!(*stream, a);
                assert_eq!(fields[0].1, b"200");
            }
            other => panic!("expected a's head second, got {other:?}"),
        }
    }

    #[test]
    fn closing_a_stream_discards_a_half_built_section() {
        let mut events = Events::default();
        let stream = StreamId::new(0).expect("a stream");

        events.begin_section(stream, FieldSection::Headers);
        events.push_field(stream, b":status", b"200");
        events.push_closed(stream, StreamClosed::clean());

        // The half-built section is gone, so a later completion cannot resurrect it.
        events.end_section(stream, FieldSection::Headers);
        let observed = events.drain();
        assert_eq!(observed.len(), 1);
        assert!(matches!(observed[0], Observation::Closed { .. }));
    }

    #[test]
    fn discarding_one_close_preserves_other_observations() {
        let mut events = Events::default();
        let target = StreamId::new(0).expect("a stream");
        let other = StreamId::new(4).expect("a stream");

        events.push_closed(target, StreamClosed::clean());
        let checkpoint = events.observed.len();
        events.push_end(target);
        events.push_closed(other, StreamClosed::clean());
        events.push_closed(target, StreamClosed::clean());

        events.discard_closed_since(checkpoint, target);

        let observed = events.drain();
        assert_eq!(observed.len(), 3);
        assert!(matches!(
            observed[0],
            Observation::Closed { stream, .. } if stream == target
        ));
        assert!(matches!(
            observed[1],
            Observation::End { stream } if stream == target
        ));
        assert!(matches!(
            observed[2],
            Observation::Closed { stream, .. } if stream == other
        ));
    }
}
