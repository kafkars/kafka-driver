//! Reproducible hostile-byte and chunk-partition corpus for frame invariants.

use std::num::NonZeroUsize;

use super::{FrameDecodeError, FrameDecoder, FrameLimits};

const MAX_FRAME_BYTES: usize = 32;
const MAX_BUFFERED_BYTES: usize = 64;
const ARBITRARY_CASES: u64 = 4_096;
const VALID_CASES: u64 = 1_024;

#[test]
fn arbitrary_stream_corpus_preserves_bounds_and_terminal_failure() {
    for seed in 1..=ARBITRARY_CASES {
        let mut random = Random::new(seed);
        let length = random.bounded(160);
        let input = random.bytes(length);
        exercise_arbitrary_chunks(&input, &mut random);
    }
}

#[test]
fn valid_frame_corpus_is_invariant_under_chunk_partitioning() {
    for seed in 1..=VALID_CASES {
        let mut random = Random::new(seed ^ 0xa076_1d64_78bd_642f);
        let frame_count = random.bounded(4) + 1;
        let mut expected = Vec::with_capacity(frame_count);
        let mut stream = Vec::new();
        for _ in 0..frame_count {
            let body_length = random.bounded(MAX_FRAME_BYTES + 1);
            let body = random.bytes(body_length);
            stream.extend_from_slice(&framed(&body));
            expected.push(body);
        }

        let actual = decode_partitioned(&stream, &mut random);
        assert_eq!(actual, expected, "partition corpus seed {seed}");
    }
}

fn exercise_arbitrary_chunks(input: &[u8], random: &mut Random) {
    let mut decoder = decoder();
    let mut offset = 0;
    let mut terminal = false;
    while offset < input.len() && !terminal {
        let chunk_len = (random.bounded(24) + 1).min(input.len() - offset);
        let chunk = &input[offset..offset + chunk_len];
        let buffered_before = decoder.buffered_bytes();
        match decoder.feed(chunk) {
            Ok(()) => {
                offset += chunk_len;
                terminal = drain_arbitrary(&mut decoder);
            }
            Err(FrameDecodeError::BufferCapacityExceeded { .. }) => {
                assert_eq!(decoder.buffered_bytes(), buffered_before);
                break;
            }
            Err(FrameDecodeError::DecoderFailed) => terminal = true,
            Err(error) => panic!("feed returned non-feed error: {error}"),
        }
        assert!(decoder.buffered_bytes() <= MAX_BUFFERED_BYTES);
    }
    if terminal {
        assert_eq!(decoder.feed(&[]), Err(FrameDecodeError::DecoderFailed));
        assert_eq!(decoder.next_frame(), Err(FrameDecodeError::DecoderFailed));
    }
}

fn drain_arbitrary(decoder: &mut FrameDecoder) -> bool {
    loop {
        match decoder.next_frame() {
            Ok(Some(frame)) => assert!(frame.len() <= MAX_FRAME_BYTES),
            Ok(None) => return false,
            Err(
                FrameDecodeError::NegativeFrameLength { .. }
                | FrameDecodeError::FrameTooLarge { .. },
            ) => return true,
            Err(error) => panic!("unexpected decoder state: {error}"),
        }
        assert!(decoder.buffered_bytes() <= MAX_BUFFERED_BYTES);
    }
}

fn decode_partitioned(stream: &[u8], random: &mut Random) -> Vec<Vec<u8>> {
    let mut decoder = decoder();
    let mut decoded = Vec::new();
    let mut offset = 0;
    while offset < stream.len() {
        let chunk_len = (random.bounded(11) + 1).min(stream.len() - offset);
        assert!(decoder.feed(&stream[offset..offset + chunk_len]).is_ok());
        offset += chunk_len;
        drain_valid(&mut decoder, &mut decoded);
    }
    drain_valid(&mut decoder, &mut decoded);
    assert_eq!(decoder.buffered_bytes(), 0);
    decoded
}

fn drain_valid(decoder: &mut FrameDecoder, decoded: &mut Vec<Vec<u8>>) {
    loop {
        match decoder.next_frame() {
            Ok(Some(frame)) => decoded.push(frame.as_bytes().to_vec()),
            Ok(None) => return,
            Err(error) => panic!("valid framed corpus must decode: {error}"),
        }
    }
}

fn decoder() -> FrameDecoder {
    let Ok(limits) = FrameLimits::new(nonzero(MAX_FRAME_BYTES), nonzero(MAX_BUFFERED_BYTES)) else {
        panic!("fuzz corpus limits must hold one maximum frame");
    };
    FrameDecoder::new(limits)
}

fn framed(body: &[u8]) -> Vec<u8> {
    let Ok(length) = i32::try_from(body.len()) else {
        panic!("fuzz corpus body length must fit Kafka framing");
    };
    let mut frame = length.to_be_bytes().to_vec();
    frame.extend_from_slice(body);
    frame
}

const fn nonzero(value: usize) -> NonZeroUsize {
    let Some(value) = NonZeroUsize::new(value) else {
        panic!("fuzz corpus limit must be nonzero");
    };
    value
}

struct Random(u64);

impl Random {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    fn bounded(&mut self, upper: usize) -> usize {
        let upper = u64::try_from(upper).unwrap_or(u64::MAX);
        usize::try_from(self.next() % upper).unwrap_or(usize::MAX)
    }

    fn bytes(&mut self, length: usize) -> Vec<u8> {
        (0..length).map(|_| self.next().to_le_bytes()[0]).collect()
    }
}
