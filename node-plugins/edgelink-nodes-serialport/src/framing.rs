//! Byte framing, independent of the operating system and wall clock.

use std::time::{Duration, Instant};

const MAX_FRAME: usize = 32768;

#[derive(Clone, Copy, Debug)]
pub enum Split {
    Byte,
    Character(u8),
    Count(usize),
    Time(Duration),
    Interbyte(Duration),
}

pub struct Framer {
    split: Split,
    wait_for: Option<u8>,
    active: bool,
    buffer: Vec<u8>,
    deadline: Option<Instant>,
}

impl Framer {
    pub fn new(split: Split, wait_for: Option<u8>) -> Result<Self, &'static str> {
        if matches!(split, Split::Count(0)) {
            return Err("serial frame count must be positive");
        }
        Ok(Self { split, wait_for, active: wait_for.is_none(), buffer: Vec::new(), deadline: None })
    }

    pub fn deadline(&self) -> Option<Instant> {
        if self.buffer.is_empty() { None } else { self.deadline }
    }

    fn take(&mut self) -> Vec<u8> {
        self.active = self.wait_for.is_none();
        self.deadline = None;
        std::mem::take(&mut self.buffer)
    }

    /// Drain bytes received for an incomplete frame, as the request node does on timeout.
    pub fn take_partial(&mut self) -> Option<Vec<u8>> {
        if self.buffer.is_empty() { None } else { Some(self.take()) }
    }

    pub fn expire(&mut self, now: Instant) -> Option<Vec<u8>> {
        if self.deadline.is_some_and(|deadline| now >= deadline) { Some(self.take()) } else { None }
    }

    /// Deliver a transport read, preserving frame state across read boundaries.
    pub fn push(&mut self, input: &[u8], now: Instant) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        for &byte in input {
            if self.wait_for == Some(byte) {
                self.active = true;
            }
            if !self.active {
                continue;
            }
            self.buffer.push(byte);
            let ready = match self.split {
                Split::Byte => true,
                Split::Character(delimiter) => byte == delimiter || self.buffer.len() == MAX_FRAME,
                Split::Count(count) => self.buffer.len() >= count,
                Split::Time(delay) => {
                    if self.buffer.len() == 1 {
                        self.deadline = Some(now + delay);
                    }
                    false
                }
                Split::Interbyte(delay) => {
                    self.deadline = Some(now + delay);
                    false
                }
            };
            if ready {
                frames.push(self.take());
            }
        }
        frames
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delimiter_is_included_and_reads_do_not_define_frames() {
        let now = Instant::now();
        let mut framer = Framer::new(Split::Character(b'\n'), None).unwrap();
        assert!(framer.push(b"ab", now).is_empty());
        assert_eq!(framer.push(b"c\nd\n", now), [b"abc\n".to_vec(), b"d\n".to_vec()]);
    }

    #[test]
    fn waitfor_is_included_and_rearmed_after_each_frame() {
        let mut framer = Framer::new(Split::Count(3), Some(b':')).unwrap();
        assert_eq!(framer.push(b"noise:abignored:cd", Instant::now()), [b":ab".to_vec(), b":cd".to_vec()]);
    }

    #[test]
    fn time_starts_at_first_byte_but_interbyte_restarts() {
        let now = Instant::now();
        let delay = Duration::from_millis(10);
        for (split, expected) in [(Split::Time(delay), b"ab".to_vec()), (Split::Interbyte(delay), Vec::new())] {
            let mut framer = Framer::new(split, None).unwrap();
            framer.push(b"a", now);
            framer.push(b"b", now + delay / 2);
            assert_eq!(framer.expire(now + delay).unwrap_or_default(), expected);
            if matches!(split, Split::Interbyte(_)) {
                assert_eq!(framer.expire(now + delay * 2), Some(b"ab".to_vec()));
            }
            assert_eq!(framer.expire(now + delay * 3), None);
        }
    }

    #[test]
    fn time_deadline_does_not_restart_when_later_chunks_arrive() {
        let now = Instant::now();
        let delay = Duration::from_millis(20);
        let mut framer = Framer::new(Split::Time(delay), None).unwrap();
        framer.push(b"a", now);
        let first_deadline = framer.deadline();
        framer.push(b"b", now + delay / 2);
        assert_eq!(framer.deadline(), first_deadline);
        assert_eq!(framer.expire(now + delay), Some(b"ab".to_vec()));
    }

    #[test]
    fn character_frames_have_upstream_size_boundary() {
        let mut framer = Framer::new(Split::Character(b'\n'), None).unwrap();
        let frames = framer.push(&vec![b'x'; MAX_FRAME + 1], Instant::now());
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].len(), MAX_FRAME);
        assert_eq!(framer.push(b"\n", Instant::now()), [b"x\n".to_vec()]);
    }

    #[test]
    fn byte_mode_and_invalid_count() {
        let mut framer = Framer::new(Split::Byte, None).unwrap();
        assert_eq!(framer.push(&[0, 255], Instant::now()), [vec![0], vec![255]]);
        assert!(Framer::new(Split::Count(0), None).is_err());
    }

    #[test]
    fn incomplete_response_can_be_drained_once_on_request_timeout() {
        let mut framer = Framer::new(Split::Character(b'\n'), None).unwrap();
        framer.push(b"partial", Instant::now());
        assert_eq!(framer.take_partial(), Some(b"partial".to_vec()));
        assert_eq!(framer.take_partial(), None);
    }
}
