//! Framing for a batch of CRDT payload chunks.
//!
//! Deliberately trivial and self-describing so the relay and the client can
//! never disagree about it, and so a batch can be dumped to a file and replayed
//! by hand when debugging.
//!
//! ```text
//! magic       4 bytes   "CKS1"
//! watermark   8 bytes   u64 big-endian
//! count       4 bytes   u32 big-endian
//! repeated count times:
//!   len       4 bytes   u32 big-endian
//!   payload   len bytes
//! ```

use anyhow::{Result, bail, ensure};

pub const MAGIC: &[u8; 4] = b"CKS1";

/// Refuse absurd frames rather than allocating on a bad length prefix. The
/// extension's own ceiling for a single chunk is 32 MB.
const MAX_PAYLOAD_LEN: u32 = 64 * 1024 * 1024;
const MAX_CHUNKS: u32 = 100_000;

#[derive(Debug, Default, Clone)]
pub struct Batch {
    /// Stable upper watermark for this stream; the receiver stores it only
    /// after every chunk has been durably applied.
    pub watermark: i64,
    pub payloads: Vec<Vec<u8>>,
}

impl Batch {
    pub fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }

    pub fn encode(&self) -> Vec<u8> {
        let total: usize =
            16 + self.payloads.iter().map(|p| 4 + p.len()).sum::<usize>();
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&(self.watermark as u64).to_be_bytes());
        out.extend_from_slice(&(self.payloads.len() as u32).to_be_bytes());
        for payload in &self.payloads {
            out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            out.extend_from_slice(payload);
        }
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        ensure!(buf.len() >= 16, "truncated batch header ({} bytes)", buf.len());
        if &buf[0..4] != MAGIC {
            bail!("bad magic - not a checklist sync batch");
        }
        let watermark = u64::from_be_bytes(buf[4..12].try_into().unwrap()) as i64;
        let count = u32::from_be_bytes(buf[12..16].try_into().unwrap());
        ensure!(count <= MAX_CHUNKS, "implausible chunk count {count}");

        let mut payloads = Vec::with_capacity(count as usize);
        let mut pos = 16usize;
        for i in 0..count {
            ensure!(pos + 4 <= buf.len(), "truncated length prefix for chunk {i}");
            let len = u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap());
            ensure!(len <= MAX_PAYLOAD_LEN, "implausible chunk length {len}");
            pos += 4;
            let end = pos + len as usize;
            ensure!(end <= buf.len(), "truncated body for chunk {i}");
            payloads.push(buf[pos..end].to_vec());
            pos = end;
        }
        ensure!(pos == buf.len(), "{} trailing bytes after batch", buf.len() - pos);

        Ok(Self { watermark, payloads })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let batch = Batch {
            watermark: 42,
            payloads: vec![vec![1, 2, 3], vec![], vec![9; 1000]],
        };
        let decoded = Batch::decode(&batch.encode()).unwrap();
        assert_eq!(decoded.watermark, 42);
        assert_eq!(decoded.payloads, batch.payloads);
    }

    #[test]
    fn empty_batch_roundtrips() {
        let decoded = Batch::decode(&Batch::default().encode()).unwrap();
        assert!(decoded.is_empty());
        assert_eq!(decoded.watermark, 0);
    }

    #[test]
    fn rejects_garbage() {
        assert!(Batch::decode(b"").is_err());
        assert!(Batch::decode(b"NOPE000000000000").is_err());
    }

    #[test]
    fn rejects_truncated_body() {
        let mut buf = Batch {
            watermark: 1,
            payloads: vec![vec![7; 8]],
        }
        .encode();
        buf.truncate(buf.len() - 2);
        assert!(Batch::decode(&buf).is_err());
    }

    #[test]
    fn rejects_trailing_bytes() {
        let mut buf = Batch::default().encode();
        buf.push(0);
        assert!(Batch::decode(&buf).is_err());
    }
}
