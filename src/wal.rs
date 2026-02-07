use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::model::Event;

/// Append-only Write-Ahead Log.
///
/// Format per entry: `[u32: len][bincode: Event][u32: crc32]`
/// - `len` is the byte length of the bincode payload (not including the CRC).
/// - Truncated last entry (crash) is safely discarded via length-prefix + CRC check.
pub struct Wal {
    writer: BufWriter<File>,
}

impl Wal {
    /// Open (or create) the WAL file at `path`.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    /// Append a single event to the WAL and fsync.
    pub fn append(&mut self, event: &Event) -> io::Result<()> {
        let payload = bincode::serialize(event)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let len = payload.len() as u32;
        let crc = crc32fast::hash(&payload);

        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&payload)?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Replay the WAL from disk, returning all valid events.
    /// Truncated/corrupt trailing entries are silently discarded.
    pub fn replay(path: &Path) -> io::Result<Vec<Event>> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut reader = BufReader::new(file);
        let mut events = Vec::new();

        loop {
            // Read length prefix
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            // Read payload
            let mut payload = vec![0u8; len];
            match reader.read_exact(&mut payload) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // truncated
                Err(e) => return Err(e),
            }

            // Read CRC
            let mut crc_buf = [0u8; 4];
            match reader.read_exact(&mut crc_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break, // truncated
                Err(e) => return Err(e),
            }
            let stored_crc = u32::from_le_bytes(crc_buf);
            let computed_crc = crc32fast::hash(&payload);

            if stored_crc != computed_crc {
                // Corrupt entry — stop replaying
                break;
            }

            match bincode::deserialize::<Event>(&payload) {
                Ok(event) => events.push(event),
                Err(_) => break, // corrupt payload
            }
        }

        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use ulid::Ulid;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("deltat_test_wal");
        fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn append_and_replay() {
        let path = tmp_path("append_and_replay.wal");
        let _ = fs::remove_file(&path);

        let events = vec![
            Event::ResourceCreated {
                id: Ulid::new(),
                parent_id: None,
                capacity: 1,
                buffer_after: None,
            },
            Event::RuleAdded {
                id: Ulid::new(),
                resource_id: Ulid::new(),
                span: crate::model::Span::new(1000, 2000),
                blocking: false,
            },
        ];

        {
            let mut wal = Wal::open(&path).unwrap();
            for e in &events {
                wal.append(e).unwrap();
            }
        }

        let replayed = Wal::replay(&path).unwrap();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed, events);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn replay_handles_truncation() {
        let path = tmp_path("truncation.wal");
        let _ = fs::remove_file(&path);

        let event = Event::ResourceCreated {
            id: Ulid::new(),
            parent_id: None,
            capacity: 1,
            buffer_after: None,
        };

        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&event).unwrap();
        }

        // Append garbage to simulate a truncated second entry
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0u8; 6]).unwrap(); // partial length + some bytes
        }

        let replayed = Wal::replay(&path).unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0], event);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn replay_nonexistent_file() {
        let path = tmp_path("nonexistent.wal");
        let _ = fs::remove_file(&path);
        let replayed = Wal::replay(&path).unwrap();
        assert!(replayed.is_empty());
    }

    #[test]
    fn replay_corrupt_crc() {
        let path = tmp_path("corrupt_crc.wal");
        let _ = fs::remove_file(&path);

        let event = Event::ResourceDeleted { id: Ulid::new() };

        // Manually write an entry with bad CRC
        {
            let payload = bincode::serialize(&event).unwrap();
            let len = payload.len() as u32;
            let bad_crc: u32 = 0xDEADBEEF;

            let mut f = File::create(&path).unwrap();
            f.write_all(&len.to_le_bytes()).unwrap();
            f.write_all(&payload).unwrap();
            f.write_all(&bad_crc.to_le_bytes()).unwrap();
        }

        let replayed = Wal::replay(&path).unwrap();
        assert!(replayed.is_empty());

        let _ = fs::remove_file(&path);
    }
}
