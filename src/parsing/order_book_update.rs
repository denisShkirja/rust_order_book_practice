use binread::{BinRead, Error, ReadOptions, derive_binread};
use std::io::{Read, Seek};

const MAX_UPDATES: u64 = 100_000;

#[derive(Debug, BinRead)]
#[br(little)]
pub struct Update {
    pub side: u8,
    pub price: f64,
    pub qty: u64,
}

fn parse_num_updates<R: Read + Seek>(
    reader: &mut R,
    ro: &ReadOptions,
    _: (),
) -> binread::BinResult<u64> {
    let num_updates = u64::read_options(reader, ro, ())?;

    if num_updates >= MAX_UPDATES {
        return Err(Error::Custom {
            pos: reader.stream_position()? - 8,
            err: Box::new(format!(
                "Too many updates: {} (maximum is {})",
                num_updates, MAX_UPDATES
            )),
        });
    }

    Ok(num_updates)
}

#[derive_binread]
#[derive(Debug)]
#[br(little)]
pub struct OrderBookUpdate {
    pub timestamp: u64,
    pub seq_no: u64,
    pub security_id: u64,
    #[br(temp, parse_with = parse_num_updates)]
    pub num_updates: u64,
    // TODO: Use ring buffer for updates to avoid allocation
    #[br(count = num_updates)]
    pub updates: Vec<Update>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use binread::BinRead;
    use std::io::Cursor;

    #[test]
    fn test_read_orderbook_update() {
        let timestamp: u64 = 1627846265;
        let seq_no: u64 = 42;
        let security_id: u64 = 1001;
        let updates = vec![
            Update {
                side: 0,
                price: 123.45,
                qty: 100,
            },
            Update {
                side: 1,
                price: 67.89,
                qty: 200,
            },
        ];
        let num_updates = updates.len() as u64;

        let mut buf = Vec::new();
        buf.extend(&timestamp.to_le_bytes());
        buf.extend(&seq_no.to_le_bytes());
        buf.extend(&security_id.to_le_bytes());
        buf.extend(&num_updates.to_le_bytes());
        for update in &updates {
            buf.push(update.side);
            buf.extend(&update.price.to_le_bytes());
            buf.extend(&update.qty.to_le_bytes());
        }

        let mut cursor = Cursor::new(&buf);
        let obu = OrderBookUpdate::read(&mut cursor).unwrap();

        assert_eq!(obu.timestamp, timestamp);
        assert_eq!(obu.seq_no, seq_no);
        assert_eq!(obu.security_id, security_id);
        assert_eq!(obu.updates.len() as u64, num_updates);
        for (i, upd) in updates.iter().enumerate() {
            assert_eq!(obu.updates[i].side, upd.side);
            assert_eq!(obu.updates[i].price, upd.price);
            assert_eq!(obu.updates[i].qty, upd.qty);
        }
    }

    #[test]
    fn test_too_many_updates() {
        let timestamp: u64 = 0;
        let seq_no: u64 = 0;
        let security_id: u64 = 0;
        let num_updates = MAX_UPDATES + 1;

        let mut buf = Vec::new();
        buf.extend(&timestamp.to_le_bytes());
        buf.extend(&seq_no.to_le_bytes());
        buf.extend(&security_id.to_le_bytes());
        buf.extend(&num_updates.to_le_bytes());
        let mut cursor = Cursor::new(&buf);

        match OrderBookUpdate::read(&mut cursor) {
            Ok(_) => panic!("Expected error but got Ok"),
            Err(e) => {
                let expected_msg = format!(
                    "Too many updates: {} (maximum is {})",
                    num_updates, MAX_UPDATES
                );
                if let binread::Error::Custom { pos, err } = e {
                    assert_eq!(*err.downcast::<String>().unwrap(), expected_msg);
                    assert_eq!(pos, 24);
                } else {
                    panic!("Expected Custom error but got: {:?}", e);
                }
            }
        }
    }

    #[test]
    fn test_unexpected_eof() {
        let timestamp: u64 = 0;
        let seq_no: u64 = 0;
        let security_id: u64 = 0;
        let num_updates: u64 = 2;
        let update = Update {
            side: 0,
            price: 123.45,
            qty: 100,
        };

        let mut buf = Vec::new();
        buf.extend(&timestamp.to_le_bytes());
        buf.extend(&seq_no.to_le_bytes());
        buf.extend(&security_id.to_le_bytes());
        buf.extend(&num_updates.to_le_bytes());
        // Create just one update instead of the expected 2
        buf.extend(&update.side.to_le_bytes());
        buf.extend(&update.price.to_le_bytes());
        buf.extend(&update.qty.to_le_bytes());
        let mut cursor = Cursor::new(&buf);

        match OrderBookUpdate::read(&mut cursor) {
            Ok(_) => panic!("Expected error but got Ok"),
            Err(e) => {
                if let binread::Error::Io(e) = e {
                    assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof);
                } else {
                    panic!("Expected Io error but got: {:?}", e);
                }
            }
        }
    }
}
