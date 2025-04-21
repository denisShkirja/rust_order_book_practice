use crate::parsing::parser::{DefaultParser, Parser, ParserError};
use std::io::{self, Read};

#[derive(Debug)]
pub struct Level {
    pub price: f64,
    pub qty: u64,
}

#[derive(Debug)]
pub struct OrderBookSnapshot {
    pub timestamp: u64,
    pub seq_no: u64,
    pub security_id: u64,
    pub bid1: Level,
    pub ask1: Level,
    pub bid2: Level,
    pub ask2: Level,
    pub bid3: Level,
    pub ask3: Level,
    pub bid4: Level,
    pub ask4: Level,
    pub bid5: Level,
    pub ask5: Level,
}

struct LevelParser;

impl Parser<Level> for LevelParser {
    fn read<R: Read>(&mut self, reader: &mut R) -> Result<Level, ParserError> {
        let price = {
            let mut price = [0; 8];
            reader.read_exact(&mut price).map_err(ParserError::Io)?;
            f64::from_le_bytes(price)
        };
        let qty = {
            let mut qty = [0; 8];
            reader.read_exact(&mut qty).map_err(ParserError::Io)?;
            u64::from_le_bytes(qty)
        };
        Ok(Level { price, qty })
    }
}

#[derive(Debug, Default)]
pub struct OrderBookSnapshotParser;

impl DefaultParser<OrderBookSnapshot> for OrderBookSnapshot {
    type ParserType = OrderBookSnapshotParser;

    fn default_parser() -> OrderBookSnapshotParser {
        OrderBookSnapshotParser
    }
}

impl Parser<OrderBookSnapshot> for OrderBookSnapshotParser {
    fn read<R: Read>(&mut self, reader: &mut R) -> Result<OrderBookSnapshot, ParserError> {
        let timestamp = {
            let mut timestamp = [0; 8];
            match reader.read_exact(&mut timestamp) {
                Ok(_) => (),
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        return Err(ParserError::ExpectedEof);
                    }
                    return Err(ParserError::Io(e));
                }
            }
            u64::from_le_bytes(timestamp)
        };
        let seq_no = {
            let mut seq_no = [0; 8];
            reader.read_exact(&mut seq_no).map_err(ParserError::Io)?;
            u64::from_le_bytes(seq_no)
        };
        let security_id = {
            let mut security_id = [0; 8];
            reader
                .read_exact(&mut security_id)
                .map_err(ParserError::Io)?;
            u64::from_le_bytes(security_id)
        };

        let mut level_parser = LevelParser;
        Ok(OrderBookSnapshot {
            timestamp,
            seq_no,
            security_id,
            bid1: level_parser.read(reader)?,
            ask1: level_parser.read(reader)?,
            bid2: level_parser.read(reader)?,
            ask2: level_parser.read(reader)?,
            bid3: level_parser.read(reader)?,
            ask3: level_parser.read(reader)?,
            bid4: level_parser.read(reader)?,
            ask4: level_parser.read(reader)?,
            bid5: level_parser.read(reader)?,
            ask5: level_parser.read(reader)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn create_test_data() -> Vec<u8> {
        let mut data = Vec::new();

        // timestamp (u64)
        data.extend_from_slice(&1234567890u64.to_le_bytes());
        // seq_no (u64)
        data.extend_from_slice(&42u64.to_le_bytes());
        // security_id (u64)
        data.extend_from_slice(&123456u64.to_le_bytes());

        // 10 levels (bid1, ask1, bid2, ask2, etc.)
        for i in 0..10 {
            // price (f64)
            let price = 1000.0 + (i as f64) * 0.5;
            data.extend_from_slice(&price.to_le_bytes());

            // qty (u64)
            let qty = 100 + (i as u64) * 10;
            data.extend_from_slice(&qty.to_le_bytes());
        }

        data
    }

    #[test]
    fn test_parse_order_book_snapshot() {
        let test_data = create_test_data();
        let mut cursor = Cursor::new(test_data);
        let mut parser = OrderBookSnapshotParser;

        let result = parser.read(&mut cursor);
        assert!(result.is_ok(), "Failed to parse order book snapshot");

        let snapshot = result.unwrap();
        assert_eq!(snapshot.timestamp, 1234567890);
        assert_eq!(snapshot.seq_no, 42);
        assert_eq!(snapshot.security_id, 123456);

        // Check all bid levels
        assert_eq!(snapshot.bid1.price, 1000.0);
        assert_eq!(snapshot.bid1.qty, 100);

        assert_eq!(snapshot.bid2.price, 1001.0);
        assert_eq!(snapshot.bid2.qty, 120);

        assert_eq!(snapshot.bid3.price, 1002.0);
        assert_eq!(snapshot.bid3.qty, 140);

        assert_eq!(snapshot.bid4.price, 1003.0);
        assert_eq!(snapshot.bid4.qty, 160);

        assert_eq!(snapshot.bid5.price, 1004.0);
        assert_eq!(snapshot.bid5.qty, 180);

        // Check all ask levels
        assert_eq!(snapshot.ask1.price, 1000.5);
        assert_eq!(snapshot.ask1.qty, 110);

        assert_eq!(snapshot.ask2.price, 1001.5);
        assert_eq!(snapshot.ask2.qty, 130);

        assert_eq!(snapshot.ask3.price, 1002.5);
        assert_eq!(snapshot.ask3.qty, 150);

        assert_eq!(snapshot.ask4.price, 1003.5);
        assert_eq!(snapshot.ask4.qty, 170);

        assert_eq!(snapshot.ask5.price, 1004.5);
        assert_eq!(snapshot.ask5.qty, 190);
    }

    #[test]
    fn test_incomplete_data() {
        // Test with incomplete data (only timestamp)
        let incomplete_data = 1234567890u64.to_le_bytes().to_vec();
        let mut cursor = Cursor::new(incomplete_data);
        let mut parser = OrderBookSnapshotParser;

        let result = parser.read(&mut cursor);
        assert!(result.is_err());

        match result {
            Err(ParserError::Io(_)) => (), // Expected IO error
            err => panic!("Expected IO error, got {:?}", err),
        }
    }

    #[test]
    fn test_empty_data() {
        // Test with empty data
        let empty_data: Vec<u8> = Vec::new();
        let mut cursor = Cursor::new(empty_data);
        let mut parser = OrderBookSnapshotParser;

        let result = parser.read(&mut cursor);
        assert!(result.is_err());

        match result {
            Err(ParserError::ExpectedEof) => (), // Expected EOF error
            err => panic!("Expected EOF error, got {:?}", err),
        }
    }

    #[test]
    fn test_level_parser() {
        let mut data = Vec::new();
        data.extend_from_slice(&123.45f64.to_le_bytes()); // price
        data.extend_from_slice(&789u64.to_le_bytes()); // qty

        let mut cursor = Cursor::new(data);
        let mut parser = LevelParser;

        let result = parser.read(&mut cursor);
        assert!(result.is_ok());

        let level = result.unwrap();
        assert_eq!(level.price, 123.45);
        assert_eq!(level.qty, 789);
    }
}
