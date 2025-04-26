use crate::batched_deque::batched_deque::BatchGuard;
use crate::batched_deque::batched_deque::BatchedDeque;
use crate::parsing::parser::ParserError;
use crate::parsing::parser::{DefaultParser, Parser};
use std::collections::HashMap;
use std::io::{self, Read};

const DEFAULT_UPDATE_DEQUE_CAPACITY: usize = 10_000;
const MAX_NUM_UPDATES: usize = 100_000;

#[derive(Debug)]
pub struct Level {
    pub side: u8,
    pub price: f64,
    pub qty: u64,
}

#[derive(Debug)]
pub struct OrderBookUpdate {
    pub timestamp: u64,
    pub seq_no: u64,
    pub security_id: u64,
    pub updates: BatchGuard<Level>,
}

#[derive(Debug)]
struct LevelParser;

impl Parser<Level> for LevelParser {
    fn read<R: Read>(&mut self, reader: &mut R) -> Result<Level, ParserError> {
        // parse side
        let side = {
            let mut side = [0; 1];
            reader.read_exact(&mut side).map_err(ParserError::Io)?;
            side[0]
        };
        // parse price
        let price = {
            let mut price = [0; 8];
            reader.read_exact(&mut price).map_err(ParserError::Io)?;
            f64::from_le_bytes(price)
        };
        // parse qty
        let qty = {
            let mut qty = [0; 8];
            reader.read_exact(&mut qty).map_err(ParserError::Io)?;
            u64::from_le_bytes(qty)
        };
        Ok(Level { side, price, qty })
    }
}

#[derive(Debug, Default)]
pub struct OrderBookUpdateParser {
    // Each security_id has its own deque for updates
    security_id_to_deque: HashMap<u64, BatchedDeque<Level>>,
}

impl DefaultParser<OrderBookUpdate> for OrderBookUpdate {
    type ParserType = OrderBookUpdateParser;

    fn default_parser() -> OrderBookUpdateParser {
        OrderBookUpdateParser::default()
    }
}

impl Parser<OrderBookUpdate> for OrderBookUpdateParser {
    fn read<R: Read>(&mut self, reader: &mut R) -> Result<OrderBookUpdate, ParserError> {
        // parse timestamp
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
        // parse seq_no
        let seq_no = {
            let mut seq_no = [0; 8];
            reader.read_exact(&mut seq_no).map_err(ParserError::Io)?;
            u64::from_le_bytes(seq_no)
        };
        // parse security_id
        let security_id = {
            let mut security_id = [0; 8];
            reader
                .read_exact(&mut security_id)
                .map_err(ParserError::Io)?;
            u64::from_le_bytes(security_id)
        };
        // parse num_updates
        let num_updates = {
            let mut num_updates = [0; 8];
            reader
                .read_exact(&mut num_updates)
                .map_err(ParserError::Io)?;
            let num_updates = u64::from_le_bytes(num_updates) as usize;
            if num_updates > MAX_NUM_UPDATES {
                return Err(ParserError::Custom(format!(
                    "Number of updates is too large: {}",
                    num_updates
                )));
            }
            num_updates
        };

        let deque = self
            .security_id_to_deque
            .entry(security_id)
            .or_insert_with(|| BatchedDeque::new(DEFAULT_UPDATE_DEQUE_CAPACITY));

        let levels_iter = (0..num_updates).map(move |_| LevelParser.read(reader));

        Ok(OrderBookUpdate {
            timestamp,
            seq_no,
            security_id,
            updates: deque.push_back_batch(levels_iter)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn create_test_update_data(seq_no: u64, num_updates: usize) -> Vec<u8> {
        let mut data = Vec::new();

        // timestamp (u64)
        data.extend_from_slice(&1234567890u64.to_le_bytes());
        // seq_no (u64)
        data.extend_from_slice(&seq_no.to_le_bytes());
        // security_id (u64)
        data.extend_from_slice(&123456u64.to_le_bytes());
        // num_updates (u64)
        data.extend_from_slice(&(num_updates as u64).to_le_bytes());

        // Add update levels
        for i in 0..num_updates {
            // side (u8) - alternate between bid (0) and ask (1)
            data.push(if i % 2 == 0 { 0 } else { 1 });

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
    fn test_parse_order_book_update() {
        let num_updates = 5;
        let test_data = create_test_update_data(42, num_updates);
        let mut cursor = Cursor::new(test_data);
        let mut parser = OrderBookUpdateParser::default();

        let result = parser.read(&mut cursor);
        assert!(result.is_ok(), "Failed to parse order book update");

        let update = result.unwrap();
        assert_eq!(update.timestamp, 1234567890);
        assert_eq!(update.seq_no, 42);
        assert_eq!(update.security_id, 123456);

        // Check that all updates were parsed
        let mut count = 0;
        update
            .updates
            .for_each(|level| {
                assert_eq!(level.side, if count % 2 == 0 { 0 } else { 1 });
                assert_eq!(level.price, 1000.0 + (count as f64) * 0.5);
                assert_eq!(level.qty, 100 + (count as u64) * 10);
                count += 1;
                Ok::<(), ()>(())
            })
            .unwrap();
        assert_eq!(count, num_updates);
    }

    #[test]
    fn test_multiple_updates_same_security_id() {
        let num_updates = 3;
        let test_data1 = create_test_update_data(42, num_updates);
        let test_data2 = create_test_update_data(43, num_updates);

        let mut parser = OrderBookUpdateParser::default();

        // Parse first update
        let mut cursor1 = Cursor::new(test_data1);
        let result1 = parser.read(&mut cursor1);
        assert!(result1.is_ok());

        // Parse second update for same security_id
        let mut cursor2 = Cursor::new(test_data2);
        let result2 = parser.read(&mut cursor2);
        assert!(result2.is_ok());

        // Verify both updates were processed correctly
        let update1 = result1.unwrap();
        let update2 = result2.unwrap();

        assert_eq!(update1.seq_no, 42);
        assert_eq!(update2.seq_no, 43);

        // Check that updates were added to the same deque
        assert_eq!(parser.security_id_to_deque.len(), 1);

        // Verify the number of updates through for_each
        let mut count = 0;
        update1
            .updates
            .for_each(|_| {
                count += 1;
                Ok::<(), ()>(())
            })
            .unwrap();
        assert_eq!(count, num_updates);

        count = 0;
        update2
            .updates
            .for_each(|_| {
                count += 1;
                Ok::<(), ()>(())
            })
            .unwrap();
        assert_eq!(count, num_updates);
    }

    #[test]
    fn test_empty_data() {
        // Test with empty data
        let empty_data: Vec<u8> = Vec::new();
        let mut cursor = Cursor::new(empty_data);
        let mut parser = OrderBookUpdateParser::default();

        let result = parser.read(&mut cursor);
        assert!(result.is_err());

        match result {
            Err(ParserError::ExpectedEof) => (), // Expected EOF error
            err => panic!("Expected EOF error, got {:?}", err),
        }
    }

    #[test]
    fn test_incomplete_data() {
        // Test with incomplete data (only timestamp + seq_no)
        let mut incomplete_data = Vec::new();
        incomplete_data.extend_from_slice(&1234567890u64.to_le_bytes()); // timestamp
        incomplete_data.extend_from_slice(&42u64.to_le_bytes()); // seq_no

        let mut cursor = Cursor::new(incomplete_data);
        let mut parser = OrderBookUpdateParser::default();

        let result = parser.read(&mut cursor);
        assert!(result.is_err());

        match result {
            Err(ParserError::Io(_)) => (), // Expected IO error
            err => panic!("Expected IO error, got {:?}", err),
        }
    }

    #[test]
    fn test_max_num_updates_exceeded() {
        let mut data = Vec::new();

        // Add header information
        data.extend_from_slice(&1234567890u64.to_le_bytes()); // timestamp
        data.extend_from_slice(&42u64.to_le_bytes()); // seq_no
        data.extend_from_slice(&123456u64.to_le_bytes()); // security_id

        // Set num_updates to exceed MAX_NUM_UPDATES
        data.extend_from_slice(&(MAX_NUM_UPDATES as u64 + 1).to_le_bytes());

        let mut cursor = Cursor::new(data);
        let mut parser = OrderBookUpdateParser::default();

        let result = parser.read(&mut cursor);
        assert!(result.is_err());

        match result {
            Err(ParserError::Custom(msg)) => {
                assert!(msg.contains("Number of updates is too large"));
            }
            err => panic!("Expected Custom error, got {:?}", err),
        }
    }

    #[test]
    fn test_level_parser() {
        let mut data = Vec::new();
        data.push(1); // side (ask)
        data.extend_from_slice(&123.45f64.to_le_bytes()); // price
        data.extend_from_slice(&789u64.to_le_bytes()); // qty

        let mut cursor = Cursor::new(data);
        let level = LevelParser.read(&mut cursor).unwrap();
        assert_eq!(level.side, 1);
        assert_eq!(level.price, 123.45);
        assert_eq!(level.qty, 789);
    }

    #[test]
    fn test_multiple_updates_different_security_ids() {
        let num_updates = 3;

        // Create test data for two different security IDs
        let mut test_data1 = Vec::new();
        test_data1.extend_from_slice(&1234567890u64.to_le_bytes()); // timestamp
        test_data1.extend_from_slice(&42u64.to_le_bytes()); // seq_no
        test_data1.extend_from_slice(&111111u64.to_le_bytes()); // security_id 1
        test_data1.extend_from_slice(&(num_updates as u64).to_le_bytes()); // num_updates

        // Add update levels for first security ID
        for i in 0..num_updates {
            test_data1.push(if i % 2 == 0 { 0 } else { 1 }); // side
            test_data1.extend_from_slice(&(1000.0 + (i as f64) * 0.5).to_le_bytes()); // price
            test_data1.extend_from_slice(&(100 + (i as u64) * 10).to_le_bytes()); // qty
        }

        // Create test data for second security ID
        let mut test_data2 = Vec::new();
        test_data2.extend_from_slice(&1234567891u64.to_le_bytes()); // timestamp
        test_data2.extend_from_slice(&43u64.to_le_bytes()); // seq_no
        test_data2.extend_from_slice(&222222u64.to_le_bytes()); // security_id 2
        test_data2.extend_from_slice(&(num_updates as u64).to_le_bytes()); // num_updates

        // Add update levels for second security ID
        for i in 0..num_updates {
            test_data2.push(if i % 2 == 0 { 0 } else { 1 }); // side
            test_data2.extend_from_slice(&(2000.0 + (i as f64) * 0.5).to_le_bytes()); // price
            test_data2.extend_from_slice(&(200 + (i as u64) * 10).to_le_bytes()); // qty
        }

        let mut parser = OrderBookUpdateParser::default();

        // Parse first update
        let mut cursor1 = Cursor::new(test_data1);
        let result1 = parser.read(&mut cursor1);
        assert!(result1.is_ok());

        // Parse second update with different security_id
        let mut cursor2 = Cursor::new(test_data2);
        let result2 = parser.read(&mut cursor2);
        assert!(result2.is_ok());

        // Verify both updates were processed correctly
        let update1 = result1.unwrap();
        let update2 = result2.unwrap();

        assert_eq!(update1.seq_no, 42);
        assert_eq!(update1.security_id, 111111);

        assert_eq!(update2.seq_no, 43);
        assert_eq!(update2.security_id, 222222);

        // Check that we have two different deques for different security IDs
        assert_eq!(parser.security_id_to_deque.len(), 2);

        // Verify the contents of the first update's levels through counting
        let mut count1 = 0;
        update1
            .updates
            .for_each(|level| {
                assert_eq!(level.side, if count1 % 2 == 0 { 0 } else { 1 });
                assert_eq!(level.price, 1000.0 + (count1 as f64) * 0.5);
                assert_eq!(level.qty, 100 + (count1 as u64) * 10);
                count1 += 1;
                Ok::<(), ()>(())
            })
            .unwrap();
        assert_eq!(count1, num_updates);

        // Verify the contents of the second update's levels
        let mut count2 = 0;
        update2
            .updates
            .for_each(|level| {
                assert_eq!(level.side, if count2 % 2 == 0 { 0 } else { 1 });
                assert_eq!(level.price, 2000.0 + (count2 as f64) * 0.5);
                assert_eq!(level.qty, 200 + (count2 as u64) * 10);
                count2 += 1;
                Ok::<(), ()>(())
            })
            .unwrap();
        assert_eq!(count2, num_updates);
    }
}
