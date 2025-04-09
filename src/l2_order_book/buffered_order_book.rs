use crate::l2_order_book::errors::Errors;
use crate::l2_order_book::order_book::OrderBook;
use crate::parsing::order_book_snapshot::OrderBookSnapshot;
use crate::parsing::order_book_update::OrderBookUpdate;
use std::collections::BTreeMap;
use std::fmt::Display;

pub struct BufferedOrderBook {
    pub order_book: OrderBook,
    pub pending_updates: BTreeMap<u64, OrderBookUpdate>,
}

impl BufferedOrderBook {
    pub const MAX_PENDING_UPDATES: usize = 1000;

    pub fn new(order_book: OrderBook) -> Self {
        Self {
            order_book,
            pending_updates: BTreeMap::new(),
        }
    }

    pub fn apply_update(&mut self, update: OrderBookUpdate) -> Result<(), Errors> {
        match self.order_book.apply_update(&update) {
            Ok(_) => {
                self.try_apply_pending_updates();
                Ok(())
            }
            Err(e) => match e {
                Errors::SequenceNumberGap => {
                    if self.pending_updates.len() >= Self::MAX_PENDING_UPDATES {
                        // Drop the oldest update (smallest sequence number)
                        self.pending_updates.pop_first();
                    }
                    self.pending_updates.insert(update.seq_no, update);
                    Err(e)
                }
                _ => Err(e),
            },
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: &OrderBookSnapshot) -> Result<(), Errors> {
        match self.order_book.apply_snapshot(snapshot) {
            Ok(_) => {
                self.try_apply_pending_updates();
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn try_apply_pending_updates(&mut self) {
        let mut last_successful_key = None;
        for (key, update) in &self.pending_updates {
            match self.order_book.apply_update(update) {
                Ok(_) => {
                    last_successful_key = Some(*key);
                }
                Err(e) => match e {
                    Errors::OldSequenceNumber => {
                        last_successful_key = Some(*key);
                    }
                    _ => break,
                },
            }
        }
        if let Some(key) = last_successful_key {
            // Find the next key strictly greater than last_successful_key
            if let Some(&next_key) = self
                .pending_updates
                .range((key + 1)..)
                .map(|(k, _)| k)
                .next()
            {
                // Split at the next key, keeping only elements with keys >= next_key
                self.pending_updates = self.pending_updates.split_off(&next_key);
            } else {
                // No keys greater than last_successful_key, so clear the map
                self.pending_updates.clear();
            }
        }
    }
}

impl Display for BufferedOrderBook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.order_book)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsing::order_book_snapshot::Level;
    use crate::parsing::order_book_update::Update;

    fn create_test_snapshot(security_id: u64, seq_no: u64) -> OrderBookSnapshot {
        OrderBookSnapshot {
            timestamp: 1627846265,
            seq_no,
            security_id,
            bid1: Level {
                price: 100.00,
                qty: 10,
            },
            ask1: Level {
                price: 101.00,
                qty: 15,
            },
            bid2: Level {
                price: 99.00,
                qty: 20,
            },
            ask2: Level {
                price: 102.00,
                qty: 25,
            },
            bid3: Level {
                price: 98.00,
                qty: 30,
            },
            ask3: Level {
                price: 103.00,
                qty: 35,
            },
            bid4: Level {
                price: 97.00,
                qty: 40,
            },
            ask4: Level {
                price: 104.00,
                qty: 45,
            },
            bid5: Level {
                price: 96.00,
                qty: 50,
            },
            ask5: Level {
                price: 105.00,
                qty: 55,
            },
        }
    }

    fn create_test_update(security_id: u64, seq_no: u64) -> OrderBookUpdate {
        OrderBookUpdate {
            timestamp: 1627846266,
            seq_no,
            security_id,
            updates: vec![
                Update {
                    side: 0,
                    price: 99.50,
                    qty: 25,
                },
                Update {
                    side: 1,
                    price: 100.50,
                    qty: 30,
                },
            ],
        }
    }

    #[test]
    fn test_buffered_successful_update() {
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let order_book = OrderBook::new(&snapshot).unwrap();
        let mut buffered_book = BufferedOrderBook::new(order_book);

        let update = create_test_update(security_id, 101);
        let result = buffered_book.apply_update(update);

        assert!(result.is_ok());
        assert_eq!(buffered_book.order_book.seq_no, 101);
        assert!(buffered_book.pending_updates.is_empty());
    }

    #[test]
    fn test_buffered_sequence_number_gap() {
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let order_book = OrderBook::new(&snapshot).unwrap();
        let mut buffered_book = BufferedOrderBook::new(order_book);

        // Create an update with a sequence number gap
        let update = create_test_update(security_id, 102);
        let result = buffered_book.apply_update(update);

        assert!(matches!(result, Err(Errors::SequenceNumberGap)));
        assert_eq!(buffered_book.order_book.seq_no, 100);
        assert_eq!(buffered_book.pending_updates.len(), 1);
        assert!(buffered_book.pending_updates.contains_key(&102));
    }

    #[test]
    fn test_buffered_apply_snapshot() {
        let security_id = 1001;
        let snapshot1 = create_test_snapshot(security_id, 100);
        let order_book = OrderBook::new(&snapshot1).unwrap();
        let mut buffered_book = BufferedOrderBook::new(order_book);

        // Add an update with a sequence number gap
        let update = create_test_update(security_id, 102);
        let result = buffered_book.apply_update(update);
        assert!(matches!(result, Err(Errors::SequenceNumberGap)));

        // Apply a new snapshot with a higher sequence number
        let snapshot2 = create_test_snapshot(security_id, 101);
        let result = buffered_book.apply_snapshot(&snapshot2);

        // Should apply pending update after snapshot
        assert!(result.is_ok());
        assert_eq!(buffered_book.order_book.seq_no, 102);
        assert!(buffered_book.pending_updates.is_empty());
    }

    #[test]
    fn test_buffered_multiple_pending_updates() {
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let order_book = OrderBook::new(&snapshot).unwrap();
        let mut buffered_book = BufferedOrderBook::new(order_book);

        // Add updates with increasing sequence gaps
        let update1 = create_test_update(security_id, 102);
        let update2 = create_test_update(security_id, 103);
        let update3 = create_test_update(security_id, 104);

        buffered_book.apply_update(update1).unwrap_err();
        buffered_book.apply_update(update2).unwrap_err();
        buffered_book.apply_update(update3).unwrap_err();

        assert_eq!(buffered_book.pending_updates.len(), 3);

        // Apply the missing update
        let fill_gap_update = create_test_update(security_id, 101);
        let result = buffered_book.apply_update(fill_gap_update);

        // Should apply all pending updates
        assert!(result.is_ok());
        assert_eq!(buffered_book.order_book.seq_no, 104);
        assert!(buffered_book.pending_updates.is_empty());
    }

    #[test]
    fn test_buffered_max_pending_updates() {
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let order_book = OrderBook::new(&snapshot).unwrap();
        let mut buffered_book = BufferedOrderBook::new(order_book);

        // Add MAX_PENDING_UPDATES pending updates
        let start_seq = 102;
        for i in 0..BufferedOrderBook::MAX_PENDING_UPDATES {
            let seq_no = start_seq + i as u64;
            let update = create_test_update(security_id, seq_no);
            buffered_book.apply_update(update).unwrap_err();
        }

        assert_eq!(
            buffered_book.pending_updates.len(),
            BufferedOrderBook::MAX_PENDING_UPDATES
        );

        // Add 5 more updates which should cause the oldest ones to be dropped
        for i in 0..5 {
            let seq_no = start_seq + BufferedOrderBook::MAX_PENDING_UPDATES as u64 + i;
            let update = create_test_update(security_id, seq_no);
            buffered_book.apply_update(update).unwrap_err();
        }

        // Still should have MAX_PENDING_UPDATES
        assert_eq!(
            buffered_book.pending_updates.len(),
            BufferedOrderBook::MAX_PENDING_UPDATES
        );

        // The first 5 keys should be dropped
        for i in 0..5 {
            let seq_no = start_seq + i as u64;
            assert!(!buffered_book.pending_updates.contains_key(&seq_no));
        }

        // The last 5 keys should be present
        for i in 0..5 {
            let seq_no = start_seq + BufferedOrderBook::MAX_PENDING_UPDATES as u64 + i;
            assert!(buffered_book.pending_updates.contains_key(&seq_no));
        }
    }

    #[test]
    fn test_buffered_old_update_ignored() {
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let order_book = OrderBook::new(&snapshot).unwrap();
        let mut buffered_book = BufferedOrderBook::new(order_book);

        // Create an update with an older sequence number
        let old_update = create_test_update(security_id, 99);
        let result = buffered_book.apply_update(old_update);

        assert!(matches!(result, Err(Errors::OldSequenceNumber)));
        assert_eq!(buffered_book.order_book.seq_no, 100);
        assert!(buffered_book.pending_updates.is_empty());
    }

    #[test]
    fn test_buffered_partial_update_application() {
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let order_book = OrderBook::new(&snapshot).unwrap();
        let mut buffered_book = BufferedOrderBook::new(order_book);

        // Add updates with sequence gaps between 103 and 105
        let update1 = create_test_update(security_id, 102);
        let update2 = create_test_update(security_id, 103);
        let update3 = create_test_update(security_id, 105);

        buffered_book.apply_update(update1).unwrap_err();
        buffered_book.apply_update(update2).unwrap_err();
        buffered_book.apply_update(update3).unwrap_err();

        assert_eq!(buffered_book.pending_updates.len(), 3);

        // Apply the first missing update
        let fill_first_gap = create_test_update(security_id, 101);
        let result = buffered_book.apply_update(fill_first_gap);

        // Check that uppdated applied up to seq_no 103
        assert!(result.is_ok());
        assert_eq!(buffered_book.order_book.seq_no, 103);

        // Check that the update with seq_no 105 is still in pending
        assert_eq!(buffered_book.pending_updates.len(), 1);
        assert!(buffered_book.pending_updates.contains_key(&105));
    }
}
