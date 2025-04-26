use crate::order_book::errors::Errors;
use crate::order_book::order_book::OrderBook;
use crate::parsing::order_book_snapshot::OrderBookSnapshot;
use crate::parsing::order_book_update::OrderBookUpdate;
use std::collections::HashMap;
use std::fmt::Display;

pub struct BufferedOrderBook {
    pub order_book: OrderBook,
    pub pending_updates: HashMap<u64, OrderBookUpdate>,
}

impl BufferedOrderBook {
    pub const MAX_PENDING_UPDATES: usize = 10000;

    pub fn new(order_book: OrderBook) -> Self {
        Self {
            order_book,
            pending_updates: HashMap::new(),
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
                        // In the real world, with the snapshot and update streams open,
                        // this most likely means that most of the updates are old and we
                        // can just drop them because the next snapshot will include them all.
                        self.pending_updates.clear();
                    }
                    self.pending_updates.insert(update.seq_no, update);
                    Err(e)
                }
                _ => Err(e),
            },
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: &OrderBookSnapshot) -> Result<(), Errors> {
        let old_seq_no = self.order_book.seq_no;

        match self.order_book.apply_snapshot(snapshot) {
            Ok(_) => {
                // Remove all pending updates that are now in the snapshot
                for seq_no in old_seq_no..snapshot.seq_no {
                    self.pending_updates.remove(&seq_no);
                }
                self.try_apply_pending_updates();
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn try_apply_pending_updates(&mut self) {
        loop {
            let next_seq_no = self.order_book.seq_no + 1;

            if let Some(update) = self.pending_updates.remove(&next_seq_no) {
                if self.order_book.apply_update(&update).is_err() {
                    break;
                }
            } else {
                break;
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
    use crate::batched_deque::batched_deque::BatchedDeque;
    use crate::parsing::order_book_snapshot::Level as SnapshotLevel;
    use crate::parsing::order_book_update::Level as UpdateLevel;
    use num_traits::FromPrimitive;
    use rust_decimal::Decimal;

    fn create_test_snapshot(security_id: u64, seq_no: u64) -> OrderBookSnapshot {
        OrderBookSnapshot {
            timestamp: 1627846265,
            seq_no,
            security_id,
            bid1: SnapshotLevel {
                price: 100.00,
                qty: 10,
            },
            ask1: SnapshotLevel {
                price: 101.00,
                qty: 15,
            },
            bid2: SnapshotLevel {
                price: 99.00,
                qty: 20,
            },
            ask2: SnapshotLevel {
                price: 102.00,
                qty: 25,
            },
            bid3: SnapshotLevel {
                price: 98.00,
                qty: 30,
            },
            ask3: SnapshotLevel {
                price: 103.00,
                qty: 35,
            },
            bid4: SnapshotLevel {
                price: 97.00,
                qty: 40,
            },
            ask4: SnapshotLevel {
                price: 104.00,
                qty: 45,
            },
            bid5: SnapshotLevel {
                price: 96.00,
                qty: 50,
            },
            ask5: SnapshotLevel {
                price: 105.00,
                qty: 55,
            },
        }
    }

    fn create_test_update(security_id: u64, seq_no: u64) -> OrderBookUpdate {
        // Create a deque and add test levels
        let deque = BatchedDeque::new(10);
        let levels: Vec<Result<UpdateLevel, ()>> = vec![
            Ok(UpdateLevel {
                side: 0,
                price: 99.50,
                qty: 25,
            }),
            Ok(UpdateLevel {
                side: 1,
                price: 100.50,
                qty: 30,
            }),
        ];

        OrderBookUpdate {
            timestamp: 1627846266,
            seq_no,
            security_id,
            updates: deque.push_back_batch(levels.into_iter()).unwrap(),
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

        let update = create_test_update(security_id, 104);
        let result = buffered_book.apply_update(update);
        assert!(matches!(result, Err(Errors::SequenceNumberGap)));

        // Apply a new snapshot with a higher sequence number
        let snapshot2 = create_test_snapshot(security_id, 103);
        let result = buffered_book.apply_snapshot(&snapshot2);

        // Should apply pending update after snapshot
        assert!(result.is_ok());
        assert_eq!(buffered_book.order_book.seq_no, 104);
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

        // Add one more update which should cause all previous updates to be cleared
        let new_seq_no = start_seq + BufferedOrderBook::MAX_PENDING_UPDATES as u64;
        let new_update = create_test_update(security_id, new_seq_no);
        buffered_book.apply_update(new_update).unwrap_err();

        // Should now just have the single new update
        assert_eq!(buffered_book.pending_updates.len(), 1);
        assert!(buffered_book.pending_updates.contains_key(&new_seq_no));
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

    #[test]
    fn test_buffered_duplicate_update_handling() {
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let order_book = OrderBook::new(&snapshot).unwrap();
        let mut buffered_book = BufferedOrderBook::new(order_book);

        // Create an update with a sequence number gap
        let deque = BatchedDeque::new(10);
        let update102 = {
            let levels: Vec<Result<UpdateLevel, ()>> = vec![Ok(UpdateLevel {
                side: 0,
                price: 99.51,
                qty: 100,
            })];
            deque.push_back_batch(levels.into_iter()).unwrap()
        };
        let result = buffered_book.apply_update(OrderBookUpdate {
            timestamp: 1627846266,
            seq_no: 102,
            security_id,
            updates: update102,
        });
        // Should be added to pending updates
        assert!(matches!(result, Err(Errors::SequenceNumberGap)));
        assert_eq!(buffered_book.pending_updates.len(), 1);
        assert!(buffered_book.pending_updates.contains_key(&102));

        // Create another update with a sequence number gap
        let levels: Vec<Result<UpdateLevel, ()>> = vec![Ok(UpdateLevel {
            side: 0,
            price: 99.50,
            qty: 200,
        })];
        let update103 = deque.push_back_batch(levels.into_iter()).unwrap();
        let result = buffered_book.apply_update(OrderBookUpdate {
            timestamp: 1627846266,
            seq_no: 103,
            security_id,
            updates: update103,
        });
        // Should be added to pending updates
        assert!(matches!(result, Err(Errors::SequenceNumberGap)));
        assert_eq!(buffered_book.pending_updates.len(), 2);
        assert!(buffered_book.pending_updates.contains_key(&102));
        assert!(buffered_book.pending_updates.contains_key(&103));

        // Create duplicate update with the same sequence number
        let levels: Vec<Result<UpdateLevel, ()>> = vec![Ok(UpdateLevel {
            side: 0,
            price: 99.50,
            qty: 200,
        })];
        let update103 = deque.push_back_batch(levels.into_iter()).unwrap();
        let result = buffered_book.apply_update(OrderBookUpdate {
            timestamp: 1627846266,
            seq_no: 103,
            security_id,
            updates: update103,
        });
        // Still should have only two pending updates
        assert!(matches!(result, Err(Errors::SequenceNumberGap)));
        assert_eq!(buffered_book.pending_updates.len(), 2);
        assert!(buffered_book.pending_updates.contains_key(&102));
        assert!(buffered_book.pending_updates.contains_key(&103));

        // Now fill the gap and apply pending updates
        let levels: Vec<Result<UpdateLevel, ()>> = vec![Ok(UpdateLevel {
            side: 0,
            price: 99.52,
            qty: 99,
        })];
        let update101 = deque.push_back_batch(levels.into_iter()).unwrap();
        let result = buffered_book.apply_update(OrderBookUpdate {
            timestamp: 1627846266,
            seq_no: 101,
            security_id,
            updates: update101,
        });
        // Should successfully apply both the gap-filling update and the pending update
        assert!(result.is_ok());
        assert_eq!(buffered_book.order_book.seq_no, 103);
        assert!(buffered_book.pending_updates.is_empty());

        // Check that all price levels from the pending updates exist in the order book
        assert_eq!(
            buffered_book
                .order_book
                .bids
                .get(&Decimal::from_f64(99.51).unwrap()),
            Some(&100)
        );
        assert_eq!(
            buffered_book
                .order_book
                .bids
                .get(&Decimal::from_f64(99.50).unwrap()),
            Some(&200)
        );
        assert_eq!(
            buffered_book
                .order_book
                .bids
                .get(&Decimal::from_f64(99.52).unwrap()),
            Some(&99)
        );
    }
}
