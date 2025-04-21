use std::collections::BTreeMap;
use std::fmt::Display;

use crate::order_book::buffered_order_book::BufferedOrderBook;
use crate::order_book::errors::Errors;
use crate::order_book::order_book::OrderBook;
use crate::parsing::order_book_snapshot::OrderBookSnapshot;
use crate::parsing::order_book_update::OrderBookUpdate;

#[derive(Default)]
pub struct Manager {
    pub buffered_order_books: BTreeMap<u64, BufferedOrderBook>,
}

impl Manager {
    pub fn apply_update(&mut self, update: OrderBookUpdate) -> Result<(), Errors> {
        if let Some(order_book) = self.buffered_order_books.get_mut(&update.security_id) {
            order_book.apply_update(update)
        } else {
            Err(Errors::OrderBookNotFound)
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: &OrderBookSnapshot) -> Result<(), Errors> {
        match self.buffered_order_books.entry(snapshot.security_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                let order_book = OrderBook::new(snapshot)?;
                let buffered_order_book = BufferedOrderBook::new(order_book);
                entry.insert(buffered_order_book);
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                entry.get_mut().apply_snapshot(snapshot)
            }
        }
    }
}

impl Display for Manager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for buffered_order_book in self.buffered_order_books.values() {
            write!(f, "{}", buffered_order_book)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generational_deque::generation_guard::GenerationGuard;
    use crate::generational_deque::generational_deque::GenerationalDeque;
    use crate::parsing::order_book_snapshot::Level as SnapshotLevel;
    use crate::parsing::order_book_update::Level as UpdateLevel;
    use std::cell::RefCell;
    use std::rc::Rc;

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
        let deque = Rc::new(RefCell::new(GenerationalDeque::new(10)));
        let start_index = deque.borrow().end_index();

        {
            let mut deque_ref = deque.borrow_mut();
            // Add bid level
            deque_ref.push_back(UpdateLevel {
                side: 0,
                price: 99.00,
                qty: 25,
                seq_no,
            });
            // Add ask level
            deque_ref.push_back(UpdateLevel {
                side: 1,
                price: 101.00,
                qty: 30,
                seq_no,
            });
        }

        OrderBookUpdate {
            timestamp: 1627846266,
            seq_no,
            security_id,
            updates: GenerationGuard::new(Rc::clone(&deque), start_index, 2, seq_no as usize),
        }
    }

    #[test]
    fn test_apply_snapshot_to_new_security_id() {
        let mut manager = Manager::default();
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);

        let result = manager.apply_snapshot(&snapshot);

        assert!(result.is_ok());
        assert!(manager.buffered_order_books.contains_key(&security_id));
        assert_eq!(manager.buffered_order_books.len(), 1);
    }

    #[test]
    fn test_apply_snapshot_to_existing_security_id() {
        let mut manager = Manager::default();
        let security_id = 1001;

        // Add first snapshot
        let snapshot1 = create_test_snapshot(security_id, 100);
        let _ = manager.apply_snapshot(&snapshot1);

        // Add second snapshot with same security_id but higher seq_no
        let snapshot2 = create_test_snapshot(security_id, 101);
        let result = manager.apply_snapshot(&snapshot2);

        assert!(result.is_ok());
        assert_eq!(manager.buffered_order_books.len(), 1);
    }

    #[test]
    fn test_apply_update_to_nonexistent_security_id() {
        let mut manager = Manager::default();
        let security_id = 1001;
        let update = create_test_update(security_id, 100);

        let result = manager.apply_update(update);

        assert!(matches!(result, Err(Errors::OrderBookNotFound)));
        assert!(manager.buffered_order_books.is_empty());
    }

    #[test]
    fn test_apply_update_to_existing_security_id() {
        let mut manager = Manager::default();
        let security_id = 1001;

        // First add a snapshot
        let snapshot = create_test_snapshot(security_id, 100);
        let _ = manager.apply_snapshot(&snapshot);

        // Then apply an update
        let update = create_test_update(security_id, 101);
        let result = manager.apply_update(update);

        assert!(result.is_ok());
    }

    #[test]
    fn test_multiple_security_ids() {
        let mut manager = Manager::default();

        // Add snapshots for different security_ids
        let security_id1 = 1001;
        let security_id2 = 1002;

        let snapshot1 = create_test_snapshot(security_id1, 100);
        let snapshot2 = create_test_snapshot(security_id2, 200);

        let result1 = manager.apply_snapshot(&snapshot1);
        let result2 = manager.apply_snapshot(&snapshot2);

        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert_eq!(manager.buffered_order_books.len(), 2);
        assert!(manager.buffered_order_books.contains_key(&security_id1));
        assert!(manager.buffered_order_books.contains_key(&security_id2));
    }
}
