use num_traits::FromPrimitive;
use rust_decimal::{Decimal, dec};
use std::collections::BTreeMap;
use std::fmt::Display;

use crate::order_book::errors::Errors;
use crate::order_book::errors::UpdateMessageInfo;
use crate::parsing::order_book_snapshot::OrderBookSnapshot;
use crate::parsing::order_book_update::Level as UpdateLevel;
use crate::parsing::order_book_update::OrderBookUpdate;

#[derive(Debug)]
pub struct OrderBook {
    pub timestamp: u64,
    pub seq_no: u64,
    pub security_id: u64,
    pub bids: BTreeMap<Decimal, u64>,
    pub asks: BTreeMap<Decimal, u64>,

    bid_updates: Vec<(Decimal, u64)>,
    ask_updates: Vec<(Decimal, u64)>,
}

impl OrderBook {
    pub const PRICE_TICK: Decimal = dec!(0.01);

    pub fn new(snapshot: &OrderBookSnapshot) -> Result<Self, Errors> {
        let mut order_book = Self {
            timestamp: snapshot.timestamp,
            seq_no: snapshot.seq_no,
            security_id: snapshot.security_id,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            bid_updates: Vec::new(),
            ask_updates: Vec::new(),
        };
        Self::apply_snapshot_sides(&mut order_book, snapshot)?;

        Ok(order_book)
    }

    pub fn apply_update(&mut self, update: &OrderBookUpdate) -> Result<(), Errors> {
        if update.security_id != self.security_id {
            return Err(Errors::SecurityIdMismatch);
        }
        if update.seq_no <= self.seq_no {
            return Err(Errors::OldSequenceNumber);
        }
        if update.seq_no != self.seq_no + 1 {
            return Err(Errors::SequenceNumberGap);
        }

        self.ask_updates.clear();
        self.bid_updates.clear();

        // Prepare updates
        update
            .updates
            .for_each(|upd: &UpdateLevel| -> Result<(), Errors> {
                let price = Self::normalized_price(update.security_id, update.seq_no, upd.price)?;
                match upd.side {
                    0 => self.bid_updates.push((price, upd.qty)),
                    1 => self.ask_updates.push((price, upd.qty)),
                    _ => {
                        return Err(Errors::InvalidSide(
                            UpdateMessageInfo {
                                security_id: update.security_id,
                                seq_no: update.seq_no,
                            },
                            format!("{}", upd.side),
                        ));
                    }
                }
                Ok(())
            })?;

        // Apply updates atomically
        for (price, qty) in self.bid_updates.drain(..) {
            if qty == 0 {
                self.bids.remove(&price);
            } else {
                self.bids.insert(price, qty);
            }
        }
        for (price, qty) in self.ask_updates.drain(..) {
            if qty == 0 {
                self.asks.remove(&price);
            } else {
                self.asks.insert(price, qty);
            }
        }

        self.timestamp = update.timestamp;
        self.seq_no = update.seq_no;

        Ok(())
    }

    pub fn apply_snapshot(&mut self, snapshot: &OrderBookSnapshot) -> Result<(), Errors> {
        if snapshot.security_id != self.security_id {
            return Err(Errors::SecurityIdMismatch);
        }
        if snapshot.seq_no <= self.seq_no {
            return Err(Errors::OldSequenceNumber);
        }

        Self::apply_snapshot_sides(self, snapshot)?;

        self.timestamp = snapshot.timestamp;
        self.seq_no = snapshot.seq_no;

        Ok(())
    }

    fn apply_snapshot_sides(&mut self, snapshot: &OrderBookSnapshot) -> Result<(), Errors> {
        self.ask_updates.clear();
        self.bid_updates.clear();

        // Prepare asks
        if snapshot.ask1.qty > 0 {
            self.ask_updates.push((
                Self::normalized_price(snapshot.security_id, snapshot.seq_no, snapshot.ask1.price)?,
                snapshot.ask1.qty,
            ));
        }
        if snapshot.ask2.qty > 0 {
            self.ask_updates.push((
                Self::normalized_price(snapshot.security_id, snapshot.seq_no, snapshot.ask2.price)?,
                snapshot.ask2.qty,
            ));
        }
        if snapshot.ask3.qty > 0 {
            self.ask_updates.push((
                Self::normalized_price(snapshot.security_id, snapshot.seq_no, snapshot.ask3.price)?,
                snapshot.ask3.qty,
            ));
        }
        if snapshot.ask4.qty > 0 {
            self.ask_updates.push((
                Self::normalized_price(snapshot.security_id, snapshot.seq_no, snapshot.ask4.price)?,
                snapshot.ask4.qty,
            ));
        }
        if snapshot.ask5.qty > 0 {
            self.ask_updates.push((
                Self::normalized_price(snapshot.security_id, snapshot.seq_no, snapshot.ask5.price)?,
                snapshot.ask5.qty,
            ));
        }

        // Prepare bids
        if snapshot.bid1.qty > 0 {
            self.bid_updates.push((
                Self::normalized_price(snapshot.security_id, snapshot.seq_no, snapshot.bid1.price)?,
                snapshot.bid1.qty,
            ));
        }
        if snapshot.bid2.qty > 0 {
            self.bid_updates.push((
                Self::normalized_price(snapshot.security_id, snapshot.seq_no, snapshot.bid2.price)?,
                snapshot.bid2.qty,
            ));
        }
        if snapshot.bid3.qty > 0 {
            self.bid_updates.push((
                Self::normalized_price(snapshot.security_id, snapshot.seq_no, snapshot.bid3.price)?,
                snapshot.bid3.qty,
            ));
        }
        if snapshot.bid4.qty > 0 {
            self.bid_updates.push((
                Self::normalized_price(snapshot.security_id, snapshot.seq_no, snapshot.bid4.price)?,
                snapshot.bid4.qty,
            ));
        }
        if snapshot.bid5.qty > 0 {
            self.bid_updates.push((
                Self::normalized_price(snapshot.security_id, snapshot.seq_no, snapshot.bid5.price)?,
                snapshot.bid5.qty,
            ));
        }

        // Apply updates atomically
        self.asks.clear();
        for (price, qty) in self.ask_updates.drain(..) {
            self.asks.insert(price, qty);
        }
        self.bids.clear();
        for (price, qty) in self.bid_updates.drain(..) {
            self.bids.insert(price, qty);
        }

        Ok(())
    }

    fn normalized_price(security_id: u64, seq_no: u64, price: f64) -> Result<Decimal, Errors> {
        match Decimal::from_f64(price) {
            Some(dec) => {
                if dec % Self::PRICE_TICK == dec!(0.0) {
                    Ok(dec)
                } else {
                    Err(Errors::InvalidPrice(
                        UpdateMessageInfo {
                            security_id,
                            seq_no,
                        },
                        format!(
                            "The price {} is not a multiple of {}",
                            price,
                            Self::PRICE_TICK
                        ),
                    ))
                }
            }
            None => Err(Errors::InvalidPrice(
                UpdateMessageInfo {
                    security_id,
                    seq_no,
                },
                format!("Failed to convert f64 value {} to Decimal", price),
            )),
        }
    }
}

impl Display for OrderBook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "OrderBook {{")?;

        let datetime =
            chrono::DateTime::<chrono::Utc>::from_timestamp_millis(self.timestamp as i64);
        let formatted_time = datetime
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
            .unwrap_or_else(|| "Invalid timestamp".to_string());
        writeln!(f, "  timestamp: {} ({})", self.timestamp, formatted_time)?;

        writeln!(f, "  seq_no: {}", self.seq_no)?;
        writeln!(f, "  security_id: {}", self.security_id)?;

        writeln!(f, "  asks: [")?;
        for (price, qty) in self.asks.iter().rev() {
            writeln!(f, "    {:.2} @ {}", price, qty)?;
        }
        writeln!(f, "  ]")?;

        writeln!(f, "  bids: [")?;
        for (price, qty) in self.bids.iter().rev() {
            writeln!(f, "    {:.2} @ {}", price, qty)?;
        }
        writeln!(f, "  ]")?;

        writeln!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generational_deque::generation_guard::GenerationGuard;
    use crate::generational_deque::generational_deque::GenerationalDeque;
    use crate::parsing::order_book_snapshot::Level as SnapshotLevel;
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
                price: 99.50,
                qty: 25,
                seq_no,
            });
            // Add ask level
            deque_ref.push_back(UpdateLevel {
                side: 1,
                price: 100.50,
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

    fn create_invalid_price_snapshot(security_id: u64, seq_no: u64) -> OrderBookSnapshot {
        let mut snapshot = create_test_snapshot(security_id, seq_no);
        // Make price invalid by setting it to a non-multiple of PRICE_TICK
        snapshot.ask5.price += 0.005;
        snapshot
    }

    #[test]
    fn test_successful_update() {
        // Create order book
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let mut order_book = OrderBook::new(&snapshot).unwrap();

        // Create an update and apply it
        let update = create_test_update(security_id, 101);
        let result = order_book.apply_update(&update);

        assert!(result.is_ok());
        assert_eq!(order_book.seq_no, 101);
        assert_eq!(order_book.timestamp, update.timestamp);

        assert_eq!(
            order_book.bids.get(&Decimal::from_f64(99.50).unwrap()),
            Some(&25)
        );
        assert_eq!(
            order_book.asks.get(&Decimal::from_f64(100.50).unwrap()),
            Some(&30)
        );
    }

    #[test]
    fn test_sequence_number_gap() {
        // Create order book
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let mut order_book = OrderBook::new(&snapshot).unwrap();

        // Create an update with a sequence number gap of 1
        let update = create_test_update(security_id, 102);
        let result = order_book.apply_update(&update);

        assert!(matches!(result, Err(Errors::SequenceNumberGap)));

        assert_eq!(order_book.seq_no, 100);
        assert_eq!(order_book.timestamp, snapshot.timestamp);
    }

    #[test]
    fn test_security_id_mismatch() {
        // Create order book
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let mut order_book = OrderBook::new(&snapshot).unwrap();

        // Create an update with a different security ID
        let different_security_id = 1002;
        let update = create_test_update(different_security_id, 101);
        let result = order_book.apply_update(&update);

        assert!(matches!(result, Err(Errors::SecurityIdMismatch)));

        // Try to apply a snapshot with a different security ID
        let different_snapshot = create_test_snapshot(different_security_id, 101);
        let snapshot_result = order_book.apply_snapshot(&different_snapshot);

        assert!(matches!(snapshot_result, Err(Errors::SecurityIdMismatch)));
    }

    #[test]
    fn test_invalid_price_in_new_order_book() {
        let security_id = 1001;
        let snapshot = create_invalid_price_snapshot(security_id, 100);

        // Attempt to create a new OrderBook with invalid price
        let result = OrderBook::new(&snapshot);

        assert!(matches!(result, Err(Errors::InvalidPrice(_, _))));
    }

    #[test]
    fn test_invalid_price_in_snapshot() {
        // Create order book
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let mut order_book = OrderBook::new(&snapshot).unwrap();

        // Apply a snapshot with an invalid price
        let invalid_snapshot = create_invalid_price_snapshot(security_id, 101);
        let result = order_book.apply_snapshot(&invalid_snapshot);

        assert!(matches!(result, Err(Errors::InvalidPrice(_, _))));

        assert_eq!(order_book.seq_no, 100);
    }

    #[test]
    fn test_invalid_tick_price_in_update() {
        // Create order book
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let mut order_book = OrderBook::new(&snapshot).unwrap();

        // Apply an update with an invalid price
        // Create a deque and add test levels with an invalid price
        let deque = Rc::new(RefCell::new(GenerationalDeque::new(10)));
        let start_index = deque.borrow().end_index();

        {
            let mut deque_ref = deque.borrow_mut();
            // Add bid level
            deque_ref.push_back(UpdateLevel {
                side: 0,
                price: 99.50,
                qty: 25,
                seq_no: 101,
            });
            // Add ask level with invalid price (not a multiple of PRICE_TICK)
            deque_ref.push_back(UpdateLevel {
                side: 1,
                price: 100.505, // Invalid price
                qty: 30,
                seq_no: 101,
            });
        }

        let invalid_update = OrderBookUpdate {
            timestamp: 1627846266,
            seq_no: 101,
            security_id,
            updates: GenerationGuard::new(Rc::clone(&deque), start_index, 2, 101),
        };

        let result = order_book.apply_update(&invalid_update);

        assert!(matches!(result, Err(Errors::InvalidPrice(_, _))));

        assert_eq!(order_book.seq_no, 100);
    }

    #[test]
    fn test_invalid_nan_price_in_update() {
        // Create order book
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let mut order_book = OrderBook::new(&snapshot).unwrap();

        // Apply an update with an invalid price
        // Create a deque and add test levels with a NaN price
        let deque = Rc::new(RefCell::new(GenerationalDeque::new(10)));
        let start_index = deque.borrow().end_index();

        {
            let mut deque_ref = deque.borrow_mut();
            // Add bid level
            deque_ref.push_back(UpdateLevel {
                side: 0,
                price: 99.50,
                qty: 25,
                seq_no: 101,
            });
            // Add ask level with NaN price
            deque_ref.push_back(UpdateLevel {
                side: 1,
                price: f64::NAN, // Invalid price
                qty: 30,
                seq_no: 101,
            });
        }

        let invalid_update = OrderBookUpdate {
            timestamp: 1627846266,
            seq_no: 101,
            security_id,
            updates: GenerationGuard::new(Rc::clone(&deque), start_index, 2, 101),
        };

        let result = order_book.apply_update(&invalid_update);

        assert!(matches!(result, Err(Errors::InvalidPrice(_, _))));

        assert_eq!(order_book.seq_no, 100);
    }

    #[test]
    fn test_invalid_side_in_update() {
        // Create order book
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let mut order_book = OrderBook::new(&snapshot).unwrap();

        // Apply an update with an invalid side
        // Create a deque and add test levels with an invalid side
        let deque = Rc::new(RefCell::new(GenerationalDeque::new(10)));
        let start_index = deque.borrow().end_index();

        {
            let mut deque_ref = deque.borrow_mut();
            // Add bid level
            deque_ref.push_back(UpdateLevel {
                side: 0,
                price: 99.50,
                qty: 25,
                seq_no: 101,
            });
            // Add level with invalid side
            deque_ref.push_back(UpdateLevel {
                side: 2, // Invalid side (not 0 or 1)
                price: 100.50,
                qty: 30,
                seq_no: 101,
            });
        }

        let invalid_update = OrderBookUpdate {
            timestamp: 1627846266,
            seq_no: 101,
            security_id,
            updates: GenerationGuard::new(Rc::clone(&deque), start_index, 2, 101),
        };

        let result = order_book.apply_update(&invalid_update);

        assert!(matches!(result, Err(Errors::InvalidSide(_, _))));

        assert_eq!(order_book.seq_no, 100);
    }

    #[test]
    fn test_old_snapshot_ignored() {
        // Create order book
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let mut order_book = OrderBook::new(&snapshot).unwrap();

        // Apply a snapshot with an older sequence number
        let old_snapshot = create_test_snapshot(security_id, 99);
        let result = order_book.apply_snapshot(&old_snapshot);

        assert!(matches!(result, Err(Errors::OldSequenceNumber)));

        assert_eq!(order_book.seq_no, 100);
    }

    #[test]
    fn test_old_update_ignored() {
        // Create order book
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let mut order_book = OrderBook::new(&snapshot).unwrap();

        // Apply an update with an older sequence number
        let old_update = create_test_update(security_id, 100);
        let result = order_book.apply_update(&old_update);

        assert!(matches!(result, Err(Errors::OldSequenceNumber)));

        assert_eq!(order_book.seq_no, 100);
    }

    #[test]
    fn test_apply_snapshot_clears_previous_state() {
        // Create order book
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let mut order_book = OrderBook::new(&snapshot).unwrap();

        // Apply an update to modify the state
        let update = create_test_update(security_id, 101);
        order_book.apply_update(&update).unwrap();

        // Apply a new snapshot with a higher sequence number
        let new_snapshot = create_test_snapshot(security_id, 102);
        order_book.apply_snapshot(&new_snapshot).unwrap();

        assert_eq!(order_book.seq_no, 102);
        assert_eq!(order_book.bids.len(), 5);
        assert_eq!(order_book.asks.len(), 5);

        assert!(
            !order_book
                .bids
                .contains_key(&Decimal::from_f64(99.50).unwrap())
        );
    }

    #[test]
    fn test_zero_quantity_removes_price_level() {
        // Create order book
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let mut order_book = OrderBook::new(&snapshot).unwrap();

        // Create an update that sets quantity to 0 for a specific price level
        // Create a deque with a level that has qty=0
        let deque = Rc::new(RefCell::new(GenerationalDeque::new(10)));
        let start_index = deque.borrow().end_index();

        {
            let mut deque_ref = deque.borrow_mut();
            // Add bid level with zero quantity to remove the price level
            deque_ref.push_back(UpdateLevel {
                side: 0,
                price: 100.00, // This price exists in the initial snapshot
                qty: 0,        // Setting to 0 should remove it
                seq_no: 101,
            });
        }

        let update = OrderBookUpdate {
            timestamp: 1627846266,
            seq_no: 101,
            security_id,
            updates: GenerationGuard::new(Rc::clone(&deque), start_index, 1, 101),
        };

        // Apply the update
        order_book.apply_update(&update).unwrap();

        assert!(
            !order_book
                .bids
                .contains_key(&Decimal::from_f64(100.00).unwrap())
        );
    }

    #[test]
    fn test_valid_update_after_invalid_update() {
        // Create order book
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let mut order_book = OrderBook::new(&snapshot).unwrap();

        // First try to apply an update with an invalid price
        let deque = Rc::new(RefCell::new(GenerationalDeque::new(10)));
        let start_index = deque.borrow().end_index();

        {
            let mut deque_ref = deque.borrow_mut();
            // Add bid level with valid price
            deque_ref.push_back(UpdateLevel {
                side: 0,
                price: 98.50,
                qty: 25,
                seq_no: 101,
            });
            // Add ask level with invalid price
            deque_ref.push_back(UpdateLevel {
                side: 1,
                price: 100.505, // Invalid price (not a multiple of PRICE_TICK)
                qty: 30,
                seq_no: 101,
            });
        }

        let invalid_update = OrderBookUpdate {
            timestamp: 1627846266,
            seq_no: 101,
            security_id,
            updates: GenerationGuard::new(Rc::clone(&deque), start_index, 2, 101),
        };

        let result = order_book.apply_update(&invalid_update);
        assert!(matches!(result, Err(Errors::InvalidPrice(_, _))));

        // Now apply a valid update with the same sequence number
        let valid_update = create_test_update(security_id, 101);
        let result = order_book.apply_update(&valid_update);
        assert!(result.is_ok());

        assert_eq!(order_book.seq_no, 101);

        // Check that the bid from the valid update is present at 99.50
        assert!(
            order_book
                .bids
                .contains_key(&Decimal::from_f64(99.50).unwrap())
        );

        // Verify the 98.50 price level is not in the bids (from the invalid update)
        assert!(
            !order_book
                .bids
                .contains_key(&Decimal::from_f64(98.50).unwrap())
        );
    }

    #[test]
    fn test_valid_snapshot_after_invalid_snapshot() {
        // Create order book
        let security_id = 1001;
        let snapshot = create_test_snapshot(security_id, 100);
        let mut order_book = OrderBook::new(&snapshot).unwrap();

        // Try to apply an invalid snapshot
        let mut invalid_snapshot = create_test_snapshot(security_id, 101);
        invalid_snapshot.ask4.price = 104.01;
        invalid_snapshot.bid4.price = 97.01;
        // Make price invalid by setting it to NaN
        invalid_snapshot.ask5.price = f64::NAN;
        let result = order_book.apply_snapshot(&invalid_snapshot);
        assert!(matches!(result, Err(Errors::InvalidPrice(_, _))));

        // Now apply a valid snapshot with the same sequence number
        let valid_snapshot = create_test_snapshot(security_id, 101);
        let result = order_book.apply_snapshot(&valid_snapshot);
        assert!(result.is_ok());

        assert_eq!(order_book.seq_no, 101);
        assert_eq!(order_book.bids.len(), 5);
        assert_eq!(order_book.asks.len(), 5);

        // Check that the levels from the invalid snapshot are not present
        assert!(
            !order_book
                .asks
                .contains_key(&Decimal::from_f64(104.01).unwrap())
        );
        assert!(
            !order_book
                .bids
                .contains_key(&Decimal::from_f64(97.01).unwrap())
        );
    }
}
