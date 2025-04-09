use chrono;
use num_traits::FromPrimitive;
use rust_decimal::{Decimal, dec};
use std::collections::BTreeMap;
use std::fmt::Display;

use crate::l2_order_book::errors::Errors;
use crate::l2_order_book::errors::UpdateMessageInfo;
use crate::parsing::order_book_snapshot::OrderBookSnapshot;
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

        // Prepare updates
        for upd in &update.updates {
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
        }

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
        let mut invalid_update = create_test_update(security_id, 101);
        // Make price invalid by setting it to a non-multiple of PRICE_TICK
        invalid_update.updates[1].price += 0.005;
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
        let mut invalid_update = create_test_update(security_id, 101);
        // Make price invalid by setting it to NaN
        invalid_update.updates[1].price = f64::NAN;
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
        let mut invalid_update = create_test_update(security_id, 101);
        // Make side invalid by setting it to a value other than 0 or 1
        invalid_update.updates[1].side = 2;
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
        let mut update = create_test_update(security_id, 101);
        update.updates[0] = Update {
            side: 0,
            price: 100.00,
            qty: 0,
        };

        // Apply the update
        order_book.apply_update(&update).unwrap();

        assert!(
            !order_book
                .bids
                .contains_key(&Decimal::from_f64(100.00).unwrap())
        );
    }
}
