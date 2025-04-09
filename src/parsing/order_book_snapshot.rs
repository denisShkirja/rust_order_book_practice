use binread::BinRead;

#[derive(Debug, BinRead)]
#[br(little)]
pub struct Level {
    pub price: f64,
    pub qty: u64,
}

#[derive(Debug, BinRead)]
#[br(little)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use binread::BinRead;
    use std::io::Cursor;

    #[test]
    fn test_read_orderbook_snapshot() {
        let timestamp: u64 = 1627846265;
        let seq_no: u64 = 42;
        let security_id: u64 = 1001;

        let bid1 = Level {
            price: 100.50,
            qty: 10,
        };
        let ask1 = Level {
            price: 101.00,
            qty: 15,
        };
        let bid2 = Level {
            price: 100.25,
            qty: 20,
        };
        let ask2 = Level {
            price: 101.25,
            qty: 25,
        };
        let bid3 = Level {
            price: 100.00,
            qty: 30,
        };
        let ask3 = Level {
            price: 101.50,
            qty: 35,
        };
        let bid4 = Level {
            price: 99.75,
            qty: 40,
        };
        let ask4 = Level {
            price: 101.75,
            qty: 45,
        };
        let bid5 = Level {
            price: 99.50,
            qty: 50,
        };
        let ask5 = Level {
            price: 102.00,
            qty: 55,
        };

        let mut buf = Vec::new();
        buf.extend(&timestamp.to_le_bytes());
        buf.extend(&seq_no.to_le_bytes());
        buf.extend(&security_id.to_le_bytes());

        buf.extend(&bid1.price.to_le_bytes());
        buf.extend(&bid1.qty.to_le_bytes());
        buf.extend(&ask1.price.to_le_bytes());
        buf.extend(&ask1.qty.to_le_bytes());

        buf.extend(&bid2.price.to_le_bytes());
        buf.extend(&bid2.qty.to_le_bytes());
        buf.extend(&ask2.price.to_le_bytes());
        buf.extend(&ask2.qty.to_le_bytes());

        buf.extend(&bid3.price.to_le_bytes());
        buf.extend(&bid3.qty.to_le_bytes());
        buf.extend(&ask3.price.to_le_bytes());
        buf.extend(&ask3.qty.to_le_bytes());

        buf.extend(&bid4.price.to_le_bytes());
        buf.extend(&bid4.qty.to_le_bytes());
        buf.extend(&ask4.price.to_le_bytes());
        buf.extend(&ask4.qty.to_le_bytes());

        buf.extend(&bid5.price.to_le_bytes());
        buf.extend(&bid5.qty.to_le_bytes());
        buf.extend(&ask5.price.to_le_bytes());
        buf.extend(&ask5.qty.to_le_bytes());

        let mut cursor = Cursor::new(&buf);
        let snapshot = OrderBookSnapshot::read(&mut cursor).unwrap();

        assert_eq!(snapshot.timestamp, timestamp);
        assert_eq!(snapshot.seq_no, seq_no);
        assert_eq!(snapshot.security_id, security_id);

        assert_eq!(snapshot.bid1.price, bid1.price);
        assert_eq!(snapshot.bid1.qty, bid1.qty);
        assert_eq!(snapshot.ask1.price, ask1.price);
        assert_eq!(snapshot.ask1.qty, ask1.qty);

        assert_eq!(snapshot.bid2.price, bid2.price);
        assert_eq!(snapshot.bid2.qty, bid2.qty);
        assert_eq!(snapshot.ask2.price, ask2.price);
        assert_eq!(snapshot.ask2.qty, ask2.qty);

        assert_eq!(snapshot.bid3.price, bid3.price);
        assert_eq!(snapshot.bid3.qty, bid3.qty);
        assert_eq!(snapshot.ask3.price, ask3.price);
        assert_eq!(snapshot.ask3.qty, ask3.qty);

        assert_eq!(snapshot.bid4.price, bid4.price);
        assert_eq!(snapshot.bid4.qty, bid4.qty);
        assert_eq!(snapshot.ask4.price, ask4.price);
        assert_eq!(snapshot.ask4.qty, ask4.qty);

        assert_eq!(snapshot.bid5.price, bid5.price);
        assert_eq!(snapshot.bid5.qty, bid5.qty);
        assert_eq!(snapshot.ask5.price, ask5.price);
        assert_eq!(snapshot.ask5.qty, ask5.qty);
    }

    #[test]
    fn test_unexpected_eof() {
        let timestamp: u64 = 1627846265;
        let seq_no: u64 = 42;
        let mut buf = Vec::new();
        buf.extend(&timestamp.to_le_bytes());
        buf.extend(&seq_no.to_le_bytes());

        let mut cursor = Cursor::new(&buf);

        match OrderBookSnapshot::read(&mut cursor) {
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
