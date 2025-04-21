#[derive(Debug)]
pub struct UpdateMessageInfo {
    pub security_id: u64,
    pub seq_no: u64,
}

#[derive(Debug)]
pub enum Errors {
    SequenceNumberGap,
    OldSequenceNumber,
    InvalidPrice(UpdateMessageInfo, String),
    InvalidSide(UpdateMessageInfo, String),
    SecurityIdMismatch,
    OrderBookNotFound,
}
