use std::io::{self, Read};

#[derive(Debug)]
pub enum ParserError {
    ExpectedEof,
    Custom(String),
    Io(io::Error),
}

pub trait Parser<T> {
    fn read<R: Read>(&mut self, reader: &mut R) -> Result<T, ParserError>;
}

pub trait DefaultParser<T> {
    type ParserType: Parser<T>;

    fn default_parser() -> Self::ParserType;
}
