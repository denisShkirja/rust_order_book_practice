use crate::parsing::parser::ParserError;
use crate::parsing::parser::{DefaultParser, Parser};
use std::fs::File;
use std::io::{self, BufReader};

pub struct BinaryFileIterator<T: DefaultParser<T>> {
    reader: BufReader<File>,
    parser: T::ParserType,
}

impl<T: DefaultParser<T>> BinaryFileIterator<T> {
    pub fn new(file: File) -> Self {
        Self {
            reader: BufReader::new(file),
            parser: T::default_parser(),
        }
    }
}

impl<T: DefaultParser<T>> Iterator for BinaryFileIterator<T> {
    type Item = io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.parser.read(&mut self.reader) {
            Ok(item) => Some(Ok(item)),
            Err(err) => match err {
                ParserError::Io(io_err) => Some(Err(io_err)),
                ParserError::ExpectedEof => None,
                ParserError::Custom(msg) => {
                    Some(Err(io::Error::new(io::ErrorKind::InvalidData, msg)))
                }
            },
        }
    }
}
