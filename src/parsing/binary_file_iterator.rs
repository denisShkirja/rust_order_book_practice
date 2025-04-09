use binread::{self, BinRead};
use std::fs::File;
use std::io::{self, BufReader};

pub struct BinaryFileIterator<T: BinRead> {
    reader: BufReader<File>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: BinRead> BinaryFileIterator<T> {
    pub fn new(file: File) -> Self {
        Self {
            reader: BufReader::new(file),
            _phantom: std::marker::PhantomData,
        }
    }
}

fn format_binread_error(err: &binread::Error) -> String {
    match err {
        binread::Error::Custom { pos, err } => {
            let pos_str = format!("at position 0x{:x}", pos);

            if let Some(string_err) = err.downcast_ref::<String>() {
                format!("Parsing error {} - {}", pos_str, string_err)
            } else {
                format!("Parsing error {}: {:?}", pos_str, err)
            }
        }
        _ => format!("Parsing error: {:?}", err),
    }
}

impl<T: BinRead> Iterator for BinaryFileIterator<T> {
    type Item = io::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match T::read(&mut self.reader) {
            Ok(item) => Some(Ok(item)),
            Err(err) => {
                if let binread::Error::Io(io_err) = err {
                    if io_err.kind() == io::ErrorKind::UnexpectedEof {
                        return None;
                    }
                    Some(Err(io_err))
                } else {
                    Some(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format_binread_error(&err),
                    )))
                }
            }
        }
    }
}
