use std::{fmt::Display, fmt::Formatter, io};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Corruption(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", self)
    }
}
