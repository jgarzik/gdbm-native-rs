use std::{fmt::Display, fmt::Formatter, io};

#[derive(Debug)]
pub enum Error {
    /// IO error.
    Io(io::Error),
    /// Bucket has too many elements or bucket bits > directory bits.
    BadBucket {
        /// Bucket file offset.
        offset: u64,
        /// Number of elements in bucket.
        elems: u32,
        /// Bucket bits.
        bits: u32,
        /// Number of elements expected.
        max_elems: u32,
        /// Directory bits.
        dir_bits: u32,
    },
    /// Tried to open with readonly and either creat or write.
    ConflictingOpenOptions,
    /// There is no data.
    EmptyFile,
    /// Could not use supplied block size and bsexact was specified.
    BadBlockSize {
        /// Requested blocksize.
        requested: u32,
        /// Actual blocksize.
        actual: u32,
    },
    /// A directory entry points outside the file.
    BadDirectory {
        /// Start of directory in file.
        offset: u64,
        /// Directory length in bytes.
        length: u32,
    },
    /// Attempting write operation on readonly database.
    WriteToReadonly,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", self)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
