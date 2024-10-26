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
    /// Block size found in header is impossibly small.
    BadHeaderBlockSize {
        /// Block size from header.
        size: u32,
        /// Minimum is sizeof header.
        minimum: u32,
    },
    /// Next block offset in header is greater than file size.
    BadHeaderNextBlock {
        /// Next block from header.
        next_block: u64,
        /// Database file size.
        file_size: u64,
    },
    /// Directory offset + size in header puts it outside the file.
    BadHeaderDirectoryOffset {
        /// Directory offset from header.
        offset: u64,
        /// Directory sise from header.
        size: u32,
        /// Database file size.
        file_size: u64,
    },
    /// Directory size and bits in header are inconsistent.
    BadHeaderDirectory {
        /// Directory size in header.
        size: u32,
        /// Directory bits in header.
        bits: u32,
        /// Minimum size calculated from header block size.
        minimum_size: u32,
        /// Expected bits calculated from header directory size.
        expected_bits: u32,
    },
    /// Bucket size in header is impossibly small.
    BadHeaderBucketSize {
        /// Size found in header.
        size: u32,
        /// Minimum is sizeof bucket header and one bucket element.
        minimum: u32,
    },
    /// Bucket elements in header inconsistent with bucket size.
    BadHeaderBucketElems { elems: u32, expected: u32 },
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
