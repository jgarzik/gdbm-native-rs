use std::{fmt::Display, fmt::Formatter, io};

#[derive(Debug)]
pub enum Error {
    /// Failed to convert a stored key or value into requested type.
    BadData(String),
    /// IO error.
    Io(io::Error),
    /// Database may be inconsistent since an earlier error. Writes are disabled.
    Inconsistent,
    /// Bucket element describes a record outside the allocated area of the file.
    BadBucketElement {
        /// Record file offset.
        data_offset: u64,
        /// Key size.
        key_size: u32,
        /// Data size.
        data_size: u32,
        /// End of allocated file space (`next_block` from the header).
        next_block: u64,
    },
    /// Key plus value exceeds the maximum record size supported by the GDBM file format.
    RecordTooLarge {
        /// Key plus value size.
        size: usize,
        /// Maximum record size.
        maximum: usize,
    },
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
    /// Next block offset in header is less than file size.
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
    /// Free space lies outside the allocated area of the file.
    BadAvailElem {
        /// Start of the avail block in the file.
        block_offset: u64,
        /// Elem number.
        elem: usize,
        /// Offset of free space.
        offset: u64,
        /// Size of free space.
        size: u32,
        /// End of allocated file space (`next_block` from the header).
        next_block: u64,
    },
    /// Avail size is 0 or blocksize in header not sufficient for header + available block.
    BadHeaderAvail {
        /// Number of avail elements per block in header.
        elems: u32,
        /// Size in bytes of avail block.
        size: u32,
        /// Block size.
        block_size: u32,
    },
    /// Too many items in an avail block.
    BadAvailCount {
        /// Start of the avail block in the file.
        block_offset: u64,
        /// Number of elements in avail block.
        elems: u32,
        /// Maximum the block can hold.
        maximum: u32,
    },
    /// Numsync version must be 0.
    BadNumsyncVersion {
        /// Numsync version from header.
        version: u32,
    },
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{self:?}")
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
