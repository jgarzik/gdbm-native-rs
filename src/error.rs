use std::{fmt::Display, fmt::Formatter, io};

/// Errors from database operations.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to convert a stored key or value into requested type.
    BadData(String),
    /// IO error.
    Io(io::Error),
    /// File does not start with a known GDBM magic number, so is not a GDBM database.
    BadMagic {
        /// First four bytes of the file, big endian.
        magic: u32,
    },
    /// The database file would grow beyond the maximum offset its format can represent.
    FileTooLarge {
        /// Required file size.
        size: u64,
        /// Maximum file size.
        maximum: u64,
    },
    /// Database may be inconsistent since an earlier error. Writes are disabled.
    Inconsistent,
    /// The database file is locked by another user.
    Locked,
    /// [`Gdbm::verify`](crate::Gdbm::verify) found an inconsistency.
    Corrupt {
        /// What is wrong.
        description: String,
    },
    /// A record being imported with [`ImportMode::Insert`](crate::ImportMode::Insert) has a key
    /// that is already in the database.
    KeyExists {
        /// The key.
        key: Vec<u8>,
    },
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
    /// Bucket element count does not match the number of occupied slots in its table.
    BadBucketCount {
        /// Bucket file offset.
        offset: u64,
        /// Element count in the bucket header.
        count: u32,
        /// Number of occupied slots.
        occupied: u32,
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
    /// A key hashes to a directory entry beyond the end of the directory (like C GDBM's
    /// `GDBM_BAD_DIR_ENTRY`).
    BadDirectoryEntry {
        /// Directory entry index.
        index: usize,
        /// Number of directory entries.
        entries: usize,
    },
    /// The directory entries for a bucket being split are not the aligned range of
    /// `2^(dir_bits - bits)` entries that its bit count implies.
    BadDirectoryRange {
        /// Bucket file offset.
        offset: u64,
        /// Bucket bits.
        bits: u32,
        /// Index of a directory entry referring to the bucket.
        index: usize,
        /// Directory bits.
        dir_bits: u32,
    },
    /// The directory cannot grow any further (like C GDBM's `GDBM_DIR_OVERFLOW`).
    DirectoryOverflow {
        /// Directory size in bytes.
        size: u32,
    },
    /// Write operation on a database opened read-only.
    ReadOnly,
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
    BadHeaderBucketElems {
        /// Bucket elements in header.
        elems: u32,
        /// Bucket elements expected from the bucket size.
        expected: u32,
    },
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
        match self {
            Error::BadData(msg) => write!(f, "stored data cannot be converted: {msg}"),
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::BadMagic { magic } => {
                write!(f, "not a GDBM database (magic number {magic:#010x})")
            }
            Error::FileTooLarge { size, maximum } => write!(
                f,
                "database would grow to {size} bytes, beyond its format's limit of {maximum}"
            ),
            Error::Inconsistent => write!(
                f,
                "database may be inconsistent after an earlier error; writes are disabled"
            ),
            Error::Locked => write!(f, "database is locked by another user"),
            Error::Corrupt { description } => write!(f, "corrupt database: {description}"),
            Error::KeyExists { key } => write!(f, "key {key:x?} already exists"),
            Error::BadBucketElement {
                data_offset,
                key_size,
                data_size,
                next_block,
            } => write!(
                f,
                "corrupt database: record at {data_offset} ({key_size}+{data_size} bytes) \
                 extends beyond the end of the database ({next_block})"
            ),
            Error::RecordTooLarge { size, maximum } => write!(
                f,
                "key and value total {size} bytes, more than the maximum of {maximum}"
            ),
            Error::BadBucket {
                offset,
                elems,
                bits,
                max_elems,
                dir_bits,
            } => write!(
                f,
                "corrupt database: bucket at {offset} has {elems} elements (maximum \
                 {max_elems}) and {bits} bits (maximum {dir_bits})"
            ),
            Error::BadBucketCount {
                offset,
                count,
                occupied,
            } => write!(
                f,
                "corrupt database: bucket at {offset} has a count of {count} but {occupied} \
                 occupied slots"
            ),
            Error::BadBlockSize { requested, actual } => write!(
                f,
                "block size {requested} is not possible; the nearest is {actual}"
            ),
            Error::BadDirectory { offset, length } => write!(
                f,
                "corrupt database: directory at {offset} ({length} bytes) refers to a \
                 bucket outside the database"
            ),
            Error::BadDirectoryEntry { index, entries } => write!(
                f,
                "corrupt database: directory entry {index} is beyond the {entries}-entry \
                 directory"
            ),
            Error::BadDirectoryRange {
                offset,
                bits,
                index,
                dir_bits,
            } => write!(
                f,
                "corrupt database: the directory entries ({dir_bits} bits) around entry {index} \
                 do not all refer to its bucket at {offset} ({bits} bits)"
            ),
            Error::DirectoryOverflow { size } => {
                write!(f, "directory of {size} bytes cannot grow any further")
            }
            Error::ReadOnly => write!(f, "database is open read-only"),
            Error::BadHeaderBlockSize { size, minimum } => write!(
                f,
                "corrupt database header: block size {size} is less than the minimum {minimum}"
            ),
            Error::BadHeaderNextBlock {
                next_block,
                file_size,
            } => write!(
                f,
                "corrupt database header: end of allocated space {next_block} is before the \
                 end of the file {file_size}"
            ),
            Error::BadHeaderDirectoryOffset {
                offset,
                size,
                file_size,
            } => write!(
                f,
                "corrupt database header: directory at {offset} ({size} bytes) is beyond the \
                 end of the file {file_size}"
            ),
            Error::BadHeaderDirectory {
                size,
                bits,
                minimum_size,
                expected_bits,
            } => write!(
                f,
                "corrupt database header: directory of {size} bytes and {bits} bits (expected \
                 at least {minimum_size} bytes and {expected_bits} bits)"
            ),
            Error::BadHeaderBucketSize { size, minimum } => write!(
                f,
                "corrupt database header: bucket size {size} is less than the minimum {minimum}"
            ),
            Error::BadHeaderBucketElems { elems, expected } => write!(
                f,
                "corrupt database header: {elems} elements per bucket (expected {expected})"
            ),
            Error::BadAvailElem {
                block_offset,
                elem,
                offset,
                size,
                next_block,
            } => write!(
                f,
                "corrupt database: free space entry {elem} of the table at {block_offset} \
                 ({offset}+{size} bytes) is outside the database ({next_block})"
            ),
            Error::BadHeaderAvail {
                elems,
                size,
                block_size,
            } => write!(
                f,
                "corrupt database header: free space table of {elems} entries ({size} bytes) \
                 does not fit block size {block_size}"
            ),
            Error::BadAvailCount {
                block_offset,
                elems,
                maximum,
            } => write!(
                f,
                "corrupt database: free space table at {block_offset} has {elems} entries \
                 (maximum {maximum})"
            ),
            Error::BadNumsyncVersion { version } => {
                write!(f, "unsupported numsync header version {version}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
