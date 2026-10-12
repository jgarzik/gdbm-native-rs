//
// lib.rs -- GDBM core library API routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! A pure-Rust implementation of the [GNU GDBM](https://www.gnu.org.ua/software/gdbm/)
//! key-value database, file-compatible with C GDBM databases and dumps from any platform.
//!
//! # Quick start
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::tempdir()?;
//! # let path = dir.path().join("users.db");
//! use gdbm_native::OpenOptions;
//!
//! // Create the database if needed, and open it for reading and writing.
//! let mut db = OpenOptions::new().create(true).open(&path)?;
//!
//! db.insert("alice", "alice@example.com")?;
//! db.insert("bob", "bob@example.com")?;
//!
//! // Values are untyped bytes; ask for them as the type you stored.
//! let email: Option<String> = db.get("alice")?;
//! assert_eq!(email.as_deref(), Some("alice@example.com"));
//!
//! for entry in db.iter::<String, String>() {
//!     let (user, email) = entry?;
//!     println!("{user}: {email}");
//! }
//!
//! // Write out changes and learn of any error (dropping the database also writes them).
//! db.close()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Compatibility with C GDBM
//!
//! C GDBM writes files in the native format of the platform it was built for, and records only
//! part of that format in the file. gdbm-native reads and writes every variant, whatever the
//! platform it runs on, and detects what the file does not record:
//!
//! | Property | Varies with | Recorded | gdbm-native |
//! |---|---|---|---|
//! | Byte order | CPU | yes | either |
//! | File offset size | 32-bit `off_t` (no large file support) or 64-bit | yes | either; [`Layout`] |
//! | Struct alignment | 4 bytes on i386, 8 elsewhere | no | detected; [`Layout`] |
//! | Key hash of bytes >= 0x80 | `char` signedness: signed on x86 and MIPS, unsigned on Linux ARM, PowerPC, RISC-V, s390x | no | detected; [`CharSign`] |
//! | Numsync header | how the database was created | yes | either; [`Gdbm::set_numsync`] |
//! | Binary dump length fields | `sizeof(long)` and byte order | no | detected; [`BinDumpFormat`] |
//!
//! New databases default to little endian, 64-bit offsets aligned on 8 bytes and a numsync
//! header, as C GDBM writes on x86-64 and arm64; [`OpenOptions`] selects other formats, e.g.
//! to create a database for C GDBM on another platform. ASCII dumps
//! ([`Gdbm::export_ascii`], [`Gdbm::import_ascii`]) are portable between all platforms.
//!
//! Like C GDBM, gdbm-native locks the database file (shared for readers, exclusive for a
//! writer; see [`OpenOptions::lock`]). The test suite exercises databases and dumps written by
//! C GDBM on x86-64, i386, arm64, armv7, s390x and 32-bit-offset builds, and runs C GDBM and
//! gdbm-native against the same files.
//!
//! # Keys and values
//!
//! Keys and values are byte strings. Anything implementing [`ToBytesRef`] can be stored, and
//! anything implementing [`FromBytes`] can be read back; both are implemented for byte slices
//! and arrays, `Vec<u8>`, `str` and `String`. Implement them for your own types, choosing an
//! encoding every program using the database understands.
//!
//! # Integrity
//!
//! Database files and dumps are treated as untrusted input: corruption is reported as an
//! [`Error`] rather than a panic or unbounded allocation. [`Gdbm::verify`] checks a database's
//! use of file space and [`Gdbm::recover`] rebuilds a damaged database from what can still be
//! read. Writes are not atomic: if one fails part way, the database refuses further writes
//! ([`Error::Inconsistent`]) until reopened or recovered.
//!
//! # Features
//!
//! * `diagnostic`: [`Gdbm::show_header`] and [`Gdbm::show_directory`], which print internal
//!   structures.
extern crate base64;

use base64::Engine;
use std::cell::{Cell, RefCell};
use std::io::{self, Read, Seek, SeekFrom, Write};

mod avail;
mod bucket;
mod bytes;
#[cfg(test)]
mod check;
mod dir;
mod dump;
mod error;
mod hashutil;
mod header;
mod import;
mod magic;
mod options;
mod ser;

use avail::{AvailBlock, AvailElem};
use bucket::{Bucket, BucketCache, BucketElement};
pub use bytes::{FromBytes, ToBytesRef};
use dir::{Directory, build_dir_size};
pub use dump::{BinDumpFormat, DumpInfo, ImportMode};
pub use error::Error;
pub use hashutil::CharSign;
use hashutil::{PartialKey, bucket_dir, hash_key, key_loc};
use header::Header;
use import::{ASCIIImportIterator, BinaryImportIterator};
pub use magic::Magic;
pub use options::{BlockSize, OpenOptions};
pub use ser::{Endian, Layout};
use ser::{Format, Offset};
use std::fs::File;

/// Our claimed GDBM lib version compatibility. Appears in dump files.
pub const COMPAT_GDBM_VERSION: &str = "1.26";

const IGNORE_SMALL: usize = 4;

/// The default approximate size of heap memory used by each open database.
/// The actual value used can be set when opening the database.
pub const DEFAULT_CACHESIZE: usize = 4 * 1024 * 1024;

/// The crate Result type.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Copy, Clone, Debug, Default, PartialEq)]
enum WriteState {
    #[default]
    Clean,
    Dirty,
    Inconsistent,
}

// state of a database opened for writing
#[derive(Copy, Clone, Debug, Default)]
struct WriteMode {
    sync: bool,
    state: WriteState,
}

// Write a bucket padded to the header's bucket size, which may exceed the serialized size of a
// bucket (C GDBM sets bucket size to the block size).
fn write_bucket(mut f: &File, header: &Header, bucket: &Bucket, offset: u64) -> io::Result<()> {
    let mut buffer = Vec::with_capacity(header.bucket_sz as usize);
    bucket.serialize(header.layout, &mut buffer)?;
    buffer.resize(buffer.len().max(header.bucket_sz as usize), 0);
    f.seek(SeekFrom::Start(offset))?;
    f.write_all(&buffer)
}

// Return elem if it describes a record within allocated file space, else an error.
fn valid_element(elem: BucketElement, next_block: u64) -> Result<BucketElement> {
    elem.is_valid(next_block)
        .then_some(elem)
        .ok_or(Error::BadBucketElement {
            data_offset: elem.data_ofs,
            key_size: elem.key_size,
            data_size: elem.data_size,
            next_block,
        })
}

// Size of a record holding key and data. C GDBM uses int for record sizes.
fn record_size(key_len: usize, data_len: usize) -> Result<u32> {
    const MAXIMUM: usize = i32::MAX as usize;
    key_len
        .checked_add(data_len)
        .filter(|&size| size <= MAXIMUM)
        .map(|size| size as u32)
        .ok_or(Error::RecordTooLarge {
            size: key_len.saturating_add(data_len),
            maximum: MAXIMUM,
        })
}

// preferred I/O block size of the filesystem holding f
fn fs_block_size(f: &File) -> io::Result<u32> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(u32::try_from(f.metadata()?.blksize()).unwrap_or(u32::MAX))
    }
    #[cfg(not(unix))]
    {
        let _ = f;
        Ok(4096)
    }
}

// read and return file data stored at (ofs,total_size)
fn read_ofs(mut f: &File, ofs: u64, total_size: usize) -> io::Result<Vec<u8>> {
    let mut data: Vec<u8> = vec![0; total_size];

    f.seek(SeekFrom::Start(ofs))?;
    f.read_exact(&mut data)?;

    Ok(data)
}

/// A GDBM database, opened with [`OpenOptions`].
pub struct Gdbm {
    f: std::fs::File,
    header: Header,
    dir: Directory,
    // RefCell so that reads can take &self
    bucket_cache: RefCell<BucketCache>,
    // None until known: see CharSign
    char_sign: Cell<Option<CharSign>>,

    // None if opened read-only
    read_write: Option<WriteMode>,
    // None for temporary databases
    pub(crate) path: Option<std::path::PathBuf>,
}

/// Summary of a consistent database, returned by [`Gdbm::verify`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct VerifyReport {
    /// Number of distinct buckets.
    pub buckets: usize,
    /// Number of records (entries).
    pub records: usize,
    /// Number of free space extents.
    pub free_extents: usize,
    /// Total free space in bytes.
    pub free_bytes: u64,
}

/// Outcome of [`Gdbm::recover`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RecoverReport {
    /// Records copied to the rebuilt database.
    pub recovered_keys: usize,
    /// Records that could not be read.
    pub failed_keys: usize,
    /// Records skipped because their key had already been recovered.
    pub duplicate_keys: usize,
    /// Buckets that could not be read.
    pub failed_buckets: usize,
    /// Where the original database file was copied, if a backup was requested.
    pub backup: Option<std::path::PathBuf>,
}

impl Gdbm {
    // Cache a bucket, writing any dirty bucket it displaces (only possible when writable).
    fn cache_bucket(&self, offset: u64, bucket: Bucket) -> Result<()> {
        let evicted = self.bucket_cache.borrow_mut().insert(offset, bucket);
        if let Some((evicted_offset, evicted_bucket)) = evicted {
            write_bucket(&self.f, &self.header, &evicted_bucket, evicted_offset)?;
        }

        Ok(())
    }

    // Write state, for internal use by write operations, which check_writable() first.
    fn rw(&mut self) -> &mut WriteMode {
        self.read_write
            .as_mut()
            .expect("write operation on a read-only database")
    }

    fn check_writable(&self) -> Result<()> {
        self.read_write
            .is_some()
            .then_some(())
            .ok_or(Error::ReadOnly)
    }

    /// Whether the database was opened for writing.
    pub fn is_writable(&self) -> bool {
        self.read_write.is_some()
    }

    fn open(mut f: File, open_options: &OpenOptions, writable: bool) -> Result<Gdbm> {
        let metadata = f.metadata()?;

        f.seek(SeekFrom::Start(0))?;
        let header = Header::from_reader(metadata.len(), &mut f)?;

        f.seek(SeekFrom::Start(header.dir_ofs))?;
        let dir = Directory::from_reader(header.layout, header.dir_sz, &mut f)?;

        // ensure all bucket offsets are reasonable
        if !dir.validate(
            u64::from(header.block_sz),
            header.next_block,
            header.block_sz,
        ) {
            return Err(Error::BadDirectory {
                offset: header.dir_ofs,
                length: header.dir_sz,
            });
        }

        let bucket_cache = {
            let cache_buckets = {
                let bytes = open_options.cachesize.unwrap_or(DEFAULT_CACHESIZE);
                let buckets = bytes / header.bucket_sz as usize;
                buckets.max(1)
            };
            BucketCache::new(cache_buckets, None)
        };

        Ok(Gdbm {
            f,
            header,
            dir,
            bucket_cache: RefCell::new(bucket_cache),
            char_sign: Cell::new(open_options.char_sign),
            path: None,
            read_write: writable.then_some(WriteMode {
                sync: open_options.sync,
                state: WriteState::Clean,
            }),
        })
    }

    // The header C GDBM's gdbm_dump writes, less the user and group names, which C GDBM also
    // omits when it cannot look them up.
    fn export_ascii_header(
        &self,
        outf: &mut impl Write,
        pathname: Option<&std::path::Path>,
    ) -> io::Result<()> {
        writeln!(
            outf,
            "# GDBM dump file created by gdbm-native {} (GDBM {COMPAT_GDBM_VERSION} format)",
            env!("CARGO_PKG_VERSION")
        )?;
        writeln!(outf, "#:version=1.1")?;
        if let Some(pathname) = pathname {
            writeln!(outf, "#:file={}", pathname.to_string_lossy())?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = self.f.metadata()?;
            writeln!(
                outf,
                "#:uid={},gid={},mode={:03o}",
                metadata.uid(),
                metadata.gid(),
                metadata.mode() & 0o777
            )?;
        }
        let format = if self.header.magic.is_numsync() {
            "numsync"
        } else {
            "standard"
        };
        writeln!(outf, "#:format={format}")?;
        writeln!(outf, "# End of header")?;
        Ok(())
    }

    fn export_ascii_datum(outf: &mut impl Write, bindata: Vec<u8>) -> io::Result<()> {
        const MAX_DUMP_LINE_LEN: usize = 76;

        writeln!(outf, "#:len={}", bindata.len())?;

        // C GDBM's gdbm_dump writes no data line for zero-length data, but its gdbm_load (1.23
        // to at least 1.26) then fails with "Malformed data"; an empty line loads everywhere.
        let b64 = base64::prelude::BASE64_STANDARD.encode(bindata);
        if b64.is_empty() {
            writeln!(outf)?;
        }
        b64.as_bytes()
            .chunks(MAX_DUMP_LINE_LEN)
            .try_for_each(|line| outf.write_all(line).and_then(|()| writeln!(outf)))
    }

    fn export_ascii_records(&self, outf: &mut impl Write) -> Result<usize> {
        self.iter().try_fold(0, |count, kv| {
            kv.and_then(|(key, value)| {
                Self::export_ascii_datum(outf, key)
                    .and_then(|()| Self::export_ascii_datum(outf, value))
                    .map(|()| count + 1)
                    .map_err(Error::Io)
            })
        })
    }

    fn export_ascii_footer(outf: &mut impl Write, n_written: usize) -> io::Result<()> {
        writeln!(outf, "#:count={n_written}")?;
        writeln!(outf, "# End of data")?;
        Ok(())
    }

    /// Dumps the database in ASCII format.
    ///
    /// Dumps the database in ASCII format to the supplied [`io::Write`], in the format of C GDBM's
    /// `gdbm_dump`, readable by C GDBM's `gdbm_load` on any platform. If a filename is provided
    /// it will be used to set the `#:file=<filename>` header line. On Unix the header also records
    /// the owner and permissions of the database file.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() -> Result<(), String> {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     || -> gdbm_native::Result<()> {
    /// #         let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path).unwrap();
    /// let mut buffer = Vec::new();
    /// db.export_ascii(&mut buffer, None::<&str>)?;
    /// println!("{}", std::str::from_utf8(&buffer).unwrap());
    /// #         Ok(())
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Fails if a record cannot be read or `outf` cannot be written.
    pub fn export_ascii<P: AsRef<std::path::Path> + ?Sized>(
        &self,
        outf: &mut impl Write,
        pathname: Option<&P>,
    ) -> Result<()> {
        self.export_ascii_header(outf, pathname.as_ref().map(std::convert::AsRef::as_ref))
            .map_err(Error::Io)
            .and_then(|()| self.export_ascii_records(outf))
            .and_then(|n_written| Gdbm::export_ascii_footer(outf, n_written).map_err(Error::Io))
    }

    fn export_bin_header(outf: &mut impl Write) -> io::Result<()> {
        write!(
            outf,
            "!\r\n! GDBM FLAT FILE DUMP -- THIS IS NOT A TEXT FILE\r\n"
        )?;
        write!(
            outf,
            "! gdbm-native {} (GDBM {COMPAT_GDBM_VERSION} format)\r\n!\r\n",
            env!("CARGO_PKG_VERSION")
        )?;
        Ok(())
    }

    // record sizes are at most i32::MAX (see record_size()), so lengths fit C's 32 bits
    fn export_bin_datum(
        outf: &mut impl Write,
        format: BinDumpFormat,
        bindata: &[u8],
    ) -> io::Result<()> {
        format.write_len(outf, bindata.len() as u32)?;
        outf.write_all(bindata)
    }

    fn export_bin_records(&self, outf: &mut impl Write, format: BinDumpFormat) -> Result<()> {
        self.iter::<Vec<_>, Vec<_>>().try_for_each(|kv| {
            kv.and_then(|(key, value)| {
                Self::export_bin_datum(outf, format, &key)
                    .and_then(|()| Self::export_bin_datum(outf, format, &value))
                    .map_err(Error::Io)
            })
        })
    }

    /// Dumps the database in binary format.
    ///
    /// Dumps the database in binary ("flat file") format to the supplied [`io::Write`], in the
    /// format of C GDBM's `gdbm_export`. The layout of its length fields depends on the platform
    /// of the C GDBM that is to read it; see [`BinDumpFormat`] (`BinDumpFormat::host()` matches
    /// the local platform).
    ///
    /// Note: The binary format is more compact than the ASCII equivalent, but platform specific,
    /// and so ASCII dumps are preferred.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// use gdbm_native::BinDumpFormat;
    /// # fn main() -> Result<(), String> {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     || -> gdbm_native::Result<()> {
    /// #         let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path).unwrap();
    /// let mut buffer = Vec::new();
    /// db.export_bin(&mut buffer, BinDumpFormat::host())?;
    /// println!("{buffer:?}");
    /// #         Ok(())
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Fails if a record cannot be read or `outf` cannot be written.
    pub fn export_bin(&self, outf: &mut impl Write, format: BinDumpFormat) -> Result<()> {
        Gdbm::export_bin_header(outf)
            .map_err(Error::Io)
            .and_then(|()| self.export_bin_records(outf, format))
    }

    // read bucket into bucket cache.
    // Load the bucket for directory entry bucket_dir into the cache, make it current, and apply f
    // to it. The bucket is only borrowed for the duration of f, so the cache stays usable.
    fn with_bucket<T>(&self, bucket_dir: usize, f: impl FnOnce(&Bucket) -> T) -> Result<T> {
        let offset = self.dir.dir[bucket_dir];

        if !self.bucket_cache.borrow().contains(offset) {
            let mut f = &self.f;
            f.seek(SeekFrom::Start(offset))?;
            let bucket = Bucket::from_reader(self.header.bucket_elems, self.header.layout, &mut f)?;

            if bucket.count > self.header.bucket_elems || bucket.bits > self.header.dir_bits {
                return Err(Error::BadBucket {
                    offset,
                    elems: bucket.count,
                    bits: bucket.bits,
                    max_elems: self.header.bucket_elems,
                    dir_bits: self.header.dir_bits,
                });
            }

            // Inserting relies on a bucket with count < bucket_elems having a free slot.
            if bucket.occupied() != bucket.count {
                return Err(Error::BadBucketCount {
                    offset,
                    count: bucket.count,
                    occupied: bucket.occupied(),
                });
            }

            avail::validate_elems(
                &bucket.avail,
                offset,
                u64::from(self.header.block_sz),
                self.header.next_block,
            )?;

            self.cache_bucket(offset, bucket)?;
        }

        self.bucket_cache.borrow_mut().set_current(offset);

        Ok(f(self.bucket_cache.borrow().current_bucket().unwrap()))
    }

    // since one bucket dir entry may duplicate another,
    // this function returns the next non-dup bucket dir
    fn next_bucket_dir(&self, bucket_dir_in: usize) -> usize {
        let dir_max_elem = self.dir.dir.len();
        if bucket_dir_in >= dir_max_elem {
            return dir_max_elem;
        }

        let mut bucket_dir = bucket_dir_in;

        let cur_ofs = self.dir.dir[bucket_dir];
        while bucket_dir < dir_max_elem && cur_ofs == self.dir.dir[bucket_dir] {
            bucket_dir += 1;
        }

        bucket_dir
    }

    /// Block size of the database file.
    pub fn block_size(&self) -> u32 {
        self.header.block_sz
    }

    /// Size of a hash bucket in the database file.
    pub fn bucket_size(&self) -> u32 {
        self.header.bucket_sz
    }

    /// Number of bits of key hash that select a bucket: the directory has `2^dir_depth()`
    /// entries.
    pub fn dir_depth(&self) -> u32 {
        self.header.dir_bits
    }

    /// Number of distinct hash buckets, like C GDBM's `gdbm_bucket_count`.
    pub fn bucket_count(&self) -> usize {
        self.dir
            .dir
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    /// Whether the database has a numsync header.
    pub fn is_numsync(&self) -> bool {
        self.header.magic.is_numsync()
    }

    /// The numsync counter, incremented on every sync, if the database has a numsync header.
    pub fn numsync(&self) -> Option<u32> {
        self.header.numsync
    }

    /// The path the database was opened from; `None` for temporary databases.
    pub fn path(&self) -> Option<&std::path::Path> {
        self.path.as_deref()
    }

    /// Check the consistency of the database's use of file space, like C GDBM's
    /// `gdbm_avail_verify` but covering every structure: the header, directory, buckets,
    /// records and free space must all lie within the allocated file space and must not
    /// overlap.
    ///
    /// # Errors
    /// [`Error::Corrupt`] describing the first inconsistency found, or an I/O error.
    pub fn verify(&self) -> Result<VerifyReport> {
        let corrupt = |description: String| Error::Corrupt { description };
        let header = &self.header;
        let mut report = VerifyReport::default();
        let mut extents: Vec<(u64, u64, String)> = vec![
            (0, u64::from(header.block_sz), "header".into()),
            (header.dir_ofs, u64::from(header.dir_sz), "directory".into()),
        ];
        let mut free = |extents: &mut Vec<_>, elem: &AvailElem, name: String| {
            report.free_extents += 1;
            report.free_bytes += u64::from(elem.sz);
            extents.push((elem.addr, u64::from(elem.sz), name));
        };

        for elem in &header.avail.elems {
            free(&mut extents, elem, "header free space".into());
        }

        let mut seen = std::collections::HashSet::new();
        for index in 0..self.dir.dir.len() {
            let offset = self.dir.dir[index];
            if !seen.insert(offset) {
                continue;
            }
            let (elems, avail, count) = self.with_bucket(index, |bucket| {
                (
                    bucket
                        .tab
                        .iter()
                        .filter(|elem| elem.is_occupied())
                        .copied()
                        .collect::<Vec<_>>(),
                    bucket.avail.clone(),
                    bucket.count as usize,
                )
            })?;
            extents.push((
                offset,
                u64::from(header.bucket_sz),
                format!("bucket at {offset}"),
            ));
            for elem in elems {
                extents.push((
                    elem.data_ofs,
                    u64::from(elem.key_size) + u64::from(elem.data_size),
                    format!("record in bucket at {offset}"),
                ));
            }
            report.records += count;
            for elem in &avail {
                free(
                    &mut extents,
                    elem,
                    format!("free space in bucket at {offset}"),
                );
            }
        }
        report.buckets = seen.len();

        let stack_block_size = AvailBlock::stack_block_size(header.layout, header.avail.sz);
        let mut next = header.avail.next_block;
        let mut blocks = std::collections::HashSet::new();
        while next != 0 {
            if !blocks.insert(next) {
                return Err(corrupt(format!("avail stack loops at {next}")));
            }
            let mut f = &self.f;
            f.seek(SeekFrom::Start(next))?;
            let block = AvailBlock::from_reader(header.layout, &mut f)?;
            block.validate(
                next,
                AvailBlock::stack_block_capacity(header.layout, header.avail.sz),
                u64::from(header.block_sz),
                header.next_block,
            )?;
            extents.push((
                next,
                u64::from(stack_block_size),
                format!("avail block at {next}"),
            ));
            for elem in &block.elems {
                free(
                    &mut extents,
                    elem,
                    format!("free space in avail block at {next}"),
                );
            }
            next = block.next_block;
        }

        extents.sort();
        for pair in extents.windows(2) {
            let ((a_ofs, a_len, a_name), (b_ofs, _, b_name)) = (&pair[0], &pair[1]);
            if a_ofs.saturating_add(*a_len) > *b_ofs {
                return Err(corrupt(format!(
                    "{a_name} ({a_ofs}+{a_len}) overlaps {b_name} ({b_ofs})"
                )));
            }
        }
        if let Some((ofs, len, name)) = extents.last()
            && ofs.saturating_add(*len) > header.next_block
        {
            return Err(corrupt(format!(
                "{name} ({ofs}+{len}) is beyond the end of the database ({})",
                header.next_block
            )));
        }

        Ok(report)
    }

    /// Whether the database has no entries. Unlike [`len`](Gdbm::len), this stops at the first
    /// entry found.
    ///
    /// # Errors
    /// Fails if a bucket cannot be read.
    pub fn is_empty(&self) -> Result<bool> {
        let mut cur_dir: usize = 0;
        while cur_dir < self.dir.dir.len() {
            if self.with_bucket(cur_dir, |bucket| bucket.count > 0)? {
                return Ok(false);
            }
            cur_dir = self.next_bucket_dir(cur_dir);
        }

        Ok(true)
    }

    /// Write out any changes and close the database, reporting any error. Dropping a `Gdbm`
    /// also writes out changes, but cannot report errors.
    ///
    /// # Errors
    /// Fails if changes cannot be written.
    pub fn close(mut self) -> Result<()> {
        self.sync()
    }

    /// Number of entries in the database. This reads every bucket; use
    /// [`is_empty`](Gdbm::is_empty) to test for an empty database.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() -> Result<(), String> {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     || -> gdbm_native::Result<()> {
    /// #         let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path).unwrap();
    /// let count = db.len()?;
    /// #         Ok(())
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Fails if a bucket cannot be read.
    pub fn len(&self) -> Result<usize> {
        let mut len: usize = 0;
        let mut cur_dir: usize = 0;
        let dir_max_elem = self.dir.dir.len();
        while cur_dir < dir_max_elem {
            len += self.with_bucket(cur_dir, |bucket| bucket.count as usize)?;
            cur_dir = self.next_bucket_dir(cur_dir);
        }

        Ok(len)
    }

    /// Get an [`Iterator`] over the values in the database.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() -> Result<(), String> {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     || -> gdbm_native::Result<()> {
    /// #         let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path).unwrap();
    /// #         let spam = |_| ();
    /// for address in db.values::<String>() {
    ///     let address = address?;
    ///     spam(address);
    /// }
    /// #         Ok(())
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    pub fn values<V: FromBytes>(&self) -> impl std::iter::Iterator<Item = Result<V>> + '_ {
        GDBMIterator::new(self, KeyOrValue::Value)
            .map(|data| data.and_then(|(_, value)| V::from_bytes(&value)))
    }

    /// Get an [`Iterator`] over the keys in the database.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() -> Result<(), String> {
    /// #     let tmp_dir = tempdir().map_err(|e| e.to_string())?;
    /// #     let path = tmp_dir.path().join("test");
    /// #     || -> gdbm_native::Result<()> {
    /// #         let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path)?;
    /// let mut longest = None::<String>;
    ///
    /// for key in db.keys::<String>() {
    ///     let key = key?;
    ///     longest = Some(match longest {
    ///         Some(longest) if longest.len() >= key.len() => longest,
    ///         _ => key,
    ///     });
    /// }
    ///
    /// println!("{longest:?}");
    /// #         Ok(())
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    pub fn keys<K: FromBytes>(&self) -> impl std::iter::Iterator<Item = Result<K>> + '_ {
        GDBMIterator::new(self, KeyOrValue::Key)
            .map(|data| data.and_then(|(key, _)| K::from_bytes(&key)))
    }

    /// Get an [`Iterator`] over the entries (key, value) pairs in the database.
    ///
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() -> Result<(), String> {
    /// #     let tmp_dir = tempdir().map_err(|e| e.to_string())?;
    /// #     let path = tmp_dir.path().join("test");
    /// #     || -> gdbm_native::Result<()> {
    /// #         let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path)?;
    /// for kv in db.iter::<String, [u8; 4]>() {
    ///     let (button, count) = kv?;
    ///     println!("button <{button}> was clicked {} times", u32::from_be_bytes(count))
    /// }
    /// #         Ok(())
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    pub fn iter<K: FromBytes, V: FromBytes>(
        &self,
    ) -> impl std::iter::Iterator<Item = Result<(K, V)>> + '_ {
        GDBMIterator::new(self, KeyOrValue::Both).map(|data| {
            data.and_then(|(key, value)| {
                K::from_bytes(&key).and_then(|k| V::from_bytes(&value).map(|v| (k, v)))
            })
        })
    }

    /// Checks whether the database contains a specific key.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() -> Result<(), String> {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path).unwrap();
    /// #     db.insert("fred", "dibnah");
    /// #     let username = "fred";
    /// if db.contains_key(username).unwrap() {
    ///     Ok(())
    /// } else {
    ///     Err("Access deied".to_string())
    /// }
    /// # }
    /// ```
    ///
    /// # Errors
    /// Fails if the database cannot be read or is corrupt.
    pub fn contains_key<K: ToBytesRef + ?Sized>(&self, key: &K) -> Result<bool> {
        self.int_get(key.to_bytes_ref().as_ref())
            .map(|result| result.is_some())
    }

    // Hash key as the C GDBM that wrote this database would. Keys of bytes < 0x80 hash the same
    // whatever the signedness of C's char; other keys need it, so it is determined on first use.
    fn hash(&self, key: &[u8]) -> u32 {
        let char_sign = if key.is_ascii() {
            CharSign::Signed
        } else {
            self.resolve_char_sign(Some(key))
        };
        hash_key(char_sign, key)
    }

    // The char signedness of this database: as set or detected earlier, else detected now
    // (looking first where key would be stored, if given), falling back to Signed.
    fn resolve_char_sign(&self, key: Option<&[u8]>) -> CharSign {
        if let Some(char_sign) = self.char_sign.get() {
            return char_sign;
        }
        let char_sign = self.detect_char_sign(key).unwrap_or_default();
        self.char_sign.set(Some(char_sign));
        char_sign
    }

    // Find a stored key with a byte >= 0x80, and see which char signedness reproduces its stored
    // hash. None if the database has no such key. Unreadable buckets and records are skipped, so
    // that corruption elsewhere does not stop lookups or recover().
    //
    // Scanning every bucket can be slow, so when resolving for a key, first look in the bucket(s)
    // that key maps to under each signedness (at most two): a key being looked up is likely to
    // share its bucket with similar keys. Only if neither decides are all other buckets scanned.
    fn detect_char_sign(&self, key: Option<&[u8]>) -> Option<CharSign> {
        let mut seen = std::collections::HashSet::new();
        let likely = key.into_iter().flat_map(|key| {
            [CharSign::Signed, CharSign::Unsigned]
                .map(|char_sign| bucket_dir(self.header.dir_bits, hash_key(char_sign, key)))
        });
        likely
            .chain(0..self.dir.dir.len())
            .filter(|&index| seen.insert(self.dir.dir[index]))
            .find_map(|index| self.bucket_char_sign(index))
    }

    // The char signedness that the stored hash of a key with a byte >= 0x80 in the bucket for
    // directory entry index shows, if any. None if the bucket cannot be read.
    fn bucket_char_sign(&self, index: usize) -> Option<CharSign> {
        let elems = self
            .with_bucket(index, |bucket| {
                bucket
                    .tab
                    .iter()
                    .filter(|elem| elem.is_occupied())
                    .copied()
                    .collect::<Vec<_>>()
            })
            .ok()?;
        elems.into_iter().find_map(|elem| {
            // keys of up to 4 bytes are entirely in the bucket
            if elem.key_size <= PartialKey::SIZEOF
                && elem.key_start.is_ascii(elem.key_size as usize)
            {
                return None;
            }
            let key = valid_element(elem, self.header.next_block)
                .and_then(|elem| Ok(read_ofs(&self.f, elem.data_ofs, elem.key_size as usize)?))
                .ok()?;
            if key.is_ascii() {
                return None;
            }
            match (
                hash_key(CharSign::Signed, &key) == elem.hash,
                hash_key(CharSign::Unsigned, &key) == elem.hash,
            ) {
                (true, false) => Some(CharSign::Signed),
                (false, true) => Some(CharSign::Unsigned),
                _ => None,
            }
        })
    }

    /// The signedness of `char` this database's keys are hashed with, if known yet: set by
    /// [`OpenOptions::char_sign`], or detected the first time a key with a byte >= 0x80 is used.
    /// See [`CharSign`].
    pub fn char_sign(&self) -> Option<CharSign> {
        self.char_sign.get()
    }

    // retrieve record data, and element offset in bucket, for given key
    fn int_get(&self, key: &[u8]) -> Result<Option<(usize, Vec<u8>)>> {
        let key_hash = self.hash(key);
        let (bucket_dir, elem_ofs) =
            key_loc(self.header.dir_bits, self.header.bucket_elems, key_hash);
        let bucket_entries = self.with_bucket(bucket_dir, |bucket| {
            (0..bucket.tab.len())
                .map(|index| (index + elem_ofs as usize) % bucket.tab.len())
                .map(|offset| (offset, bucket.tab[offset]))
                .take_while(|(_, elem)| elem.is_occupied())
                .filter(|(_, elem)| {
                    elem.hash == key_hash
                        && elem.key_size == key.len() as u32
                        && elem.key_start.matches(key)
                })
                .collect::<Vec<_>>()
        })?;

        let next_block = self.header.next_block;
        let data_entries = bucket_entries
            .into_iter()
            .map(|(offset, elem)| {
                let elem = valid_element(elem, next_block)?;
                read_ofs(
                    &self.f,
                    elem.data_ofs,
                    elem.key_size as usize + elem.data_size as usize,
                )
                .map(|data| (offset, data))
                .map_err(Error::Io)
            })
            .collect::<Result<Vec<_>>>()?;

        let result = data_entries
            .into_iter()
            .filter(|(_, data)| data[..key.len()] == *key)
            .map(|(offset, data)| (offset, data[key.len()..].to_vec()))
            .next();

        Ok(result)
    }

    /// Get the value for a specific key from the database.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() -> Result<(), String> {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     || -> gdbm_native::Result<()> {
    /// #         let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path)?;
    /// #     let display = |_: Vec<u8>| ();
    /// #     let image_name = "";
    /// if let Some(image) = db.get(image_name)? {
    ///     display(image);
    /// }
    /// #         Ok(())
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Fails if the database cannot be read or is corrupt, or with [`Error::BadData`] if the value
    /// cannot be converted to `V`.
    pub fn get<K: ToBytesRef + ?Sized, V: FromBytes>(&self, key: &K) -> Result<Option<V>> {
        match self.int_get(key.to_bytes_ref().as_ref())? {
            None => Ok(None),
            Some(data) => V::from_bytes(&data.1).map(|v| Some(v)),
        }
    }

    /// Gets the database [`Magic`] number.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path).unwrap();
    /// println!("database magic: {:?}", db.magic());
    /// # }
    /// ```
    pub fn magic(&self) -> Magic {
        self.header.magic
    }

    /// The C struct [`Layout`] of the database file.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path).unwrap();
    /// println!("database layout: {:?}", db.layout());
    /// # }
    /// ```
    pub fn layout(&self) -> Layout {
        self.header.layout.layout()
    }

    /// The byte order of the database file.
    pub fn endian(&self) -> Endian {
        self.header.layout.endian
    }

    #[cfg(feature = "diagnostic")]
    /// Show diagnostic information about the database header.
    ///
    /// Only available when the `diagnostic` feature is enabled.
    ///
    /// # Errors
    /// Fails if `w` cannot be written.
    pub fn show_header(&self, w: &mut impl Write) -> io::Result<()> {
        let (dir_sz, dir_bits) = build_dir_size(self.header.layout.offset, self.header.block_sz);

        writeln!(w, "magic {}", self.header.magic)?;
        writeln!(w, "dir-offset {}", self.header.dir_ofs)?;
        writeln!(w, "dir-size {dir_sz}")?;
        writeln!(w, "dir-bits {dir_bits}")?;
        writeln!(w, "block-size {}", self.header.block_sz)?;
        writeln!(w, "bucket-elems {}", self.header.bucket_elems)?;
        writeln!(w, "bucket-size {}", self.header.bucket_sz)?;
        writeln!(w, "next-block {}", self.header.next_block)?;
        writeln!(w, "avail-size {}", self.header.avail.sz)?;
        writeln!(w, "avail-count {}", self.header.avail.elems.len())?;
        writeln!(w, "avail-next-block {}", self.header.avail.next_block)?;

        Ok(())
    }

    #[cfg(feature = "diagnostic")]
    /// Show diagnostic information about the database directory.
    ///
    /// Only available when the `diagnostic` feature is enabled.
    ///
    /// # Errors
    /// Fails if `w` cannot be written.
    pub fn show_directory(&self, w: &mut impl Write) -> io::Result<()> {
        writeln!(w, "size {}", self.header.dir_sz)?;
        writeln!(w, "bits {}", self.header.dir_bits)?;

        for n in 0..self.dir.dir.len() {
            writeln!(w, "{n}: {}", self.dir.dir[n])?;
        }

        Ok(())
    }
}

impl Gdbm {
    fn create(f: File, open_options: &OpenOptions) -> Result<Gdbm> {
        let layout = Format::new(open_options.layout, open_options.endian);

        let (block_size, dir_bits) = match open_options.block_size {
            BlockSize::Roughly(size) | BlockSize::Exactly(size) => {
                build_dir_size(layout.offset, size)
            }
            BlockSize::Filesystem => build_dir_size(layout.offset, fs_block_size(&f)?),
        };

        if let BlockSize::Exactly(size) = open_options.block_size
            && block_size != size
        {
            return Err(Error::BadBlockSize {
                requested: size,
                actual: block_size,
            });
        }

        let header = Header::new(block_size, layout, dir_bits, open_options.numsync);
        let bucket = Bucket::new(0, header.bucket_elems as usize, vec![]);
        let bucket_offset = header.next_block - u64::from(block_size);
        let dir = Directory::new(vec![bucket_offset; 1 << header.dir_bits]);

        let bucket_cache = {
            let cache_buckets = {
                let bytes = open_options.cachesize.unwrap_or(DEFAULT_CACHESIZE);
                let buckets = bytes / header.bucket_sz as usize;
                buckets.max(1)
            };
            BucketCache::new(cache_buckets, Some((bucket_offset, bucket)))
        };

        let mut db = Gdbm {
            f,
            header,
            dir,
            bucket_cache: RefCell::new(bucket_cache),
            char_sign: Cell::new(open_options.char_sign),
            path: None,
            read_write: Some(WriteMode {
                sync: open_options.sync,
                state: WriteState::Dirty,
            }),
        };

        if db.rw().sync {
            db.sync()?;
        }

        Ok(db)
    }

    /// Set the database sync mode.
    ///
    /// When sync mode is enabled, database metadata is written for every write operation.
    /// This impacts performance, but increases the chances of the database surviving a system
    /// crash.
    ///
    /// By default sync mode is disabled.
    ///
    /// Sync mode is not stored with the database and should be selected whenever the database is
    /// opened using either [`set_sync()`](Gdbm::set_sync) or [`OpenOptions::sync()`].
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path).unwrap();
    /// // Turn on sync mode.
    /// db.set_sync(true);
    /// # }
    /// ```
    pub fn set_sync(&mut self, sync: bool) {
        if let Some(rw) = self.read_write.as_mut() {
            rw.sync = sync;
        }
    }

    /// Imports entries from an ASCII dump into the database.
    ///
    /// Adds all entries from a dump created with [`export_ascii`](Gdbm::export_ascii) or C GDBM's
    /// `gdbm_dump` to the database, storing them as `mode` says when their key already exists.
    /// Returns the number of records imported and the dump's header information.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() -> Result<(), String> {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     let new_path = tmp_dir.path().join("test1");
    /// #     || -> gdbm_native::Result<()> {
    /// #         let mut old_db = gdbm_native::OpenOptions::new().write(true).create(true).open(path)?;
    /// use gdbm_native::{ImportMode, OpenOptions};
    ///
    /// let mut buf = Vec::new();
    /// old_db.export_ascii(&mut buf, None::<&str>)?;
    ///
    /// let mut new_db = OpenOptions::new().write(true).create(true).open(new_path)?;
    /// let info = new_db.import_ascii(&mut buf.as_slice(), ImportMode::Replace)?;
    /// println!("imported {} records", info.count);
    /// #         Ok(())
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Fails if the database is read-only, the dump is malformed, a key already exists with
    /// [`ImportMode::Insert`], or a record cannot be stored.
    pub fn import_ascii(&mut self, reader: &mut impl Read, mode: ImportMode) -> Result<DumpInfo> {
        self.check_writable()?;
        let mut records = ASCIIImportIterator::new(reader)?;
        let mut info = DumpInfo::from_header(&records.header);
        info.count = self.import_records(&mut records, mode)?;
        Ok(info)
    }

    fn import_records(
        &mut self,
        records: &mut impl Iterator<Item = io::Result<(Vec<u8>, Vec<u8>)>>,
        mode: ImportMode,
    ) -> Result<usize> {
        records.try_fold(0, |count, record| {
            let (key, value) = record?;
            match mode {
                ImportMode::Replace => self.insert(&key, &value).map(|_| ()),
                ImportMode::Insert => match self.try_insert(&key, &value)? {
                    None => Ok(()),
                    Some(_) => Err(Error::KeyExists { key }),
                },
            }
            .map(|()| count + 1)
        })
    }

    /// Imports entries from a binary dump into the database.
    ///
    /// Adds all entries from a dump created with [`export_bin`](Gdbm::export_bin) or C GDBM's
    /// `gdbm_export` to the database, storing them as `mode` says when their key already exists.
    /// `format` gives the layout of the dump's length fields; `None` detects it from the first
    /// 64 KiB of records, as described for [`BinDumpFormat`]. Returns the number of records
    /// imported.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() -> Result<(), String> {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     let new_path = tmp_dir.path().join("test1");
    /// #     || -> gdbm_native::Result<()> {
    /// #         let mut old_db = gdbm_native::OpenOptions::new().write(true).create(true).open(path)?;
    /// use gdbm_native::{BinDumpFormat, ImportMode, OpenOptions};
    ///
    /// let mut buf = Vec::new();
    /// old_db.export_bin(&mut buf, BinDumpFormat::Long32)?;
    ///
    /// let mut new_db = OpenOptions::new().write(true).create(true).open(new_path)?;
    /// new_db.import_bin(&mut buf.as_slice(), None, ImportMode::Replace)?;
    /// #         Ok(())
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Fails if the database is read-only, the dump is malformed, `format` is `None` and no
    /// format is consistent with the dump, a key already exists with [`ImportMode::Insert`], or a
    /// record cannot be stored.
    pub fn import_bin(
        &mut self,
        reader: &mut impl Read,
        format: Option<BinDumpFormat>,
        mode: ImportMode,
    ) -> Result<usize> {
        self.check_writable()?;
        let mut records = BinaryImportIterator::new(format, reader)?;
        self.import_records(&mut records, mode)
    }

    // virtually allocate N blocks of data, at end of db file (no I/O)
    fn extend(&mut self, size: u32) -> Result<(u64, u32)> {
        let offset = self.header.next_block;
        let length = size.div_ceil(self.header.block_sz) * self.header.block_sz;

        // C GDBM's off_t is signed, so 32-bit offset databases are limited to 2GiB
        let maximum = match self.header.layout.offset {
            Offset::Small => i32::MAX as u64,
            Offset::Lfs => i64::MAX as u64,
        };
        let next_block = offset
            .checked_add(u64::from(length))
            .filter(|&next_block| next_block <= maximum)
            .ok_or(Error::FileTooLarge {
                size: offset.saturating_add(u64::from(length)),
                maximum,
            })?;

        self.header.next_block = next_block;
        self.header.dirty = true;

        Ok((offset, length))
    }

    // Header avail table is full: move half of it (the odd-indexed elements) into a new block
    // pushed onto the avail stack. Mirrors push_avail_block() in C GDBM's falloc.c, including
    // the on-disk block size, so that C GDBM can pop blocks written here and vice versa.
    fn push_avail_block(&mut self) -> Result<()> {
        let layout = self.header.layout;
        let size = AvailBlock::stack_block_size(layout, self.header.avail.sz);

        // space for the new block comes from the header table itself, else end of file
        let (offset, length) = match self.header.allocate(size) {
            Some(block) => block,
            None => self.extend(size)?,
        };

        let (keep, pushed) = avail::partition_elems(&self.header.avail.elems);
        let block = AvailBlock::new(self.header.avail.sz, self.header.avail.next_block, pushed);
        self.header.avail = AvailBlock::new(self.header.avail.sz, offset, keep);
        self.header.dirty = true;

        // header table is now half empty, so this cannot recurse back here
        self.free_record(offset + u64::from(size), length - size)?;

        let mut buffer = Vec::with_capacity(size as usize);
        block.serialize(layout, &mut buffer)?;
        buffer.resize(size as usize, 0);
        self.f.seek(SeekFrom::Start(offset))?;
        self.f.write_all(&buffer)?;

        Ok(())
    }

    // Pop the block at the top of the avail stack, merging its elements into the header avail
    // table, then free the block itself. Mirrors pop_avail_block() in C GDBM's falloc.c.
    fn pop_avail_block(&mut self) -> Result<()> {
        let layout = self.header.layout;
        let offset = self.header.avail.next_block;
        let size = AvailBlock::stack_block_size(layout, self.header.avail.sz);

        self.f.seek(SeekFrom::Start(offset))?;
        let block = AvailBlock::from_reader(layout, &mut self.f)?;
        block.validate(
            offset,
            AvailBlock::stack_block_capacity(layout, self.header.avail.sz),
            u64::from(self.header.block_sz),
            self.header.next_block,
        )?;

        // Unlink the block before merging, so that any push below links to the remainder of
        // the stack. (C GDBM unlinks afterwards, leaking blocks pushed while merging.)
        self.header.avail.next_block = block.next_block;
        self.header.dirty = true;

        for elem in block.elems.iter().chain(&[AvailElem {
            addr: offset,
            sz: size,
        }]) {
            if self.header.avail.is_full() {
                self.push_avail_block()?;
            }
            self.header.avail.insert_elem_coalesce(elem.addr, elem.sz);
        }

        Ok(())
    }

    // Return (addr, sz) to the free space pool. Mirrors _gdbm_free() in C GDBM's falloc.c.
    fn free_record(&mut self, addr: u64, sz: u32) -> Result<()> {
        // simply forget elements too small to worry about
        if (sz as usize) <= IGNORE_SMALL {
            return Ok(());
        }

        // small extents go into the current bucket's avail table, if there is room
        if sz < self.header.block_sz
            && let Some(bucket) = self
                .bucket_cache
                .get_mut()
                .current_bucket_mut()
                .filter(|bucket| (bucket.avail.len() as u32) < Bucket::AVAIL)
        {
            bucket.free(addr, sz);
            return Ok(());
        }

        // everything else goes into the header avail table
        if self.header.avail.is_full() {
            self.push_avail_block()?;
        }
        self.header.free(addr, sz);

        Ok(())
    }

    fn write_bucket(&mut self, bucket: &Bucket, offset: u64) -> io::Result<()> {
        write_bucket(&self.f, &self.header, bucket, offset)
    }

    // write out any cached, not-yet-written metadata and data to storage
    fn write_buckets(&mut self) -> io::Result<()> {
        self.bucket_cache
            .get_mut()
            .dirty_list()
            .iter()
            .try_for_each(|(offset, bucket)| write_bucket(&self.f, &self.header, bucket, *offset))
            .map(|()| self.bucket_cache.get_mut().clear_dirty())
    }

    // write out any cached, not-yet-written metadata and data to storage
    fn write_dir(&mut self) -> io::Result<()> {
        if !self.dir.dirty {
            return Ok(());
        }

        let mut buffer = Vec::with_capacity(self.dir.extent(self.header.layout) as usize);
        self.dir.serialize(self.header.layout, &mut buffer)?;
        self.f.seek(SeekFrom::Start(self.header.dir_ofs))?;
        self.f.write_all(&buffer)?;

        self.dir.dirty = false;

        Ok(())
    }

    // write out any cached, not-yet-written metadata and data to storage
    fn write_header(&mut self) -> io::Result<()> {
        if !self.header.dirty {
            return Ok(());
        }

        let mut buffer = Vec::with_capacity(self.header.block_sz as usize);
        self.header.serialize(&mut buffer)?;
        self.f.seek(SeekFrom::Start(0))?;
        self.f.write_all(&buffer)?;

        // Like C GDBM, extend the file to cover all allocated space, some of which may not have
        // been written (e.g. free space at the end of the file).
        if self.f.metadata()?.len() < self.header.next_block {
            self.f.set_len(self.header.next_block)?;
        }

        self.header.dirty = false;

        Ok(())
    }

    // write out any cached, not-yet-written metadata and data to storage
    fn write_dirty(&mut self) -> io::Result<()> {
        self.rw().state = WriteState::Inconsistent;

        self.write_buckets()?;
        self.write_dir()?;
        self.write_header()?;

        self.rw().state = WriteState::Clean;

        Ok(())
    }

    /// Writes all database state to the database file.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     std::panic::catch_unwind(|| {
    /// #         let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path).unwrap();
    /// // Ensure all data is saved ...
    /// db.sync();
    ///
    /// // ... otherwise this could cause an inconsistent state.
    /// panic!("pulled the plug");
    /// #     });
    /// # }
    /// ```
    ///
    /// # Errors
    /// Fails with [`Error::Inconsistent`] after an earlier failed write, or if writing the file
    /// fails.
    pub fn sync(&mut self) -> Result<()> {
        // nothing to write if read-only
        let Some(rw) = self.read_write else {
            return Ok(());
        };
        match rw.state {
            WriteState::Clean => Ok(()),
            WriteState::Inconsistent => Err(Error::Inconsistent),
            WriteState::Dirty => {
                self.header.increment_numsync();
                self.write_dirty()
                    .and_then(|()| self.f.sync_data())
                    .map_err(Error::Io)
            }
        }
    }

    fn int_remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let get_opt = self.int_get(key)?;

        if get_opt.is_none() {
            return Ok(None);
        }

        if self.rw().state == WriteState::Inconsistent {
            return Err(Error::Inconsistent);
        }

        self.rw().state = WriteState::Inconsistent;

        let (elem_ofs, data) = get_opt.unwrap();

        let elem = self
            .bucket_cache
            .get_mut()
            .current_bucket_mut()
            .unwrap()
            .remove(elem_ofs);

        // release record bytes to available-space pool
        self.free_record(elem.data_ofs, elem.key_size + elem.data_size)?;

        self.rw().state = WriteState::Dirty;

        Ok(Some(data))
    }

    /// Remove an entry from the database.
    ///
    /// Remove the entry for the specified `key` from the database, and return the raw bytes value
    /// if the entry existed before [`remove`](Gdbm::remove) was called.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() -> Result<(), String> {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     || -> gdbm_native::Result<()> {
    /// #         let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path)?;
    /// match db.remove("sylvian")? {
    ///     Some(old) => println!("removed \"{:?}\" from the database.", std::str::from_utf8(&old)),
    ///     None => println!("\"sylvian\" wasn't in the database."),
    /// };
    /// #         Ok(())
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Fails if the database is read-only ([`Error::ReadOnly`]) or inconsistent after an earlier
    /// failed write ([`Error::Inconsistent`]), if the record is too large, or if reading or writing
    /// the file fails.
    pub fn remove<K: ToBytesRef + ?Sized>(&mut self, key: &K) -> Result<Option<Vec<u8>>> {
        self.check_writable()?;
        self.int_remove(key.to_bytes_ref().as_ref())
            .and_then(|old_value| {
                if old_value.is_some() && self.rw().sync {
                    self.sync()?;
                }

                Ok(old_value)
            })
    }

    // Allocate size bytes of file space. Mirrors _gdbm_alloc() in C GDBM's falloc.c.
    fn allocate_record(&mut self, size: u32) -> Result<u64> {
        let (offset, length) = if let Some(block) = self
            .bucket_cache
            .get_mut()
            .current_bucket_mut()
            .and_then(|bucket| bucket.allocate(size))
        {
            block
        } else {
            // refill the header avail table from the stack when it is half empty
            if self.header.avail.elems.len() as u32 <= self.header.avail.sz / 2
                && self.header.avail.next_block != 0
            {
                self.pop_avail_block()?;
            }

            match self.header.allocate(size) {
                Some(block) => block,
                None => self.extend(size)?,
            }
        };

        self.free_record(offset + u64::from(size), length - size)?;

        Ok(offset)
    }

    fn int_insert(&mut self, key: &[u8], data: &[u8]) -> Result<()> {
        if self.rw().state == WriteState::Inconsistent {
            return Err(Error::Inconsistent);
        }

        let size = record_size(key.len(), data.len())?;
        let hash = self.hash(key);

        self.rw().state = WriteState::Inconsistent;

        let offset = self.allocate_record(size)?;

        self.f
            .seek(SeekFrom::Start(offset))
            .and_then(|_| self.f.write_all(key))
            .and_then(|()| self.f.write_all(data))?;

        let bucket_elem = BucketElement::new(hash, key, data, offset);
        self.with_bucket(bucket_dir(self.header.dir_bits, bucket_elem.hash), |_| ())?;

        while self.bucket_cache.get_mut().current_bucket().unwrap().count
            == self.header.bucket_elems
        {
            self.split_bucket()?;
            self.with_bucket(bucket_dir(self.header.dir_bits, bucket_elem.hash), |_| ())?;
        }

        let cache = self.bucket_cache.get_mut();
        let offset = cache.current_bucket_offset().unwrap();
        let bucket = cache.current_bucket_mut().unwrap();
        bucket
            .insert(bucket_elem)
            .ok_or_else(|| Error::BadBucketCount {
                offset,
                count: bucket.count,
                occupied: bucket.occupied(),
            })?;

        self.rw().state = WriteState::Dirty;

        Ok(())
    }

    /// Insert an entry into the database.
    ///
    /// Adds an entry with the specified `key` an `value` to the database.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() -> Result<(), String> {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     || -> gdbm_native::Result<()> {
    /// #         let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path)?;
    /// db.insert("marmite", "dog")?;
    /// #         Ok(())
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Fails if the database is read-only ([`Error::ReadOnly`]) or inconsistent after an earlier
    /// failed write ([`Error::Inconsistent`]), if the record is too large, or if reading or writing
    /// the file fails.
    pub fn insert<V: ToBytesRef + ?Sized, K: ToBytesRef + ?Sized>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<Option<Vec<u8>>> {
        self.check_writable()?;
        let key = key.to_bytes_ref();
        self.int_remove(key.as_ref())
            .and_then(|oldvalue| {
                self.int_insert(key.as_ref(), value.to_bytes_ref().as_ref())
                    .map(|()| oldvalue)
            })
            .and_then(|oldvalue| {
                if self.rw().sync {
                    self.sync()?;
                }

                Ok(oldvalue)
            })
    }

    /// Try to insert an entry into the database.
    ///
    /// Adds an entry with the specified `key` an `value` to the database. Fails if an entry with
    /// the specified key already exists and returns the existing value as raw bytes.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() -> Result<(), String> {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     || -> gdbm_native::Result<()> {
    /// #         let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path)?;
    /// match db.try_insert("marmite", "cat")? {
    ///     Some(v) => {
    ///         println!("marmite exists and is a {}", std::str::from_utf8(&v).unwrap())
    ///     }
    ///     None => println!("added marmite to the database"),
    /// };
    /// #         Ok(())
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Fails if the database is read-only ([`Error::ReadOnly`]) or inconsistent after an earlier
    /// failed write ([`Error::Inconsistent`]), if the record is too large, or if reading or writing
    /// the file fails.
    pub fn try_insert<K: ToBytesRef + ?Sized, V: ToBytesRef + ?Sized>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<Option<Vec<u8>>> {
        self.check_writable()?;
        self.get(key).and_then(|olddata| match olddata {
            Some(_) => Ok(olddata),
            _ => self
                .int_insert(key.to_bytes_ref().as_ref(), value.to_bytes_ref().as_ref())
                .map(|()| None)
                .and_then(|result| {
                    if self.rw().sync {
                        self.sync()?;
                    }

                    Ok(result)
                }),
        })
    }

    fn split_bucket(&mut self) -> Result<()> {
        if self.bucket_cache.get_mut().current_bucket().unwrap().bits == self.header.dir_bits {
            self.extend_directory()?;
        }

        // allocate space for new bucket in an aligned block at the end of file
        let new_bucket_offset = {
            let (offset, size) = self.extend(self.header.bucket_sz)?;
            self.free_record(
                offset + u64::from(self.header.bucket_sz),
                size - self.header.bucket_sz,
            )?;
            offset
        };

        let cache = self.bucket_cache.get_mut();
        let cur_bucket_offset = cache.current_bucket_offset().unwrap();
        let bucket = cache.current_bucket().unwrap();
        let (bucket0, bucket1) = bucket.split().ok_or_else(|| Error::BadBucketCount {
            offset: cur_bucket_offset,
            count: bucket.count,
            occupied: bucket.occupied(),
        })?;
        let bits = bucket0.bits;

        let _ = self
            .bucket_cache
            .get_mut()
            .insert(cur_bucket_offset, bucket0);
        if let Some((evicted_offset, evicted_bucket)) = self
            .bucket_cache
            .get_mut()
            .insert(new_bucket_offset, bucket1)
        {
            self.write_bucket(&evicted_bucket, evicted_offset)?;
        }

        self.dir.update_bucket_split(
            self.header.dir_bits,
            bits,
            cur_bucket_offset,
            new_bucket_offset,
        );

        Ok(())
    }

    // Extends the directory by duplicating each bucket offset.
    // Old storage is freed and new storage is allocated.
    // The maximum number of hash_bits represented by each element is increased by 1.
    // The header is updated with new offset, size and bits.
    // Both the directory and header are marked dirty, but not written.
    fn extend_directory(&mut self) -> Result<()> {
        let directory = self.dir.extend();
        let size = directory.extent(self.header.layout);
        let offset = self.allocate_record(size)?;

        self.free_record(self.header.dir_ofs, self.header.dir_sz)?;
        self.header.dir_bits += 1;
        self.header.dir_ofs = offset;
        self.header.dir_sz = size;
        self.header.dirty = true;

        self.dir = directory;

        Ok(())
    }

    /// Adds or removes numsync from the database.
    ///
    /// For information on `numsync` refer to the [original GDBM
    /// documentaion](https://www.gnu.org.ua/software/gdbm/manual/Numsync.html).
    ///
    /// By default `numsync` is enabled for new databases.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() -> Result<(), String> {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     || -> gdbm_native::Result<()> {
    /// #         let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path)?;
    /// // Turn on numsync for this database.
    /// db.set_numsync(true);
    /// assert!(db.magic().is_numsync());
    /// #         Ok(())
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Fails if the database is read-only or inconsistent, or if writing the file fails.
    pub fn set_numsync(&mut self, numsync: bool) -> Result<()> {
        self.check_writable()?;
        if self.rw().state == WriteState::Inconsistent {
            return Err(Error::Inconsistent);
        }

        if self.header.magic.is_numsync() == numsync {
            return Ok(());
        }

        self.rw().state = WriteState::Inconsistent;

        self.header
            .convert_numsync(numsync)
            .into_iter()
            .try_for_each(|(offset, length)| self.free_record(offset, length))?;

        self.rw().state = WriteState::Dirty;

        Ok(())
    }

    /// Compact the database.
    ///
    /// This is an expensive operation that involves creating a new database file with all entries
    /// from `self` and then copying the new database file over the current one. As a worst case,
    /// this operation requires free disk space equal to the size of the current database file.
    ///
    /// # Examples
    /// ```
    /// # use tempfile::tempdir;
    /// # fn main() -> Result<(), String> {
    /// #     let tmp_dir = tempdir().unwrap();
    /// #     let path = tmp_dir.path().join("test");
    /// #     || -> gdbm_native::Result<()> {
    /// #         let mut db = gdbm_native::OpenOptions::new().write(true).create(true).open(path)?;
    /// let result = db.compact();
    /// #         result
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Fails if the database is read-only, or cannot be read or rebuilt.
    pub fn compact(&mut self) -> Result<()> {
        self.check_writable()?;
        let mut tmpdb = self.new_like()?;
        self.iter::<Vec<u8>, Vec<u8>>().try_for_each(|entry| {
            let (key, value) = entry?;
            tmpdb.insert(&key, &value).map(|_| ())
        })?;
        self.replace_with(tmpdb)
    }

    // An empty temporary database with the same format as self.
    fn new_like(&self) -> Result<Gdbm> {
        let char_sign = self.resolve_char_sign(None);
        let magic = self.magic();
        let mut tmpdb = OpenOptions::new()
            .char_sign(char_sign)
            .layout(self.layout())
            .endian(magic.endian())
            .numsync(magic.is_numsync())
            .block_size(BlockSize::Exactly(self.header.block_sz))
            .tempfile()?;
        tmpdb.header.numsync = self.header.numsync;
        Ok(tmpdb)
    }

    // Replace the contents of self with those of tmpdb.
    fn replace_with(&mut self, mut tmpdb: Gdbm) -> Result<()> {
        tmpdb.sync()?;
        tmpdb.f.seek(SeekFrom::Start(0))?;

        // Until the new contents are copied and reloaded, the in-memory header, directory and
        // bucket cache do not match the file: refuse writes, and sync (or Drop) writing them.
        self.rw().state = WriteState::Inconsistent;

        self.f.seek(SeekFrom::Start(0))?;
        std::io::copy(&mut tmpdb.f, &mut self.f)?;
        self.f.set_len(tmpdb.header.next_block)?;

        self.f.seek(SeekFrom::Start(0))?;
        self.header = Header::from_reader(tmpdb.header.next_block, &mut self.f)?;

        self.f.seek(SeekFrom::Start(self.header.dir_ofs))?;
        self.dir = Directory::from_reader(self.header.layout, self.header.dir_sz, &mut self.f)?;

        self.bucket_cache = RefCell::new(BucketCache::new(
            self.bucket_cache.get_mut().cachesize,
            None,
        ));
        // the file now holds a consistent, synced database
        self.rw().state = WriteState::Clean;

        Ok(())
    }

    /// Rebuild the database from whatever records can still be read, like C GDBM's
    /// `gdbm_recover`. Unreadable buckets and records are skipped and counted. With `backup`, the
    /// database file is first copied to an unused `<path>.bak`, `<path>.bak.1`, ... file.
    ///
    /// This also clears the [`Error::Inconsistent`] state left by a failed write.
    ///
    /// # Errors
    /// Fails if the database is read-only, `backup` is requested for a temporary database, or
    /// the backup or rebuilt database cannot be written.
    pub fn recover(&mut self, backup: bool) -> Result<RecoverReport> {
        self.check_writable()?;
        let mut report = RecoverReport::default();

        if backup {
            report.backup = Some(self.backup()?);
        }

        let mut tmpdb = self.new_like()?;
        let mut seen = std::collections::HashSet::new();
        for index in 0..self.dir.dir.len() {
            if !seen.insert(self.dir.dir[index]) {
                continue;
            }
            let Ok(elems) = self.with_bucket(index, |bucket| {
                bucket
                    .tab
                    .iter()
                    .filter(|elem| elem.is_occupied())
                    .copied()
                    .collect::<Vec<_>>()
            }) else {
                report.failed_buckets += 1;
                continue;
            };
            for elem in elems {
                let record = valid_element(elem, self.header.next_block).and_then(|elem| {
                    Ok(read_ofs(
                        &self.f,
                        elem.data_ofs,
                        elem.key_size as usize + elem.data_size as usize,
                    )?)
                });
                let Ok(record) = record else {
                    report.failed_keys += 1;
                    continue;
                };
                let (key, value) = record.split_at(elem.key_size as usize);
                match tmpdb.try_insert(key, value)? {
                    None => report.recovered_keys += 1,
                    Some(_) => report.duplicate_keys += 1,
                }
            }
        }

        self.replace_with(tmpdb)?;
        Ok(report)
    }

    // Copy the database file to an unused <path>.bak[.N] and return its name.
    fn backup(&mut self) -> Result<std::path::PathBuf> {
        let path = self.path.clone().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "temporary databases cannot be backed up",
            )
        })?;
        for n in 0.. {
            let mut name = path.clone().into_os_string();
            name.push(if n == 0 {
                ".bak".to_string()
            } else {
                format!(".bak.{n}")
            });
            let backup = std::path::PathBuf::from(name);
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            // no more accessible than the database itself
            #[cfg(unix)]
            {
                use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
                options.mode(self.f.metadata()?.permissions().mode() & 0o777);
            }
            match options.open(&backup) {
                Ok(mut out) => {
                    self.f.seek(SeekFrom::Start(0))?;
                    std::io::copy(&mut self.f, &mut out)?;
                    out.sync_all()?;
                    return Ok(backup);
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
                Err(e) => return Err(e.into()),
            }
        }
        unreachable!()
    }
}

impl Drop for Gdbm {
    // Best effort: use close() to learn of errors writing out the database.
    fn drop(&mut self) {
        let _ = self.sync();
    }
}

impl std::fmt::Debug for Gdbm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gdbm")
            .field("layout", &self.layout())
            .field("endian", &self.endian())
            .field("numsync", &self.header.magic.is_numsync())
            .field("block_size", &self.header.block_sz)
            .field("char_sign", &self.char_sign())
            .field("writable", &self.is_writable())
            .finish_non_exhaustive()
    }
}

struct GDBMIterator<'a> {
    key_or_value: KeyOrValue,
    db: &'a Gdbm,
    slot: Option<Result<Slot>>,
}

enum KeyOrValue {
    Key,
    Value,
    Both,
}

#[derive(Debug)]
struct Slot {
    bucket: usize,
    element: usize,
}

impl<'a> GDBMIterator<'a> {
    fn next_slot(db: &Gdbm, slot: &Slot) -> Option<Slot> {
        match slot {
            Slot { bucket, element } if element + 1 < db.header.bucket_elems as usize => {
                Some(Slot {
                    bucket: *bucket,
                    element: element + 1,
                })
            }
            Slot { bucket, .. } => {
                let current_bucket_offset = db.dir.dir[*bucket];
                (db.dir.dir)
                    .iter()
                    .enumerate()
                    .skip(bucket + 1)
                    .find(|&(_, &offset)| offset != current_bucket_offset)
                    .map(|(bucket, _)| Slot { bucket, element: 0 })
            }
        }
    }

    fn next_occupied_slot(db: &Gdbm, slot: &Slot) -> Option<Result<Slot>> {
        let mut next_slot = Self::next_slot(db, slot);
        while let Some(slot) = next_slot {
            let is_occupied = db.with_bucket(slot.bucket, |bucket| {
                bucket.tab.get(slot.element).unwrap().is_occupied()
            });
            match is_occupied {
                Ok(false) => (),
                Ok(true) => return Some(Ok(slot)),
                Err(e) => return Some(Err(e)),
            }
            next_slot = Self::next_slot(db, &slot);
        }

        None
    }

    fn new(db: &'a Gdbm, key_or_value: KeyOrValue) -> GDBMIterator<'a> {
        let slot = {
            let slot = Slot {
                bucket: 0,
                element: 0,
            };
            match db.with_bucket(0, |bucket| bucket.tab.first().unwrap().is_occupied()) {
                Ok(true) => Some(Ok(slot)),
                Ok(false) => Self::next_occupied_slot(db, &slot),
                Err(e) => Some(Err(e)),
            }
        };
        Self {
            key_or_value,
            db,
            slot,
        }
    }
}

impl Iterator for GDBMIterator<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let slot = self.slot.take();
        match slot {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(slot)) => {
                let next_block = self.db.header.next_block;
                let data = self
                    .db
                    .with_bucket(slot.bucket, |bucket| bucket.tab[slot.element])
                    .and_then(|elem| valid_element(elem, next_block))
                    .map(|e| (e.data_ofs, e.key_size as usize, e.data_size as usize))
                    .and_then(
                        |(offset, key_length, data_length)| match self.key_or_value {
                            KeyOrValue::Key => read_ofs(&self.db.f, offset, key_length)
                                .map(|data| (data, vec![]))
                                .map_err(Error::Io),
                            KeyOrValue::Value => {
                                read_ofs(&self.db.f, offset + key_length as u64, data_length)
                                    .map(|data| (vec![], data))
                                    .map_err(Error::Io)
                            }
                            KeyOrValue::Both => {
                                read_ofs(&self.db.f, offset, key_length + data_length)
                                    .map(|data| {
                                        let (key, value) = data.split_at(key_length);
                                        (key.to_vec(), value.to_vec())
                                    })
                                    .map_err(Error::Io)
                            }
                        },
                    );

                match data {
                    Ok(data) => {
                        self.slot = Self::next_occupied_slot(self.db, &slot);
                        Some(Ok(data))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
        }
    }
}
