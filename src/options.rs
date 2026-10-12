//
// options.rs -- GDBM database open options
//
// Copyright (c) 2024 Jeff Garzik, John Hedges
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use crate::{CharSign, Endian, Error, Gdbm, Layout, Result};

/// `Blocksize` can be used when creating a database to override the default block size, which is
/// the underlying filesystem block size, or 512, whichever is largest. The minimum blocksize is
/// 512.
#[derive(Copy, Clone, Debug, PartialOrd, PartialEq, Default)]
pub enum BlockSize {
    /// Use the filesystem default block size. This is the default.
    #[default]
    Filesystem,
    /// Use this blocksize unless it is not a power of 2, in which case round up to the next power
    /// of two (minimum 512).
    Roughly(u32),
    /// Use this blocksize only if it is a power of 2 and >= 512. Any other value will result in
    /// failure when creating the database.
    Exactly(u32),
}

/// Options for opening or creating a database, in the style of [`std::fs::OpenOptions`].
///
/// By default a database is opened read-only. Use [`write`](OpenOptions::write) to open it for
/// reading and writing, and [`create`](OpenOptions::create) to create it if needed. Options
/// marked "new databases" are stored in the database when it is created and are ignored when
/// opening an existing one, whose format is detected.
///
/// # Example
/// ```
/// # fn main() -> Result<(), String> {
/// #     || -> gdbm_native::Result<()> {
/// # use gdbm_native::OpenOptions;
/// // Open a database in read-only mode.
/// let db = OpenOptions::new().open("my-data")?;
///
/// // Open a database for read-write access.
/// let mut db = OpenOptions::new().write(true).open("my-data")?;
///
/// // Open a database for read-write access, creating it if needed.
/// let mut db = OpenOptions::new().create(true).open("my-data")?;
///
/// // Create a temporary database.
/// let mut db = OpenOptions::new().tempfile()?;
/// #         Ok(())
/// #     }().or_else(|_| Ok(()))
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct OpenOptions {
    pub(crate) write: bool,
    pub(crate) create: bool,
    pub(crate) sync: bool,
    pub(crate) lock: bool,
    pub(crate) cachesize: Option<usize>,
    pub(crate) char_sign: Option<CharSign>,
    pub(crate) numsync: bool,
    pub(crate) block_size: BlockSize,
    pub(crate) layout: Layout,
    pub(crate) endian: Endian,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            write: false,
            create: false,
            sync: false,
            lock: true,
            cachesize: None,
            char_sign: None,
            numsync: true,
            block_size: BlockSize::default(),
            layout: Layout::default(),
            endian: Endian::default(),
        }
    }
}

impl OpenOptions {
    /// Default options: open an existing database read-only.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Open the database for writing as well as reading.
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.write = write;
        self
    }

    /// Create the database if the file does not exist or is empty. Implies
    /// [`write`](OpenOptions::write). As with any open, an existing file that is not a valid
    /// database results in an error and is left untouched.
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    /// Sync mode (default `false`) flushes data to disk after every database update. It improves
    /// the chances of surviving a crash, at the expense of performance. See
    /// [`Gdbm::set_sync`](crate::Gdbm::set_sync).
    pub fn sync(&mut self, sync: bool) -> &mut Self {
        self.sync = sync;
        self
    }

    /// Whether to lock the database file (default `true`). Like C GDBM, a database opened for
    /// reading takes a shared lock and one opened for writing takes an exclusive lock, so many
    /// readers or one writer may use the database at a time. Opening fails with
    /// [`Error::Locked`] rather than waiting if the lock is not available.
    pub fn lock(&mut self, lock: bool) -> &mut Self {
        self.lock = lock;
        self
    }

    /// Size in bytes of the in-memory bucket cache (default
    /// [`DEFAULT_CACHESIZE`](crate::DEFAULT_CACHESIZE)).
    pub fn cachesize(&mut self, cachesize: usize) -> &mut Self {
        self.cachesize = Some(cachesize);
        self
    }

    /// Hash keys with `char_sign` instead of detecting it; see [`CharSign`](crate::CharSign).
    pub fn char_sign(&mut self, char_sign: CharSign) -> &mut Self {
        self.char_sign = Some(char_sign);
        self
    }

    /// New databases: whether to give them a numsync header (default `true`). For information on
    /// numsync see <https://www.gnu.org.ua/software/gdbm/manual/Numsync.html>.
    pub fn numsync(&mut self, numsync: bool) -> &mut Self {
        self.numsync = numsync;
        self
    }

    /// New databases: their [`BlockSize`].
    pub fn block_size(&mut self, block_size: BlockSize) -> &mut Self {
        self.block_size = block_size;
        self
    }

    /// New databases: their [`Layout`](crate::Layout) (default `Layout::Off64Align8`). Use this to
    /// create databases for use by C GDBM on another platform.
    pub fn layout(&mut self, layout: Layout) -> &mut Self {
        self.layout = layout;
        self
    }

    /// New databases: their byte order (default `Endian::Little`).
    pub fn endian(&mut self, endian: Endian) -> &mut Self {
        self.endian = endian;
        self
    }

    // Lock f, shared or exclusive, unless locking is disabled.
    fn lock_file(&self, f: &std::fs::File, exclusive: bool) -> Result<()> {
        if !self.lock {
            return Ok(());
        }

        if exclusive {
            f.try_lock()
        } else {
            f.try_lock_shared()
        }
        .map_err(|e| match e {
            std::fs::TryLockError::WouldBlock => Error::Locked,
            std::fs::TryLockError::Error(e) => Error::Io(e),
        })
    }

    /// Open the database at `path` with these options.
    ///
    /// # Errors
    /// Fails if the file cannot be opened, locked or created, or is not a valid GDBM database.
    pub fn open<P: AsRef<std::path::Path>>(&self, path: P) -> Result<Gdbm> {
        let writable = self.write || self.create;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(writable)
            .create(self.create)
            .truncate(false)
            .open(&path)?;
        self.lock_file(&f, writable)?;

        if self.create && f.metadata()?.len() == 0 {
            Gdbm::create(f, self)
        } else {
            Gdbm::open(f, self, writable)
        }
    }

    /// Create a temporary database.
    ///
    /// The database file is created using `tempfile::tempfile` and therefore is never visible in
    /// the filesystem, and is deleted when the `Gdbm` struct is dropped/closed. This is useful for
    /// creating a key-value dataset that is too large to fit in system memory, but is not required
    /// to be opened again.
    ///
    /// # Errors
    /// Fails if the temporary file cannot be created.
    ///
    /// # Example
    /// ```
    /// # fn main() -> Result<(), String> {
    /// #     || -> gdbm_native::Result<()> {
    /// let db = gdbm_native::OpenOptions::new()
    ///     .cachesize(100 * 1024)
    ///     .tempfile()?;
    /// #         Ok(())
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    pub fn tempfile(&self) -> Result<Gdbm> {
        Gdbm::create(tempfile::tempfile()?, self)
    }
}
