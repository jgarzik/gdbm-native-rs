//
// options.rs -- GDBM core library API routines
//
// Copyright (c) 2024 Jeff Garzik, John Hedges
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use crate::{Alignment, Endian, Error, Gdbm, Offset, ReadOnly, ReadWrite, Result};

/// `Blocksize` can be used when creating a database to override the default block size, which is
/// the underlying filesystem block size, or 512, whichever is largest. The minimum blocksize is
/// 512.
#[derive(Copy, Clone, Debug, PartialOrd, PartialEq, Default)]
pub enum BlockSize {
    /// Use the filesystem default block size. This is the dafault.
    #[default]
    Filesystem,
    /// Use this blocksize unless it is not a power of 2, in which case round up to the next power
    /// of two (minimum 512).
    Roughly(u32),
    /// Use this blocksize only if it is a power of 2 and >= 512. Any other value will result in
    /// failure when creating the database.
    Exactly(u32),
}

/// Database creation options used to override defaults. You won't need to access this struct
/// directly, but instead use the helpers available on [`OpenOptions<Write<Create>>`].
/// These values are stored in the database header and, with the exception of `numsync` cannot be
/// altered after the database is created.
#[derive(Default, Copy, Clone, Debug)]
pub struct Create {
    /// Overide default [`Offset`](crate::Offset). Set via [`OpenOptions::offset()`].
    pub offset: Option<Offset>,
    /// Overide default [`Endian`](crate::Endian). Set via [`OpenOptions::endian()`].
    pub endian: Option<Endian>,
    /// Overide default `numsync` (`false`). Set via [`OpenOptions::numsync()`].
    pub no_numsync: bool,
    /// Overrids the default [`BlockSize`](crate::BlockSize). Set via
    /// [`OpenOptions::block_size()`].
    pub block_size: BlockSize,
}
#[derive(Default, Copy, Clone, Debug)]
pub struct NotCreate;

#[derive(Copy, Clone, Debug, Default)]
pub struct NotWrite;

/// Database options specific to opening a database in read-write mode.
/// These values are not stored as part of the database; if the default is not applicable they must
/// be set each time the database is openend.
#[derive(Copy, Clone, Debug, Default)]
pub struct Write<C> {
    /// Overide default `sync` (`false`). Set via [`OpenOptions::sync()`].
    pub sync: bool,
    pub(crate) create: C,
}

/// GDBM database open builder, containing values to override defaults when opening a database.
/// These values are not stored as part of the database; if the default is not applicable they must
/// be set each time the database is openend.
#[must_use]
#[derive(Copy, Clone, Debug, Default)]
pub struct OpenOptions<W> {
    /// Override default [Alignment](crate::Alignment) when opening a database. Default alignment
    /// is system specific and need only be considered if database files are to be used on
    /// different architectures, as might be the case if the database is stored on a network
    /// filesystem.
    pub alignment: Option<Alignment>,
    /// Bytesize of in-memory bucket cache (defaults to [`DEFAULT_CACHESIZE`])
    pub cachesize: Option<usize>,

    pub(crate) write: W,
}

/// Create an `OpenOptions` instance. By default, the resulting options can be used to open a
/// database in read-only mode. This can be changed by calling the various methods before opening
/// the database.
///
/// # Example
/// ```
/// # fn main() -> Result<(), String> {
/// #     || -> gdbm_native::Result<()> {
/// # use gdbm_native::OpenOptions;
/// // Open a database in read-only mode..
/// let mut db = OpenOptions::new().open("my-data")?;
///
/// // Open a database for read-write access.
/// let mut db = OpenOptions::new()
///     .write()
///     .open("my-data")?;
///
/// // Open a temporary database.
/// let mut db = OpenOptions::new()
///     .write()
///     .create()
///     .tempfile()?;
/// #         Ok(())
/// #     }().or_else(|_| Ok(()))
/// # }
/// ```
impl OpenOptions<NotWrite> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<W> OpenOptions<W> {
    /// Override default [alignment](crate::Alignment) value.
    pub fn alignment(self, alignment: Option<Alignment>) -> OpenOptions<W> {
        OpenOptions { alignment, ..self }
    }

    /// Override default [cachesize](crate::DEFAULT_CACHESIZE) value.
    pub fn cachesize(self, cachesize: Option<usize>) -> OpenOptions<W> {
        OpenOptions { cachesize, ..self }
    }
}

impl OpenOptions<NotWrite> {
    /// Cause the builder to open the data base for read and write access.
    pub fn write(self) -> OpenOptions<Write<NotCreate>> {
        OpenOptions {
            alignment: self.alignment,
            cachesize: self.cachesize,
            write: Write {
                sync: false,
                create: NotCreate,
            },
        }
    }
}

impl<C> OpenOptions<Write<C>> {
    /// Cause the builder to open the database with sync mode enabled. Sync mode attempts to flush
    /// data to disk after every database update It improves the chances of surviving a crash, at
    /// the expense of performance.
    pub fn sync(self, sync: bool) -> OpenOptions<Write<C>> {
        OpenOptions {
            alignment: self.alignment,
            cachesize: self.cachesize,
            write: Write {
                sync,
                create: self.write.create,
            },
        }
    }
}

impl OpenOptions<Write<NotCreate>> {
    /// When opening a database in reade-write mode, `create` will cause the database to be created
    /// if it doesn't exist or is not a valid database. Take care, as this will cause a corrupted
    /// or non-database file to be replaced with an empty database.
    pub fn create(self) -> OpenOptions<Write<Create>> {
        OpenOptions {
            alignment: self.alignment,
            cachesize: self.cachesize,
            write: Write {
                create: Create::default(),
                sync: self.write.sync,
            },
        }
    }
}

impl OpenOptions<Write<Create>> {
    /// Override the default [`Offset`](crate::Offset) when creating a database.
    pub fn offset(self, offset: Option<Offset>) -> OpenOptions<Write<Create>> {
        OpenOptions {
            alignment: self.alignment,
            cachesize: self.cachesize,
            write: Write {
                create: Create {
                    offset,
                    ..self.write.create
                },
                ..self.write
            },
        }
    }

    /// Override the default [`Endian`](crate::Endian) when creating a database.
    pub fn endian(self, endian: Option<Endian>) -> OpenOptions<Write<Create>> {
        OpenOptions {
            alignment: self.alignment,
            cachesize: self.cachesize,
            write: Write {
                create: Create {
                    endian,
                    ..self.write.create
                },
                ..self.write
            },
        }
    }

    /// Specify whether or not a new database should have a numsync header. For information on
    /// numsync see <https://www.gnu.org.ua/software/gdbm/manual/Numsync.html>
    pub fn numsync(self, numsync: bool) -> OpenOptions<Write<Create>> {
        OpenOptions {
            alignment: self.alignment,
            cachesize: self.cachesize,
            write: Write {
                create: Create {
                    no_numsync: !numsync,
                    ..self.write.create
                },
                ..self.write
            },
        }
    }

    /// Override the default [`Blocksize`](BlockSize) of a new database.
    pub fn block_size(self, block_size: BlockSize) -> OpenOptions<Write<Create>> {
        OpenOptions {
            alignment: self.alignment,
            cachesize: self.cachesize,
            write: Write {
                create: Create {
                    block_size,
                    ..self.write.create
                },
                ..self.write
            },
        }
    }
}

impl OpenOptions<NotWrite> {
    /// The default `open`; opens a database file at `path` in read-only mode.
    pub fn open<P: AsRef<std::path::Path>>(&self, path: P) -> Result<Gdbm<ReadOnly>> {
        std::fs::OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(Error::Io)
            .and_then(|f| Gdbm::<ReadOnly>::open(f, self.alignment, self.cachesize))
    }
}

impl OpenOptions<Write<NotCreate>> {
    /// The `open` called when [`write()`](OpenOptions::write) was called on `OpenOptions`, causes
    /// the database file at `path` to be opened in read-write mode.
    pub fn open<P: AsRef<std::path::Path>>(&self, path: P) -> Result<Gdbm<ReadWrite>> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(Error::Io)
            .and_then(|f| Gdbm::<ReadWrite>::open(f, self.alignment, self.cachesize))
            .map(|mut db| {
                db.set_sync(self.write.sync);
                db
            })
    }
}

impl OpenOptions<Write<Create>> {
    /// The `open` called when [`write()`](OpenOptions::write)`.`[`create()`](OpenOptions::create)
    /// was called on `OpenOptions`, causes the database file at `path` to be opened in read-write
    /// mode, and created if the file at `path` isn't already a database.
    pub fn open<P: AsRef<std::path::Path>>(&self, path: P) -> Result<Gdbm<ReadWrite>> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(Error::Io)
            .and_then(|f| Gdbm::<ReadWrite>::open(f, self.alignment, self.cachesize))
            .or_else(|_| {
                std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .map_err(Error::Io)
                    .and_then(|f| Gdbm::create(f, self))
            })
            .map(|mut db| {
                db.set_sync(self.write.sync);
                db
            })
    }

    /// Open a temporary database.
    ///
    /// The database file is created using `tempfile::tempfile` and therefore is never visible in
    /// the filesystem, and is deleted when the `Gdbm` struct is dropped/closed. This is useful for
    /// creating a key-value dataset that is too large to fit in system memory, but is not required
    /// to be opened again.
    ///
    /// # Example
    /// ```
    /// # fn main() -> Result<(), String> {
    /// #     || -> gdbm_native::Result<()> {
    /// let db = gdbm_native::OpenOptions::new()
    ///     .cachesize(Some(100 * 1024))
    ///     .write()
    ///     .create()
    ///     .tempfile()?;
    /// #         Ok(())
    /// #     }().map_err(|e| e.to_string())
    /// # }
    /// ```
    pub fn tempfile(&self) -> Result<Gdbm<ReadWrite>> {
        tempfile::tempfile()
            .map_err(Error::Io)
            .and_then(|f| Gdbm::create(f, self))
            .map(|mut db| {
                db.set_sync(self.write.sync);
                db
            })
    }
}
