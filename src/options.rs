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

#[derive(Copy, Clone, Debug, PartialOrd, PartialEq, Default)]
pub enum BlockSize {
    #[default]
    Filesystem,
    Roughly(u32),
    Exactly(u32),
}

#[derive(Default, Copy, Clone, Debug)]
pub struct Create {
    pub offset: Option<Offset>,
    pub endian: Option<Endian>,
    pub no_numsync: bool,
    pub newdb: bool,
    pub block_size: BlockSize,
}
#[derive(Default, Copy, Clone, Debug)]
pub struct NotCreate;

#[derive(Copy, Clone, Debug, Default)]
pub struct NotWrite;
#[derive(Copy, Clone, Debug, Default)]
pub struct Write<C> {
    pub sync: bool,
    pub create: C,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct OpenOptions<W> {
    /// Override default alignement when opening a database.
    pub alignment: Option<Alignment>,
    /// Bytesize of in-memory bucket cache (defaults to DEFAULT_CACHESIZE)
    pub cachesize: Option<usize>,

    pub write: W,
}

impl OpenOptions<NotWrite> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<W> OpenOptions<W> {
    pub fn alignment(self, alignment: Option<Alignment>) -> OpenOptions<W> {
        OpenOptions { alignment, ..self }
    }

    pub fn cachesize(self, cachesize: Option<usize>) -> OpenOptions<W> {
        OpenOptions { cachesize, ..self }
    }
}

impl OpenOptions<NotWrite> {
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
    pub fn not_write(self) -> OpenOptions<NotWrite> {
        OpenOptions {
            alignment: self.alignment,
            cachesize: self.cachesize,
            write: NotWrite,
        }
    }

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
    pub fn not_create(self) -> OpenOptions<Write<NotCreate>> {
        OpenOptions {
            alignment: self.alignment,
            cachesize: self.cachesize,
            write: Write {
                create: NotCreate,
                sync: self.write.sync,
            },
        }
    }

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

    pub fn newdb(self, newdb: bool) -> OpenOptions<Write<Create>> {
        OpenOptions {
            alignment: self.alignment,
            cachesize: self.cachesize,
            write: Write {
                create: Create {
                    newdb,
                    ..self.write.create
                },
                ..self.write
            },
        }
    }

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
    pub fn open<P: AsRef<std::path::Path>>(&self, path: P) -> Result<Gdbm<ReadOnly>> {
        std::fs::OpenOptions::new()
            .read(true)
            .open(path.as_ref())
            .map_err(Error::Io)
            .and_then(|f| Gdbm::<ReadOnly>::open(f, path, self.alignment, self.cachesize))
    }
}

impl OpenOptions<Write<NotCreate>> {
    pub fn open<P: AsRef<std::path::Path>>(&self, path: P) -> Result<Gdbm<ReadWrite>> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())
            .map_err(Error::Io)
            .and_then(|f| Gdbm::<ReadWrite>::open(f, path, self.alignment, self.cachesize))
            .map(|mut db| {
                db.set_sync(self.write.sync);
                db
            })
    }
}

impl OpenOptions<Write<Create>> {
    pub fn open<P: AsRef<std::path::Path>>(&self, path: P) -> Result<Gdbm<ReadWrite>> {
        if self.write.create.newdb {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path.as_ref())
                .map_err(Error::Io)
                .and_then(|f| Gdbm::create(f, path, self))
        } else {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path.as_ref())
                .map_err(Error::Io)
                .and_then(|f| {
                    Gdbm::<ReadWrite>::open(f, path.as_ref(), self.alignment, self.cachesize)
                        .or_else(|e| match e {
                            Error::EmptyFile(f) => Gdbm::create(f, path, self),
                            e => Err(e),
                        })
                })
        }
        .map(|mut db| {
            db.set_sync(self.write.sync);
            db
        })
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ConvertOptions {
    pub numsync: bool,
}
