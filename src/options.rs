//
// options.rs -- GDBM core library API routines
//
// Copyright (c) 2024 Jeff Garzik, John Hedges
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use crate::{Alignment, Error, Gdbm, ReadOnly, ReadWrite, Result};

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
    pub fn write(self) -> OpenOptions<Write> {
        OpenOptions {
            alignment: self.alignment,
            cachesize: self.cachesize,
            write: Write,
        }
    }
}

impl OpenOptions<Write> {
    pub fn not_write(self) -> OpenOptions<NotWrite> {
        OpenOptions {
            alignment: self.alignment,
            cachesize: self.cachesize,
            write: NotWrite,
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

impl OpenOptions<Write> {
    pub fn open<P: AsRef<std::path::Path>>(&self, path: P) -> Result<Gdbm<ReadWrite>> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())
            .map_err(Error::Io)
            .and_then(|f| Gdbm::<ReadWrite>::open(f, path, self.alignment, self.cachesize))
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct NotWrite;
#[derive(Copy, Clone, Debug, Default)]
pub struct Write;

#[derive(Copy, Clone, Debug)]
pub struct ConvertOptions {
    pub numsync: bool,
}
