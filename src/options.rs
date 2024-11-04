//
// options.rs -- GDBM core library API routines
//
// Copyright (c) 2024 Jeff Garzik, John Hedges
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use crate::{Alignment, Error, Gdbm, ReadOnly, Result};

#[derive(Clone, Debug, Default)]
pub struct OpenOptions {
    /// Override default alignement when opening a database.
    pub alignment: Option<Alignment>,
    /// Bytesize of in-memory bucket cache (defaults to DEFAULT_CACHESIZE)
    pub cachesize: Option<usize>,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OpenOptions {
    pub fn alignment(self, alignment: Option<Alignment>) -> OpenOptions {
        OpenOptions { alignment, ..self }
    }

    pub fn cachesize(self, cachesize: Option<usize>) -> OpenOptions {
        OpenOptions { cachesize, ..self }
    }
}

impl OpenOptions {
    pub fn open<P: AsRef<std::path::Path>>(&self, path: P) -> Result<Gdbm<ReadOnly>> {
        std::fs::OpenOptions::new()
            .read(true)
            .open(path.as_ref())
            .map_err(Error::Io)
            .and_then(|f| Gdbm::<ReadOnly>::open_ro(f, path, self.alignment, self.cachesize))
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ConvertOptions {
    pub numsync: bool,
}
