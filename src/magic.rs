use std::fmt;
use std::io::{self, ErrorKind};

use crate::ser::{Alignment, Endian, Offset};

const GDBM_OMAGIC_LE: [u8; 4] = [0xce, 0x9a, 0x57, 0x13];
const GDBM_OMAGIC_BE: [u8; 4] = [0x13, 0x57, 0x9a, 0xce];
const GDBM_MAGIC_LE_32: [u8; 4] = [0xcd, 0x9a, 0x57, 0x13];
const GDBM_MAGIC_LE_64: [u8; 4] = [0xcf, 0x9a, 0x57, 0x13];
const GDBM_NUMSYNC_MAGIC_LE_32: [u8; 4] = [0xd0, 0x9a, 0x57, 0x13];
const GDBM_NUMSYNC_MAGIC_LE_64: [u8; 4] = [0xd1, 0x9a, 0x57, 0x13];
const GDBM_MAGIC_BE_32: [u8; 4] = [0x13, 0x57, 0x9a, 0xcd];
const GDBM_MAGIC_BE_64: [u8; 4] = [0x13, 0x57, 0x9a, 0xcf];
const GDBM_NUMSYNC_MAGIC_BE_32: [u8; 4] = [0x13, 0x57, 0x9a, 0xd0];
const GDBM_NUMSYNC_MAGIC_BE_64: [u8; 4] = [0x13, 0x57, 0x9a, 0xd1];

/// Database magic numbers stored in the first 4 bytes of the database file. The value describes
/// the layout options used internally by the database instance, and can be inspected using
/// [`magic()`](crate::Gdbm::magic()`).
#[must_use]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Magic {
    /// [`Offset::Small`], [`Endian::Little`], [`numsync`](crate::OpenOptions::numsync): `false`
    LE,
    /// [`Offset::Small`], [`Endian::Big`], [`numsync`](crate::OpenOptions::numsync): `false`
    BE,
    /// [`Offset::Small`], [`Endian::Little`], [`numsync`](crate::OpenOptions::numsync): `false`
    LE32,
    /// [`Offset::Small`], [`Endian::Big`], [`numsync`](crate::OpenOptions::numsync): `false`
    BE32,
    /// [`Offset::LFS`], [`Endian::Little`], [`numsync`](crate::OpenOptions::numsync): `false`
    LE64,
    /// [`Offset::LFS`], [`Endian::Big`], [`numsync`](crate::OpenOptions::numsync): `false`
    BE64,
    /// [`Offset::Small`], [`Endian::Little`], [`numsync`](crate::OpenOptions::numsync): `true`
    LE32NS,
    /// [`Offset::Small`], [`Endian::Big`], [`numsync`](crate::OpenOptions::numsync): `true`
    BE32NS,
    /// [`Offset::LFS`], [`Endian::Little`], [`numsync`](crate::OpenOptions::numsync): `true`
    LE64NS,
    /// [`Offset::LFS`], [`Endian::Big`], [`numsync`](crate::OpenOptions::numsync): `true`
    BE64NS,
}

impl Magic {
    /// Create a new [`Magic`].
    pub fn new(endian: Endian, offset: Offset, numsync: bool) -> Self {
        match (endian, offset, numsync) {
            (Endian::Little, Offset::Small, false) => Magic::LE32,
            (Endian::Little, Offset::Small, true) => Magic::LE32NS,
            (Endian::Little, Offset::LFS, false) => Magic::LE64,
            (Endian::Little, Offset::LFS, true) => Magic::LE64NS,
            (Endian::Big, Offset::Small, false) => Magic::BE32,
            (Endian::Big, Offset::Small, true) => Magic::BE32NS,
            (Endian::Big, Offset::LFS, false) => Magic::BE64,
            (Endian::Big, Offset::LFS, true) => Magic::BE64NS,
        }
    }

    pub(super) fn from_reader(rdr: &mut impl std::io::Read) -> io::Result<Self> {
        let mut buf = [0u8; 4];
        rdr.read_exact(&mut buf)?;
        match buf {
            GDBM_OMAGIC_LE => Ok(Magic::LE),
            GDBM_OMAGIC_BE => Ok(Magic::BE),
            GDBM_MAGIC_LE_32 => Ok(Magic::LE32),
            GDBM_MAGIC_BE_32 => Ok(Magic::BE32),
            GDBM_MAGIC_LE_64 => Ok(Magic::LE64),
            GDBM_MAGIC_BE_64 => Ok(Magic::BE64),
            GDBM_NUMSYNC_MAGIC_LE_32 => Ok(Magic::LE32NS),
            GDBM_NUMSYNC_MAGIC_BE_32 => Ok(Magic::BE32NS),
            GDBM_NUMSYNC_MAGIC_LE_64 => Ok(Magic::LE64NS),
            GDBM_NUMSYNC_MAGIC_BE_64 => Ok(Magic::BE64NS),
            _ => Err(io::Error::new(
                ErrorKind::Other,
                "Unknown/invalid magic number",
            )),
        }
    }

    /// Get [`Magic`] [`Endian`].
    #[must_use]
    pub fn endian(&self) -> Endian {
        match self {
            Magic::LE | Magic::LE32 | Magic::LE64 | Magic::LE32NS | Magic::LE64NS => Endian::Little,
            _ => Endian::Big,
        }
    }

    /// Get [`Magic`] [`Offset`].
    #[must_use]
    pub fn offset(&self) -> Offset {
        match self {
            Magic::LE64 | Magic::BE64 | Magic::LE64NS | Magic::BE64NS => Offset::LFS,
            _ => Offset::Small,
        }
    }

    /// Get [`Magic`] `numsync`.
    #[must_use]
    pub fn is_numsync(&self) -> bool {
        matches!(
            self,
            Magic::BE64NS | Magic::LE64NS | Magic::BE32NS | Magic::LE32NS
        )
    }

    /// Get the default alignment for this [`Magic`]. A flaw in the design of GNU GDBM is that the
    /// alignment cannot be derived from the database [`Magic`]. We can guess, based on the
    /// [`Offset`], but this sometimes needs to be overridden.
    #[must_use]
    pub fn default_alignment(&self) -> Alignment {
        match self {
            Magic::BE64 | Magic::LE64 | Magic::BE64NS | Magic::LE64NS => Alignment::Align64,
            _ => Alignment::Align32,
        }
    }

    pub(super) fn as_bytes(&self) -> &[u8] {
        match self {
            Magic::LE => &GDBM_OMAGIC_LE,
            Magic::LE32 => &GDBM_MAGIC_LE_32,
            Magic::LE64 => &GDBM_MAGIC_LE_64,
            Magic::LE32NS => &GDBM_NUMSYNC_MAGIC_LE_32,
            Magic::LE64NS => &GDBM_NUMSYNC_MAGIC_LE_64,
            Magic::BE => &GDBM_OMAGIC_BE,
            Magic::BE32 => &GDBM_MAGIC_BE_32,
            Magic::BE64 => &GDBM_MAGIC_BE_64,
            Magic::BE32NS => &GDBM_NUMSYNC_MAGIC_BE_32,
            Magic::BE64NS => &GDBM_NUMSYNC_MAGIC_BE_64,
        }
    }
}

impl fmt::Display for Magic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Magic::LE => "GDBM_OMAGIC",
            Magic::LE32 => "GDBM_MAGIC32",
            Magic::LE64 => "GDBM_MAGIC64",
            Magic::LE32NS => "GDBM_NUMSYNC_MAGIC32",
            Magic::LE64NS => "GDBM_NUMSYNC_MAGIC64",
            Magic::BE => "GDBM_OMAGIC_SWAP",
            Magic::BE32 => "GDBM_MAGIC32_SWAP",
            Magic::BE64 => "GDBM_MAGIC64_SWAP",
            Magic::BE32NS => "GDBM_NUMSYNC_MAGIC32_SWAP",
            Magic::BE64NS => "GDBM_NUMSYNC_MAGIC64_SWAP",
        };
        write!(f, "{name}")
    }
}
