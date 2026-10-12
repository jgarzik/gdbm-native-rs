use std::fmt;
use std::io;

use crate::ser::{Endian, Offset};
use crate::{Error, Result};

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

/// Database magic numbers stored in the first 4 bytes of the database file. They record the byte
/// order, the size of file offsets and whether the database has a numsync header, and can be
/// inspected using [`magic()`](crate::Gdbm::magic()). `LE` and `BE` are the original GDBM magic
/// numbers, which C GDBM treats as 32-bit offsets.
#[must_use]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Magic {
    /// 32-bit offsets, [`Endian::Little`], [`numsync`](crate::OpenOptions::numsync): `false`
    LE,
    /// 32-bit offsets, [`Endian::Big`], [`numsync`](crate::OpenOptions::numsync): `false`
    BE,
    /// 32-bit offsets, [`Endian::Little`], [`numsync`](crate::OpenOptions::numsync): `false`
    LE32,
    /// 32-bit offsets, [`Endian::Big`], [`numsync`](crate::OpenOptions::numsync): `false`
    BE32,
    /// 64-bit offsets, [`Endian::Little`], [`numsync`](crate::OpenOptions::numsync): `false`
    LE64,
    /// 64-bit offsets, [`Endian::Big`], [`numsync`](crate::OpenOptions::numsync): `false`
    BE64,
    /// 32-bit offsets, [`Endian::Little`], [`numsync`](crate::OpenOptions::numsync): `true`
    LE32NS,
    /// 32-bit offsets, [`Endian::Big`], [`numsync`](crate::OpenOptions::numsync): `true`
    BE32NS,
    /// 64-bit offsets, [`Endian::Little`], [`numsync`](crate::OpenOptions::numsync): `true`
    LE64NS,
    /// 64-bit offsets, [`Endian::Big`], [`numsync`](crate::OpenOptions::numsync): `true`
    BE64NS,
}

impl Magic {
    pub(crate) fn new(endian: Endian, offset: Offset, numsync: bool) -> Self {
        match (endian, offset, numsync) {
            (Endian::Little, Offset::Small, false) => Magic::LE32,
            (Endian::Little, Offset::Small, true) => Magic::LE32NS,
            (Endian::Little, Offset::Lfs, false) => Magic::LE64,
            (Endian::Little, Offset::Lfs, true) => Magic::LE64NS,
            (Endian::Big, Offset::Small, false) => Magic::BE32,
            (Endian::Big, Offset::Small, true) => Magic::BE32NS,
            (Endian::Big, Offset::Lfs, false) => Magic::BE64,
            (Endian::Big, Offset::Lfs, true) => Magic::BE64NS,
        }
    }

    pub(super) fn from_reader(rdr: &mut impl io::Read) -> Result<Self> {
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
            _ => Err(Error::BadMagic {
                magic: u32::from_be_bytes(buf),
            }),
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

    pub(crate) fn offset(&self) -> Offset {
        match self {
            Magic::LE64 | Magic::BE64 | Magic::LE64NS | Magic::BE64NS => Offset::Lfs,
            _ => Offset::Small,
        }
    }

    /// Size of file offsets in bits: 32 or 64.
    #[must_use]
    pub fn offset_bits(&self) -> u32 {
        match self.offset() {
            Offset::Small => 32,
            Offset::Lfs => 64,
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
