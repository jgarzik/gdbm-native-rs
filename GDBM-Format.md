# The GDBM File Format

## Overview

The GDBM file format is an on-disk format inspired by in-memory hash
table data structures.  The *header* describes the entire database.  The
*directory* is a vector of storage offsets for buckets, which is the
core, 1st level hash table.  A *bucket* is 2nd, smaller hash table which
contains *bucket elements*, the actual key/value data items themselves.

## Database Header

The **GDBM Header** data structures summarizes the entire database file,
similar to a filesystem superblock.  The Header provides the root of all
data lookups and updates.

```
typedef struct
{
  int32_t   header_magic;  /* Version of file. */
  int32_t   block_size;    /* The optimal i/o blocksize from stat. */
  off_t dir;               /* File address of hash directory table. */
  int32_t   dir_size;      /* Size in bytes of the table.  */
  int32_t   dir_bits;      /* The number of address bits used in the table.*/
  int32_t   bucket_size;   /* Size in bytes of a hash bucket struct. */
  int32_t   bucket_elems;  /* Number of elements in a hash bucket. */
  off_t next_block;        /* The next unallocated block address. */
} gdbm_file_header;
```

## Bucket Directory

The 1st hash table accessed during lookups and storage is the *bucket
directory*.  This is a vector of file offsets.  Each vector element
points to a bucket.

## Bucket

The 2nd hash table accessed during lookups and storage is a small hash
table that consists of a number of bucket elements, plus some metadata.

## Bucket Element

An element is the final, atomic unit resultng from a lookup or update: a
single key/value data record.

## Avail Table

The database-wide free list; a list of allocated, on-disk storage space
(free space) available for re-use.  It is stored as a linked list of
*avail blocks*, with the initial avail block referenced in the database
header.

## Avail Block

A data structure containing a portion of the free list (*avail
elements*), and a pointer to the next block in the linked list.

## Avail Element

A single (offset, length) pair indicating a byte range within the
on-disk file that may be re-used for the next database storage
operation.


