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

```
typedef struct
{
  off_t bucket_offset_vector[1]; /* The table. Make it look like an array. */
} gdbm_bucket_directory;
```

## Bucket

The 2nd hash table accessed during lookups and storage is a small hash
table that consists of a number of bucket elements, plus some metadata.

```
typedef struct
{
  int   av_count;            /* The number of bucket_avail entries. */
  avail_elem bucket_avail[BUCKET_AVAIL];  /* Distributed avail. */
  int   bucket_bits;         /* The number of bits used to get here. */
  int   count;               /* The number of element buckets full. */
  bucket_element h_table[1]; /* The table.  Make it look like an array.*/
} hash_bucket;
```

## Bucket Element

An element is the final, atomic unit resultng from a lookup or update: a
single key/value data record.

```
typedef struct
{
  int   hash_value;       /* The complete 31 bit value. */
  char  key_start[SMALL]; /* Up to the first SMALL bytes of the key.  */
  off_t data_pointer;     /* The file address of the key record. The
                             data record directly follows the key.  */
  int   key_size;         /* Size of key data in the file. */
  int   data_size;        /* Size of associated data in the file. */
} bucket_element;
```

## Avail Table

The database-wide free list; a list of allocated, on-disk storage space
(free space) available for re-use.  It is stored as a linked list of
*avail blocks*, with the initial avail block referenced in the database
header.

## Avail Block

A data structure containing a portion of the free list (*avail
elements*), and a pointer to the next block in the linked list.

```
typedef struct
{
  int   size;             /* The number of avail elements in the table.*/
  int   count;            /* The number of entries in the table. */
  off_t next_block;       /* The file address of the next avail block. */
  avail_elem av_table[1]; /* The table.  Make it look like an array.  */
} avail_block;
```

## Avail Element

A single (offset, length) pair indicating a byte range within the
on-disk file that may be re-used for the next database storage
operation.

```
typedef struct
{
  int   av_size;                /* The size of the available block. */
  off_t  av_adr;                /* The file address of the available block. */
} avail_elem;
```
