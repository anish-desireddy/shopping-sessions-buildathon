# Shopping Sessions engine

Builders Buildathon, Databricks "Shopping Sessions" track. A single multi-threaded Rust
binary that implements `rules.md` exactly: it reads `/data`, and writes
`/output/user_daily/` and `/output/campaign_daily/` as Parquet.

How it works:

1. All CSV files are memory-mapped and split into line-aligned chunks that worker threads (one per
   CPU, pinned) pull from a shared counter. Each line's field boundaries come from AVX2
   comma/newline/quote bitmasks over two 64-byte windows (BMI2 `pdep`/`pext` to pick the commas,
   quote placement checked with mask arithmetic); unusual lines take an exact scalar path.
   Timestamps use a small table of the months around the window plus SWAR digit decoding, with
   exact slow paths for anything unusual. The input is software-prefetched 1 KB ahead.
2. Every user ID is interned once during parsing into a concurrent lock-free table (dense indices,
   lookups prefetched 16 rows ahead). Link files are parsed first; one thread runs a prefetching
   union-find over the dense indices (each person named by its byte-wise smallest ID, compared by
   an 8-byte key carried in each edge with a full byte compare on ties) while the others parse
   events and clicks.
3. Events become 24-byte records scattered by a 128-bit `event_id` hash (64-bit key, partition
   from independent bits) into cache-sized partitions (about 12K rows), through write-combining
   staging buffers flushed with non-temporal stores. Each partition is deduplicated with a 2-bit
   seen/seen-again filter: unique keys pass straight through, and only possible repeats go through
   an exact table (smallest `ingest_ts` wins, before validation; invalid copies are kept as
   tombstones). The output is written into chunks carved from phase-1 memory that has already been
   read, so this stage touches no fresh pages.
4. Surviving events and valid clicks are scattered into person-range partitions. Each partition
   groups rows per person with a counting sort. Sessions come from 30-minute cells (first, last,
   count, revenue per cell; a session can only break between non-empty cells), so events are never
   sorted. Clicks are ordered by ts with one counting pass plus a bounded repair; equal-ts groups
   are resolved only when needed. Credit is the latest click within 7 days (ties: smallest
   `click_id`); assists are index ranges of sessions per click, merged per campaign, split by day.
5. A minimal streaming Parquet writer (uncompressed, INT32 counts, dictionary-encoded
   `top_category`) writes one user_daily file per worker as soon as that worker finishes.
6. Big buffers come from 2 MB-aligned mmaps with transparent huge pages requested; memory and
   input mappings are released in parallel (overlapping the last file write) before `_exit`.

Open-source libraries used: `memmap2`, `memchr`, `xxhash-rust`, `libc` (all MIT/Apache-2.0).
All other code was written by our team during the buildathon.

License: MIT.
