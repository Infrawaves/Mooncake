# SSD Backup

## Overview

With SSD offload, an object whose DRAM copy has been evicted lives only on the
local SSD of the client that offloaded it. If that client or its host fails,
the object is lost until it is written again.

SSD backup keeps a second SSD copy of each offloaded object on a client on a
**different host**. When a read from the primary fails with a transport
error, the reader retries the same key from its backup in the same call, and
marks the failed endpoint as suspect for a short time so later reads of its
keys go to their backups directly.

SSD backup is **off by default**. With both switches unset, the master,
clients and readers behave exactly as without this feature.

It builds on [SSD Offload](ssd-offload); configure and verify offload first.

## Requirements

- SSD offload is enabled (`--enable_offload=true` on the master, a local disk
  path on every Real Client).
- Eager offload: the master offloads at put time, not on eviction
  (`offload_on_evict` off, the default). Backups are queued when an object is
  queued for offload.
- HA OpLog is off. Backup registrations are not written to the OpLog; if both
  are enabled the master logs an error and disables SSD backup.
- At least two hosts with SSD offload clients. A backup is never placed on
  the primary's host.
- The same Mooncake version on the master and on all clients (see
  [Upgrade order](#upgrade-order)).

## Enabling

There are two independent switches.

| Variable | Where | Effect |
|---|---|---|
| `MC_SSD_BACKUP_ENABLE=1` | master and every SSD offload client | The master schedules backup batches; clients write and serve backups |
| `MC_SSD_BACKUP_READ_FALLBACK=1` | every reader (Real Client) | Failed reads are retried from the backup; failed endpoints are marked suspect |

`MC_SSD_BACKUP_READ_FALLBACK` is off unless it is exactly `1`. Enable it
together with `MC_SSD_BACKUP_ENABLE`.

## Capacity

Backups are stored in each holder's SSD next to its own primaries and count
against the same disk.

| Variable | Default | Meaning |
|---|---|---|
| `MC_SSD_BACKUP_CAPACITY_BYTES` | 2 × the client's DRAM segment (SSD limit / 8 if unknown) | Pinned backup bytes a client may hold; clamped to half of its SSD limit |
| `MC_SSD_BACKUP_PIN_HIGH_RATIO` / `MC_SSD_BACKUP_PIN_LOW_RATIO` | 0.95 / 0.90 | When pinned backups reach the high ratio of the capacity, the holder demotes backups until it is below the low ratio |

The capacity limits **pinned** backups only. A demoted backup is unpinned
and becomes ordinary evictable cache: it still occupies SSD space, and still
serves reads, until the normal eviction policy removes it. Total SSD use by
backups can therefore exceed `MC_SSD_BACKUP_CAPACITY_BYTES`.

Backup buckets do not survive a client restart: the master drops a client's
backups when it goes away, and the client deletes them at startup.

## Tuning

Defaults are suitable for most deployments.

| Variable | Default | Meaning |
|---|---|---|
| `MC_SSD_BACKUP_BATCH_KEYS` / `MC_SSD_BACKUP_BATCH_BYTES` | 500 / 256 MiB | A backup batch is sent when it reaches either limit |
| `MC_SSD_BACKUP_BATCH_MAX_AGE_MS` | 10000 | A partial batch is sent after this age |
| `MC_SSD_BACKUP_TASK_TIMEOUT_MS` | 30000 | A delivered batch not acknowledged within this time is dropped and its reservation released |
| `MC_SSD_BACKUP_MAX_INFLIGHT_BYTES` | 2 GiB | Unacknowledged backup bytes per holder |
| `MC_SSD_BACKUP_READ_CHUNK_BYTES` | 1 MiB | A holder pulls a batch's source data in chunks of this size, so backup reads do not block foreground transfers on the same connection |
| `MC_SSD_BACKUP_MAX_WRITE_BPS` | 0 (unlimited) | Backup write rate limit per holder (see the note below) |

A holder sends its next backup heartbeat only after it has finished the
batches it received. If a low `MC_SSD_BACKUP_MAX_WRITE_BPS` (or a very slow
source) makes one round take longer than about 30 seconds, the master treats
the holder as stale and drops the batches still waiting for it. Keep the
limit high enough that `4 × MC_SSD_BACKUP_BATCH_BYTES` is written well
within 30 seconds, or lower `MC_SSD_BACKUP_BATCH_BYTES` together with it.

## Cost

Measured on a single-node, three-host test setup (1 MiB objects, SSD:DRAM =
5:1, TCP loopback), compared with the same workload without SSD backup:

- SSD writes are about twice as large: every object is written on its
  primary and on its backup holder.
- With a steady write load, successful reads and misses are close to
  `replica_num=2`.
- After the cache fills and the load becomes read-mostly, backups occupy SSD
  space that would otherwise hold more primaries, so fewer objects stay
  cached. Lower `MC_SSD_BACKUP_CAPACITY_BYTES` if this matters more than
  coverage.
- Master CPU is about 10% higher during sustained load.
- During a write burst, batches wait for busy holders instead of being
  dropped; objects written in the burst may have no backup for tens of
  seconds.

## Upgrade order

1. Deploy the new version to the master and all clients with both switches
   unset.
2. Set `MC_SSD_BACKUP_ENABLE=1` on the master and the SSD offload clients and
   restart them.
3. Set `MC_SSD_BACKUP_READ_FALLBACK=1` on the readers.

To disable, unset the switches in the reverse order. Existing backups are
dropped when their holders restart.

## Limitations

- One backup per object.
- Reads fail over to the backup only after the transport reports the failure
  of the primary; the first reads after a host failure can take a few
  seconds.
- A same-size `Upsert` of an object whose only copies are on SSD fails with
  `NO_AVAILABLE_HANDLE` and leaves the object unreadable until it is written
  again (as without SSD backup); with SSD backup enabled the backup is dropped
  as well.
