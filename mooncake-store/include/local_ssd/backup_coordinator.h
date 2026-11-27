#pragma once

#include <boost/functional/hash.hpp>
#include <chrono>
#include <cstdint>
#include <deque>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "mutex.h"
#include "types.h"

namespace mooncake {

// Master-side bookkeeping for SSD backups: a second, SSD-only copy of each
// object kept on a client other than the one holding its DRAM copy, so that
// losing one client does not break every prefix that touched it.
//
// The coordinator only decides *where* backups go and queues the work. It
// never touches object metadata and never holds a reference on the source
// DRAM copy: a backup client reads the source with an ordinary leased read,
// and a source evicted before that read simply yields no backup.
//
// Thread-safe. All methods take one internal mutex for a short, bounded time
// and never call back into the master, so they are safe to call while a key
// lock is held.
class SsdBackupCoordinator {
   public:
    using Clock = std::chrono::steady_clock;

    struct Config {
        bool enabled = false;
        // A batch is dispatched once it reaches either limit (one bucket).
        uint64_t batch_bytes = 256ULL << 20;
        uint32_t batch_keys = 500;
        // A partial batch older than this is dispatched anyway.
        std::chrono::milliseconds batch_max_age{10000};
        // A dispatched batch not acknowledged within this time is dropped.
        std::chrono::milliseconds task_timeout{30000};
        // Candidates whose last backup heartbeat is older than this are
        // excluded from selection.
        std::chrono::milliseconds candidate_stale_after{30000};
        // Dispatched-but-unacknowledged bytes allowed per backup client.
        uint64_t max_inflight_bytes_per_client = 2ULL << 30;
        // A client whose backup usage reaches this fraction of its backup
        // capacity receives no new batches until it reports less (its
        // batches wait meanwhile, see defer_max_age). Clients demote pinned
        // backups from a lower high watermark (FileStorage::BackupConfig::
        // pin_high_ratio, which must stay below this).
        double max_usage_ratio = 0.98;
        // Upper bounds on queued work, so an absent client cannot grow
        // master memory without limit. Overflow drops the newest work.
        size_t max_pending_batches_per_client = 64;
        size_t max_refreshes_per_client = 200000;
        // Batches handed to one client per heartbeat.
        size_t max_batches_per_heartbeat = 4;
        // A batch whose eligible clients are all busy (usage limit, inflight
        // budget or queue full) waits on the master for up to defer_max_age
        // and is placed on a later heartbeat instead of being dropped: in
        // order within a source, oldest first across sources. A write burst
        // faster than the backup clients then delays its backups rather than
        // losing them. 0 drops busy batches at once.
        std::chrono::milliseconds defer_max_age{600000};
        // Waiting limits, per source client (all its batches, whatever other
        // client they must also avoid) and in total. Tasks bound master
        // memory (each holds a tenant and a key string); bytes bound the
        // backup work queued behind one source.
        uint64_t max_deferred_bytes_per_source = 16ULL << 30;
        uint64_t max_deferred_tasks_per_source = 1000000;
        uint64_t max_deferred_tasks_total = 4000000;
        // A client whose last `max_consecutive_timeouts` batches all timed
        // out is skipped for `failure_cooldown`; any completed batch resets.
        uint32_t max_consecutive_timeouts = 3;
        std::chrono::milliseconds failure_cooldown{60000};
        // Interval of the INFO summary line (per-dispatch detail is VLOG(1)).
        std::chrono::milliseconds summary_interval{60000};

        static Config FromEnvironment();
    };

    struct Stats {
        uint64_t batches_dispatched = 0;
        uint64_t batches_completed = 0;
        uint64_t batches_timed_out = 0;
        uint64_t batches_unplaced = 0;  // no eligible backup client
        uint64_t refreshes_dropped = 0;
        uint64_t candidates_cooling_down = 0;  // skipped for timeouts
        uint64_t batches_unknown_host = 0;     // source host unknown
        // Objects whose backup was dropped (not placed): no eligible
        // target, unknown host, or target queue full. Best effort: these
        // stay without a backup until written or promoted again.
        uint64_t objects_unplaced = 0;
        // Batches that waited because every eligible client was busy.
        uint64_t batches_deferred = 0;
        // Batches not kept or dropped while waiting, by cause (their objects
        // are also counted in objects_unplaced): a waiting limit was full,
        // the wait reached defer_max_age, no eligible client was left when
        // retried, or the source client was removed.
        uint64_t deferred_overflow = 0;
        uint64_t deferred_expired = 0;
        uint64_t deferred_no_candidate = 0;
        uint64_t deferred_removed = 0;
        // Now (not counters): batches, bytes and tasks waiting, and the age
        // of the oldest waiting batch (0 when none).
        uint64_t waiting_batches = 0;
        uint64_t waiting_bytes = 0;
        uint64_t waiting_tasks = 0;
        double waiting_oldest_age_seconds = 0;
        // Put-to-backup latency of batches that registered objects: from
        // the first put of the batch to the registration of its holder's ACK
        // (an upper bound for every object of the batch). Sum and count since
        // start, maximum since start and over the current summary interval
        // (reset by the summary log line).
        // Successful registration calls (RecordBackedUp), not distinct
        // batches: concurrent ACKs of one batch may each register some keys.
        uint64_t backed_up_batches = 0;
        // Of those, the ones that took a latency sample, and the ones that
        // found no timing (already claimed, or dropped after
        // kBatchTimingHorizon). Timing records dropped unclaimed are counted
        // apart (batch_timing_dropped): they are records, not batches, and
        // overlap with later untimed registrations of the same batch.
        uint64_t put_to_backup_timed = 0;
        uint64_t backed_up_untimed = 0;
        uint64_t batch_timing_dropped = 0;
        double put_to_backup_seconds_sum = 0;
        double put_to_backup_seconds_max = 0;
        double put_to_backup_seconds_window_max = 0;
    };

    explicit SsdBackupCoordinator(Config config);

    [[nodiscard]] bool Enabled() const { return config_.enabled; }
    [[nodiscard]] const Config& GetConfig() const { return config_; }

    // Record a newly written object whose DRAM copy lives on `source`.
    // `also_avoid`: another client whose machine must not hold the backup
    // either (e.g. the primary SSD owner when it differs from the DRAM
    // source). Items with different `also_avoid` are batched separately.
    void OnPut(const UUID& source, const std::string& source_host,
               BackupTaskItem item, Clock::time_point now = Clock::now(),
               std::optional<UUID> also_avoid = std::nullopt);

    // Backup heartbeat from `client`: refresh its usage, then hand it queued
    // batches and refreshes. Also dispatches aged partial batches and expires
    // timed-out ones.
    BackupHeartbeatResponse OnHeartbeat(const UUID& client,
                                        const std::string& client_host,
                                        const std::string& endpoint,
                                        int64_t backup_used_bytes,
                                        int64_t backup_capacity_bytes,
                                        Clock::time_point now = Clock::now());

    // The backup client finished (successfully or not) a dispatched batch.
    void OnBatchDone(const UUID& client, uint64_t batch_id);
    // Batch `batch_id` registered backups now. The first call for a batch
    // claims its timing (kept from dispatch on, apart from the inflight
    // reservation, so a late ACK after a timeout or the winner of two
    // concurrent ACKs still finds it) and returns the seconds since the
    // batch's first put; later calls, or a batch whose timing is gone,
    // return none (counted as untimed).
    std::optional<double> RecordBackedUp(uint64_t batch_id,
                                         Clock::time_point now = Clock::now());

    // Transport endpoint a reader uses to fetch `client`'s backups. Backup
    // replicas do not store it (saves ~32 B per backup on the master); it is
    // filled in when a backup descriptor is handed to a reader. Empty when
    // the client has not sent a backup heartbeat to this master yet.
    [[nodiscard]] std::string GetEndpoint(const UUID& client) const;

    // Ask the holder of a backup to refresh its LRU time.
    void EnqueueRefresh(const UUID& holder, BackupRefreshItem item);

    // Physical host of `client`, from its segment mount (host_id). Batches
    // from a source whose host is unknown are not placed (fail closed), and
    // candidates with an unknown host are not chosen.
    void SetClientHost(const UUID& client, const std::string& host);

    // Forget a client (unregistered or offboarded): drop its queued work and
    // its pending source batch.
    void RemoveClient(const UUID& client);

    [[nodiscard]] Stats GetStats() const;

    // Tests: one candidate's reservation state (nullopt: not a candidate).
    struct ClientSnapshot {
        uint64_t inflight_bytes = 0;
        std::vector<uint64_t> inflight_batches;  // sorted
        size_t queued_batches = 0;
        bool operator==(const ClientSnapshot&) const = default;
    };
    [[nodiscard]] std::optional<ClientSnapshot> SnapshotForTest(
        const UUID& client) const;

   private:
    struct PendingSource {
        UUID source{};
        std::optional<UUID> also_avoid;
        std::string host;
        std::string avoid_host;
        std::vector<BackupTaskItem> items;
        // Position in `items` of each pending tenant/key: a later put of the
        // same key replaces its task (one task identity per key and batch).
        std::unordered_map<std::string, size_t> index;
        uint64_t bytes = 0;
        Clock::time_point first_item_time{};
        // Formed batches waiting for a client that is not busy, oldest
        // first (see Config::defer_max_age).
        struct Deferred {
            std::vector<BackupTaskItem> items;
            uint64_t bytes = 0;
            Clock::time_point since{};
            Clock::time_point oldest_put{};
        };
        std::deque<Deferred> deferred;
    };

    struct Inflight {
        uint64_t bytes = 0;
        // Enqueue time until delivered, then the delivery time.
        Clock::time_point dispatched_at{};
        // Handed to the holder in a heartbeat response. Only delivered
        // batches time out (R8-Q2); a queued one waits for its heartbeat.
        bool delivered = false;
    };

    struct Candidate {
        std::string host;
        std::string endpoint;
        int64_t used_bytes = 0;
        int64_t capacity_bytes = 0;
        Clock::time_point last_heartbeat{};
        uint64_t inflight_bytes = 0;
        uint32_t consecutive_timeouts = 0;
        Clock::time_point cooldown_until{};
        std::deque<BackupBatch> queued;
        std::unordered_map<uint64_t, Inflight> inflight;
        std::vector<BackupRefreshItem> refreshes;
        std::unordered_set<std::string> refresh_keys;  // dedup
    };

    using ClientMap = std::unordered_map<UUID, Candidate, boost::hash<UUID>>;

    // Requires mutex_.
    void FlushSource(PendingSource& pending, Clock::time_point now);
    // `busy` (optional): set when no client is eligible but some would be
    // if it were not busy (usage limit, inflight budget or queue full).
    std::optional<UUID> PickBackupClient(const PendingSource& pending,
                                         uint64_t batch_bytes,
                                         Clock::time_point now,
                                         bool* busy = nullptr);
    bool Eligible(const UUID& id, const Candidate& c,
                  const PendingSource& pending, uint64_t batch_bytes,
                  Clock::time_point now, bool* busy = nullptr) const;
    enum class PlaceResult { kPlaced, kBusy, kNoCandidate };
    // Queues `items` on a picked client (moved from on success).
    PlaceResult PlaceBatch(PendingSource& pending,
                           std::vector<BackupTaskItem>& items, uint64_t bytes,
                           Clock::time_point now, Clock::time_point oldest_put);
    // Whether a batch of `bytes` / `tasks` from `source` fits the waiting
    // limits, and the accounting of a batch entering or leaving the wait.
    bool DeferFits(const UUID& source, uint64_t bytes, uint64_t tasks) const;
    void AccountDeferred(const UUID& source, uint64_t bytes, uint64_t tasks,
                         bool add);
    // Drops the oldest waiting batch of `pending`, counted under `cause`.
    void DropDeferredFront(PendingSource& pending, uint64_t* cause);
    double Load(const Candidate& c) const;
    void FlushAgedSources(Clock::time_point now);
    void MaybeLogSummary(Clock::time_point now);
    void ExpireInflight(const UUID& id, Candidate& c, Clock::time_point now);

    const Config config_;
    mutable Mutex mutex_;
    // Keyed by (source, also_avoid or zero UUID).
    std::unordered_map<std::pair<UUID, UUID>, PendingSource,
                       boost::hash<std::pair<UUID, UUID>>>
        sources_ GUARDED_BY(mutex_);
    ClientMap clients_ GUARDED_BY(mutex_);
    uint64_t next_batch_id_ GUARDED_BY(mutex_) = 1;
    Stats stats_ GUARDED_BY(mutex_);
    Stats last_summary_stats_ GUARDED_BY(mutex_);
    Clock::time_point last_summary_ GUARDED_BY(mutex_){};
    std::unordered_map<UUID, std::string, boost::hash<UUID>> hosts_
        GUARDED_BY(mutex_);
    // Waiting batches over all sources, and whether a client may have
    // become able to take some since the last retry pass (a completion, an
    // expiry, a delivered queue, a new or recovered client, more capacity,
    // less use, a removed client). Heartbeats retry waiting batches then, when
    // the oldest one reaches defer_max_age, and in any case at most every
    // kDeferredRetryFallback (a bound on the delay should a change not be
    // noticed), so an idle wait costs little under mutex_ however often
    // clients heartbeat.
    struct DeferredUsage {
        uint64_t bytes = 0;
        uint64_t tasks = 0;
    };
    std::unordered_map<UUID, DeferredUsage, boost::hash<UUID>> deferred_usage_
        GUARDED_BY(mutex_);
    // Timing of dispatched batches (first put, dispatch time) by batch id,
    // until a registration claims it; ids grow with dispatch, so the oldest
    // are dropped from the front after kBatchTimingHorizon (and beyond
    // kMaxBatchTimings entries).
    struct BatchTiming {
        Clock::time_point oldest_put{};
        Clock::time_point dispatched_at{};
    };
    std::map<uint64_t, BatchTiming> batch_timing_ GUARDED_BY(mutex_);
    static constexpr std::chrono::minutes kBatchTimingHorizon{30};
    static constexpr size_t kMaxBatchTimings = 1000000;
    void PruneBatchTimingLocked(Clock::time_point now);
    size_t deferred_batches_ GUARDED_BY(mutex_) = 0;
    uint64_t deferred_tasks_total_ GUARDED_BY(mutex_) = 0;
    bool deferred_room_changed_ GUARDED_BY(mutex_) = false;
    Clock::time_point next_deferred_expiry_ GUARDED_BY(mutex_){
        Clock::time_point::max()};
    Clock::time_point last_deferred_retry_ GUARDED_BY(mutex_){};
    static constexpr std::chrono::milliseconds kDeferredRetryFallback{1000};
    // One pass over all waiting batches: the oldest first across sources,
    // in order within a source; a source whose next batch is busy is
    // skipped for the rest of the pass.
    void RetryAllDeferred(Clock::time_point now);
    uint64_t WaitingBytesLocked() const;
};

}  // namespace mooncake
