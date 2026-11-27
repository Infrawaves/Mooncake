#pragma once

#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <unordered_set>

#include "client_service.h"
#include "client_buffer.h"
#include "storage_backend.h"
#include "pinned_buffer_pool.h"

namespace mooncake {

struct SsdMetric;

class FileStorage {
   public:
    FileStorage(const FileStorageConfig& config, std::shared_ptr<Client> client,
                const std::string& local_rpc_addr,
                SsdMetric* ssd_metric = nullptr);
    ~FileStorage();

    tl::expected<void, ErrorCode> Init();

    void RemoveAll();

    /**
     * @brief Deregisters this store's disk tier from the master, then waits out
     * a grace period so offload reads already in flight can finish.
     *
     * Meant for a shutdown hook that runs before the process is signalled. Once
     * this returns, the master no longer names this store as the owner of any
     * offloaded key, so a reader gets a clean miss instead of a peer that is
     * about to disappear. Unlike a memory replica, which the NIC serves without
     * the process, a disk replica is read and pushed by this process -- hence
     * the grace period rather than an immediate exit.
     *
     * Latches offloading off first: the heartbeat re-mounts the segment
     * whenever the master answers SEGMENT_NOT_FOUND, which would otherwise
     * undo this within one heartbeat interval. The latch and the
     * deregistration happen under offloading_mutex_, the lock the heartbeat
     * holds across its master RPCs, so a tick that read the latch before it
     * was set cannot resume after the deregistration and re-mount: it either
     * finished before the drain latched, or it re-checks the latch when it
     * acquires the lock. The latch is not reversible on success; the process
     * is expected to exit.
     */
    tl::expected<void, ErrorCode> DrainLocalDiskSegment(
        uint64_t grace_period_ms);

    /**
     * @brief Result of BatchGet operation containing batch_id and buffer
     * pointers.
     */
    struct BatchGetResult {
        uint64_t batch_id;
        std::vector<uint64_t> pointers;
    };

    struct LocalBatchResult {
        std::vector<uint64_t> pointers;

       private:
        friend class FileStorage;
        std::shared_ptr<void> owner;
    };

    /**
     * @brief Reads multiple key-value (KV) entries from local storage and
     * forwards them to a remote node.
     * @param keys                 List of keys to read from the local KV store
     * @param sizes                Expected size in bytes for each value
     * @return tl::expected<BatchGetResult, ErrorCode> containing batch_id and
     * buffer pointers.
     */
    tl::expected<BatchGetResult, ErrorCode> BatchGet(
        const std::vector<std::string>& keys,
        const std::vector<int64_t>& sizes);

    // Reads [offset, offset + length) of each value. Returned pointers address
    // the first requested byte, not the start of the object.
    tl::expected<BatchGetResult, ErrorCode> BatchGetRanges(
        const std::vector<std::string>& keys,
        const std::vector<int64_t>& offsets,
        const std::vector<int64_t>& lengths,
        const std::vector<int64_t>& object_sizes);

    tl::expected<LocalBatchResult, ErrorCode> BatchGetLocal(
        const std::vector<std::string>& keys,
        const std::vector<int64_t>& sizes);

    tl::expected<LocalBatchResult, ErrorCode> BatchGetLocalRanges(
        const std::vector<std::string>& keys,
        const std::vector<int64_t>& offsets,
        const std::vector<int64_t>& lengths,
        const std::vector<int64_t>& object_sizes);

    [[nodiscard]] bool HasPinnedRestoreArena() const {
        return pinned_restore_arena_allocator_ != nullptr;
    }

    // Allocate request-scoped staging from the fixed-capacity accelerator-
    // pinned restore arena. The returned handle releases its sub-allocation
    // back to the arena; FileStorage retains ownership of the backing memory.
    [[nodiscard]] std::optional<BufferHandle> AllocatePinnedStagingBuffer(
        size_t size) {
        if (!pinned_restore_arena_allocator_) return std::nullopt;
        return pinned_restore_arena_allocator_->allocate(size);
    }

    /**
     * @brief Reports whether the backend still holds an entry for the key.
     *
     * Answers existence only, without allocating read buffers. After a
     * physical wipe the per-key backend's filesystem lookup and the bucket
     * backend's in-memory index both report false, which is exactly the
     * divergence a dangling-replica probe needs to detect.
     */
    tl::expected<bool, ErrorCode> Exists(const std::string& key);

    FileStorageConfig config_;

    /**
     * @brief Promote a set of keys from local SSD to DRAM (SSD prefetch).
     *
     * Each key must already have a promotion task registered on the master
     * (Client::RegisterPrefetchTask) with this client as holder; this method
     * runs the promotion execution chain (PromotionAllocStart -> staging
     * AllocateBatch -> BatchLoad -> PromotionWrite -> NotifyPromotionSuccess).
     * Failures are reported per-key via on_key_done and do not propagate;
     * this is best-effort.
     *
     * @param keys  Keys to promote
     * @param sizes Corresponding object sizes in bytes
     * @param dram_pressure Optional out-flag, set to true if any key failed
     *        because the master is out of DRAM (PromotionAllocStart returned
     *        NO_AVAILABLE_HANDLE), so the caller can back off prefetch and let
     *        eviction/offload reclaim memory. Never reset to false here;
     *        the caller initializes it.
     * @param on_key_done Optional per-key completion callback.
     * @return void on success, ErrorCode on critical (pre-loop) failure
     */
    using PrefetchKeyCallback =
        std::function<void(const std::string& key, bool success)>;

    tl::expected<void, ErrorCode> PrefetchKeys(
        const std::vector<std::string>& keys, const std::vector<int64_t>& sizes,
        bool* dram_pressure = nullptr,
        PrefetchKeyCallback on_key_done = nullptr,
        const std::string& tenant_id = "");

    /**
     * @brief Local SSD object size from the storage backend's authoritative
     * metadata (with tenant-scoped key fallback). Remote callers' size hints
     * must not be trusted for allocation sizing.
     */
    std::optional<int64_t> LookupLocalObjectSize(
        const std::string& key, const std::string& tenant_id = "") const;

    /**
     * @brief Releases buffer associated with a specific batch_id.
     * Called by remote client after transfer completion.
     * @param batch_id The unique identifier of the batch to release
     * @return true if batch was found and released, false otherwise
     */
    bool ReleaseBuffer(uint64_t batch_id);

   private:
    friend class FileStorageTest;
    friend class FileStoragePromotionTest;
    friend class FileStorageBackupTest;
    // TEST_F bodies are generated subclasses and do not inherit the fixture's
    // friendship, so the dangling-replica tests are friended by their
    // generated class names (plain friend, no gtest include in a production
    // header).
    friend class
        FileStorageTest_PutAfterPhysicalWipeHealsDanglingLocalDiskReplica_Test;
    friend class
        FileStorageTest_PutAfterPhysicalWipeHealsDanglingLocalDiskReplicaOnBucket_Test;
    struct AllocatedBatch {
        uint64_t batch_id;
        std::vector<BufferHandle> handles;
        std::unordered_map<std::string, Slice> slices;
        std::chrono::steady_clock::time_point lease_timeout;
        std::vector<uint64_t> pointers;
        uint64_t total_size;

        AllocatedBatch() : batch_id(0), total_size(0) {}
        AllocatedBatch(AllocatedBatch&&) = default;
        AllocatedBatch& operator=(AllocatedBatch&&) = default;

        AllocatedBatch(const AllocatedBatch&) = delete;
        AllocatedBatch& operator=(const AllocatedBatch&) = delete;

        ~AllocatedBatch() = default;
    };

    /**
     * @brief Offload object data and metadata.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> OffloadObjects(
        const std::vector<OffloadTaskItem>& offloading_objects);

    /**
     * @brief Classifies a BatchOffload error as affecting only the current
     * bucket rather than the whole offload cycle.
     *
     * Such an error means the bucket's keys simply cannot be persisted this
     * round; OffloadObjects reports them back to the master as failed (so their
     * offloading tasks and source-replica refcounts are released) and continues
     * with the remaining buckets, instead of aborting the entire cycle.
     *
     *   - INVALID_READ: source data for these keys could not be read/staged.
     *   - OBJECT_ALREADY_EXISTS: the key(s) were already offloaded, or are
     *     being offloaded concurrently. The bucket backend rejects the whole
     *     bucket atomically by design (see BucketStorageBackend::BatchOffload
     *     and its PrepareEviction duplicate guard). Treating this as fatal
     *     aborted the cycle, leaked offloading tasks, and left master/SSD
     *     metadata inconsistent, which surfaced as spurious INVALID_KEY on the
     *     read path (issue #2827).
     */
    static bool IsPerBucketSoftOffloadError(ErrorCode error);

    /**
     * @brief Performs a heartbeat operation for the FileStorage component.
     * 1. Sends object status (e.g., access frequency, size) to the master via
     * client.
     * 2. Receives feedback on which objects should be offloaded.
     * 3. Triggers asynchronous offloading of pending objects.
     * 4. If offload work was returned, pulls and processes any pending L2->L1
     *    promotion tasks queued by the master (mirror of step 1+2 in the
     *    reverse direction).
     * 5. Runs proactive local-disk watermark eviction.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> Heartbeat();

    /**
     * @brief Drives the L2->L1 promotion pipeline for one heartbeat tick.
     * Pulls promotion work from the master, stages a MEMORY replica for each
     * key, copies the bytes from local SSD into that replica, and notifies the
     * master on success. A failure on any single key is logged and skipped;
     * the master-side reaper decrements the source replica's refcnt and
     * erases the task entry on TTL expiry, and any orphaned PROCESSING
     * MEMORY replica is reaped via the standard discarded-replicas path.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> ProcessPromotionTasks();

    /**
     * @brief Execute the promotion chain for one key whose LOCAL_DISK replica
     * lives on this node: PromotionAllocStart -> staging AllocateBatch ->
     * BatchLoad -> PromotionWrite -> NotifyPromotionSuccess, with eager
     * NotifyPromotionFailure on every failure path (RAII guard).
     *
     * Shared by the heartbeat-driven promotion-on-hit path
     * (ProcessPromotionTasks) and the prefetch path (PrefetchKeys).
     * @param dram_pressure Optional out-flag set when PromotionAllocStart
     *        failed with NO_AVAILABLE_HANDLE (master out of DRAM).
     */
    tl::expected<void, ErrorCode> PromoteOneKeyFromLocalDisk(
        const std::string& key, const std::string& tenant_id, int64_t size,
        bool* dram_pressure);

    tl::expected<bool, ErrorCode> IsEnableOffloading();

    tl::expected<void, ErrorCode> RunDiskWatermarkEviction();

    tl::expected<void, ErrorCode> NotifyEvictedDiskReplicas(
        const std::vector<std::string>& evicted_keys);

    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& batch_object);

    tl::expected<void, ErrorCode> BatchLoadSpans(
        std::unordered_map<std::string, StorageValueSpan>& spans);

    tl::expected<std::shared_ptr<AllocatedBatch>, ErrorCode> LoadRanges(
        const std::vector<std::string>& keys,
        const std::vector<int64_t>& offsets,
        const std::vector<int64_t>& lengths,
        const std::vector<int64_t>& object_sizes, bool prefer_pinned);

    tl::expected<void, ErrorCode> BatchQuerySegmentSlices(
        const std::vector<std::string>& keys, const std::string& tenant_id,
        std::unordered_map<std::string, std::vector<Slice>>& batched_slices);

    tl::expected<void, ErrorCode> RegisterLocalMemory();

    tl::expected<std::shared_ptr<AllocatedBatch>, ErrorCode> AllocateBatch(
        const std::vector<std::string>& keys, const std::vector<int64_t>& sizes,
        ClientBufferAllocator& allocator);

    tl::expected<std::shared_ptr<AllocatedBatch>, ErrorCode> LoadBatch(
        const std::vector<std::string>& keys, const std::vector<int64_t>& sizes,
        bool prefer_pinned);

    void ClientBufferGCThreadFunc();

    /**
     * @brief Re-registers all offloaded objects with the master.
     * Called after master restart recovery to sync SSD object metadata.
     * This is the same logic as the ScanMeta step in Init().
     */
    tl::expected<void, ErrorCode> ReRegisterOffloadedObjects();

    std::shared_ptr<Client> client_;
    SsdMetric* ssd_metric_{nullptr};
    std::string local_rpc_addr_;
    // Pinned memory for GPU staging and SSD-to-GPU restores.
    std::unique_ptr<PinnedBufferPool> pinned_buffer_pool_;
    PinnedBufferPool::Buffer pinned_restore_arena_;
    std::shared_ptr<ClientBufferAllocator> pinned_restore_arena_allocator_;
    std::shared_ptr<StorageBackendInterface> storage_backend_;
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator_;
    mutable Mutex client_buffer_mutex_;
    std::unordered_map<uint64_t, std::shared_ptr<AllocatedBatch>> GUARDED_BY(
        client_buffer_mutex_) client_buffer_allocated_batches_;
    std::atomic<uint64_t> next_batch_id_{1};

    /**
     * @brief Partition drained-but-unbucketed offload tasks into NACKs vs
     * carries (pure function; extracted for direct unit testing).
     *
     * A key absent from all_bucket_keys was either deliberately skipped by
     * the backend (size over bucket limit / already on disk) — those must be
     * NACKed so the master releases the task and source-replica refcount — or
     * DEFERRED to the backend's ungrouped pool. FileStorage replays carried
     * keys next cycle and may flush a partial bucket. Deferred keys must NOT
     * be NACKed: the backend carries only key+size across the deferral, so
     * their tasks must be
     * carried here or the re-emitted keys arrive task-less and are silently
     * dropped (upstream #3006 — measured as ~12% of pool entries never
     * reaching the SSD tier).
     *
     * Previously carried keys must also be RETAINED while un-emitted: the
     * backend's pool only moves on a call with non-empty input, so a cycle
     * can pass without the pooled keys being either emitted or re-reported
     * (empty effective input after the carry merge). Rebuilding the carry
     * from deferred_keys alone would NACK such keys while the pool still
     * holds them — re-introducing the task-less drop.
     *
     * Retention is void for a key re-drained this cycle (redrained_carried_
     * keys): the collision handling in OffloadObjects dropped its pooled copy,
     * so it is no longer waiting and the stale carried task must not be
     * retained. Without the exclusion, a re-drained key the backend then
     * skipped (e.g. re-PUT at a size over the bucket limit) is neither
     * bucketed nor deferred, so the stale task would be retained forever as a
     * zombie AND its presence in carried_tasks would suppress the NACK for the
     * fresh task — losing the fresh task and leaking its source refcount.
     * Note the exclusion cannot be expressed as "absent from
     * task_by_storage_key": the carry merge inserts every non-collision
     * carried task into that map, so such a test would void retention
     * entirely.
     *
     * @param task_by_storage_key All tasks known this cycle (drained +
     * carried).
     * @param all_bucket_keys Storage keys emitted in a bucket this cycle.
     * @param deferred_keys Storage keys the backend deferred this cycle.
     * @param previously_carried Carry map from the previous cycle; entries
     * not emitted this cycle are retained in carried_tasks.
     * @param redrained_carried_keys Carried keys re-drained this cycle, whose
     * pooled copies the caller already removed; excluded from retention.
     * @param failed_tasks Output: tasks to NACK (appended).
     * @param carried_tasks Output: tasks to carry to the next cycle.
     */
    static void PartitionUnbucketedTasks(
        const std::unordered_map<std::string, OffloadTaskItem>&
            task_by_storage_key,
        const std::unordered_set<std::string>& all_bucket_keys,
        const std::vector<std::string>& deferred_keys,
        const std::unordered_map<std::string, OffloadTaskItem>&
            previously_carried,
        const std::unordered_set<std::string>& redrained_carried_keys,
        std::vector<OffloadTaskItem>& failed_tasks,
        std::unordered_map<std::string, OffloadTaskItem>& carried_tasks);

    // Tasks for keys the bucket backend deferred to its ungrouped pool, keyed
    // by tenant-scoped storage key. Consumed (merged into task_by_storage_key)
    // on the next OffloadObjects call, when the backend re-emits those keys in
    // a bucket. Only touched from the offload worker path (single-threaded);
    // lost on restart together with the backend's in-memory ungrouped pool,
    // in which case the master's TTL reaper reclaims the orphaned tasks.
    std::unordered_map<std::string, OffloadTaskItem>
        deferred_task_by_storage_key_;

    mutable Mutex offloading_mutex_;
    bool GUARDED_BY(offloading_mutex_) enable_offloading_;
    std::atomic<bool> heartbeat_running_;
    std::thread heartbeat_thread_;
    std::atomic<bool> client_buffer_gc_running_;
    std::thread client_buffer_gc_thread_;
    std::future<void> rescan_future_;
    std::atomic<bool> metadata_resync_pending_{false};
    // Set by DrainLocalDiskSegment under offloading_mutex_. Stops the
    // heartbeat -- which would otherwise re-mount the segment the drain just
    // deregistered -- and aborts an in-flight metadata rescan. Checked at
    // Heartbeat entry (fast path, also guards the rescan-retry block) and
    // again under offloading_mutex_ before the heartbeat RPC, because a tick
    // parked on that lock passed the entry check before the drain latched.
    std::atomic<bool> draining_{false};

    // ---- SSD backup (this client stores SSD backups of other clients'
    // objects). Off unless MC_SSD_BACKUP_ENABLE is set; needs the bucket
    // backend. Runs on its own thread so a slow backup never delays the
    // offload/promotion heartbeat. ----
    struct BackupConfig {
        bool enabled = false;
        // Pin region cap: backup bytes kept out of eviction while their
        // primaries are in DRAM (reported to the master). Default 2x the
        // DRAM segment (steady state is about 1x), else 1/8 of the SSD cap.
        int64_t capacity_bytes = 0;
        // Registered staging buffer the source DRAM copy is read into.
        int64_t staging_bytes = 512LL << 20;
        std::chrono::milliseconds interval{1000};
        // Pin scanner: how often, how many keys sampled per bucket, and the
        // share of sampled keys still in their owner's DRAM needed to stay
        // pinned (keep_ratio_tight once pinned bytes pass tight_fraction).
        std::chrono::milliseconds scan_interval{300000};
        size_t sample_keys = 32;
        double keep_ratio = 0.5;
        double keep_ratio_tight = 0.7;
        // Below pin_low_ratio, so that between the pin watermarks (where
        // pinned bytes stay under steady writes) every scan is tight: room is
        // made by residency, and the age-ordered cap demotion is the backstop.
        double tight_fraction = 0.85;
        size_t min_buckets_per_scan = 100;
        size_t max_buckets_per_scan = 600;
        uint32_t max_backoff_rounds = 24;  // 24 x 5 min = 2 h
        // Backup disk-write pacing; 0 = unlimited.
        int64_t max_write_bytes_per_sec = 0;
        // Pin watermarks, as fractions of capacity_bytes: once pinned bytes
        // reach pin_high_ratio, demote pinned buckets until they are below
        // pin_low_ratio. The high mark must stay below the master's
        // SsdBackupCoordinator::Config::max_usage_ratio (0.98), where it
        // stops dispatching to this client: demoting only above that would
        // leave the client stuck just under it, sent nothing and freeing
        // nothing. Validated as 0 < low < high < 0.98.
        double pin_high_ratio = 0.95;
        double pin_low_ratio = 0.90;
        // Bytes of source reads in flight per backup read (at least one
        // key); 0 reads a whole tenant group of a batch at once. Every
        // foreground transfer of this process and of the source can queue
        // behind the chunk in flight (shared lanes and I/O threads), so the
        // chunk is kept small: 1 MiB put mixed-traffic tails within ~0.1 ms
        // of no backup in the TCP A/B runs, 16 MiB about 0.5 ms above.
        int64_t read_chunk_bytes = 1LL << 20;
        // Bytes one leased source query covers (several chunks; at least one
        // chunk): one master query per window rather than per chunk. 0: one
        // query per chunk.
        int64_t read_query_bytes = 64LL << 20;
        // How long staging whose read left a transfer unquiesced stays out
        // of use before it goes back to the allocator: about 2.5x the worst
        // transfer engine lifetime of such a transfer (9 retries x (0.54 s
        // hardware retry + ~25 s reconnect and handshake)).
        std::chrono::milliseconds staging_quarantine{600000};
        // Longest wait for another batch to give its staging back when an
        // allocation does not fit (the pipeline's other slot is still being
        // written); then the group is dropped.
        std::chrono::milliseconds staging_wait{60000};
        static BackupConfig FromEnvironment(int64_t ssd_total_size_limit,
                                            int64_t dram_segment_bytes);
    };
    tl::expected<void, ErrorCode> StartBackupWorker();
    void BackupTick();
    // One backup batch read from its source into staging, ready to write.
    struct PreparedBackup {
        const BackupBatch* batch = nullptr;
        std::chrono::steady_clock::time_point start{};
        std::unordered_map<std::string, BackupTaskItem> task_by_storage_key;
        std::unordered_map<std::string, std::vector<Slice>> batch_object;
        std::vector<std::shared_ptr<AllocatedBatch>> staging;
        int64_t bytes = 0;
        size_t read_failed = 0;
    };
    PreparedBackup ReadBackupBatch(const BackupBatch& batch);
    // Most keys one residency query may carry
    // (MasterService::kMaxBackupResidencyKeys); also caps sample_keys.
    static constexpr size_t kBackupScanMaxKeysPerCall = 1000;
    // Upper bound of the lease margin of a backup read: a chunk is not read
    // on a lease ending sooner than min(this, a quarter of the lease the
    // query granted); the rest of its query window is queried again first.
    static constexpr std::chrono::milliseconds kBackupReadLeaseMargin{1000};
    // Staging whose read left a transfer unquiesced may still receive a late
    // DMA, and nothing reports when that transfer really ends (a wait timeout
    // does not cancel it). It is quarantined for staging_quarantine: not
    // handed out again until then, and leaked rather than freed if the
    // FileStorage goes away earlier.
    void QuarantineBackupStaging(std::shared_ptr<AllocatedBatch> staging);
    // Returns quarantined staging whose time is up to the allocator.
    void ReleaseExpiredBackupQuarantine();
    struct QuarantinedStaging {
        std::shared_ptr<AllocatedBatch> staging;
        std::chrono::steady_clock::time_point release_at;
    };
    std::mutex backup_quarantine_mutex_;
    // Guarded by the mutex above.
    std::vector<QuarantinedStaging> backup_staging_quarantine_;
    int64_t backup_quarantined_bytes_ = 0;
    uint64_t backup_quarantined_total_ = 0;  // cumulative count
    uint64_t backup_quarantine_released_total_ = 0;
    void UpdateBackupQuarantineMetricsLocked();
    // Staging for one tenant group. When it does not fit while other batches
    // still hold staging (`own_held` of the held allocations belong to the
    // calling batch), waits up to staging_wait for them to give it back and
    // retries; stop or drain ends the wait.
    tl::expected<std::shared_ptr<AllocatedBatch>, ErrorCode>
    AllocateBackupStaging(const std::vector<std::string>& keys,
                          const std::vector<int64_t>& sizes, size_t own_held);
    // Staging allocations held by read batches not yet written (and a count
    // of releases, to wake an allocation waiting for one). Guarded by
    // backup_wait_mutex_.
    size_t backup_staging_held_ = 0;
    uint64_t backup_staging_releases_ = 0;
    // Moves `staging` into out.staging, counted as held until released.
    void HoldBackupStaging(PreparedBackup& out,
                           std::shared_ptr<AllocatedBatch> staging);
    // Clock of the staging quarantine (null: steady_clock); a test seam.
    std::function<std::chrono::steady_clock::time_point()>
        backup_clock_for_test_;
    std::chrono::steady_clock::time_point BackupNow() const {
        return backup_clock_for_test_ ? backup_clock_for_test_()
                                      : std::chrono::steady_clock::now();
    }

   public:
    struct BackupStagingStats {
        size_t quarantined_batches = 0;  // now
        int64_t quarantined_bytes = 0;   // now (payload bytes)
        uint64_t quarantined_total = 0;  // cumulative
        uint64_t quarantine_released = 0;
        uint64_t alloc_waits = 0;   // allocations that waited for a release
        uint64_t alloc_failed = 0;  // groups dropped without staging
    };
    BackupStagingStats GetBackupStagingStats();

   private:
    std::atomic<uint64_t> backup_staging_alloc_waits_{0};
    std::atomic<uint64_t> backup_staging_alloc_failed_{0};
    void WriteBackupBatch(PreparedBackup& prepared);
    // Registering a written backup bucket (NotifyBackupDone). A transport
    // failure leaves the outcome unknown: retried up to kBackupAckAttempts
    // times; if still unknown, *outcome_unknown is set and the caller keeps
    // the bucket (the master may advertise it). Other errors are refusals.
    using BackupAckFn =
        std::function<tl::expected<std::vector<uint32_t>, ErrorCode>()>;
    static constexpr int kBackupAckAttempts = 3;
    static constexpr std::chrono::milliseconds kBackupAckRetryBackoff{200};
    static bool IsBackupAckOutcomeUnknown(ErrorCode e);
    static tl::expected<std::vector<uint32_t>, ErrorCode> AckBackupBucket(
        const BackupAckFn& notify, int attempts,
        std::chrono::milliseconds backoff, bool* outcome_unknown);
    void RunBackupBatches(const std::vector<BackupBatch>& batches);
    // Paces backup disk writes. Returns false when stop or drain was
    // requested (before or during the wait): the write must not happen.
    bool ThrottleBackupWrite(int64_t bytes);
    // Time `bytes` take at `rate` bytes/s, saturated at the clock's maximum.
    static std::chrono::steady_clock::duration BackupPacingDebt(int64_t bytes,
                                                                int64_t rate);
    // Stop (destructor) or drain requested.
    bool BackupStopRequested() const;
    // Empty-ACKs batches[from..] (never read) so the master releases them.
    void ReleaseUnreadBackupBatches(const std::vector<BackupBatch>& batches,
                                    size_t from);
    // Waits until `t` or until stop (and drain, if `wake_on_drain`); true
    // if the wait was not cut short that way.
    bool WaitBackupUntil(std::chrono::steady_clock::time_point t,
                         bool wake_on_drain = true);
    void WakeBackupWorker();
    std::mutex backup_wait_mutex_;
    std::condition_variable backup_wait_cv_;
    // Test seams for worker start-up failures (null: the real calls).
    std::function<std::shared_ptr<ClientBufferAllocator>(size_t)>
        backup_staging_alloc_for_test_;
    std::function<tl::expected<void, ErrorCode>(void*, size_t)>
        backup_staging_register_for_test_;
    std::function<tl::expected<void, ErrorCode>(void*)>
        backup_staging_unregister_for_test_;
    // Test hook at backup worker stages (null in production): the stage
    // name and a count (batches, keys).
    std::function<void(const char*, size_t)> backup_stage_hook_for_test_;
    // Test observer of every NotifyBackupDone attempt the worker makes:
    // batch id, number of tasks, and whether the RPC succeeded.
    std::function<void(uint64_t, size_t, bool)> backup_ack_observer_for_test_;
    // Test observer of each key read by a quiesced backup transfer: tenant,
    // key, the read's result, and the staging slice it was read into (valid
    // only during the call; the staging is still owned then).
    std::function<void(const std::string&, const std::string&, ErrorCode,
                       const Slice&)>
        backup_transfer_observer_for_test_;
    // Test observer of the backup staging lifetime (null in production):
    // event, lowest address and span. "register", "unregister" (before the
    // call), "unregister_failed", "staging_freed" and "staging_leaked" are
    // about the whole staging; "alloc", "hold", "quarantine",
    // "quarantine_expired", "leak" (still quarantined at destruction) and
    // "free" (its memory is back in the allocator) about one allocation.
    std::function<void(const char*, const void*, size_t)>
        backup_staging_event_for_test_;
    void StagingEvent(const char* event, const void* ptr, size_t bytes) {
        if (backup_staging_event_for_test_) {
            backup_staging_event_for_test_(event, ptr, bytes);
        }
    }
    void StagingEvent(const char* event, const AllocatedBatch& batch) {
        if (backup_staging_event_for_test_) {
            const auto span = StagingSpan(batch);
            backup_staging_event_for_test_(event, span.first, span.second);
        }
    }
    // Lowest slice address and the span up to the end of the highest slice.
    static std::pair<const void*, size_t> StagingSpan(
        const AllocatedBatch& batch);
    void BackupAcked(uint64_t batch_id, size_t tasks, bool ok) {
        if (backup_ack_observer_for_test_) {
            backup_ack_observer_for_test_(batch_id, tasks, ok);
        }
    }
    void BackupStage(const char* stage, size_t n) {
        if (backup_stage_hook_for_test_) backup_stage_hook_for_test_(stage, n);
    }
    // Unregisters the backup staging from the transfer engine and releases
    // it; called by the destructor once the worker has been joined.
    void ReleaseBackupStaging();
    std::chrono::steady_clock::time_point backup_write_ready_{};
    void ApplyBackupRefreshes(const std::vector<BackupRefreshItem>& items);
    // From pin_high_ratio of the capacity, demote least-useful pinned buckets
    // until pinned bytes drop below pin_low_ratio. Uses ratios from earlier
    // scans; no RPC.
    void EnforceBackupPinCap();
    void ScanPinnedBackups();

    struct BackupScanState {
        std::chrono::steady_clock::time_point next_check{};
        double last_ratio = 1.0;  // unknown until first scan
        uint32_t keeps = 0;       // consecutive keep decisions
    };

   public:
    struct BackupScanStats {
        uint64_t scans = 0;
        uint64_t buckets_checked = 0;
        uint64_t demoted_cold = 0;
        uint64_t demoted_owner_gone = 0;
        uint64_t demoted_cap = 0;
        uint64_t demoted_unmapped = 0;  // no key maps to the bucket any more
        uint64_t refreshed = 0;
    };
    // For tests and logs; safe to call from any thread while the backup
    // worker runs (each counter is read on its own, the set is not one
    // atomic snapshot).
    BackupScanStats GetBackupScanStats() const {
        const auto& s = backup_scan_stats_;
        constexpr auto r = std::memory_order_relaxed;
        return {s.scans.load(r),        s.buckets_checked.load(r),
                s.demoted_cold.load(r), s.demoted_owner_gone.load(r),
                s.demoted_cap.load(r),  s.demoted_unmapped.load(r),
                s.refreshed.load(r)};
    }
    // Test hook: run one scanner round now.
    void RunBackupScanForTest() { ScanPinnedBackups(); }

   private:
    std::unordered_map<int64_t, BackupScanState> backup_scan_state_;
    std::chrono::steady_clock::time_point next_backup_scan_{};
    // Pin-cap demotions ask for a scan right away (their ratios were stale),
    // at most once per kBackupCapRescanInterval; their log line is
    // rate-limited the same way.
    static constexpr std::chrono::seconds kBackupCapRescanInterval{30};
    static constexpr std::chrono::seconds kBackupCapLogInterval{10};
    std::chrono::steady_clock::time_point last_cap_rescan_{};
    std::chrono::steady_clock::time_point last_cap_log_{};
    uint64_t cap_demoted_bytes_total_ = 0;
    size_t backup_new_buckets_since_scan_ = 0;
    // Updated by the backup worker, read by GetBackupScanStats from other
    // threads.
    struct AtomicScanStats {
        std::atomic<uint64_t> scans{0};
        std::atomic<uint64_t> buckets_checked{0};
        std::atomic<uint64_t> demoted_cold{0};
        std::atomic<uint64_t> demoted_owner_gone{0};
        std::atomic<uint64_t> demoted_cap{0};
        std::atomic<uint64_t> demoted_unmapped{0};
        std::atomic<uint64_t> refreshed{0};
    };
    AtomicScanStats backup_scan_stats_;

    BackupConfig backup_config_;
    std::shared_ptr<ClientBufferAllocator> backup_staging_allocator_;
    // StartBackupWorker registered the staging with the transfer engine.
    bool backup_staging_registered_ = false;
    std::atomic<bool> backup_running_{false};
    std::atomic<bool> backup_stop_{false};  // set once, by the destructor
    std::thread backup_thread_;
};

}  // namespace mooncake
