// FileStorage side of the SSD backup worker: configuration parsing, staging
// quarantine, registration of written buckets and the backup source read.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <tuple>
#include <condition_variable>
#include <cstdlib>
#include <map>
#include <mutex>
#include <thread>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <memory>
#include <set>
#include <string>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "aligned_client_buffer.h"
#include "allocator.h"
#include "client_metric.h"
#include "client_service.h"
#include "common/network.h"
#include "common/client_buffer_allocation.h"
#include "file_storage.h"
#include "local_hot_cache.h"
#include "master_service.h"
#include "master_service/master_service_test_peer.h"
#include "real_client.h"
#include "rpc_service.h"
#include "storage_backend.h"
#include "tenant/quota_policy_store.h"
#include "test_server_helpers.h"
#include "transfer_task.h"

#include <span>

namespace mooncake {

namespace fs = std::filesystem;

// A client endpoint name that this process has not handed out before. The
// port from getFreeTcpPort() is only a name (the transfer engine listens on
// its own port), so two clients could get the same one and the second
// registration would be rejected as a duplicate rpc_meta key.
std::string UniqueLocalEndpoint() {
    static std::mutex mu;
    static std::set<int> used;
    std::lock_guard<std::mutex> lock(mu);
    for (;;) {
        const int port = getFreeTcpPort();
        if (port > 0 && used.insert(port).second) {
            return "127.0.0.1:" + std::to_string(port);
        }
    }
}

class FileStorageBackupTest : public ::testing::Test {
   protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("mc_file_storage_backup_test_" + std::to_string(::getpid()));
        fs::remove_all(dir_);
        fs::create_directories(dir_);
        unsetenv("MC_SSD_BACKUP_ENABLE");
        unsetenv("MC_SSD_BACKUP_SAMPLE_KEYS");
    }
    void TearDown() override {
        unsetenv("MC_SSD_BACKUP_ENABLE");
        unsetenv("MC_SSD_BACKUP_SAMPLE_KEYS");
        fs::remove_all(dir_);
    }

    FileStorageConfig Config() {
        FileStorageConfig config = FileStorageConfig::FromEnvironment();
        config.storage_filepath = dir_.string();
        config.local_buffer_size = 1024 * 1024;
        config.storage_backend_type = StorageBackendType::kBucket;
        return config;
    }

    // Evidence of passing death-test children (their stderr is not kept):
    // appends `line` to $MC_TEST_TRACE_DIR/<test>.trace when that is set.
    static void Trace(const std::string& line) {
        const char* dir = std::getenv("MC_TEST_TRACE_DIR");
        if (dir == nullptr) return;
        const auto* info =
            ::testing::UnitTest::GetInstance()->current_test_info();
        const std::string name =
            info ? std::string(info->test_suite_name()) + "." + info->name()
                 : std::string("unknown");
        std::ofstream out(std::string(dir) + "/" + name + ".trace",
                          std::ios::app);
        out << ::getpid() << " " << line << "\n";
    }
    // ---- private access (FileStorage befriends this fixture) ----
    static bool BackupEnabledFromEnv() {
        return FileStorage::BackupConfig::FromEnvironment(1LL << 40, 1LL << 30)
            .enabled;
    }
    struct WorkerConfig {
        int64_t capacity_bytes, staging_bytes, interval_ms, scan_interval_ms;
        size_t min_buckets, max_buckets;
    };
    static WorkerConfig WorkerConfigFromEnv() {
        auto c =
            FileStorage::BackupConfig::FromEnvironment(1LL << 40, 1LL << 30);
        return {c.capacity_bytes,
                c.staging_bytes,
                static_cast<int64_t>(c.interval.count()),
                static_cast<int64_t>(c.scan_interval.count()),
                c.min_buckets_per_scan,
                c.max_buckets_per_scan};
    }
    static size_t SampleKeysFromEnv() {
        return FileStorage::BackupConfig::FromEnvironment(1LL << 40, 1LL << 30)
            .sample_keys;
    }
    static void SetStaging(FileStorage& fs,
                           std::shared_ptr<ClientBufferAllocator> staging) {
        fs.backup_staging_allocator_ = std::move(staging);
    }
    static tl::expected<std::shared_ptr<FileStorage::AllocatedBatch>, ErrorCode>
    AllocateStaging(FileStorage& fs, const std::vector<std::string>& keys,
                    const std::vector<int64_t>& sizes) {
        return fs.AllocateBatch(keys, sizes, *fs.backup_staging_allocator_);
    }
    static void Quarantine(FileStorage& fs,
                           std::shared_ptr<FileStorage::AllocatedBatch> b) {
        fs.QuarantineBackupStaging(std::move(b));
    }
    using AckRes = tl::expected<std::vector<uint32_t>, ErrorCode>;
    static AckRes Ack(const std::function<AckRes()>& notify, bool* unknown) {
        return FileStorage::AckBackupBucket(
            notify, 3, std::chrono::milliseconds(1), unknown);
    }
    // Reads `batch` from its source into staging, then returns the bytes
    // staged for `storage_key` ("" if not read).
    static std::string ReadBackupBytes(FileStorage& fs,
                                       const BackupBatch& batch,
                                       const std::string& storage_key) {
        auto prepared = fs.ReadBackupBatch(batch);
        auto it = prepared.batch_object.find(storage_key);
        if (it == prepared.batch_object.end() || it->second.empty()) return "";
        const auto& s = it->second.front();
        return std::string(static_cast<const char*>(s.ptr), s.size);
    }
    static tl::expected<void, ErrorCode> InitBackend(FileStorage& fs) {
        return fs.storage_backend_->Init();
    }
    static tl::expected<void, ErrorCode> StartWorker(FileStorage& fs) {
        return fs.StartBackupWorker();
    }
    static bool HasStaging(FileStorage& fs) {
        return fs.backup_staging_allocator_ != nullptr;
    }
    static bool WorkerRunning(FileStorage& fs) {
        return fs.backup_running_.load() || fs.backup_thread_.joinable();
    }
    static tl::expected<void, ErrorCode> RunHeartbeat(FileStorage& fs) {
        return fs.Heartbeat();
    }
    // Backup worker pieces driven directly (no worker thread).
    static void ConfigureBackup(FileStorage& fs, size_t sample_keys) {
        fs.backup_config_.enabled = true;
        fs.backup_config_.capacity_bytes = 1LL << 30;
        fs.backup_config_.sample_keys = sample_keys;
        fs.backup_config_.scan_interval = std::chrono::milliseconds(0);
        fs.next_backup_scan_ = {};  // a scan is due
    }
    static void RunBackupTick(FileStorage& fs) { fs.BackupTick(); }
    static void RunBatches(FileStorage& fs,
                           const std::vector<BackupBatch>& batches) {
        fs.RunBackupBatches(batches);
    }
    static void RunScan(FileStorage& fs) { fs.ScanPinnedBackups(); }
    // Drain latched (as DrainLocalDiskSegment does first).
    static void SetDrainingFlag(FileStorage& fs, bool v) { fs.draining_ = v; }
    // Round-6 test access. The stage hook is added to the red trees too
    // (same lines, same places), so these compile on every tree.
    static void SetStageHook(FileStorage& fs,
                             std::function<void(const char*, size_t)> hook) {
        fs.backup_stage_hook_for_test_ = std::move(hook);
    }
    static bool StopLatched(FileStorage& fs) { return fs.backup_stop_.load(); }
    static void SetAckObserver(
        FileStorage& fs, std::function<void(uint64_t, size_t, bool)> obs) {
        fs.backup_ack_observer_for_test_ = std::move(obs);
    }
    static bool DrainingFlag(FileStorage& fs) { return fs.draining_.load(); }
    static void SetReadChunking(FileStorage& fs, int64_t chunk_bytes,
                                int64_t query_bytes) {
        fs.backup_config_.read_chunk_bytes = chunk_bytes;
        fs.backup_config_.read_query_bytes = query_bytes;
    }
    static void* StagingBase(FileStorage& fs) {
        return fs.backup_staging_allocator_
                   ? fs.backup_staging_allocator_->getBase()
                   : nullptr;
    }
    static void SetStagingAlloc(
        FileStorage& fs,
        std::function<std::shared_ptr<ClientBufferAllocator>(size_t)> alloc) {
        fs.backup_staging_alloc_for_test_ = std::move(alloc);
    }
    // R6-NEW-BEGIN (the unregister seam exists only on the fixed code)
    static void SetStagingUnregister(
        FileStorage& fs,
        std::function<tl::expected<void, ErrorCode>(void*)> unregister) {
        fs.backup_staging_unregister_for_test_ = std::move(unregister);
    }
    // R6-NEW-END
    // Loop-2 round-1 test access (C1 timed quarantine, P2-02 staging wait,
    // P2-03 cap order). The red tree is the previous commit's product code
    // with this test file, where the GREEN-ONLY blocks are cut and the
    // "//R11-RED " lines enabled: those seams do not exist there, so they do
    // nothing and the stats read as empty.
    using Clock = std::chrono::steady_clock;
    static void SetBackupClock(FileStorage& fs,
                               std::function<Clock::time_point()> clock) {
        // R11-GREEN-ONLY-BEGIN
        fs.backup_clock_for_test_ = std::move(clock);
        // R11-GREEN-ONLY-END
        // R11-RED (void)fs; (void)clock;
    }
    static void ReleaseExpiredQuarantine(FileStorage& fs) {
        // R11-GREEN-ONLY-BEGIN
        fs.ReleaseExpiredBackupQuarantine();
        // R11-GREEN-ONLY-END
        // R11-RED (void)fs;
    }
    static void SetStagingTimes(FileStorage& fs, int64_t quarantine_ms,
                                int64_t wait_ms) {
        // R11-GREEN-ONLY-BEGIN
        fs.backup_config_.staging_quarantine =
            std::chrono::milliseconds(quarantine_ms);
        fs.backup_config_.staging_wait = std::chrono::milliseconds(wait_ms);
        // R11-GREEN-ONLY-END
        // R11-RED (void)fs; (void)quarantine_ms; (void)wait_ms;
    }
    struct StagingView {
        size_t quarantined_batches = 0;
        int64_t quarantined_bytes = -1;
        uint64_t quarantined_total = 0, released = 0, waits = 0, failed = 0;
    };
    static StagingView StagingStats(FileStorage& fs) {
        StagingView v;
        // R11-GREEN-ONLY-BEGIN
        const auto s = fs.GetBackupStagingStats();
        v = {s.quarantined_batches, s.quarantined_bytes, s.quarantined_total,
             s.quarantine_released, s.alloc_waits,       s.alloc_failed};
        // R11-GREEN-ONLY-END
        // R11-RED (void)fs;
        return v;
    }
    static uint64_t DemotedUnmapped(FileStorage& fs) {
        // R11-GREEN-ONLY-BEGIN
        return fs.GetBackupScanStats().demoted_unmapped;
        // R11-GREEN-ONLY-END
        // R11-RED (void)fs; return 0;
    }
    // R11-GREEN-ONLY-BEGIN
    static std::pair<int64_t, int64_t> StagingTimesFromEnv() {
        auto c =
            FileStorage::BackupConfig::FromEnvironment(1LL << 40, 1LL << 30);
        return {static_cast<int64_t>(c.staging_quarantine.count()),
                static_cast<int64_t>(c.staging_wait.count())};
    }
    // R11-GREEN-ONLY-END
    static void WakeWorker(FileStorage& fs) { fs.WakeBackupWorker(); }
    // Loop-2 round-1 access. Staging allocations currently held by read
    // batches (present on 1eab90b8 too).
    static size_t StagingHeld(FileStorage& fs) {
        std::lock_guard<std::mutex> l(fs.backup_wait_mutex_);
        return fs.backup_staging_held_;
    }
    // Observer of each quiesced backup transfer (new seam: a no-op on the
    // red tree, whose tests using it are stripped there).
    using TransferObs = std::function<void(
        const std::string&, const std::string&, ErrorCode, const Slice&)>;
    static void SetTransferObserver(FileStorage& fs, TransferObs obs) {
        // R12-GREEN-ONLY-BEGIN
        fs.backup_transfer_observer_for_test_ = std::move(obs);
        // R12-GREEN-ONLY-END
        // R12-RED (void)fs; (void)obs;
    }
    // Loop-2 round-12 access: staging lifetime events, and the registration
    // flag StartBackupWorker sets (for a staging installed by SetStaging).
    static void SetStagingEvents(
        FileStorage& fs,
        std::function<void(const char*, const void*, size_t)> hook) {
        fs.backup_staging_event_for_test_ = std::move(hook);
    }
    static void MarkStagingRegistered(FileStorage& fs) {
        fs.backup_staging_registered_ = true;
    }
    static void EnforceCap(FileStorage& fs) { fs.EnforceBackupPinCap(); }
    static void SetCapacity(FileStorage& fs, int64_t bytes) {
        fs.backup_config_.capacity_bytes = bytes;
    }
    static size_t ScanStateEntries(FileStorage& fs) {
        return fs.backup_scan_state_.size();
    }
    static std::pair<double, double> PinRatiosFromEnv() {
        const auto c = FileStorage::BackupConfig::FromEnvironment(0, 0);
        return {c.pin_high_ratio, c.pin_low_ratio};
    }
    static std::chrono::steady_clock::time_point NextScan(FileStorage& fs) {
        return fs.next_backup_scan_;
    }
    static void SetNextScan(FileStorage& fs,
                            std::chrono::steady_clock::time_point t) {
        fs.next_backup_scan_ = t;
    }
    static void SetScanInterval(FileStorage& fs,
                                std::chrono::milliseconds interval) {
        fs.backup_config_.scan_interval = interval;
    }
    static void ResetScanSchedule(FileStorage& fs) {
        for (auto& entry : fs.backup_scan_state_) entry.second.next_check = {};
        fs.next_backup_scan_ = {};
    }
    static void SetHeartbeatInterval(FileStorageConfig& c, int64_t seconds) {
        c.heartbeat_interval_seconds = seconds;
    }
    // R1-Q1: the backup worker paced at 1 byte/s. The thread's third write
    // waits about 1e6 s; destroying the FileStorage (stop) must end it.
    // Child-process body: exits 0 if destruction returns within 6 s.
    static void PacedWorkerStopsOnDestroy(const FileStorageConfig& cfg) {
        std::thread([] {
            std::this_thread::sleep_for(std::chrono::seconds(8));
            std::_Exit(1);  // destructor still blocked in the pacing wait
        }).detach();
        auto fs = std::make_unique<FileStorage>(cfg, nullptr, "127.0.0.1:1");
        fs->backup_config_.max_write_bytes_per_sec = 1;
        fs->backup_running_ = true;
        FileStorage* p = fs.get();
        fs->backup_thread_ = std::thread([p] {
            p->ThrottleBackupWrite(1);        // no wait; ready += 1 s
            p->ThrottleBackupWrite(1 << 20);  // waits 1 s; ready += ~1e6 s
            p->ThrottleBackupWrite(1);        // would wait ~1e6 s
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));
        const auto t0 = std::chrono::steady_clock::now();
        fs.reset();  // stop + join
        const auto took = std::chrono::steady_clock::now() - t0;
        std::_Exit(took < std::chrono::seconds(6) ? 0 : 1);
    }
    // R6-B1: the real pacing method and its stored deadline.
    static std::chrono::steady_clock::time_point WriteReady(FileStorage& fs) {
        return fs.backup_write_ready_;
    }
    static void SetWriteReady(FileStorage& fs,
                              std::chrono::steady_clock::time_point t) {
        fs.backup_write_ready_ = t;
    }
    static bool Throttle(FileStorage& fs, int64_t bytes) {
        return fs.ThrottleBackupWrite(bytes);
    }
    // R6-B1 child body: after a 10 GiB write at 1 B/s the next write must
    // still wait (a wrapped deadline would let it through at once), and stop
    // (destroy) or drain must end that wait promptly without writing.
    // 0 on success; 9 from the watchdog.
    static int SaturatedPacingWaitChild(const FileStorageConfig& cfg,
                                        bool destroy) {
        std::thread([] {
            std::this_thread::sleep_for(std::chrono::seconds(15));
            std::fprintf(stderr, "watchdog: saturated pacing wait\n");
            std::_Exit(9);
        }).detach();
        auto fs = std::make_unique<FileStorage>(cfg, nullptr, "127.0.0.1:1");
        SetWriteRate(*fs, 1);
        if (!fs->ThrottleBackupWrite(10LL << 30)) return 1;  // not delayed
        std::atomic<int> result{-1};
        fs->backup_running_ = true;
        FileStorage* p = fs.get();
        fs->backup_thread_ = std::thread(
            [p, &result] { result = p->ThrottleBackupWrite(1) ? 1 : 0; });
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        if (result.load() != -1) {
            std::fprintf(stderr, "next write did not wait: result=%d\n",
                         result.load());
            return 2;
        }
        const auto t0 = std::chrono::steady_clock::now();
        if (destroy) {
            fs.reset();  // stop + join
        } else {
            StartDrainAndWake(*fs);
            while (result.load() == -1 &&
                   std::chrono::steady_clock::now() - t0 <
                       std::chrono::seconds(3)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        }
        if (std::chrono::steady_clock::now() - t0 >= std::chrono::seconds(2)) {
            return 3;
        }
        if (result.load() != 0) return 4;  // must not write
        fs.reset();
        return 0;
    }
    // R3-NEW-BEGIN (helpers using hooks added in round 3; stripped from red)
    static void FailStagingAlloc(FileStorage& fs) {
        fs.backup_staging_alloc_for_test_ = [](size_t) {
            return std::shared_ptr<ClientBufferAllocator>();
        };
    }
    static void FailStagingRegister(FileStorage& fs, int* calls) {
        fs.backup_staging_register_for_test_ = [calls](void*, size_t) {
            ++*calls;
            return tl::expected<void, ErrorCode>(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
        };
    }
    static void StartDrainAndWake(FileStorage& fs) {
        fs.draining_.store(true);
        fs.WakeBackupWorker();
    }
    static void SetWriteRate(FileStorage& fs, int64_t bps) {
        fs.backup_config_.max_write_bytes_per_sec = bps;
    }
    // R3-NEW-END
    // Runs the real backup write (bucket write, NotifyBackupDone through the
    // FileStorage's client, rollback or keep) for `items`, as if they had
    // just been read from the source.
    static void WriteBackup(
        FileStorage& fs, const BackupBatch& batch,
        std::vector<std::pair<BackupTaskItem, std::string*>> items) {
        FileStorage::PreparedBackup prepared;
        prepared.batch = &batch;
        prepared.start = std::chrono::steady_clock::now();
        for (auto& [task, bytes] : items) {
            const auto sk = TenantId(task.tenant_id).MakeScopedKey(task.key);
            prepared.batch_object[sk] = {Slice{bytes->data(), bytes->size()}};
            prepared.task_by_storage_key.emplace(sk, task);
            prepared.bytes += static_cast<int64_t>(bytes->size());
        }
        fs.WriteBackupBatch(prepared);
    }
    static std::shared_ptr<BucketStorageBackend> Backend(FileStorage& fs) {
        return std::dynamic_pointer_cast<BucketStorageBackend>(
            fs.storage_backend_);
    }
    static std::shared_ptr<StorageBackendInterface> BackendPtr(
        FileStorage& fs) {
        return fs.storage_backend_;
    }
    // Bytes the holder serves for `storage_key` ("" if it cannot).
    static std::string HolderRead(FileStorage& fs,
                                  const std::string& storage_key, size_t n) {
        std::string out(n, '\0');
        std::unordered_map<std::string, Slice> read{
            {storage_key, Slice{out.data(), n}}};
        if (!Backend(fs)->BatchLoad(read)) return "";
        if (read[storage_key].ptr != out.data()) {
            out.assign(static_cast<const char*>(read[storage_key].ptr), n);
        }
        return out;
    }

    fs::path dir_;
};

// A client whose master RPC for backup registration is scripted by the test
// (here: delegated to an in-process MasterService, optionally with the reply
// dropped afterwards). Everything else is the real Client.
class ScriptedBackupClient : public Client {
   public:
    using NotifyFn =
        std::function<tl::expected<std::vector<uint32_t>, ErrorCode>(
            uint64_t, const std::vector<BackupTaskItem>&,
            const std::vector<StorageObjectMetadata>&)>;
    ScriptedBackupClient() : Client("127.0.0.1:17990", "P2PHANDSHAKE", "tcp") {}
    tl::expected<BackupHeartbeatResponse, ErrorCode> BackupObjectHeartbeat(
        const std::string&, int64_t used, int64_t capacity) override {
        ++heartbeats;
        last_used = used;
        last_capacity = capacity;
        return heartbeat ? heartbeat() : BackupHeartbeatResponse{};
    }
    tl::expected<std::vector<uint8_t>, ErrorCode> QueryBackupResidency(
        const std::vector<BackupRefreshItem>& keys) override {
        ++residency_calls;
        if (residency_hook) residency_hook();
        return std::vector<uint8_t>(
            keys.size(), static_cast<uint8_t>(BackupResidency::kInDram));
    }
    std::function<BackupHeartbeatResponse()> heartbeat;
    std::function<void()> residency_hook;
    std::atomic<int> heartbeats{0};
    std::atomic<int64_t> last_used{-1};
    std::atomic<int64_t> last_capacity{-1};
    std::atomic<int> residency_calls{0};
    tl::expected<std::vector<uint32_t>, ErrorCode> NotifyBackupDone(
        uint64_t batch_id, const std::vector<BackupTaskItem>& tasks,
        const std::vector<StorageObjectMetadata>& metadatas) override {
        ++calls;
        return notify ? notify(batch_id, tasks, metadatas)
                      : tl::expected<std::vector<uint32_t>, ErrorCode>{};
    }
    NotifyFn notify;
    int calls = 0;
};

// Holder H runs a real FileStorage (bucket backend) whose NotifyBackupDone
// goes to a real in-process MasterService. Sources A and C hold the DRAM
// copies; H is the only backup candidate.
class FileStorageBackupMasterTest : public FileStorageBackupTest {
   protected:
    static constexpr size_t kSize = 4096;
    void SetUp() override {
        FileStorageBackupTest::SetUp();
        setenv("MC_SSD_BACKUP_ENABLE", "1", 1);
        setenv("MC_SSD_BACKUP_BATCH_KEYS", "1", 1);
        MasterServiceConfig config;
        config.enable_offload = true;
        config.default_kv_lease_ttl = 0;
        master_ = std::make_unique<MasterService>(config);
        Mount(a_, "seg_a", 0x100000000);
        Mount(c_, "seg_c", 0x200000000);
        Mount(h_, "seg_h", 0x300000000);
        // Only H heartbeats for backups, so every batch goes to H.
        ASSERT_TRUE(master_->BackupObjectHeartbeat(h_, "seg_h", 0, 1 << 30)
                        .has_value());
        client_ = std::make_shared<ScriptedBackupClient>();
        client_->notify = [this](uint64_t id, const auto& tasks,
                                 const auto& metas) {
            return master_->NotifyBackupDone(h_, id, tasks, metas);
        };
        storage_ =
            std::make_unique<FileStorage>(Config(), client_, "127.0.0.1:17990");
        ASSERT_TRUE(InitBackend(*storage_).has_value());
    }
    void TearDown() override {
        storage_.reset();
        client_.reset();
        master_.reset();
        unsetenv("MC_SSD_BACKUP_BATCH_KEYS");
        FileStorageBackupTest::TearDown();
    }
    void Mount(const UUID& client, const std::string& name, size_t base) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = name;
        segment.base = base;
        segment.size = 64 * 1024 * 1024;
        segment.te_endpoint = name;
        segment.host_id = "host_" + name;
        segments_[name] = segment.id;
        ASSERT_TRUE(master_->MountSegment(segment, client).has_value());
        ASSERT_TRUE(master_->MountLocalDiskSegment(client, true).has_value());
        ASSERT_TRUE(master_->ReportSsdCapacity(client, 1 << 30).has_value());
    }
    // Put `key` with its DRAM copy on `seg`; returns the backup task H gets.
    BackupBatch Put(const UUID& owner, const std::string& seg,
                    const std::string& key) {
        ReplicateConfig rc;
        rc.replica_num = 1;
        rc.preferred_segment = seg;
        EXPECT_TRUE(
            master_->PutStart(owner, key, TenantId::Default(), kSize, rc)
                .has_value());
        EXPECT_TRUE(
            master_
                ->PutEnd(owner, key, TenantId::Default(), ReplicaType::MEMORY)
                .has_value());
        auto r = master_->BackupObjectHeartbeat(h_, "seg_h", 0, 1 << 30);
        EXPECT_TRUE(r.has_value());
        if (!r || r->batches.size() != 1) {
            ADD_FAILURE() << "expected one backup batch for " << key;
            return {};
        }
        return r->batches[0];
    }
    // Number of backup replicas of `key` the master holds on H.
    size_t MasterBackupsOnH(const std::string& key) {
        auto res =
            master_->BatchGetBackupReplicaList({key}, TenantId::Default());
        if (res.size() != 1 || !res[0]) return 0;
        size_t n = 0;
        for (const auto& d : res[0]->replicas) {
            if (d.is_local_disk_replica() &&
                d.get_local_disk_descriptor().client_id == h_) {
                ++n;
            }
        }
        return n;
    }
    static std::string Sk(const std::string& key) {
        return TenantId::Default().MakeScopedKey(key);
    }
    std::string Read(const std::string& key) {
        return HolderRead(*storage_, Sk(key), kSize);
    }
    void LoseSource(const UUID& owner, const std::string& seg) {
        ASSERT_TRUE(master_->UnmountSegment(segments_[seg], owner).has_value());
        ASSERT_TRUE(master_->UnmountLocalDiskSegment(owner).has_value());
    }

    UUID a_{0xa, 0xa};
    UUID c_{0xc, 0xc};
    UUID h_{0x7, 0x7};
    std::unordered_map<std::string, UUID> segments_;
    std::unique_ptr<MasterService> master_;
    std::shared_ptr<ScriptedBackupClient> client_;
    std::unique_ptr<FileStorage> storage_;
};

// CFG-01: with MC_SSD_BACKUP_ENABLE unset the worker starts nothing: no
// staging, no thread, no backup RPC; primary offload works as before.
TEST_F(FileStorageBackupTest, DisabledWorkerDoesNothing) {
    auto client = std::make_shared<ScriptedBackupClient>();
    FileStorage storage(Config(), client, "127.0.0.1:17990");
    ASSERT_TRUE(InitBackend(storage).has_value());
    ASSERT_TRUE(StartWorker(storage).has_value());
    EXPECT_FALSE(HasStaging(storage));
    EXPECT_FALSE(WorkerRunning(storage));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(client->heartbeats.load(), 0);
    EXPECT_EQ(client->calls, 0);
    std::string v(4096, 'p');
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"p0", {Slice{v.data(), v.size()}}}};
    bool committed = false;
    ASSERT_TRUE(Backend(storage)
                    ->BatchOffload(batch,
                                   [&](const std::vector<std::string>& keys,
                                       std::vector<StorageObjectMetadata>&) {
                                       committed = keys.size() == 1;
                                       return ErrorCode::OK;
                                   })
                    .has_value());
    EXPECT_TRUE(committed);
    EXPECT_EQ(Backend(storage)->GetBackupUsage().total_bytes, 0);
}

// R1-Q1 / CLI-15 / CLI-16: a backup write paced by MC_SSD_BACKUP_MAX_WRITE_BPS
// must not hold up shutdown. Before the fix the pacing was a plain sleep and
// the destructor's join waited for it (here about 1e6 s).
TEST_F(FileStorageBackupTest, PacedWriteStopsPromptlyOnDestroy) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const auto cfg = Config();
    EXPECT_EXIT(PacedWorkerStopsOnDestroy(cfg), ::testing::ExitedWithCode(0),
                "");
}

// R6-B1: the next pacing deadline is the previous one plus bytes/rate, kept
// exact while the clock can represent it and saturated at the clock's
// maximum beyond that; it never wraps (a wrapped deadline lies in the past
// and ends the pacing). Drain is set for the cases that start from a future
// deadline, so the call returns at once after updating it.
TEST_F(FileStorageBackupTest, PacingDeadlineNeverWraps) {
    using Clock = std::chrono::steady_clock;
    static_assert(std::is_same_v<Clock::period, std::nano>);
    const auto kMax = Clock::time_point::max();
    FileStorage fs(Config(), nullptr, "127.0.0.1:1");
    const auto expect = [&](Clock::time_point from, int64_t bytes,
                            int64_t rate) {
        const __int128 ns =
            static_cast<__int128>(from.time_since_epoch().count()) +
            static_cast<__int128>(bytes) * 1000000000 / rate;
        return ns >= static_cast<__int128>(kMax.time_since_epoch().count())
                   ? kMax
                   : Clock::time_point(
                         Clock::duration(static_cast<int64_t>(ns)));
    };
    // 10 GiB at 1 B/s from now: this write is not delayed, the next
    // deadline saturates.
    SetWriteRate(fs, 1);
    SetWriteReady(fs, {});
    EXPECT_TRUE(Throttle(fs, 10LL << 30));
    EXPECT_TRUE(WriteReady(fs) == kMax) << "10 GiB at 1 B/s: deadline wrapped";

    SetDrainingFlag(fs, true);
    const auto soon = Clock::now() + std::chrono::hours(1);
    const auto near_max = kMax - std::chrono::seconds(10);
    struct Case {
        const char* name;
        Clock::time_point from;
        int64_t bytes, rate;
    };
    const Case cases[] = {
        {"one byte at 1 B/s", soon, 1, 1},
        {"accumulates onto a future deadline", soon, 1 << 20, 1 << 20},
        {"just below the clock maximum", near_max, 9, 1},
        {"exactly the clock maximum", near_max, 10, 1},
        {"one second past the maximum", near_max, 11, 1},
        {"debt fits in ns, the sum does not", soon, 9223372036LL, 1},
        {"debt beyond the ns range", soon, 9223372037LL, 1},
        {"bytes x 1e6 beyond int64 at a large rate", soon, INT64_MAX,
         3000000000LL},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(c.name);
        SetWriteRate(fs, c.rate);
        SetWriteReady(fs, c.from);
        EXPECT_FALSE(Throttle(fs, c.bytes));  // drained: no write
        const auto got = WriteReady(fs);
        EXPECT_TRUE(got >= c.from) << "deadline moved backwards";
        EXPECT_TRUE(got == expect(c.from, c.bytes, c.rate))
            << "got " << got.time_since_epoch().count() << " ns, expected "
            << expect(c.from, c.bytes, c.rate).time_since_epoch().count();
    }
    SetDrainingFlag(fs, false);
}

TEST_F(FileStorageBackupTest, SaturatedPacingWaitEndsOnDestroy) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const auto cfg = Config();
    EXPECT_EXIT(std::_Exit(SaturatedPacingWaitChild(cfg, /*destroy=*/true)),
                ::testing::ExitedWithCode(0), "");
}

TEST_F(FileStorageBackupTest, SaturatedPacingWaitEndsOnDrain) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const auto cfg = Config();
    EXPECT_EXIT(std::_Exit(SaturatedPacingWaitChild(cfg, /*destroy=*/false)),
                ::testing::ExitedWithCode(0), "");
}

// CFG-06 / CFG-08: enabled but unusable (non-bucket backend, zero capacity
// or staging, staging that cannot be allocated): StartBackupWorker fails,
// leaves no staging and no thread, sends no backup RPC, and the primary
// storage stays usable (Init only logs and sets worker_up=0).
TEST_F(FileStorageBackupTest, UnusableBackupConfigLeavesStorageUsable) {
    setenv("MC_SSD_BACKUP_ENABLE", "1", 1);
    struct Case {
        const char* name;
        StorageBackendType backend;
        const char* var;
        const char* value;
        ErrorCode expect;
    };
    const Case cases[] = {
        {"file_per_key", StorageBackendType::kFilePerKey, nullptr, nullptr,
         ErrorCode::NOT_SUPPORTED},
        {"zero_capacity", StorageBackendType::kBucket,
         "MC_SSD_BACKUP_CAPACITY_BYTES", "0", ErrorCode::INVALID_PARAMS},
        {"zero_staging", StorageBackendType::kBucket,
         "MC_SSD_BACKUP_STAGING_BYTES", "0", ErrorCode::INVALID_PARAMS},
        {"staging_alloc_fails", StorageBackendType::kBucket,
         "MC_SSD_BACKUP_STAGING_BYTES", "1125899906842624",  // 1 PiB
         ErrorCode::BUFFER_OVERFLOW},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(c.name);
        if (c.var) setenv(c.var, c.value, 1);
        auto client = std::make_shared<ScriptedBackupClient>();
        auto config = Config();
        config.storage_backend_type = c.backend;
        config.storage_filepath = (dir_ / c.name).string();
        fs::create_directories(config.storage_filepath);
        FileStorage storage(config, client, "127.0.0.1:17990");
        ASSERT_TRUE(InitBackend(storage).has_value());
        auto started = StartWorker(storage);
        ASSERT_FALSE(started.has_value());
        EXPECT_EQ(started.error(), c.expect);
        EXPECT_FALSE(HasStaging(storage));
        EXPECT_FALSE(WorkerRunning(storage));
        EXPECT_EQ(client->heartbeats.load(), 0);
        if (c.backend == StorageBackendType::kBucket) {
            std::string v(4096, 'u');
            std::unordered_map<std::string, std::vector<Slice>> batch{
                {std::string("u_") + c.name, {Slice{v.data(), v.size()}}}};
            EXPECT_TRUE(
                Backend(storage)
                    ->BatchOffload(batch,
                                   [](const std::vector<std::string>&,
                                      std::vector<StorageObjectMetadata>&) {
                                       return ErrorCode::OK;
                                   })
                    .has_value());
        }
        if (c.var) unsetenv(c.var);
    }
    unsetenv("MC_SSD_BACKUP_ENABLE");
}

// R0-B1: K/v1 is put on A; its backup task is delayed. K is removed and put
// again (v2) on C, and v2's backup lands and registers on H. Then A's late
// v1 task reaches H, reads the current bytes, and the master refuses it as
// stale. H must still serve v2 (the master keeps advertising it), also after
// C is gone.
TEST_F(FileStorageBackupMasterTest, LateStaleTaskKeepsNewerBackup) {
    const BackupBatch v1 = Put(a_, "seg_a", "k");
    ASSERT_EQ(v1.tasks.size(), 1u);
    ASSERT_TRUE(master_->Remove("k", TenantId::Default(), true).has_value());
    const BackupBatch v2 = Put(c_, "seg_c", "k");
    ASSERT_EQ(v2.tasks.size(), 1u);
    ASSERT_NE(v1.tasks[0].version, v2.tasks[0].version);
    std::string v2_bytes(kSize, 'B');
    WriteBackup(*storage_, v2, {{v2.tasks[0], &v2_bytes}});
    ASSERT_EQ(MasterBackupsOnH("k"), 1u);
    ASSERT_EQ(Read("k"), v2_bytes);

    // The late v1 task reads what is there now (v2's bytes).
    std::string current = v2_bytes;
    WriteBackup(*storage_, v1, {{v1.tasks[0], &current}});
    EXPECT_EQ(MasterBackupsOnH("k"), 1u);
    EXPECT_EQ(Read("k"), v2_bytes);

    LoseSource(c_, "seg_c");
    auto list = master_->GetReplicaList("k", TenantId::Default());
    ASSERT_TRUE(list.has_value());
    ASSERT_EQ(list->replicas.size(), 1u);
    EXPECT_EQ(list->replicas[0].get_local_disk_descriptor().client_id, h_);
    EXPECT_EQ(Read("k"), v2_bytes);
}

// The same version written twice (e.g. a batch re-dispatched after a
// timeout) stays registered once and readable.
TEST_F(FileStorageBackupMasterTest, EqualVersionRewriteStaysReadable) {
    const BackupBatch b = Put(a_, "seg_a", "k");
    ASSERT_EQ(b.tasks.size(), 1u);
    std::string bytes(kSize, 'E');
    WriteBackup(*storage_, b, {{b.tasks[0], &bytes}});
    WriteBackup(*storage_, b, {{b.tasks[0], &bytes}});
    EXPECT_EQ(MasterBackupsOnH("k"), 1u);
    EXPECT_EQ(Read("k"), bytes);
}

// One batch with an accepted key and a refused (stale) new key: only the
// accepted one is kept and registered.
TEST_F(FileStorageBackupMasterTest, MixedAcceptedAndStaleKeys) {
    const BackupBatch good = Put(a_, "seg_a", "good");
    const BackupBatch old = Put(a_, "seg_a", "old");
    ASSERT_EQ(good.tasks.size(), 1u);
    ASSERT_EQ(old.tasks.size(), 1u);
    BackupTaskItem stale = old.tasks[0];
    stale.version += 1;  // re-put since the task was formed
    std::string g(kSize, 'g'), o(kSize, 'o');
    WriteBackup(*storage_, good, {{good.tasks[0], &g}, {stale, &o}});
    EXPECT_EQ(MasterBackupsOnH("good"), 1u);
    EXPECT_EQ(Read("good"), g);
    EXPECT_EQ(MasterBackupsOnH("old"), 0u);
    EXPECT_EQ(Read("old"), "");
}

// A primary write of the key while the late stale task is being registered
// is deferred (the key is reserved), never overwritten by the restore, and
// succeeds once the registration is resolved.
TEST_F(FileStorageBackupMasterTest, PrimaryWriteDuringStaleResolution) {
    const BackupBatch v1 = Put(a_, "seg_a", "k");
    ASSERT_TRUE(master_->Remove("k", TenantId::Default(), true).has_value());
    const BackupBatch v2 = Put(c_, "seg_c", "k");
    std::string v2_bytes(kSize, 'B');
    WriteBackup(*storage_, v2, {{v2.tasks[0], &v2_bytes}});
    std::string primary(kSize, 'P');
    std::unordered_map<std::string, std::vector<Slice>> primary_batch{
        {Sk("k"), {Slice{primary.data(), primary.size()}}}};
    bool primary_committed = false;
    auto primary_ok = [&](const std::vector<std::string>& keys,
                          std::vector<StorageObjectMetadata>&) {
        primary_committed = !keys.empty();
        return ErrorCode::OK;
    };
    client_->notify = [&](uint64_t id, const auto& tasks, const auto& metas) {
        // Registration in progress: a concurrent primary write of the key.
        EXPECT_TRUE(Backend(*storage_)
                        ->BatchOffload(primary_batch, primary_ok)
                        .has_value());
        return master_->NotifyBackupDone(h_, id, tasks, metas);
    };
    std::string current = v2_bytes;
    WriteBackup(*storage_, v1, {{v1.tasks[0], &current}});
    EXPECT_FALSE(primary_committed);  // deferred while the key was reserved
    EXPECT_EQ(Read("k"), v2_bytes);
    EXPECT_EQ(MasterBackupsOnH("k"), 1u);
    // Retried after resolution: the primary write takes over.
    ASSERT_TRUE(Backend(*storage_)
                    ->BatchOffload(primary_batch, primary_ok)
                    .has_value());
    EXPECT_TRUE(primary_committed);
    EXPECT_EQ(Read("k"), primary);
}

// R0-M1 (F05): the master registered the backup but the reply was lost.
// The holder must keep the bytes the master advertises: whether a resend
// gets through (first case) or every reply is lost (second case).
TEST_F(FileStorageBackupMasterTest, CommittedRegistrationWithLostReplyIsKept) {
    for (const int lost : {1, 100}) {
        SCOPED_TRACE(lost);
        const std::string key = "lost" + std::to_string(lost);
        const BackupBatch b = Put(a_, "seg_a", key);
        ASSERT_EQ(b.tasks.size(), 1u);
        int replies = 0;
        client_->notify = [&](uint64_t id, const auto& tasks, const auto& metas)
            -> tl::expected<std::vector<uint32_t>, ErrorCode> {
            auto res = master_->NotifyBackupDone(h_, id, tasks, metas);
            if (!tasks.empty() && ++replies <= lost) {
                return tl::make_unexpected(ErrorCode::RPC_TIMEOUT);
            }
            return res;
        };
        std::string bytes(kSize, 'L');
        WriteBackup(*storage_, b, {{b.tasks[0], &bytes}});
        EXPECT_EQ(MasterBackupsOnH(key), 1u);
        EXPECT_EQ(Read(key), bytes);
    }
    LoseSource(a_, "seg_a");
    for (const char* key : {"lost1", "lost100"}) {
        SCOPED_TRACE(key);
        EXPECT_EQ(Read(key), std::string(kSize, 'L'));
        auto list = master_->GetReplicaList(key, TenantId::Default());
        ASSERT_TRUE(list.has_value());
        ASSERT_EQ(list->replicas.size(), 1u);
        ASSERT_TRUE(list->replicas[0].is_local_disk_replica());
        EXPECT_EQ(list->replicas[0].get_local_disk_descriptor().client_id, h_);
    }
}

// Writes one pinned backup bucket of `n` small keys "<prefix><i>".
static void WritePinnedBackupBucket(BucketStorageBackend& backend,
                                    const std::string& prefix, int n,
                                    std::vector<std::string>& data) {
    std::unordered_map<std::string, std::vector<Slice>> batch;
    data.reserve(data.size() + n);
    for (int i = 0; i < n; ++i) {
        data.push_back(std::string(64, static_cast<char>('a' + i % 26)));
        batch[TenantId::Default().MakeScopedKey(prefix + std::to_string(i))] = {
            Slice{data.back().data(), data.back().size()}};
    }
    ASSERT_TRUE(
        backend
            .BatchOffloadBackup(batch,
                                [](const std::vector<std::string>&,
                                   std::vector<StorageObjectMetadata>&) {
                                    return ErrorCode::OK;
                                })
            .has_value());
}

// R3-B1: a heartbeat whose reply arrives after a drain (or stop) was latched
// must not start new work. Before the fix BackupTick went on to read the
// returned batches and then ran the pinned-backup scan (a residency query).
TEST_F(FileStorageBackupTest, HeartbeatAfterDrainStartsNoScan) {
    auto client = std::make_shared<ScriptedBackupClient>();
    FileStorage storage(Config(), client, "127.0.0.1:17990");
    ASSERT_TRUE(InitBackend(storage).has_value());
    std::vector<std::string> data;
    WritePinnedBackupBucket(*Backend(storage), "hb_", 8, data);
    ConfigureBackup(storage, 8);
    RunScan(storage);  // first sighting: the bucket's first check is due now
    const int before = client->residency_calls.load();
    client->heartbeat = [&] {
        SetDrainingFlag(storage, true);  // drain latched during the RPC
        return BackupHeartbeatResponse{};
    };
    RunBackupTick(storage);
    EXPECT_EQ(client->heartbeats.load(), 1);
    EXPECT_EQ(client->residency_calls.load(), before);  // no scan admitted
    SetDrainingFlag(storage, false);
}

// R4-M1: a real holder (own Client, own host id) and a real source Client
// against an in-process master with SSD backup on. The holder's FileStorage
// worker steps (BackupTick, RunBackupBatches) are driven directly, with real
// heartbeats, source queries, transfers, disk writes and ACKs.
class FileStorageBackupHolderTest : public FileStorageBackupTest {
   protected:
    static constexpr size_t kSegment = 16 << 20;
    static constexpr size_t kValue = 4096;
    void SetUp() override {
        FileStorageBackupTest::SetUp();
        // Read by the master at construction: backup on, one key per batch,
        // and an inflight limit that holds one 4096-byte batch only.
        setenv("MC_SSD_BACKUP_ENABLE", "1", 1);
        setenv("MC_SSD_BACKUP_BATCH_KEYS", batch_keys_, 1);
        setenv("MC_SSD_BACKUP_MAX_INFLIGHT_BYTES", max_inflight_, 1);
        auto builder = InProcMasterConfigBuilder()
                           .set_enable_offload(true)
                           .set_default_kv_lease_ttl(lease_ttl_ms_)
                           .set_root_fs_dir("");
        if (multi_tenant_) {
            // Registered tenants "default" and kTenant (file policy).
            policy_ = dir_ / "tenants.yaml";
            TenantQuotaPolicySnapshot policy;
            policy.tenant_quotas = {
                {std::string(TenantId::kDefaultValue), 256 * 1024 * 1024},
                {kTenant, 256 * 1024 * 1024},
            };
            std::ofstream out(policy_);
            ASSERT_TRUE(out.is_open());
            out << FormatTenantQuotaPolicyYaml(policy);
            out.close();
            builder.set_enable_multi_tenants(true)
                .set_tenant_quota_connector_uri(policy_.string());
        }
        ASSERT_TRUE(master_.Start(builder.build()));
        src_ = MakeClient("r4m1-src", src_ep_, src_seg_, src_alloc_);
        holder_ =
            MakeClient("r4m1-holder", holder_ep_, holder_seg_, holder_alloc_);
        ASSERT_TRUE(src_ && holder_);
        if (multi_tenant_) {
            // A second writer, in kTenant. It puts its objects into src_'s
            // segment, so src_ is the DRAM owner (the backup source) of
            // both tenants' objects.
            srct_ = MakeClient("r8-tenant", srct_ep_, srct_seg_, srct_alloc_,
                               kTenant);
            ASSERT_TRUE(srct_);
        }
        // The master queues a backup only together with the primary SSD
        // offload, so the source needs a local disk too.
        ASSERT_TRUE(src_->MountLocalDiskSegment(true).has_value());
        ASSERT_TRUE(holder_->MountLocalDiskSegment(true).has_value());
        auto config = Config();
        storage_ = std::make_unique<FileStorage>(config, holder_, holder_ep_);
        ASSERT_TRUE(InitBackend(*storage_).has_value());
        staging_ = AlignedClientBufferAllocator::create(4 << 20, "tcp");
        ASSERT_TRUE(staging_);
        ASSERT_TRUE(holder_
                        ->RegisterLocalMemory(staging_->getBase(), 4 << 20,
                                              kWildcardLocation, false, true)
                        .has_value());
        SetStaging(*storage_, staging_);
        ConfigureBackup(*storage_, 32);
        RunBackupTick(*storage_);  // holder becomes a backup candidate
    }
    void TearDown() override {
        storage_.reset();
        if (holder_ && staging_) {
            (void)holder_->unregisterLocalMemory(staging_->getBase());
        }
        staging_.reset();
        for (auto* c : {&src_, &holder_}) {
            if (*c)
                (void)(*c)->UnmountSegment(*c == src_ ? src_seg_ : holder_seg_,
                                           kSegment);
        }
        if (srct_) (void)srct_->UnmountSegment(srct_seg_, kSegment);
        src_.reset();
        holder_.reset();
        srct_.reset();
        if (src_seg_) free_memory("", src_seg_);
        if (holder_seg_) free_memory("", holder_seg_);
        if (srct_seg_) free_memory("", srct_seg_);
        master_.Stop();
        unsetenv("MC_SSD_BACKUP_ENABLE");
        unsetenv("MC_SSD_BACKUP_BATCH_KEYS");
        unsetenv("MC_SSD_BACKUP_MAX_INFLIGHT_BYTES");
        FileStorageBackupTest::TearDown();
    }
    std::shared_ptr<Client> MakeClient(const std::string& host, std::string& ep,
                                       void*& seg,
                                       std::unique_ptr<SimpleAllocator>& alloc,
                                       const std::string& tenant = "default") {
        setenv("MOONCAKE_HOST_ID", host.c_str(), 1);
        ep = UniqueLocalEndpoint();
        auto created =
            Client::Create(ep, master_.metadata_url(), "tcp", std::nullopt,
                           master_.master_address(), nullptr, {}, tenant);
        unsetenv("MOONCAKE_HOST_ID");
        if (!created) return nullptr;
        auto c = *created;
        seg = allocate_buffer_allocator_memory(kSegment);
        if (!seg || !c->MountSegment(seg, kSegment, "tcp")) return nullptr;
        alloc = std::make_unique<SimpleAllocator>(kSegment);
        if (!c->RegisterLocalMemory(alloc->getBase(), kSegment, "cpu:0", false,
                                    false)) {
            return nullptr;
        }
        return c;
    }
    // Put on the source; its DRAM copy is placed in the source's segment.
    bool PutOnSource(const std::string& key, char fill) {
        return PutAs(*src_, *src_alloc_, key, fill, kValue);
    }
    // Put through `writer` (its tenant), with the DRAM copy in src_'s
    // segment; then complete src_'s primary offload tasks.
    bool PutAs(Client& writer, SimpleAllocator& alloc, const std::string& key,
               char fill, size_t size) {
        // The client buffer is a slab pool: every allocation uses one size
        // class (kValue) and the Put sends the first `size` bytes of it.
        const size_t cap = std::max(size, kValue);
        void* buf = alloc.allocate(cap);
        if (!buf) return false;
        std::memset(buf, fill, size);
        std::vector<Slice> slices{{buf, size}};
        ReplicateConfig rc;
        rc.replica_num = 1;
        rc.preferred_segment = src_ep_;
        const bool ok = writer.Put(key, slices, rc).has_value();
        alloc.deallocate(buf, cap);
        if (!ok) return false;
        // Complete the source's primary offload task (as its FileStorage
        // would), so no pending task holds the object.
        std::vector<OffloadTaskItem> tasks;
        if (!src_->OffloadObjectHeartbeat(true, tasks)) return false;
        if (tasks.empty()) return true;
        std::vector<StorageObjectMetadata> metas(tasks.size());
        for (size_t i = 0; i < tasks.size(); ++i) {
            metas[i].data_size = tasks[i].size;
            metas[i].transport_endpoint = src_ep_;
        }
        return src_->NotifyOffloadSuccess(tasks, metas).has_value();
    }
    std::string HolderBytes(const std::string& key) {
        return HolderRead(*storage_, TenantId::Default().MakeScopedKey(key),
                          kValue);
    }
    // Bytes of the holder's local backup of `key` in `tenant`, read through
    // a backend (valid after the FileStorage is gone).
    static std::string BackendBytes(BucketStorageBackend& backend,
                                    const std::string& tenant,
                                    const std::string& key, size_t n) {
        const auto sk = TenantId(tenant).MakeScopedKey(key);
        std::string out(n, '\0');
        std::unordered_map<std::string, Slice> read{{sk, Slice{out.data(), n}}};
        if (!backend.BatchLoad(read)) return "";
        if (read[sk].ptr != out.data()) {
            out.assign(static_cast<const char*>(read[sk].ptr), n);
        }
        return out;
    }
    std::optional<SsdBackupCoordinator::ClientSnapshot> HolderReservation() {
        return master_.service()
            ->GetMasterServiceForTest()
            .GetSsdBackupClientSnapshotForTest(holder_->getClientId());
    }
    // One observed backup transfer, copied out of the staging while owned.
    struct Transfer {
        std::string tenant, key;
        ErrorCode result = ErrorCode::OK;
        std::string bytes;
        const char* ptr = nullptr;
    };
    static Transfer CopyTransfer(const std::string& t, const std::string& k,
                                 ErrorCode e, const Slice& s) {
        return {t, k, e, std::string(static_cast<const char*>(s.ptr), s.size),
                static_cast<const char*>(s.ptr)};
    }
    // Staging almost fully allocatable again (4 MiB arena, 1 MiB per
    // allocation at most): nothing of a finished batch is still held.
    bool StagingFree() {
        auto probe =
            AllocateStaging(*storage_, {"p0", "p1", "p2", "p3"},
                            {1 << 20, 1 << 20, 1 << 20, (1 << 20) - 4096});
        return probe.has_value();
    }
    // Runs worker ticks until `done()` or `ticks` ticks have run.
    bool TickUntil(const std::function<bool()>& done, int ticks = 8) {
        for (int i = 0; i < ticks && !done(); ++i) RunBackupTick(*storage_);
        return done();
    }

    // Worker stages seen through the stage hook, with waits on counts.
    struct StageLog {
        std::mutex mu;
        std::condition_variable cv;
        std::map<std::string, int> count;
        void Hit(const char* stage) {
            std::lock_guard<std::mutex> l(mu);
            ++count[stage];
            cv.notify_all();
        }
        int Get(const std::string& stage) {
            std::lock_guard<std::mutex> l(mu);
            return count[stage];
        }
        bool WaitFor(const std::string& stage, int n) {
            std::unique_lock<std::mutex> l(mu);
            return cv.wait_for(l, std::chrono::seconds(10),
                               [&] { return count[stage] >= n; });
        }
    };
    // Staging lifetime events seen through the staging event seam, in order.
    struct StagingLog {
        struct Event {
            std::string what;
            const char* ptr;
            size_t bytes;
        };
        std::mutex mu;
        std::vector<Event> events;
        void Add(const char* what, const void* ptr, size_t bytes) {
            std::lock_guard<std::mutex> l(mu);
            events.push_back({what, static_cast<const char*>(ptr), bytes});
        }
        std::vector<Event> Get() {
            std::lock_guard<std::mutex> l(mu);
            return events;
        }
        // Index of the first `what` event (at `ptr` unless null), or -1.
        static int Find(const std::vector<Event>& ev, const std::string& what,
                        const char* ptr = nullptr) {
            for (size_t i = 0; i < ev.size(); ++i) {
                if (ev[i].what == what && (!ptr || ev[i].ptr == ptr)) {
                    return static_cast<int>(i);
                }
            }
            return -1;
        }
        static int Count(const std::vector<Event>& ev, const std::string& what,
                         const char* ptr = nullptr) {
            return static_cast<int>(
                std::count_if(ev.begin(), ev.end(), [&](const Event& e) {
                    return e.what == what && (!ptr || e.ptr == ptr);
                }));
        }
        // "what@offset+bytes ..." with offsets from `base`.
        static std::string Format(const std::vector<Event>& ev,
                                  const void* base) {
            std::string s;
            for (const auto& e : ev) {
                s += e.what + "@" +
                     std::to_string(e.ptr - static_cast<const char*>(base)) +
                     "+" + std::to_string(e.bytes) + " ";
            }
            return s;
        }
    };
    StagingLog slog_;
    void LogStagingEvents() {
        SetStagingEvents(*storage_, [this](const char* w, const void* p,
                                           size_t n) { slog_.Add(w, p, n); });
    }
    // Process-wide transfer faults while in scope.
    struct FaultScope {
        explicit FaultScope(std::shared_ptr<const TransferFaultsForTest> f) {
            SetTransferFaultsForTest(std::move(f));
        }
        ~FaultScope() { SetTransferFaultsForTest(nullptr); }
        FaultScope(const FaultScope&) = delete;
        FaultScope& operator=(const FaultScope&) = delete;
    };
    // A fault taken once, by the first transfer after the worker's
    // "before_transfer" stage (the backup read; nothing else transfers then).
    struct OneShot {
        std::atomic<bool> armed{false}, taken{false};
        bool Take() { return armed.load() && !taken.exchange(true); }
    };
    // Waits (5 s at most) until the bytes at `p` equal `want`: a transfer
    // left running lands late.
    static bool LandsIn(const char* p, const std::string& want) {
        const auto until = Clock::now() + std::chrono::seconds(5);
        for (;;) {
            if (std::memcmp(p, want.data(), want.size()) == 0) return true;
            if (Clock::now() >= until) return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
    // MG-1, through the real BackupTick: a backup read of k1 ('b') into a
    // staging poisoned with 'P', with one fault of `faults` taken at its
    // transfer. The read must report its transfer unquiesced: its allocation
    // is quarantined (never held, not freed), k1 is not stored, and one
    // empty ACK releases the batch's reservation. Sets `*quarantined` to the
    // quarantined allocation.
    // The events go to slog_ (it outlives the FileStorage).
    void FaultedReadIsQuarantined(std::shared_ptr<TransferFaultsForTest> faults,
                                  OneShot& shot, const char** quarantined) {
        *quarantined = nullptr;
        ASSERT_TRUE(PutOnSource("k0", 'a'));
        RunBackupTick(*storage_);
        ASSERT_EQ(HolderBytes("k0"), std::string(kValue, 'a'));
        ASSERT_TRUE(PutOnSource("k1", 'b'));
        std::memset(staging_->getBase(), 'P', staging_->size());
        LogStagingEvents();
        SetStageHook(*storage_, [&shot](const char* stage, size_t) {
            if (std::string(stage) == "before_transfer") shot.armed = true;
        });
        std::mutex mu;
        std::vector<std::tuple<uint64_t, size_t, bool>> acks;
        SetAckObserver(*storage_, [&](uint64_t id, size_t n, bool ok) {
            std::lock_guard<std::mutex> l(mu);
            acks.emplace_back(id, n, ok);
        });
        {
            FaultScope scope(faults);
            RunBackupTick(*storage_);
        }
        SetStageHook(*storage_, nullptr);
        SetAckObserver(*storage_, nullptr);
        ASSERT_TRUE(shot.taken.load()) << "fault not taken";
        const auto ev = slog_.Get();
        const auto trace = StagingLog::Format(ev, staging_->getBase());
        Trace("events after the faulted read: " + trace);
        ASSERT_EQ(StagingLog::Count(ev, "quarantine"), 1) << trace;
        const auto& q = ev[StagingLog::Find(ev, "quarantine")];
        EXPECT_EQ(q.bytes, kValue) << trace;
        EXPECT_EQ(StagingLog::Count(ev, "alloc", q.ptr), 1) << trace;
        EXPECT_EQ(StagingLog::Count(ev, "hold"), 0) << trace;
        EXPECT_EQ(StagingLog::Count(ev, "free"), 0) << trace;
        EXPECT_EQ(StagingStats(*storage_).quarantined_batches, 1u);
        EXPECT_EQ(HolderBytes("k1"), "");  // nothing of k1 registered
        EXPECT_EQ(HolderBytes("k0"), std::string(kValue, 'a'));
        {
            std::lock_guard<std::mutex> l(mu);
            ASSERT_EQ(acks.size(), 1u);
            EXPECT_EQ(std::get<1>(acks[0]), 0u);  // empty ACK
            EXPECT_TRUE(std::get<2>(acks[0]));
        }
        auto snap = HolderReservation();
        ASSERT_TRUE(snap.has_value());
        EXPECT_TRUE(snap->inflight_batches.empty());
        *quarantined = q.ptr;
    }
    static void StartWatchdog(const char* what) {
        std::thread([what] {
            std::this_thread::sleep_for(std::chrono::seconds(40));
            std::fprintf(stderr, "watchdog: %s did not finish\n", what);
            std::_Exit(9);
        }).detach();
    }
    static int Fail(int code, const std::string& why) {
        std::fprintf(stderr, "check %d failed: %s\n", code, why.c_str());
        last_fail_ = code;
        return code;
    }
    // Code of the last check that failed (T16 old-exit control).
    static inline std::atomic<int> last_fail_{0};
    // Fail before any worker thread was started (nothing to join).
    static int Join(int code, const std::string& why) {
        worker_joined_ = true;
        return Fail(code, why);
    }

    // R5-M1: a running backup worker receives a real nonempty heartbeat
    // response and is held right after the RPC returns (stage "heartbeat").
    // While it is held, the test either calls the real DrainLocalDiskSegment
    // (destroy == false) or destroys the FileStorage (destroy == true; the
    // response is released once the destructor has latched stop). After the
    // release the worker must admit no source query and no transfer for the
    // returned batch: the source keeps no read lease on k1 (Remove
    // succeeds), the holder stores nothing for k1 and keeps k0. Child body:
    // 0 on success; 9 from the watchdog.
    // R2-Q1: `inject` 1-6 takes an early failure exit after the worker
    // start (1: put k1, 2: no held response, 3: the reservation check; drain
    // only: 4 the drain, 5 an ACK while held, 6 the reservation after the
    // drain). It must return that exit's code with the worker joined before
    // any capture is destroyed (exit 77 otherwise).
    int HeldHeartbeatChild(bool destroy, int inject = 0) {
        StartWatchdog(destroy ? "held heartbeat + destroy"
                              : "held heartbeat + drain");
        JoinedBeforeUnwind unwind_check;  // first: destroyed last
        worker_joined_ = false;
        if (!PutOnSource("k0", 'a')) return Join(10, "put k0");
        RunBackupTick(*storage_);
        if (HolderBytes("k0") != std::string(kValue, 'a')) {
            return Join(11, "k0 not backed up before the schedule");
        }
        StageLog log;
        Gate held, release;
        std::atomic<bool> holding{false};
        std::atomic<bool> latched_at_release{false};
        FileStorage* fs = storage_.get();
        SetStageHook(*storage_, [&, fs](const char* stage, size_t n) {
            if (std::string(stage) == "heartbeat" && n > 0 &&
                !holding.exchange(true)) {
                held.Open();
                release.Wait(std::chrono::seconds(30));
                // The object is alive here: its destructor is still
                // joining this thread.
                latched_at_release = StopLatched(*fs) || DrainingFlag(*fs);
            }
            log.Hit(stage);
        });
        // Every NotifyBackupDone attempt the worker makes from here on.
        std::mutex acks_mu;
        std::vector<std::tuple<uint64_t, size_t, bool>> acks;
        SetAckObserver(*storage_, [&](uint64_t id, size_t n, bool ok) {
            std::lock_guard<std::mutex> l(acks_mu);
            acks.emplace_back(id, n, ok);
        });
        // The backend outlives the FileStorage, for the byte checks after
        // the worker has been joined.
        auto backend = Backend(*storage_);
        // From here on every exit releases the held response, then stops and
        // joins the worker (destroying the FileStorage) before the captures
        // above are destroyed. Idempotent: the schedule itself resets
        // storage_ as well.
        ScopeGuard cleanup{[&] {
            release.Open();
            storage_.reset();
            worker_joined_ = true;
        }};
        setenv("MC_SSD_BACKUP_STAGING_BYTES", "8388608", 1);
        setenv("MC_SSD_BACKUP_INTERVAL_MS", "50", 1);
        if (!StartWorker(*storage_)) return Fail(12, "start worker");
        if (inject == 1 || !PutOnSource("k1", 'b')) {
            return Fail(13, "put k1");
        }
        if (inject == 2 || !held.Wait()) {
            return Fail(2, "no nonempty heartbeat reached the worker");
        }
        // The held response's batch is reserved on the master.
        const auto held_snap = HolderReservation();
        if (inject == 3 || !held_snap ||
            held_snap->inflight_batches.size() != 1) {
            return Fail(15, "held batch not reserved on the master");
        }
        const uint64_t held_batch = held_snap->inflight_batches[0];
        const auto reserved =
            [&](const std::optional<SsdBackupCoordinator::ClientSnapshot>& s) {
                return s &&
                       std::count(s->inflight_batches.begin(),
                                  s->inflight_batches.end(), held_batch) > 0;
            };
        if (!destroy) {
            if (inject == 4 || !storage_->DrainLocalDiskSegment(0)) {
                return Fail(8, "DrainLocalDiskSegment");
            }
            // Drain unmounts the holder's disk: the master drops it as a
            // candidate and with it the reservation, before any ACK (the
            // worker is still held).
            {
                std::lock_guard<std::mutex> l(acks_mu);
                if (inject == 5 || !acks.empty()) {
                    return Fail(16, "ACK sent while held");
                }
            }
            if (inject == 6 || reserved(HolderReservation())) {
                return Fail(17, "drain left the batch reserved");
            }
            release.Open();
            // Join the worker before anything its callbacks capture goes.
            storage_.reset();
            worker_joined_ = true;
        } else {
            std::thread releaser([&, fs] {
                const auto until =
                    std::chrono::steady_clock::now() + std::chrono::seconds(10);
                while (!StopLatched(*fs) &&
                       std::chrono::steady_clock::now() < until) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                release.Open();
            });
            storage_.reset();  // stop + join (the worker is held)
            worker_joined_ = true;
            releaser.join();
            if (!latched_at_release) {
                return Fail(7,
                            "stop not latched when the response was "
                            "released");
            }
            // The holder stays a candidate; the empty ACK released it.
            if (reserved(HolderReservation())) {
                return Fail(18, "batch still reserved after the empty ACK");
            }
        }
        // Worker joined in both branches. Exactly one ACK attempt, empty,
        // for the held batch (in the destroy branch it must have succeeded;
        // after a drain it releases nothing more).
        {
            std::lock_guard<std::mutex> l(acks_mu);
            if (acks.size() != 1 || std::get<0>(acks[0]) != held_batch ||
                std::get<1>(acks[0]) != 0) {
                return Fail(19, "expected one empty ACK for batch " +
                                    std::to_string(held_batch) + ", got " +
                                    std::to_string(acks.size()));
            }
            if (destroy && !std::get<2>(acks[0])) {
                return Fail(20, "empty ACK failed");
            }
        }
        auto removed = src_->Remove("k1");
        if (!removed) {
            return Fail(3, "k1 was read after the stop/drain: " +
                               toString(removed.error()));
        }
        if (log.Get("before_query") != 0 || log.Get("before_transfer") != 0) {
            return Fail(4, "query/transfer admitted: before_query=" +
                               std::to_string(log.Get("before_query")) +
                               " before_transfer=" +
                               std::to_string(log.Get("before_transfer")));
        }
        if (BackendBytes(*backend, "default", "k1", kValue) != "") {
            return Fail(5, "k1 stored");
        }
        if (BackendBytes(*backend, "default", "k0", kValue) !=
            std::string(kValue, 'a')) {
            return Fail(6, "k0 lost");
        }
        return 0;
    }

    // R5-M1: cancellation observed inside ReadBackupBatch, driven through
    // the real BackupTick (mode 0: drain latched just before the first
    // tenant query; mode 1: latched while the query response is returned,
    // i.e. after the query and before the transfer). Afterwards a new
    // backup must fit the inflight limit (the batch's reservation was
    // released) and the earlier backup keeps its bytes. Child body.
    int ReadCancelChild(int mode) {
        StartWatchdog(mode == 0 ? "pre-query cancel" : "post-query cancel");
        if (!PutOnSource("q0", 'a')) return Fail(10, "put q0");
        RunBackupTick(*storage_);
        if (HolderBytes("q0") != std::string(kValue, 'a')) {
            return Fail(11, "q0 not backed up");
        }
        if (!PutOnSource("q1", 'b')) return Fail(12, "put q1");
        StageLog log;
        const std::string at = mode == 0 ? "before_query" : "after_query";
        FileStorage* fs = storage_.get();
        SetStageHook(*storage_, [&, fs](const char* stage, size_t) {
            if (at == stage && log.Get(at) == 0) SetDrainingFlag(*fs, true);
            log.Hit(stage);
        });
        RunBackupTick(*storage_);
        SetStageHook(*storage_, nullptr);
        SetDrainingFlag(*storage_, false);
        if (log.Get(at) != 1) return Fail(13, "stage " + at + " not reached");
        const int queries = log.Get("after_query");
        const int transfers = log.Get("before_transfer");
        if (queries != (mode == 0 ? 0 : 1) || transfers != 0) {
            return Fail(4, "after cancel: queries=" + std::to_string(queries) +
                               " transfers=" + std::to_string(transfers));
        }
        if (mode == 0) {
            // No query, so no read lease on q1.
            auto removed = src_->Remove("q1");
            if (!removed) {
                return Fail(3, "q1 queried: " + toString(removed.error()));
            }
        }
        if (HolderBytes("q1") != "") return Fail(5, "q1 stored");
        if (!PutOnSource("q2", 'c')) return Fail(14, "put q2");
        RunBackupTick(*storage_);
        if (HolderBytes("q2") != std::string(kValue, 'c')) {
            return Fail(6, "q2 not backed up (reservation not released?)");
        }
        if (HolderBytes("q0") != std::string(kValue, 'a')) {
            return Fail(7, "q0 lost");
        }
        return 0;
    }

    // R5-M1: cancellation between tenant groups of one real batch. The
    // master batches two keys (t1, t2) into one batch; the test moves the
    // second task to another tenant name (no object there) so the batch has
    // two tenant groups, keeping the real batch id and reservation. The
    // drain is latched when the second group starts (its "before_query"),
    // so that group must not be queried. Then t3+t4 (8192 bytes) fit the
    // 9000-byte inflight limit only if the batch's reservation was released.
    int BetweenTenantsChild() {
        StartWatchdog("between-tenant cancel");
        if (!PutOnSource("t1", 'd') || !PutOnSource("t2", 'e')) {
            return Fail(10, "put t1/t2");
        }
        auto resp = holder_->BackupObjectHeartbeat(holder_ep_, 0, 1LL << 30);
        if (!resp || resp->batches.size() != 1 ||
            resp->batches[0].tasks.size() != 2) {
            return Fail(11, "expected one batch of two tasks");
        }
        resp->batches[0].tasks[1].tenant_id = "r6-tenant-b";
        StageLog log;
        FileStorage* fs = storage_.get();
        SetStageHook(*storage_, [&, fs](const char* stage, size_t) {
            if (std::string(stage) == "before_query" &&
                log.Get("before_query") == 1) {
                SetDrainingFlag(*fs, true);  // the second group starts
            }
            log.Hit(stage);
        });
        RunBatches(*storage_, resp->batches);
        SetStageHook(*storage_, nullptr);
        SetDrainingFlag(*storage_, false);
        if (log.Get("before_query") != 2) {
            return Fail(13, "second tenant group not reached");
        }
        if (log.Get("after_query") != 1) {
            return Fail(4, "second tenant group queried: queries=" +
                               std::to_string(log.Get("after_query")));
        }
        if (!PutOnSource("t3", 'f') || !PutOnSource("t4", 'g')) {
            return Fail(12, "put t3/t4");
        }
        RunBackupTick(*storage_);
        if (HolderBytes("t3") != std::string(kValue, 'f') ||
            HolderBytes("t4") != std::string(kValue, 'g')) {
            return Fail(6, "t3/t4 not backed up (reservation not released?)");
        }
        return 0;
    }

    // Idempotent one-shot gate (a barrier that may be opened from several
    // exit paths and is safe to wait on more than once).
    struct Gate {
        std::mutex mu;
        std::condition_variable cv;
        bool open = false;
        void Open() {
            {
                std::lock_guard<std::mutex> l(mu);
                open = true;
            }
            cv.notify_all();
        }
        bool Wait(std::chrono::milliseconds d = std::chrono::seconds(15)) {
            std::unique_lock<std::mutex> l(mu);
            return cv.wait_for(l, d, [&] { return open; });
        }
        bool IsOpen() {
            std::lock_guard<std::mutex> l(mu);
            return open;
        }
    };
    // Runs `fn` when it goes out of scope, unless dismissed.
    struct ScopeGuard {
        std::function<void()> fn;
        ~ScopeGuard() {
            if (fn) fn();
        }
    };
    // Set by a cleanup guard once the worker is joined; checked by the
    // destructor of an object declared before every capture, so it runs
    // after all of them are gone: the worker must have been joined first.
    static inline std::atomic<bool> worker_joined_{false};
    struct JoinedBeforeUnwind {
        ~JoinedBeforeUnwind() {
            if (!worker_joined_.load()) {
                std::fprintf(stderr, "captures destroyed before the join\n");
                std::_Exit(77);
            }
        }
    };

    // R8-M2 #3 (round 1, harness fixed in round 2 for R1-M2): a running
    // worker is held while its real source query response is returned
    // (stage "after_query"; its staging is already allocated). Meanwhile
    // another thread calls the real DrainLocalDiskSegment, then the response
    // goes on. While held: no ACK attempt; after the drain returns, the
    // coordinator no longer keeps the held reservation (the disk unmount
    // dropped the holder as a candidate). After release and join: no
    // transfer, exactly one empty ACK attempt for the held batch (its
    // outcome recorded, not required), the worker's own staging
    // unregistered once while still allocated and freed afterwards, k1
    // holds exactly the query's read lease and is not stored, and k0's
    // earlier backup keeps its bytes.
    // `inject` 1-4 takes an early failure exit (1: after the worker start,
    // 2: no held response, 3: the reservation check, 4: the drain) and must
    // return that exit's code with the worker joined before any capture is
    // destroyed (exit 77 otherwise). Child body.
    //
    // T16 old/new control (`old_exits`): the 236a44f2 harness had no cleanup
    // guard. Its exits 13 and 2 returned without releasing the held response
    // or joining the worker, and 14 and 8 released it but did not join
    // either, so the captures went while the worker still ran. In this mode
    // the guard first records that state at the moment the old code would
    // unwind (worker not joined), then releases and joins safely (no
    // use-after-lifetime is executed) and ends the child with 78, naming the
    // check the exit came from.
    int HeldQueryResponseChild(int inject = 0, bool old_exits = false) {
        StartWatchdog("held query response + drain");
        JoinedBeforeUnwind unwind_check;  // first: destroyed last
        worker_joined_ = false;
        if (!PutOnSource("k0", 'a')) return Join(10, "put k0");
        RunBackupTick(*storage_);
        if (HolderBytes("k0") != std::string(kValue, 'a')) {
            return Join(11, "k0 not backed up");
        }
        auto backend = Backend(*storage_);
        std::mutex mu;
        std::vector<std::tuple<uint64_t, size_t, bool>> acks;
        std::vector<Transfer> transfers;
        StageLog log;
        Gate held, release;
        std::atomic<bool> holding{false};
        // The worker's own staging (allocated by StartWorker), observed
        // through the allocator and unregister seams.
        std::weak_ptr<ClientBufferAllocator> staging_weak;
        int unregister_calls = 0;
        bool alive_at_unregister = false;
        SetStagingAlloc(*storage_, [&](size_t size) {
            auto a = AlignedClientBufferAllocator::create(size, "tcp");
            staging_weak = a;
            return a;
        });
        Client* holder = holder_.get();
        SetStagingUnregister(
            *storage_,
            [&, holder](void* addr) -> tl::expected<void, ErrorCode> {
                ++unregister_calls;
                alive_at_unregister = !staging_weak.expired();
                return holder->unregisterLocalMemory(addr);
            });
        SetAckObserver(*storage_, [&](uint64_t id, size_t n, bool ok) {
            std::lock_guard<std::mutex> l(mu);
            acks.emplace_back(id, n, ok);
        });
        SetTransferObserver(*storage_,
                            [&](const std::string& t, const std::string& k,
                                ErrorCode e, const Slice& s) {
                                std::lock_guard<std::mutex> l(mu);
                                transfers.push_back(CopyTransfer(t, k, e, s));
                            });
        SetStageHook(*storage_, [&](const char* stage, size_t) {
            log.Hit(stage);
            if (std::string(stage) == "after_query" &&
                !holding.exchange(true)) {
                held.Open();
                release.Wait(std::chrono::seconds(30));
            }
        });
        // From here on every exit releases the held response, then stops and
        // joins the worker (destroying the FileStorage) before the captures
        // above are destroyed.
        ScopeGuard cleanup{[&] {
            if (old_exits) {
                const bool joined = worker_joined_.load();
                const bool worker_alive = storage_ != nullptr;
                const std::string line =
                    "old exit after check " +
                    std::to_string(last_fail_.load()) +
                    ": worker joined=" + std::to_string(joined) +
                    " storage alive=" + std::to_string(worker_alive) +
                    " at unwind";
                std::fprintf(stderr, "%s\n", line.c_str());
                Trace(line);
                release.Open();
                storage_.reset();
                worker_joined_ = true;
                std::_Exit(joined || !worker_alive ? 79 : 78);
            }
            release.Open();
            storage_.reset();
            worker_joined_ = true;
        }};
        setenv("MC_SSD_BACKUP_STAGING_BYTES", "8388608", 1);
        setenv("MC_SSD_BACKUP_INTERVAL_MS", "50", 1);
        if (!StartWorker(*storage_)) return Fail(12, "start worker");
        if (inject == 1 || !PutOnSource("k1", 'b')) {
            return Fail(13, "put k1");
        }
        if (inject == 2 || !held.Wait()) {
            return Fail(2, "the worker never queried k1");
        }
        const auto snap = HolderReservation();
        if (inject == 3 || !snap || snap->inflight_batches.size() != 1) {
            return Fail(14, "held batch not reserved");
        }
        const uint64_t held_batch = snap->inflight_batches[0];
        bool drained = false;
        std::thread drainer(
            [&] { drained = storage_->DrainLocalDiskSegment(0).has_value(); });
        drainer.join();  // the drain completes while the response is held
        if (inject == 4 || !drained || !DrainingFlag(*storage_)) {
            return Fail(8, "drain failed or not latched");
        }
        {
            std::lock_guard<std::mutex> l(mu);
            if (!acks.empty()) return Fail(17, "ACK sent while held");
        }
        const auto after_drain = HolderReservation();
        if (after_drain &&
            std::count(after_drain->inflight_batches.begin(),
                       after_drain->inflight_batches.end(), held_batch) > 0) {
            return Fail(18, "drain left the held batch reserved");
        }
        cleanup.fn = nullptr;
        release.Open();
        storage_.reset();  // stop + join the worker
        worker_joined_ = true;
        if (log.Get("before_transfer") != 0 || !transfers.empty()) {
            return Fail(4, "transfer admitted after the drain");
        }
        if (acks.size() != 1 || std::get<0>(acks[0]) != held_batch ||
            std::get<1>(acks[0]) != 0) {
            return Fail(15,
                        "expected one empty ACK attempt for the held "
                        "batch, got " +
                            std::to_string(acks.size()));
        }
        std::fprintf(stderr, "held batch empty ACK ok=%d\n",
                     static_cast<int>(std::get<2>(acks[0])));
        Trace("held batch " + std::to_string(held_batch) +
              ": ACK attempts while held=0, reservation after drain=" +
              (after_drain
                   ? std::to_string(after_drain->inflight_batches.size())
                   : std::string("no candidate")) +
              ", empty ACK attempts after join=1 ok=" +
              std::to_string(std::get<2>(acks[0])) +
              ", staging unregister calls=" + std::to_string(unregister_calls) +
              " alive_at_unregister=" + std::to_string(alive_at_unregister) +
              " freed=" + std::to_string(staging_weak.expired()));
        if (unregister_calls != 1 || !alive_at_unregister ||
            !staging_weak.expired()) {
            return Fail(19,
                        "staging: unregister calls=" +
                            std::to_string(unregister_calls) +
                            " alive_at_unregister=" +
                            std::to_string(alive_at_unregister) +
                            " freed=" + std::to_string(staging_weak.expired()));
        }
        auto removed = src_->Remove("k1");
        if (removed.has_value() ||
            removed.error() != ErrorCode::OBJECT_HAS_LEASE) {
            return Fail(3, "k1 should hold the query's lease, got " +
                               (removed ? std::string("OK")
                                        : toString(removed.error())));
        }
        if (BackendBytes(*backend, "default", "k1", kValue) != "") {
            return Fail(5, "k1 stored");
        }
        if (BackendBytes(*backend, "default", "k0", kValue) !=
            std::string(kValue, 'a')) {
            return Fail(6, "k0 lost");
        }
        return 0;
    }

    // T8 item 5 / MG-1: the running worker's backup read of k1 is held
    // inside its transfer wait (a delayed completion: the transfer is still
    // allowed to write the staging) while another thread destroys the
    // FileStorage. While held: stop is latched, the destructor does not
    // return, and nothing of the staging is freed, quarantined or
    // unregistered. After the release the transfer completes with k1's full
    // bytes; the latched stop makes the write store nothing and send one
    // empty ACK for the batch; then the read's allocation is freed, and only
    // after that is the worker's staging unregistered (once, still
    // allocated) and freed. k0's earlier backup keeps its bytes. Child body:
    // 0 on success; 9 from the watchdog; 77 if a capture went before the
    // join.
    int DelayedCompletionAcrossDestroyChild() {
        StartWatchdog("delayed completion + destroy");
        JoinedBeforeUnwind unwind_check;  // first: destroyed last
        worker_joined_ = false;
        if (!PutOnSource("k0", 'a')) return Join(10, "put k0");
        RunBackupTick(*storage_);
        if (HolderBytes("k0") != std::string(kValue, 'a')) {
            return Join(11, "k0 not backed up");
        }
        auto backend = Backend(*storage_);
        std::mutex mu;
        std::vector<std::tuple<uint64_t, size_t, bool>> acks;
        std::vector<Transfer> transfers;
        Gate held, release;
        OneShot shot;
        std::atomic<bool> latched_at_release{false};
        std::weak_ptr<ClientBufferAllocator> staging_weak;
        const char* staging_base = nullptr;
        std::atomic<int> unregister_calls{0};
        std::atomic<bool> alive_at_unregister{false};
        FileStorage* fs = storage_.get();
        SetStagingAlloc(*storage_, [&](size_t size) {
            auto a = AlignedClientBufferAllocator::create(size, "tcp");
            staging_weak = a;
            staging_base = a ? static_cast<const char*>(a->getBase()) : nullptr;
            return a;
        });
        Client* holder = holder_.get();
        SetStagingUnregister(
            *storage_,
            [&, holder](void* addr) -> tl::expected<void, ErrorCode> {
                ++unregister_calls;
                alive_at_unregister = !staging_weak.expired();
                return holder->unregisterLocalMemory(addr);
            });
        SetAckObserver(*storage_, [&](uint64_t id, size_t n, bool ok) {
            std::lock_guard<std::mutex> l(mu);
            acks.emplace_back(id, n, ok);
        });
        SetTransferObserver(*storage_,
                            [&](const std::string& t, const std::string& k,
                                ErrorCode e, const Slice& s) {
                                std::lock_guard<std::mutex> l(mu);
                                transfers.push_back(CopyTransfer(t, k, e, s));
                            });
        LogStagingEvents();
        SetStageHook(*storage_, [&](const char* stage, size_t) {
            if (std::string(stage) == "before_transfer") shot.armed = true;
        });
        auto faults = std::make_shared<TransferFaultsForTest>();
        faults->delay_completion = [&, fs](BatchID) {
            if (!shot.Take()) return false;
            held.Open();
            release.Wait(std::chrono::seconds(30));
            // The worker runs inside the object: its destructor is still
            // joining this thread.
            latched_at_release = StopLatched(*fs);
            return false;  // then wait for the real completion
        };
        FaultScope fault_scope(faults);  // removed after the join below
        // From here on every exit releases the held wait, then stops and
        // joins the worker before the captures above are destroyed.
        ScopeGuard cleanup{[&] {
            release.Open();
            storage_.reset();
            worker_joined_ = true;
        }};
        setenv("MC_SSD_BACKUP_STAGING_BYTES", "8388608", 1);
        setenv("MC_SSD_BACKUP_INTERVAL_MS", "50", 1);
        if (!StartWorker(*storage_)) return Fail(12, "start worker");
        if (!PutOnSource("k1", 'b')) return Fail(13, "put k1");
        if (!held.Wait()) {
            return Fail(2, "k1's read never reached its transfer wait");
        }
        const auto snap = HolderReservation();
        if (!snap || snap->inflight_batches.size() != 1) {
            return Fail(14, "held batch not reserved");
        }
        const uint64_t held_batch = snap->inflight_batches[0];
        // Destroy on another thread while the transfer wait is held. No
        // early return until it is joined.
        std::unique_ptr<FileStorage> victim = std::move(storage_);
        std::atomic<bool> destroyed{false};
        std::thread destroyer([&] {
            victim.reset();
            destroyed = true;
        });
        const auto until = Clock::now() + std::chrono::seconds(10);
        while (!StopLatched(*fs) && Clock::now() < until) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        const bool returned_while_held = destroyed.load();
        const auto during = slog_.Get();
        const bool alive_during = !staging_weak.expired();
        const int unregister_during = unregister_calls.load();
        size_t transfers_during;
        {
            std::lock_guard<std::mutex> l(mu);
            transfers_during = transfers.size();
        }
        release.Open();
        destroyer.join();
        worker_joined_ = true;
        const auto ev = slog_.Get();
        const auto trace = StagingLog::Format(ev, staging_base);
        Trace("held batch " + std::to_string(held_batch) +
              ": destructor returned while held=" +
              std::to_string(returned_while_held) + ", events while held: " +
              StagingLog::Format(during, staging_base) +
              "; all events: " + trace);
        if (!latched_at_release) {
            return Fail(7, "stop not latched when the wait was released");
        }
        if (returned_while_held) {
            return Fail(20,
                        "destructor returned while the transfer wait "
                        "was held");
        }
        if (!alive_during || unregister_during != 0 ||
            StagingLog::Count(during, "unregister") != 0 ||
            StagingLog::Count(during, "free") != 0 ||
            StagingLog::Count(during, "quarantine") != 0 ||
            StagingLog::Count(during, "staging_freed") != 0 ||
            transfers_during != 0) {
            return Fail(21, "staging released while held: " +
                                StagingLog::Format(during, staging_base));
        }
        {
            std::lock_guard<std::mutex> l(mu);
            if (transfers.size() != 1 || transfers[0].key != "k1" ||
                transfers[0].result != ErrorCode::OK ||
                transfers[0].bytes != std::string(kValue, 'b')) {
                return Fail(22,
                            "expected k1's complete bytes after the "
                            "release, transfers=" +
                                std::to_string(transfers.size()));
            }
            if (acks.size() != 1 || std::get<0>(acks[0]) != held_batch ||
                std::get<1>(acks[0]) != 0 || !std::get<2>(acks[0])) {
                return Fail(15,
                            "expected one successful empty ACK for the "
                            "held batch, got " +
                                std::to_string(acks.size()));
            }
        }
        // Order: staging registered; k1's allocation held, then freed; then
        // the staging unregistered and freed. Never quarantined.
        const int reg = StagingLog::Find(ev, "register", staging_base);
        const int hold = StagingLog::Find(ev, "hold");
        const int freed =
            hold < 0 ? -1 : StagingLog::Find(ev, "free", ev[hold].ptr);
        const int unreg = StagingLog::Find(ev, "unregister", staging_base);
        const int staging_freed = StagingLog::Find(ev, "staging_freed");
        if (reg != 0 || hold < 0 || freed <= hold || unreg <= freed ||
            staging_freed <= unreg ||
            StagingLog::Count(ev, "quarantine") != 0 ||
            StagingLog::Count(ev, "unregister") != 1) {
            return Fail(23, "staging lifetime order: " + trace);
        }
        if (unregister_calls != 1 || !alive_at_unregister ||
            !staging_weak.expired()) {
            return Fail(19,
                        "staging: unregister calls=" +
                            std::to_string(unregister_calls.load()) +
                            " alive_at_unregister=" +
                            std::to_string(alive_at_unregister.load()) +
                            " freed=" + std::to_string(staging_weak.expired()));
        }
        if (BackendBytes(*backend, "default", "k1", kValue) != "") {
            return Fail(5, "k1 stored");
        }
        if (BackendBytes(*backend, "default", "k0", kValue) !=
            std::string(kValue, 'a')) {
            return Fail(6, "k0 lost");
        }
        return 0;
    }

    // R8-M2 #4: the real two-stage RunBackupBatches pipeline over two
    // batches. Batch 1's read runs in the prefetch future while batch 0 is
    // written; it is held right after its transfer completed, while it owns
    // its staging. Once batch 0 is written and acknowledged, stop/drain is
    // requested (flag and wakeup), then the prefetched read is released and
    // the pipeline joined. Batch 0 is stored and registered; batch 1
    // transferred its full payload into staging that does not overlap batch
    // 0's, but is not written (one empty ACK); both reservations are
    // released, no staging stays held, and k0's earlier backup survives.
    // Child body.
    //
    // T16 old/new control (`old_handshake`, used with delay_b): the 236a44f2
    // harness did not hold A inside its ACK; it only recorded the ACK and
    // held B at its transfer, then compared the two transfer pointers. Here
    // A's ACK returns at once and B starts only after it (the delayed
    // schedule), as that harness allowed. The corrected oracle must then see
    // that A's staging was no longer live at B's observation (check 23),
    // which the old pointer comparison alone could not tell.
    int HeldPrefetchChild(bool delay_b = false, bool old_handshake = false) {
        StartWatchdog("held prefetch + drain");
        if (!PutOnSource("k0", 'a')) return Fail(10, "put k0");
        RunBackupTick(*storage_);
        if (HolderBytes("k0") != std::string(kValue, 'a')) {
            return Fail(11, "k0 not backed up");
        }
        if (!PutOnSource("p1", 'x') || !PutOnSource("p2", 'y')) {
            return Fail(12, "put p1/p2");
        }
        auto resp = holder_->BackupObjectHeartbeat(holder_ep_, 0, 1LL << 30);
        if (!resp || resp->batches.size() != 2) {
            return Fail(13, "expected two batches");
        }
        const auto batches = resp->batches;
        const std::string first_key = batches[0].tasks.at(0).key;
        const std::string second_key = batches[1].tasks.at(0).key;
        if (!StagingFree()) return Fail(19, "staging probe fails even idle");
        // Two-way handshake (R1-M1): A is held inside its registration ACK
        // (its staging still owned) until B's transfer completed into B's
        // own staging; at that moment both allocations are live, so their
        // slices must be disjoint. B is then held while A finishes and
        // releases its staging; drain is requested; B is released and the
        // pipeline joined. With `delay_b`, B's first stage also waits until
        // A is inside its ACK (B starts late).
        std::mutex mu;
        std::vector<std::tuple<uint64_t, size_t, bool>> acks;
        std::vector<Transfer> transfers;
        std::vector<std::string> events;
        const auto event = [&](const std::string& e) {
            std::lock_guard<std::mutex> l(mu);
            events.push_back(e);
        };
        Gate a_in_ack, b_observed, a_done, release_b;
        std::atomic<int> queries{0};
        std::atomic<bool> both_live{false}, disjoint{false};
        std::atomic<bool> ack_wait_timed_out{false};
        FileStorage* fs = storage_.get();
        SetStageHook(*storage_, [&](const char* stage, size_t) {
            if (std::string(stage) == "before_query" && queries++ == 1) {
                if (delay_b) (old_handshake ? a_done : a_in_ack).Wait();
                event("B_start");
            }
        });
        SetAckObserver(*storage_, [&](uint64_t id, size_t n, bool ok) {
            {
                std::lock_guard<std::mutex> l(mu);
                acks.emplace_back(id, n, ok);
            }
            if (n == 0 || a_in_ack.IsOpen()) return;
            event("A_ack");
            a_in_ack.Open();
            if (!old_handshake && !b_observed.Wait(std::chrono::seconds(10))) {
                ack_wait_timed_out = true;
            }
            event("A_ack_return");
            a_done.Open();
        });
        SetTransferObserver(*storage_, [&, fs](const std::string& t,
                                               const std::string& k,
                                               ErrorCode e, const Slice& s) {
            Transfer tr = CopyTransfer(t, k, e, s);
            {
                std::lock_guard<std::mutex> l(mu);
                transfers.push_back(tr);
            }
            if (k != second_key) return;
            // B's staging (this slice) and A's (held in its ACK) are both
            // live now.
            const char* a_ptr = nullptr;
            {
                std::lock_guard<std::mutex> l(mu);
                for (const auto& x : transfers) {
                    if (x.key == first_key) a_ptr = x.ptr;
                }
            }
            // A cannot finish its write before this point (its ACK waits
            // for b_observed), so its staging is still held.
            both_live = !a_done.IsOpen() && StagingHeld(*fs) == 1;
            disjoint = a_ptr != nullptr &&
                       !(a_ptr < tr.ptr + kValue && tr.ptr < a_ptr + kValue);
            event("B_observed");
            b_observed.Open();
            release_b.Wait(std::chrono::seconds(30));
            event("B_released");
        });
        std::thread pipeline([&] { RunBatches(*storage_, batches); });
        // Every exit opens all gates and joins the pipeline before the
        // captures above go.
        ScopeGuard join_all{[&] {
            a_in_ack.Open();
            b_observed.Open();
            release_b.Open();
            if (pipeline.joinable()) pipeline.join();
        }};
        if (!b_observed.Wait()) {
            return Fail(2, "prefetched read never completed its transfer");
        }
        if (!a_done.Wait()) {
            return Fail(14, "batch 0 not acknowledged while batch 1 held");
        }
        // A's staging goes once its write returns (B is still held).
        const auto until = Clock::now() + std::chrono::seconds(10);
        while (StagingHeld(*storage_) != 0 && Clock::now() < until) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        StartDrainAndWake(*storage_);
        release_b.Open();
        pipeline.join();
        SetStageHook(*storage_, nullptr);
        SetAckObserver(*storage_, nullptr);
        SetTransferObserver(*storage_, nullptr);
        SetDrainingFlag(*storage_, false);
        std::string trace;
        for (const auto& e : events) trace += e + " ";
        std::fprintf(stderr, "event order: %s\n", trace.c_str());
        Trace(std::string("delay_b=") + (delay_b ? "1" : "0") +
              " both_live=" + std::to_string(both_live.load()) + " disjoint=" +
              std::to_string(disjoint.load()) + " event order: " + trace);
        if (ack_wait_timed_out) return Fail(22, "A's ACK never saw B's read");
        if (!both_live) {
            return Fail(23,
                        "B's transfer did not complete while A held "
                        "staging");
        }
        if (!disjoint) return Fail(20, "the two live stagings overlap");
        const auto at = [&](const char* e) {
            return std::find(events.begin(), events.end(), e) - events.begin();
        };
        if (!(at("A_ack") < at("A_ack_return") &&
              at("B_observed") < at("A_ack_return") &&
              at("A_ack_return") < at("B_released") &&
              (!delay_b || at("A_ack") < at("B_start")))) {
            return Fail(24, "unexpected event order: " + trace);
        }
        if (transfers.size() != 2) {
            return Fail(4, "transfers=" + std::to_string(transfers.size()));
        }
        for (const auto& tr : transfers) {
            const char fill = tr.key == "p1" ? 'x' : 'y';
            if (tr.result != ErrorCode::OK ||
                tr.bytes != std::string(kValue, fill)) {
                return Fail(15, "transfer of " + tr.key + " incomplete");
            }
        }
        if (acks.size() != 2 || std::get<0>(acks[0]) != batches[0].batch_id ||
            std::get<1>(acks[0]) != 1 || !std::get<2>(acks[0]) ||
            std::get<0>(acks[1]) != batches[1].batch_id ||
            std::get<1>(acks[1]) != 0 || !std::get<2>(acks[1])) {
            return Fail(
                16, "unexpected ACKs (" + std::to_string(acks.size()) + ")");
        }
        auto snap = HolderReservation();
        if (!snap || !snap->inflight_batches.empty()) {
            return Fail(17, "reservations left");
        }
        if (HolderBytes(first_key) !=
                std::string(kValue, first_key == "p1" ? 'x' : 'y') ||
            HolderBytes(second_key) != "") {
            return Fail(5, "stored bytes wrong");
        }
        if (HolderBytes("k0") != std::string(kValue, 'a')) {
            return Fail(6, "k0 lost");
        }
        if (StagingHeld(*storage_) != 0) return Fail(21, "staging still held");
        if (!StagingFree()) return Fail(18, "staging not released");
        return 0;
    }

    static constexpr char kTenant[] = "tenant-a";
    bool multi_tenant_ = false;
    fs::path policy_;
    std::shared_ptr<Client> srct_;
    std::string srct_ep_;
    void* srct_seg_ = nullptr;
    std::unique_ptr<SimpleAllocator> srct_alloc_;
    const char* batch_keys_ = "1";
    const char* max_inflight_ = "6000";
    uint64_t lease_ttl_ms_ = 5000;
    mooncake::testing::InProcMaster master_;
    std::shared_ptr<Client> src_, holder_;
    std::string src_ep_, holder_ep_;
    void* src_seg_ = nullptr;
    void* holder_seg_ = nullptr;
    std::unique_ptr<SimpleAllocator> src_alloc_, holder_alloc_;
    std::shared_ptr<ClientBufferAllocator> staging_;
    std::unique_ptr<FileStorage> storage_;
};

// R3-B1 / R4-M1: a heartbeat that hands the holder a batch after a drain was
// latched must not read it. Observed directly: the source query a read makes
// takes a 5 s read lease on the object, so Remove of k1 would fail with
// OBJECT_HAS_LEASE; the holder would also write and register k1. The empty
// ACK must release the batch's reservation (k2 fits the 6000-byte inflight
// limit only then), and k0's earlier backup must keep its bytes.
// R2-M1 control: the scan statistics are read from another thread while the
// live worker keeps updating them (a scan on every 10 ms tick). With plain
// counters this is a data race (ThreadSanitizer reports it); the counters
// must be safe to read concurrently. In an ordinary build the test checks
// that the reads see the scan count grow while the worker runs.
TEST_F(FileStorageBackupHolderTest, ScanStatsReadWhileWorkerRuns) {
    setenv("MC_SSD_BACKUP_STAGING_BYTES", "8388608", 1);
    setenv("MC_SSD_BACKUP_INTERVAL_MS", "10", 1);
    setenv("MC_SSD_BACKUP_SCAN_INTERVAL_MS", "0", 1);
    ASSERT_TRUE(StartWorker(*storage_).has_value());
    const auto first = storage_->GetBackupScanStats().scans;
    uint64_t last = first, reads = 0;
    const auto until =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < until && last < first + 5) {
        last = storage_->GetBackupScanStats().scans;
        ++reads;
    }
    storage_.reset();  // stop + join
    unsetenv("MC_SSD_BACKUP_STAGING_BYTES");
    unsetenv("MC_SSD_BACKUP_INTERVAL_MS");
    unsetenv("MC_SSD_BACKUP_SCAN_INTERVAL_MS");
    EXPECT_GE(last, first + 5)
        << "the worker did not scan (reads=" << reads << ")";
    Trace("scans " + std::to_string(first) + "->" + std::to_string(last) +
          " concurrent reads=" + std::to_string(reads));
}

TEST_F(FileStorageBackupHolderTest, HeartbeatBatchAfterDrainIsNotRead) {
    ASSERT_TRUE(PutOnSource("k0", 'a'));
    RunBackupTick(*storage_);
    ASSERT_EQ(HolderBytes("k0"), std::string(kValue, 'a'));  // normal backup

    ASSERT_TRUE(PutOnSource("k1", 'b'));  // batch queued for the holder
    SetDrainingFlag(*storage_, true);
    RunBackupTick(*storage_);  // the heartbeat returns k1's batch
    SetDrainingFlag(*storage_, false);
    EXPECT_EQ(HolderBytes("k1"), "");
    auto removed = src_->Remove("k1");
    EXPECT_TRUE(removed.has_value())
        << "k1 was read after the drain: " << toString(removed.error());

    ASSERT_TRUE(PutOnSource("k2", 'c'));
    RunBackupTick(*storage_);
    EXPECT_EQ(HolderBytes("k2"), std::string(kValue, 'c'));  // reservation free
    EXPECT_EQ(HolderBytes("k0"), std::string(kValue, 'a'));
}

// R4-M1: RunBackupBatches entered with the drain already set reads nothing.
TEST_F(FileStorageBackupHolderTest, RunBatchesWithDrainSetReadsNothing) {
    ASSERT_TRUE(PutOnSource("r1", 'r'));
    auto resp = holder_->BackupObjectHeartbeat(holder_ep_, 0, 1LL << 30);
    ASSERT_TRUE(resp.has_value());
    ASSERT_EQ(resp->batches.size(), 1u);
    SetDrainingFlag(*storage_, true);
    RunBatches(*storage_, resp->batches);
    SetDrainingFlag(*storage_, false);
    EXPECT_EQ(HolderBytes("r1"), "");
    auto removed = src_->Remove("r1");
    EXPECT_TRUE(removed.has_value())
        << "r1 was read after the drain: " << toString(removed.error());
    ASSERT_TRUE(PutOnSource("r2", 's'));
    RunBackupTick(*storage_);
    EXPECT_EQ(HolderBytes("r2"), std::string(kValue, 's'));
}

// R5-M1 concurrent schedules, each in a watchdog child (threadsafe death
// test: the child runs SetUp again with its own master and clients).
TEST_F(FileStorageBackupHolderTest, HeldHeartbeatAcrossRealDrainReadsNothing) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeldHeartbeatChild(/*destroy=*/false)),
                ::testing::ExitedWithCode(0), "");
}

TEST_F(FileStorageBackupHolderTest, HeldHeartbeatAcrossDestroyReadsNothing) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeldHeartbeatChild(/*destroy=*/true)),
                ::testing::ExitedWithCode(0), "");
}

// R2-Q1: each early exit of the held-heartbeat schedules returns its own
// check code with the worker joined before the captures go (exit 77 if not;
// 9 from the watchdog on a hang).
TEST_F(FileStorageBackupHolderTest, HeldHeartbeatEarlyExitsJoinFirst) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const std::tuple<bool, int, int> cases[] = {
        {false, 1, 13}, {false, 2, 2},  {false, 3, 15},
        {false, 4, 8},  {false, 5, 16}, {false, 6, 17},
        {true, 1, 13},  {true, 2, 2},   {true, 3, 15}};
    for (const auto& [destroy, inject, code] : cases) {
        SCOPED_TRACE(std::string(destroy ? "destroy" : "drain") + " inject " +
                     std::to_string(inject));
        EXPECT_EXIT(std::_Exit(HeldHeartbeatChild(destroy, inject)),
                    ::testing::ExitedWithCode(code),
                    "check " + std::to_string(code) + " failed");
    }
}

TEST_F(FileStorageBackupHolderTest, DrainBeforeTenantQueryQueriesNothing) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(ReadCancelChild(0)), ::testing::ExitedWithCode(0),
                "");
}

TEST_F(FileStorageBackupHolderTest, DrainDuringQueryResponseTransfersNothing) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(ReadCancelChild(1)), ::testing::ExitedWithCode(0),
                "");
}

class FileStorageBackupHolderPairTest : public FileStorageBackupHolderTest {
   protected:
    FileStorageBackupHolderPairTest() {
        batch_keys_ = "2";
        max_inflight_ = "9000";
    }
};

// R12-GREEN-ONLY-BEGIN (the transfer observer is new)
TEST_F(FileStorageBackupHolderTest,
       HeldQueryResponseAcrossRealDrainTransfersNothing) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeldQueryResponseChild()),
                ::testing::ExitedWithCode(0), "");
}

// T8 item 5 / MG-1: destroy while the backup transfer's completion is held.
TEST_F(FileStorageBackupHolderTest, DelayedCompletionHeldAcrossDestroy) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(DelayedCompletionAcrossDestroyChild()),
                ::testing::ExitedWithCode(0), "");
}

// R1-M2 failure controls: each early exit of the held-query schedule
// returns its own check code with the worker joined before the captures go
// (exit 77 if not; 9 from the watchdog on a hang).
TEST_F(FileStorageBackupHolderTest, HeldQueryResponseEarlyExitsJoinFirst) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const std::pair<int, int> cases[] = {{1, 13}, {2, 2}, {3, 14}, {4, 8}};
    for (const auto& [inject, code] : cases) {
        SCOPED_TRACE(inject);
        EXPECT_EXIT(std::_Exit(HeldQueryResponseChild(inject)),
                    ::testing::ExitedWithCode(code), "check");
    }
}

// T16 (R1-M2 old/new): the same four exits taken the way the 236a44f2
// harness took them. At each, the worker is still running and not joined
// when the captures would go (78, with the exit's own check code named);
// the corrected harness above joins first. Nothing is used after its
// lifetime: the control joins before it ends the child.
TEST_F(FileStorageBackupHolderTest, HeldQueryOldExitsLeaveWorkerRunning) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const std::pair<int, int> cases[] = {{1, 13}, {2, 2}, {3, 14}, {4, 8}};
    for (const auto& [inject, code] : cases) {
        SCOPED_TRACE(inject);
        EXPECT_EXIT(std::_Exit(HeldQueryResponseChild(inject, true)),
                    ::testing::ExitedWithCode(78),
                    "old exit after check " + std::to_string(code) +
                        ": worker joined=0 storage alive=1");
    }
}

class FileStorageBackupHolderWideTest : public FileStorageBackupHolderTest {
   protected:
    FileStorageBackupHolderWideTest() { max_inflight_ = "20000"; }
};

TEST_F(FileStorageBackupHolderWideTest,
       HeldPrefetchAcrossDrainWritesOnlyFirst) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeldPrefetchChild()), ::testing::ExitedWithCode(0),
                "");
}

// T16 (R1-M1 old/new): with B delayed, the 236a44f2 handshake lets A's ACK
// return and its staging go before B's ownership observation; the
// corrected oracle reports that (check 23). The corrected handshake
// (HeldPrefetchWithDelayedSecondRead) keeps A live until B's barrier.
TEST_F(FileStorageBackupHolderWideTest,
       HeldPrefetchOldHandshakeReleasesAFirst) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeldPrefetchChild(/*delay_b=*/true,
                                             /*old_handshake=*/true)),
                ::testing::ExitedWithCode(23),
                "check 23 failed: B's transfer did not complete while A held");
}

TEST_F(FileStorageBackupHolderWideTest, HeldPrefetchWithDelayedSecondRead) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeldPrefetchChild(/*delay_b=*/true)),
                ::testing::ExitedWithCode(0), "");
}
// R12-GREEN-ONLY-END

TEST_F(FileStorageBackupHolderPairTest, DrainBetweenTenantGroupsStopsQueries) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(BetweenTenantsChild()), ::testing::ExitedWithCode(0),
                "");
}

// Real tenants (MST-18 / RPC-04 payloads, R7-M2 #3): a multi-tenant master
// with "default" and "tenant-a" registered, one writer Client per tenant,
// both writing into src_'s segment (src_ is the backup source of both), and
// the real holder worker reading, writing and registering the backups.
class FileStorageBackupTenantTest : public FileStorageBackupHolderTest {
   protected:
    FileStorageBackupTenantTest() {
        multi_tenant_ = true;
        // Room for several batches at once: these tests put both tenants'
        // objects before the worker runs (a full candidate would drop the
        // later batch as unplaced, MST-07).
        max_inflight_ = "40000";
    }
    bool PutD(const std::string& key, char fill, size_t size = kValue) {
        return PutAs(*src_, *src_alloc_, key, fill, size);
    }
    bool PutT(const std::string& key, char fill, size_t size = kValue) {
        return PutAs(*srct_, *srct_alloc_, key, fill, size);
    }
    std::string Bytes(const std::string& tenant, const std::string& key,
                      size_t n = kValue) {
        return HolderRead(*storage_, TenantId(tenant).MakeScopedKey(key), n);
    }
    // Size of the holder's backup of `key` as `reader` (its own tenant)
    // sees it through the backup query; -1 if none. `resolved` gets the
    // tenant the master resolved the query to.
    int64_t BackupSizeFor(Client& reader, const std::string& key,
                          std::string* resolved = nullptr) {
        auto r = reader.BatchQueryBackup({key});
        if (r.size() != 1 || !r[0]) return -1;
        if (resolved && r[0]->resolved_tenant_id) {
            *resolved = *r[0]->resolved_tenant_id;
        }
        for (const auto& d : r[0]->replicas) {
            if (!d.is_local_disk_replica()) continue;
            const auto& l = d.get_local_disk_descriptor();
            if (l.client_id == holder_->getClientId()) {
                return static_cast<int64_t>(l.object_size);
            }
        }
        return -1;
    }
    std::vector<BackupResidency> Residency(
        const std::vector<std::pair<std::string, std::string>>& items) {
        std::vector<BackupRefreshItem> keys;
        for (const auto& [t, k] : items) keys.push_back({t, k});
        auto r = holder_->QueryBackupResidency(keys);
        std::vector<BackupResidency> out;
        if (!r) return out;
        for (auto v : *r) out.push_back(static_cast<BackupResidency>(v));
        return out;
    }
};

TEST_F(FileStorageBackupTenantTest, EqualLengthPayloadsStayInTheirTenants) {
    const std::string d(kValue, 'd'), t(kValue, 't');
    ASSERT_TRUE(PutD("shared", 'd'));
    ASSERT_TRUE(PutT("shared", 't'));
    ASSERT_TRUE(TickUntil([&] {
        return Bytes("default", "shared") == d && Bytes(kTenant, "shared") == t;
    })) << "default="
        << Bytes("default", "shared").substr(0, 4)
        << " tenant-a=" << Bytes(kTenant, "shared").substr(0, 4);
    std::string rd, rt;
    EXPECT_EQ(BackupSizeFor(*src_, "shared", &rd),
              static_cast<int64_t>(kValue));
    EXPECT_EQ(rd, "default");
    EXPECT_EQ(BackupSizeFor(*srct_, "shared", &rt),
              static_cast<int64_t>(kValue));
    EXPECT_EQ(rt, kTenant);
    EXPECT_EQ(Residency({{kTenant, "shared"}, {"default", "shared"}}),
              (std::vector<BackupResidency>{BackupResidency::kInDram,
                                            BackupResidency::kInDram}));

    // Unequal-length control.
    ASSERT_TRUE(PutD("odd", 'o'));
    ASSERT_TRUE(PutT("odd", 'p', 2048));
    EXPECT_TRUE(TickUntil([&] {
        return Bytes("default", "odd") == std::string(kValue, 'o') &&
               Bytes(kTenant, "odd", 2048) == std::string(2048, 'p');
    }));
    EXPECT_EQ(BackupSizeFor(*srct_, "odd"), 2048);

    // Generation replacement in the default tenant: same size, then a
    // different size. tenant-a's bytes stay.
    ASSERT_TRUE(src_->Remove("shared", true).has_value());
    ASSERT_TRUE(PutD("shared", 'e'));
    EXPECT_TRUE(TickUntil([&] {
        return Bytes("default", "shared") == std::string(kValue, 'e');
    })) << "same-size replacement not backed up";
    EXPECT_EQ(Bytes(kTenant, "shared"), t);
    ASSERT_TRUE(src_->Remove("shared", true).has_value());
    ASSERT_TRUE(PutD("shared", 'f', 2048));
    EXPECT_TRUE(TickUntil([&] {
        return Bytes("default", "shared", 2048) == std::string(2048, 'f');
    })) << "different-size replacement not backed up";
    EXPECT_EQ(BackupSizeFor(*src_, "shared"), 2048);
    EXPECT_EQ(Bytes(kTenant, "shared"), t);
    EXPECT_EQ(BackupSizeFor(*srct_, "shared"), static_cast<int64_t>(kValue));

    // Deleting the key in tenant-a leaves the default tenant's object.
    ASSERT_TRUE(srct_->Remove("shared", true).has_value());
    EXPECT_EQ(BackupSizeFor(*srct_, "shared"), -1);
    EXPECT_EQ(Residency({{kTenant, "shared"}, {"default", "shared"}}),
              (std::vector<BackupResidency>{BackupResidency::kMissing,
                                            BackupResidency::kInDram}));
    EXPECT_EQ(BackupSizeFor(*src_, "shared"), 2048);
    EXPECT_EQ(Bytes("default", "shared", 2048), std::string(2048, 'f'));
}

class FileStorageBackupTenantPairTest : public FileStorageBackupTenantTest {
   protected:
    FileStorageBackupTenantPairTest() {
        batch_keys_ = "2";
        max_inflight_ = "9000";
    }
    // R7-M2 #3: one real batch with a task of each tenant (both DRAM copies
    // in src_'s segment). The drain is latched when the second tenant group
    // starts, whichever tenant the worker takes first; the first group was
    // queried and its transfer ran, the second is never queried. Oracle
    // independent of the order: exactly one of the two source objects
    // holds a read lease. Child body: 0 on success, 9 from the watchdog.
    // R8-M2 (round 1): the first group's transfer must have completed
    // successfully with its full payload while its staging is owned before
    // the cancellation is admitted; the queried key holds exactly
    // OBJECT_HAS_LEASE, the unqueried one is removable, and earlier real
    // backups keep their bytes.
    int RealTenantsChild() {
        StartWatchdog("real two-tenant cancel");
        // Earlier real backups (one batch of two), which must survive.
        if (!PutD("s1", 's') || !PutT("s2", 'u')) return Fail(20, "put seeds");
        if (!TickUntil([&] {
                return Bytes("default", "s1") == std::string(kValue, 's') &&
                       Bytes(kTenant, "s2") == std::string(kValue, 'u');
            })) {
            return Fail(21, "seed backups not written");
        }
        if (!PutD("t1", 'd') || !PutT("t2", 't')) return Fail(10, "put");
        auto resp = holder_->BackupObjectHeartbeat(holder_ep_, 0, 1LL << 30);
        if (!resp || resp->batches.size() != 1 ||
            resp->batches[0].tasks.size() != 2) {
            return Fail(11, "expected one batch of two tasks");
        }
        std::set<std::string> tenants;
        for (const auto& task : resp->batches[0].tasks) {
            tenants.insert(task.tenant_id);
        }
        if (tenants != std::set<std::string>{"default", kTenant}) {
            return Fail(12, "batch does not carry both tenants");
        }
        const uint64_t batch = resp->batches[0].batch_id;
        if (!StagingFree()) return Fail(19, "staging probe fails even idle");
        std::vector<std::tuple<uint64_t, size_t, bool>> acks;
        SetAckObserver(*storage_, [&](uint64_t id, size_t n, bool ok) {
            acks.emplace_back(id, n, ok);
        });
        const auto expected = [&](const std::string& tenant,
                                  const std::string& key) {
            if (tenant == "default" && key == "t1") {
                return std::string(kValue, 'd');
            }
            if (tenant == kTenant && key == "t2") {
                return std::string(kValue, 't');
            }
            return std::string("?");
        };
        std::vector<Transfer> transfers;
        SetTransferObserver(*storage_,
                            [&](const std::string& t, const std::string& k,
                                ErrorCode e, const Slice& s) {
                                transfers.push_back(CopyTransfer(t, k, e, s));
                            });
        StageLog log;
        bool first_complete_before_cancel = false;
        FileStorage* fs = storage_.get();
        SetStageHook(*storage_, [&, fs](const char* stage, size_t) {
            if (std::string(stage) == "before_query" &&
                log.Get("before_query") == 1) {
                // The second group starts: cancel only once the first
                // group's transfer completed with its full payload.
                first_complete_before_cancel =
                    transfers.size() == 1 &&
                    transfers[0].result == ErrorCode::OK &&
                    transfers[0].bytes ==
                        expected(transfers[0].tenant, transfers[0].key);
                SetDrainingFlag(*fs, true);
            }
            log.Hit(stage);
        });
        RunBatches(*storage_, resp->batches);  // synchronous: nothing left
        SetStageHook(*storage_, nullptr);
        SetAckObserver(*storage_, nullptr);
        SetTransferObserver(*storage_, nullptr);
        SetDrainingFlag(*storage_, false);
        if (!first_complete_before_cancel || transfers.size() != 1) {
            return Fail(22,
                        "first group's transfer did not complete with its "
                        "payload before the cancellation (transfers=" +
                            std::to_string(transfers.size()) + ")");
        }
        if (log.Get("before_query") != 2) return Fail(13, "no second group");
        if (log.Get("after_query") != 1 || log.Get("before_transfer") != 1) {
            return Fail(4, "queries=" + std::to_string(log.Get("after_query")) +
                               " transfers=" +
                               std::to_string(log.Get("before_transfer")));
        }
        // Drained before the write: nothing stored or registered.
        if (Bytes("default", "t1") != "" || Bytes(kTenant, "t2") != "") {
            return Fail(5, "cancelled batch stored");
        }
        if (BackupSizeFor(*src_, "t1") != -1 ||
            BackupSizeFor(*srct_, "t2") != -1) {
            return Fail(14, "cancelled batch registered");
        }
        if (acks.size() != 1 || std::get<0>(acks[0]) != batch ||
            std::get<1>(acks[0]) != 0 || !std::get<2>(acks[0])) {
            return Fail(15, "expected one successful empty ACK, got " +
                                std::to_string(acks.size()));
        }
        auto snap = HolderReservation();
        if (!snap || !snap->inflight_batches.empty()) {
            return Fail(16, "batch still reserved");
        }
        // Ordinary staging released: the same probe as before the batch.
        if (!StagingFree()) return Fail(17, "staging not released");
        // The transferred group's object holds its query's read lease
        // (exactly OBJECT_HAS_LEASE); the cancelled group's was never
        // queried and is removed.
        const bool first_default = transfers[0].tenant == "default";
        auto queried = first_default ? src_->Remove("t1") : srct_->Remove("t2");
        auto unqueried =
            first_default ? srct_->Remove("t2") : src_->Remove("t1");
        if (queried.has_value() ||
            queried.error() != ErrorCode::OBJECT_HAS_LEASE) {
            return Fail(3, "queried key: expected OBJECT_HAS_LEASE, got " +
                               (queried ? std::string("OK")
                                        : toString(queried.error())));
        }
        if (!unqueried.has_value()) {
            return Fail(23, "unqueried key not removable: " +
                                toString(unqueried.error()));
        }
        if (Bytes("default", "s1") != std::string(kValue, 's') ||
            Bytes(kTenant, "s2") != std::string(kValue, 'u')) {
            return Fail(24, "seed backups lost");
        }
        // The reservation is back: two new 4096-byte backups (8192 bytes)
        // fit the 9000-byte limit together, each with its own bytes.
        if (!PutD("t3", 'x') || !PutT("t4", 'y')) return Fail(18, "put t3/t4");
        if (!TickUntil([&] {
                return Bytes("default", "t3") == std::string(kValue, 'x') &&
                       Bytes(kTenant, "t4") == std::string(kValue, 'y');
            })) {
            return Fail(6, "t3/t4 not backed up");
        }
        return 0;
    }
};

TEST_F(FileStorageBackupTenantPairTest, DrainBetweenRealTenantGroups) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(RealTenantsChild()), ::testing::ExitedWithCode(0),
                "");
}

// P2-01 (review 2026-11-25): within the batch window K is put, removed and
// put again (v2), on a real source and holder; the same bare key is also
// put in tenant-a. Before the fix the batch carried K/v1 and K/v2: the
// holder read v2's bytes once, registered them as v1, the master refused
// them as stale and v2 got no backup. Equal lengths, then different
// lengths; tenant-a's object is the control.
TEST_F(FileStorageBackupTenantPairTest, SameKeyPutTwiceInOneBatchIsBackedUp) {
    ASSERT_TRUE(PutD("dup", 'a'));
    ASSERT_TRUE(src_->Remove("dup", true).has_value());
    ASSERT_TRUE(PutD("dup", 'b'));
    ASSERT_TRUE(PutT("dup", 'c'));
    EXPECT_TRUE(TickUntil([&] {
        return Bytes("default", "dup") == std::string(kValue, 'b') &&
               Bytes(kTenant, "dup") == std::string(kValue, 'c');
    })) << "default="
        << Bytes("default", "dup").substr(0, 1)
        << " tenant-a=" << Bytes(kTenant, "dup").substr(0, 1);
    EXPECT_EQ(BackupSizeFor(*src_, "dup"), static_cast<int64_t>(kValue));
    EXPECT_EQ(BackupSizeFor(*srct_, "dup"), static_cast<int64_t>(kValue));

    ASSERT_TRUE(PutD("dl", 'd'));
    ASSERT_TRUE(src_->Remove("dl", true).has_value());
    ASSERT_TRUE(PutD("dl", 'e', 2048));
    ASSERT_TRUE(PutT("dl", 'f'));
    EXPECT_TRUE(TickUntil([&] {
        return Bytes("default", "dl", 2048) == std::string(2048, 'e') &&
               Bytes(kTenant, "dl") == std::string(kValue, 'f');
    })) << "different-length replacement not backed up";
    EXPECT_EQ(BackupSizeFor(*src_, "dl"), 2048);
    EXPECT_EQ(BackupSizeFor(*srct_, "dl"), static_cast<int64_t>(kValue));
    auto snap = HolderReservation();
    ASSERT_TRUE(snap.has_value());
    EXPECT_TRUE(snap->inflight_batches.empty());
    EXPECT_EQ(snap->inflight_bytes, 0u);
}

// P2-01, holder side: a batch that still carries two tasks of one key (an
// older master, or a hand-built batch as here) is read once and registered
// under the later task, which is the newer put. The earlier task's version
// is stale on the master.
TEST_F(FileStorageBackupTenantPairTest, DuplicateTaskInBatchUsesTheLaterOne) {
    ASSERT_TRUE(PutD("wd", 'w'));
    ASSERT_TRUE(PutD("wx", 'x'));
    auto resp = holder_->BackupObjectHeartbeat(holder_ep_, 0, 1LL << 30);
    ASSERT_TRUE(resp.has_value());
    ASSERT_EQ(resp->batches.size(), 1u);
    auto batch = resp->batches[0];
    ASSERT_EQ(batch.tasks.size(), 2u);
    auto it = std::find_if(batch.tasks.begin(), batch.tasks.end(),
                           [](const auto& t) { return t.key == "wd"; });
    ASSERT_NE(it, batch.tasks.end());
    BackupTaskItem older = *it;
    older.version -= 1;
    batch.tasks.insert(batch.tasks.begin(), older);
    RunBatches(*storage_, {batch});
    EXPECT_EQ(Bytes("default", "wd"), std::string(kValue, 'w'));
    EXPECT_EQ(BackupSizeFor(*src_, "wd"), static_cast<int64_t>(kValue));
    EXPECT_EQ(Bytes("default", "wx"), std::string(kValue, 'x'));
    auto snap = HolderReservation();
    ASSERT_TRUE(snap.has_value());
    EXPECT_TRUE(snap->inflight_batches.empty());
}

// C1 through the real worker path: the holder's staging is used up by
// quarantined allocations, so a backup cannot be read (dropped, its batch
// released). Once the quarantine time has passed on the backup clock, the
// next backup reuses the memory.
TEST_F(FileStorageBackupHolderTest, QuarantinedStagingReusedAfterItsTime) {
    ASSERT_TRUE(PutOnSource("k0", 'a'));
    RunBackupTick(*storage_);
    ASSERT_EQ(HolderBytes("k0"), std::string(kValue, 'a'));
    auto now = std::make_shared<Clock::time_point>(Clock::now());
    SetBackupClock(*storage_, [now] { return *now; });
    SetStagingTimes(*storage_, 600000, 0);
    int quarantined = 0;
    for (int64_t size : {int64_t{1} << 20, int64_t{64} << 10, int64_t{4096}}) {
        for (;;) {
            auto a = AllocateStaging(
                *storage_, {"fill" + std::to_string(quarantined)}, {size});
            if (!a) break;
            Quarantine(*storage_, std::move(a.value()));
            ++quarantined;
        }
    }
    ASSERT_GT(quarantined, 0);
    ASSERT_FALSE(AllocateStaging(*storage_, {"probe"}, {4096}).has_value());
    ASSERT_TRUE(PutOnSource("k1", 'b'));
    RunBackupTick(*storage_);
    EXPECT_EQ(HolderBytes("k1"), "");  // no staging: dropped
    EXPECT_GE(StagingStats(*storage_).failed, 1u);
    auto snap = HolderReservation();
    ASSERT_TRUE(snap.has_value());
    EXPECT_TRUE(snap->inflight_batches.empty());
    *now += std::chrono::milliseconds(600000);
    ASSERT_TRUE(PutOnSource("k2", 'c'));
    EXPECT_TRUE(TickUntil([&] {
        return HolderBytes("k2") == std::string(kValue, 'c');
    })) << "quarantined staging not reused after its time";
    EXPECT_EQ(StagingStats(*storage_).quarantined_batches, 0u);
    EXPECT_EQ(StagingStats(*storage_).released,
              static_cast<uint64_t>(quarantined));
    EXPECT_EQ(HolderBytes("k0"), std::string(kValue, 'a'));
}

// T8 item 5 / MG-1, partial submit: the backup read of k1 posts the first
// half of its bytes, then the submit is reported as failed. Besides the
// quarantine (FaultedReadIsQuarantined): the posted half lands late in the
// quarantined memory and the other half is never written; the next backup's
// staging does not overlap it; the quarantine ends only when its configured
// time has passed on the backup clock, and then the memory goes back to the
// allocator.
TEST_F(FileStorageBackupHolderTest, PartialSubmitQuarantinesStaging) {
    auto now = std::make_shared<Clock::time_point>(Clock::now());
    SetBackupClock(*storage_, [now] { return *now; });
    SetStagingTimes(*storage_, 600000, 0);
    OneShot shot;
    auto faults = std::make_shared<TransferFaultsForTest>();
    faults->partial_submit =
        [&shot](
            const std::vector<TransferRequest>& r) -> std::optional<uint64_t> {
        if (r.empty() || r[0].opcode != TransferRequest::READ || !shot.Take()) {
            return std::nullopt;
        }
        return kValue / 2;
    };
    const char* q = nullptr;
    FaultedReadIsQuarantined(faults, shot, &q);
    ASSERT_NE(q, nullptr);
    EXPECT_TRUE(LandsIn(q, std::string(kValue / 2, 'b')))
        << "the posted half did not land in the quarantined memory";
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(std::string(q + kValue / 2, kValue / 2),
              std::string(kValue / 2, 'P'))
        << "bytes beyond the posted half were written";
    // The next backup is staged elsewhere and stored with its own bytes.
    ASSERT_TRUE(PutOnSource("k2", 'c'));
    EXPECT_TRUE(TickUntil(
        [&] { return HolderBytes("k2") == std::string(kValue, 'c'); }));
    {
        const auto ev = slog_.Get();
        const int h = StagingLog::Find(ev, "hold");
        ASSERT_GE(h, 0) << StagingLog::Format(ev, staging_->getBase());
        EXPECT_TRUE(ev[h].ptr + ev[h].bytes <= q || ev[h].ptr >= q + kValue)
            << "k2 staged over the quarantined range";
        EXPECT_EQ(StagingLog::Count(ev, "free", q), 0);
    }
    *now += std::chrono::milliseconds(599999);
    ReleaseExpiredQuarantine(*storage_);
    EXPECT_EQ(StagingLog::Count(slog_.Get(), "quarantine_expired"), 0);
    EXPECT_EQ(StagingLog::Count(slog_.Get(), "free", q), 0);
    *now += std::chrono::milliseconds(1);
    ReleaseExpiredQuarantine(*storage_);
    const auto ev = slog_.Get();
    const int expired = StagingLog::Find(ev, "quarantine_expired", q);
    const int freed = StagingLog::Find(ev, "free", q);
    ASSERT_GE(expired, 0) << StagingLog::Format(ev, staging_->getBase());
    ASSERT_GE(freed, 0) << StagingLog::Format(ev, staging_->getBase());
    EXPECT_LT(expired, freed);
    EXPECT_EQ(StagingStats(*storage_).quarantined_batches, 0u);
    EXPECT_EQ(HolderBytes("k1"), "");
    EXPECT_EQ(HolderBytes("k0"), std::string(kValue, 'a'));
}

// MG-1, status query failure: k1's transfer is fully posted, but its first
// status query fails. The staging is quarantined and k1's full bytes land
// late in the quarantined memory. Destroyed before the quarantine time
// ends, the FileStorage leaks the allocation and keeps the staging
// registered: no unregister call, nothing freed.
TEST_F(FileStorageBackupHolderTest, StatusQueryFailureQuarantinesAndLeaks) {
    MarkStagingRegistered(*storage_);
    std::atomic<int> unregister_calls{0};
    SetStagingUnregister(*storage_,
                         [&](void*) -> tl::expected<void, ErrorCode> {
                             ++unregister_calls;  // TearDown unregisters
                             return {};
                         });
    OneShot shot;
    auto faults = std::make_shared<TransferFaultsForTest>();
    faults->status_query_fails = [&shot](BatchID, size_t) {
        return shot.Take();
    };
    const char* q = nullptr;
    FaultedReadIsQuarantined(faults, shot, &q);
    ASSERT_NE(q, nullptr);
    EXPECT_TRUE(LandsIn(q, std::string(kValue, 'b')))
        << "k1's bytes did not land in the quarantined memory";
    storage_.reset();  // before the quarantine time ends
    const auto ev = slog_.Get();
    const auto trace = StagingLog::Format(ev, staging_->getBase());
    Trace("events after destroy: " + trace);
    EXPECT_EQ(unregister_calls.load(), 0) << trace;
    EXPECT_EQ(StagingLog::Count(ev, "unregister"), 0) << trace;
    EXPECT_EQ(StagingLog::Count(ev, "leak", q), 1) << trace;
    EXPECT_EQ(StagingLog::Count(ev, "staging_leaked"), 1) << trace;
    EXPECT_EQ(StagingLog::Count(ev, "free", q), 0) << trace;
    EXPECT_EQ(StagingLog::Count(ev, "staging_freed"), 0) << trace;
    EXPECT_EQ(std::string(q, kValue), std::string(kValue, 'b'));
}

// MG-1, delayed completion that times out: k1's transfer is fully posted
// and its wait ends as timed out at once, the transfer still running. The
// staging is quarantined and k1's bytes land late in the quarantined
// memory only.
TEST_F(FileStorageBackupHolderTest, CompletionTimeoutQuarantinesStaging) {
    OneShot shot;
    auto faults = std::make_shared<TransferFaultsForTest>();
    faults->delay_completion = [&shot](BatchID) { return shot.Take(); };
    const char* q = nullptr;
    FaultedReadIsQuarantined(faults, shot, &q);
    ASSERT_NE(q, nullptr);
    EXPECT_TRUE(LandsIn(q, std::string(kValue, 'b')))
        << "k1's bytes did not land in the quarantined memory";
    EXPECT_EQ(StagingLog::Count(slog_.Get(), "free", q), 0);
}

// P2-02 (review 2026-11-25): with the default POSIX two-slot staging, the
// pipeline reads batch i+1 while batch i is written; two batches need not
// fit side by side (allocator rounding, e.g. 256 x (1 MiB + 1)). Here a
// 4 MiB staging and two batches of three 1 MiB objects: the second read
// cannot get staging while the first batch holds it. It must wait for the
// first batch to give it back, then read and write normally (mode 0);
// with a short wait it is dropped with an empty ACK and counted (mode 1);
// a drain ends the wait at once (mode 2).
class FileStorageBackupHolderBigTest : public FileStorageBackupHolderTest {
   protected:
    static constexpr size_t kBig = 1 << 20;
    FileStorageBackupHolderBigTest() {
        batch_keys_ = "3";
        max_inflight_ = "16000000";
    }
    bool PutBig(const std::string& key, char fill) {
        return PutAs(*src_, *src_alloc_, key, fill, kBig);
    }
    std::string BigBytes(const std::string& key) {
        return HolderRead(*storage_, TenantId::Default().MakeScopedKey(key),
                          kBig);
    }
    int TwoBatchChild(int mode) {
        StartWatchdog("two batches, staging for one");
        if (mode == 1) SetStagingTimes(*storage_, 600000, 300);
        for (int i = 0; i < 6; ++i) {
            if (!PutBig("b" + std::to_string(i), static_cast<char>('A' + i))) {
                return Fail(10, "put b" + std::to_string(i));
            }
        }
        StageLog log;
        std::mutex mu;
        std::vector<std::tuple<uint64_t, size_t, bool>> acks;
        std::atomic<bool> held{false};
        std::atomic<int64_t> drain_to_drop_ms{-1};
        FileStorage* fs = storage_.get();
        SetStageHook(*storage_,
                     [&](const char* stage, size_t) { log.Hit(stage); });
        SetAckObserver(*storage_, [&, fs](uint64_t id, size_t n, bool ok) {
            {
                std::lock_guard<std::mutex> l(mu);
                acks.emplace_back(id, n, ok);
            }
            if (n == 0 || held.exchange(true)) return;
            // The first batch is registered; its staging is still held.
            // Wait until the second read either waits for staging (fixed)
            // or has tried its allocation and failed (previous code). Not
            // by counting queries: a batch read in chunks queries more than
            // once, so a second query need not be the second batch's.
            {
                std::unique_lock<std::mutex> l(log.mu);
                log.cv.wait_for(l, std::chrono::seconds(10), [&] {
                    return log.count["staging_wait"] >= 1 ||
                           log.count["staging_alloc_failed"] >= 1;
                });
            }
            if (mode == 0 && log.Get("staging_wait") == 0) {
                // Previous code: let its failed allocation finish.
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            } else if (mode == 1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1500));
            } else if (mode == 2) {
                const auto t0 = Clock::now();
                SetDrainingFlag(*fs, true);
                WakeWorker(*fs);
                while (StagingStats(*fs).failed == 0 &&
                       Clock::now() - t0 < std::chrono::seconds(5)) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                if (StagingStats(*fs).failed > 0) {
                    drain_to_drop_ms =
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            Clock::now() - t0)
                            .count();
                }
            }
        });
        RunBackupTick(*storage_);
        SetStageHook(*storage_, nullptr);
        SetAckObserver(*storage_, nullptr);
        SetDrainingFlag(*storage_, false);
        if (!held) return Fail(11, "no registration ACK");
        // Which keys each ACKed batch stored: two batches, three keys each.
        int stored = 0;
        for (int i = 0; i < 6; ++i) {
            const auto key = "b" + std::to_string(i);
            const auto bytes = BigBytes(key);
            if (bytes == std::string(kBig, static_cast<char>('A' + i))) {
                ++stored;
            } else if (!bytes.empty()) {
                return Fail(12, key + " stored with wrong bytes");
            }
        }
        if (acks.size() != 2 || std::get<1>(acks[0]) != 3 ||
            !std::get<2>(acks[0]) || !std::get<2>(acks[1])) {
            return Fail(
                13, "unexpected ACKs (" + std::to_string(acks.size()) + ")");
        }
        const auto st = StagingStats(*storage_);
        if (mode == 0) {
            if (stored != 6 || std::get<1>(acks[1]) != 3) {
                return Fail(2, "second batch not backed up: stored=" +
                                   std::to_string(stored));
            }
            if (st.waits != 1 || st.failed != 0) {
                return Fail(14, "waits=" + std::to_string(st.waits) +
                                    " failed=" + std::to_string(st.failed));
            }
        } else {
            if (stored != 3 || std::get<1>(acks[1]) != 0) {
                return Fail(3, "second batch should be dropped: stored=" +
                                   std::to_string(stored));
            }
            if (st.waits != 1 || st.failed != 1) {
                return Fail(15, "waits=" + std::to_string(st.waits) +
                                    " failed=" + std::to_string(st.failed));
            }
            if (mode == 2 &&
                (drain_to_drop_ms < 0 || drain_to_drop_ms > 2000)) {
                return Fail(4, "drain did not end the wait promptly (" +
                                   std::to_string(drain_to_drop_ms.load()) +
                                   " ms)");
            }
        }
        auto snap = HolderReservation();
        if (!snap || !snap->inflight_batches.empty()) {
            return Fail(16, "reservations left");
        }
        // All staging back: the 4 MiB arena takes three 1 MiB objects again.
        if (!AllocateStaging(*storage_, {"p0", "p1", "p2"},
                             {1 << 20, 1 << 20, 1 << 20})) {
            return Fail(17, "staging not released");
        }
        return 0;
    }
};

// R0-B1 (round-0 review): batch A releases its last staging handle after
// B's allocation failed but before B checks whether to wait. B is held at
// the stage right after its failed allocation (before the wait lock) until
// A's staging is gone, then resumes. Previously B found no other holder and
// gave up although the staging was free: dropped, empty ACK, counted. It
// must retry and back up all its objects. Mode 1 (control, not red): a
// drain requested while B is held ends B without a retry or a hang.
class FileStorageBackupHolderRaceTest : public FileStorageBackupHolderBigTest {
   protected:
    int ReleaseBeforeWaitCheckChild(int mode) {
        StartWatchdog("release between failed allocation and wait check");
        for (int i = 0; i < 6; ++i) {
            if (!PutBig("b" + std::to_string(i), static_cast<char>('A' + i))) {
                return Fail(10, "put b" + std::to_string(i));
            }
        }
        // Two-way handshake (R1-M1): A is held inside its registration ACK
        // (its staging still owned) until B's allocation has failed; B is
        // held right after that failure until A's last staging handle is
        // gone. So B's failure always happens while A owns the staging,
        // whenever B's prefetch thread happens to start. Mode 2 also delays
        // B's start on purpose: B's first stage waits until A is inside its
        // ACK.
        std::mutex mu;
        std::vector<std::tuple<uint64_t, size_t, bool>> acks;
        std::vector<std::string> events;
        const auto event = [&](const char* e) {
            std::lock_guard<std::mutex> l(mu);
            events.emplace_back(e);
        };
        Gate a_in_ack, b_failed;
        std::atomic<int> queries{0}, alloc_failures{0};
        std::atomic<bool> released_seen{false}, ack_wait_timed_out{false};
        FileStorage* fs = storage_.get();
        SetStageHook(*storage_, [&, fs](const char* stage, size_t) {
            const std::string s(stage);
            if (s == "before_query" && queries++ == 1) {  // B's first stage
                if (mode == 2) a_in_ack.Wait();
                event("B_start");
                return;
            }
            if (s != "staging_alloc_failed" || alloc_failures++ != 0) return;
            event("B_alloc_failed");
            b_failed.Open();
            // Hold B until A's last staging handle is gone (bounded; a stop
            // ends the hold too). This polls a condition, it does not order
            // anything by time.
            const auto until = Clock::now() + std::chrono::seconds(10);
            while (StagingHeld(*fs) != 0 && !StopLatched(*fs) &&
                   Clock::now() < until) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            released_seen = StagingHeld(*fs) == 0;
            event("B_resume");
            if (mode == 1) {
                SetDrainingFlag(*fs, true);
                WakeWorker(*fs);
            }
        });
        SetAckObserver(*storage_, [&](uint64_t id, size_t n, bool ok) {
            {
                std::lock_guard<std::mutex> l(mu);
                acks.emplace_back(id, n, ok);
            }
            if (n == 0 || a_in_ack.IsOpen()) return;
            event("A_ack");
            a_in_ack.Open();
            if (!b_failed.Wait(std::chrono::seconds(10))) {
                ack_wait_timed_out = true;
            }
            event("A_ack_return");
        });
        // Whatever happens, both sides are released before the captures go.
        ScopeGuard open_all{[&] {
            a_in_ack.Open();
            b_failed.Open();
        }};
        RunBackupTick(*storage_);  // synchronous: joins B's prefetch
        SetStageHook(*storage_, nullptr);
        SetAckObserver(*storage_, nullptr);
        SetDrainingFlag(*storage_, false);
        std::string trace;
        for (const auto& e : events) trace += e + " ";
        std::fprintf(stderr, "event order: %s\n", trace.c_str());
        Trace("mode=" + std::to_string(mode) + " event order: " + trace);
        if (alloc_failures == 0) return Fail(11, "no failed allocation");
        if (ack_wait_timed_out) {
            return Fail(12, "B's allocation never failed while A held staging");
        }
        if (!released_seen) {
            return Fail(12, "first batch's staging not released while held");
        }
        const auto at = [&](const char* e) {
            return std::find(events.begin(), events.end(), e) - events.begin();
        };
        if (!(at("A_ack") < at("A_ack_return") &&
              at("B_alloc_failed") < at("A_ack_return") &&
              at("A_ack_return") < at("B_resume") &&
              (mode != 2 || at("A_ack") < at("B_start")))) {
            return Fail(20, "unexpected event order: " + trace);
        }
        int stored = 0;
        for (int i = 0; i < 6; ++i) {
            const auto key = "b" + std::to_string(i);
            const auto bytes = BigBytes(key);
            if (bytes == std::string(kBig, static_cast<char>('A' + i))) {
                ++stored;
            } else if (!bytes.empty()) {
                return Fail(13, key + " stored with wrong bytes");
            }
        }
        const auto st = StagingStats(*storage_);
        if (acks.size() != 2 || std::get<1>(acks[0]) != 3 ||
            !std::get<2>(acks[0]) || !std::get<2>(acks[1])) {
            return Fail(
                14, "unexpected ACKs (" + std::to_string(acks.size()) + ")");
        }
        if (mode != 1) {
            if (stored != 6 || std::get<1>(acks[1]) != 3 || st.failed != 0) {
                return Fail(2, "second batch dropped: stored=" +
                                   std::to_string(stored) +
                                   " failed=" + std::to_string(st.failed));
            }
        } else if (stored != 3 || std::get<1>(acks[1]) != 0 || st.failed != 1 ||
                   st.waits != 0) {
            return Fail(3, "drain: stored=" + std::to_string(stored) +
                               " failed=" + std::to_string(st.failed) +
                               " waits=" + std::to_string(st.waits));
        }
        auto snap = HolderReservation();
        if (!snap || !snap->inflight_batches.empty()) {
            return Fail(15, "reservations left");
        }
        if (StagingHeld(*storage_) != 0) return Fail(16, "staging still held");
        if (!AllocateStaging(*storage_, {"p0", "p1", "p2"},
                             {1 << 20, 1 << 20, 1 << 20})) {
            return Fail(17, "staging not released");
        }
        return 0;
    }
};

TEST_F(FileStorageBackupHolderRaceTest, ReleaseBeforeWaitCheckIsRetried) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(ReleaseBeforeWaitCheckChild(0)),
                ::testing::ExitedWithCode(0), "");
}

// R1-M1: the same schedule with B's prefetch start delayed until A is
// inside its ACK (the order in which the round-1 harness could miss B's
// failure altogether).
TEST_F(FileStorageBackupHolderRaceTest, DelayedSecondBatchStillRetried) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(ReleaseBeforeWaitCheckChild(2)),
                ::testing::ExitedWithCode(0), "");
}

// R12-GREEN-ONLY-BEGIN (control: also passes on 1eab90b8)
TEST_F(FileStorageBackupHolderRaceTest, DrainAtFailedAllocationEndsTheBatch) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(ReleaseBeforeWaitCheckChild(1)),
                ::testing::ExitedWithCode(0), "");
}
// R12-GREEN-ONLY-END

TEST_F(FileStorageBackupHolderBigTest, SecondBatchWaitsForFirstBatchStaging) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(TwoBatchChild(0)), ::testing::ExitedWithCode(0), "");
}

TEST_F(FileStorageBackupHolderBigTest, StagingWaitIsBoundedAndCounted) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(TwoBatchChild(1)), ::testing::ExitedWithCode(0), "");
}

TEST_F(FileStorageBackupHolderBigTest, DrainEndsTheStagingWait) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(TwoBatchChild(2)), ::testing::ExitedWithCode(0), "");
}

// Chunked backup reads (three 1 MiB objects, one batch). Default chunking
// (1 MiB chunks, 64 MiB query windows): one leased query, three transfers,
// all written. Query per chunk (window 0): three queries. Chunks smaller
// than an object: still one object per transfer.
TEST_F(FileStorageBackupHolderBigTest, ChunksShareOneQueryWindow) {
    StageLog log;
    SetStageHook(*storage_, [&](const char* stage, size_t) { log.Hit(stage); });
    const auto put3 = [&](const std::string& p, char c) {
        for (int i = 0; i < 3; ++i) {
            ASSERT_TRUE(
                PutBig(p + std::to_string(i), static_cast<char>(c + i)));
        }
    };
    const auto backed = [&](const std::string& p, char c) {
        for (int i = 0; i < 3; ++i) {
            if (BigBytes(p + std::to_string(i)) !=
                std::string(kBig, static_cast<char>(c + i))) {
                return false;
            }
        }
        return true;
    };
    put3("w", 'a');
    ASSERT_TRUE(TickUntil([&] { return backed("w", 'a'); }));
    EXPECT_EQ(log.Get("after_query"), 1);
    EXPECT_EQ(log.Get("before_transfer"), 3);

    SetReadChunking(*storage_, 1 << 20, 0);  // a query per chunk
    put3("q", 'k');
    ASSERT_TRUE(TickUntil([&] { return backed("q", 'k'); }));
    EXPECT_EQ(log.Get("after_query"), 1 + 3);
    EXPECT_EQ(log.Get("before_transfer"), 3 + 3);

    SetReadChunking(*storage_, 4096, 64 << 20);  // chunk < one object
    put3("s", 'r');
    ASSERT_TRUE(TickUntil([&] { return backed("s", 'r'); }));
    EXPECT_EQ(log.Get("after_query"), 4 + 1);
    EXPECT_EQ(log.Get("before_transfer"), 6 + 3);
}

// A drain latched between chunks: the chunk after it is not read, and the
// batch writes nothing (the chunks read before share its staging and are
// released with it); its reservation goes back with an empty ACK.
TEST_F(FileStorageBackupHolderBigTest, DrainBetweenChunksWritesNothing) {
    std::vector<size_t> acks;
    SetAckObserver(*storage_,
                   [&](uint64_t, size_t n, bool) { acks.push_back(n); });
    int transfers = 0;
    FileStorage* fs = storage_.get();
    SetStageHook(*storage_, [&, fs](const char* stage, size_t) {
        if (std::string(stage) == "before_transfer" && ++transfers == 2) {
            SetDrainingFlag(*fs, true);  // after chunk 1, before chunk 3
        }
    });
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(
            PutBig("d" + std::to_string(i), static_cast<char>('a' + i)));
    }
    RunBackupTick(*storage_);
    EXPECT_EQ(transfers, 2) << "a chunk was read after the drain";
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(BigBytes("d" + std::to_string(i)), "") << i;
    }
    ASSERT_EQ(acks.size(), 1u);
    EXPECT_EQ(acks[0], 0u);
    auto snap = HolderReservation();
    ASSERT_TRUE(snap.has_value());
    EXPECT_TRUE(snap->inflight_batches.empty());
    EXPECT_EQ(StagingStats(*storage_).quarantined_batches, 0u);
    SetDrainingFlag(*fs, false);
}

// A later chunk whose transfer could not be shown to have stopped: the whole
// tenant group is dropped (its chunks share one staging allocation) and the
// staging is quarantined; no further chunk is read.
TEST_F(FileStorageBackupHolderBigTest, UnquiescedLaterChunkDropsTheGroup) {
    std::vector<size_t> acks;
    SetAckObserver(*storage_,
                   [&](uint64_t, size_t n, bool) { acks.push_back(n); });
    int transfers = 0;
    SetStageHook(*storage_, [&](const char* stage, size_t) {
        // Runs on the reading thread, after it took its unquiesced count.
        if (std::string(stage) == "before_transfer" && ++transfers == 2) {
            NoteUnquiescedTransferFailure();
        }
    });
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(
            PutBig("u" + std::to_string(i), static_cast<char>('a' + i)));
    }
    RunBackupTick(*storage_);
    EXPECT_EQ(transfers, 2) << "a chunk was read after an unquiesced one";
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(BigBytes("u" + std::to_string(i)), "") << i;
    }
    EXPECT_EQ(StagingStats(*storage_).quarantined_batches, 1u);
    ASSERT_EQ(acks.size(), 1u);
    EXPECT_EQ(acks[0], 0u);
}

// Query windows against the lease TTL (three 1 MiB objects, 1 MiB chunks).
// A 1 s lease is not re-queried at once (a fixed 1 s margin would re-query
// before every chunk); a 300 ms lease aged by slow chunks is renewed before
// it runs out and every object is still backed up; two windows of 2 MiB
// take two queries.
class FileStorageBackupHolderLeaseTest : public FileStorageBackupHolderBigTest {
   protected:
    void RunCase(uint64_t ttl_ms, int64_t window_bytes, int sleep_ms,
                 int expect_queries_min, int expect_queries_max) {
        SetReadChunking(*storage_, 1 << 20, window_bytes);
        StageLog log;
        SetStageHook(*storage_, [&](const char* stage, size_t) {
            log.Hit(stage);
            if (sleep_ms > 0 && std::string(stage) == "before_transfer") {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(sleep_ms));
            }
        });
        for (int i = 0; i < 3; ++i) {
            ASSERT_TRUE(
                PutBig("l" + std::to_string(i), static_cast<char>('a' + i)));
        }
        RunBackupTick(*storage_);
        for (int i = 0; i < 3; ++i) {
            EXPECT_EQ(BigBytes("l" + std::to_string(i)),
                      std::string(kBig, static_cast<char>('a' + i)))
                << "ttl=" << ttl_ms << " object " << i << " not backed up";
        }
        EXPECT_GE(log.Get("after_query"), expect_queries_min) << ttl_ms;
        EXPECT_LE(log.Get("after_query"), expect_queries_max) << ttl_ms;
        EXPECT_EQ(log.Get("before_transfer"), 3);
    }
};
class FileStorageBackupHolderLease1s : public FileStorageBackupHolderLeaseTest {
   protected:
    FileStorageBackupHolderLease1s() { lease_ttl_ms_ = 1000; }
};
class FileStorageBackupHolderLease300
    : public FileStorageBackupHolderLeaseTest {
   protected:
    FileStorageBackupHolderLease300() { lease_ttl_ms_ = 300; }
};
TEST_F(FileStorageBackupHolderLease1s, FreshShortLeaseIsNotRequeried) {
    RunCase(1000, 64 << 20, 0, 1, 1);
}
TEST_F(FileStorageBackupHolderLease300, AgedShortLeaseIsRenewed) {
    // Chunks ~120 ms apart: the third chunk comes after the first lease's
    // margin and needs a fresh one.
    RunCase(300, 64 << 20, 120, 2, 3);
}
TEST_F(FileStorageBackupHolderLease300, SlowChunksDoNotOutliveTheLease) {
    // Chunks take ~170 ms from their lease check to the end of the transfer:
    // a chunk checked with 170 ms or less left would end after its lease.
    // The margin covers the measured chunk time, so every object is read on
    // a lease that outlasts its transfer.
    RunCase(300, 64 << 20, 170, 2, 3);
}
// Slow chunks over a longer batch (12 objects of 256 KiB, 256 KiB chunks,
// 300 ms lease, ~170 ms per chunk): once the lease covers only one chunk the
// query window shrinks to that chunk, so each object is queried about once
// (not the whole rest of the window before every chunk), and a window's
// first chunk is never re-queried right after its own query (with a query
// per chunk configured, one query per chunk, not two).
class FileStorageBackupHolderLeaseLongTest
    : public FileStorageBackupHolderTest {
   protected:
    static constexpr size_t kObj = 256 << 10;
    FileStorageBackupHolderLeaseLongTest() {
        batch_keys_ = "12";
        max_inflight_ = "16000000";
        lease_ttl_ms_ = 300;
    }
    // `alternate`: only every other chunk is slow (1st, 3rd, ...).
    void RunCase(int64_t window_bytes, int max_queries, int max_keys,
                 bool alternate = false) {
        SetReadChunking(*storage_, kObj, window_bytes);
        StageLog log;
        std::atomic<int> queried_keys{0};
        int transfers = 0;
        SetStageHook(*storage_, [&](const char* stage, size_t n) {
            log.Hit(stage);
            if (std::string(stage) == "after_query") queried_keys += n;
            if (std::string(stage) == "before_transfer" &&
                (!alternate || transfers++ % 2 == 0)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(170));
            }
        });
        for (int i = 0; i < 12; ++i) {
            ASSERT_TRUE(PutAs(*src_, *src_alloc_, "s" + std::to_string(i),
                              static_cast<char>('a' + i), kObj));
        }
        RunBackupTick(*storage_);
        for (int i = 0; i < 12; ++i) {
            EXPECT_EQ(HolderRead(*storage_,
                                 TenantId::Default().MakeScopedKey(
                                     "s" + std::to_string(i)),
                                 kObj),
                      std::string(kObj, static_cast<char>('a' + i)))
                << "object " << i << " not backed up";
        }
        EXPECT_EQ(log.Get("before_transfer"), 12);
        EXPECT_LE(log.Get("after_query"), max_queries);
        EXPECT_LE(queried_keys.load(), max_keys);
    }
};
TEST_F(FileStorageBackupHolderLeaseLongTest, SlowChunksShrinkTheWindow) {
    // First window: all 12 keys; then about one key per query.
    RunCase(64 << 20, 13, 2 * 12);
}
TEST_F(FileStorageBackupHolderLeaseLongTest,
       AlternatingChunksKeepTheWindowSmall) {
    // A fast chunk between slow ones does not reopen the window to all the
    // remaining keys (which the next slow chunk would re-query again).
    RunCase(64 << 20, 13, 2 * 12, /*alternate=*/true);
}
TEST_F(FileStorageBackupHolderLeaseLongTest, QueryPerChunkQueriesOncePerChunk) {
    RunCase(0, 12, 12);
}
// R6: a slow query uses up most of the short lease (300 ms lease, query
// returns after ~200 ms, chunks take ~170 ms). The window's first chunk
// must still be read on a lease that outlasts it: the `slow_query`-th query
// is the slow one. The first query of a batch has no chunk time estimate
// yet; its expired first chunk is queried and read once more. A later one
// (estimate known) is re-queried before its first chunk is read.
class FileStorageBackupHolderSlowQueryTest
    : public FileStorageBackupHolderLeaseLongTest {
   protected:
    void RunSlowQuery(int slow_query, int transfers_expected) {
        SetReadChunking(*storage_, kObj, 64 << 20);
        std::atomic<int> queries{0};
        StageLog log;
        SetStageHook(*storage_, [&](const char* stage, size_t) {
            log.Hit(stage);
            if (std::string(stage) == "after_query" &&
                ++queries == slow_query) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            if (std::string(stage) == "before_transfer") {
                std::this_thread::sleep_for(std::chrono::milliseconds(170));
            }
        });
        for (int i = 0; i < 12; ++i) {
            ASSERT_TRUE(PutAs(*src_, *src_alloc_, "s" + std::to_string(i),
                              static_cast<char>('a' + i), kObj));
        }
        RunBackupTick(*storage_);
        for (int i = 0; i < 12; ++i) {
            EXPECT_EQ(HolderRead(*storage_,
                                 TenantId::Default().MakeScopedKey(
                                     "s" + std::to_string(i)),
                                 kObj),
                      std::string(kObj, static_cast<char>('a' + i)))
                << "object " << i << " not backed up";
        }
        EXPECT_EQ(log.Get("before_transfer"), transfers_expected);
    }
};
TEST_F(FileStorageBackupHolderSlowQueryTest, SlowFirstQueryIsRetried) {
    RunSlowQuery(1, 12 + 1);  // the first chunk is read twice
}
TEST_F(FileStorageBackupHolderSlowQueryTest, SlowLaterQueryIsRenewed) {
    RunSlowQuery(2, 12);  // re-queried before its first chunk is read
}
TEST_F(FileStorageBackupHolderLeaseTest, TwoWindowsTakeTwoQueries) {
    RunCase(5000, 2 << 20, 0, 2, 2);
}

// R4-NEW-BEGIN (on the old code this would read via a client without a
// master connection; not red evidence, stripped from the red tree)
// R3-B1: batches returned by a heartbeat that completed after a drain was
// latched are not read; each gets exactly one empty ACK, and no scan runs.
TEST_F(FileStorageBackupTest, HeartbeatBatchesAfterDrainAreReleasedOnce) {
    auto client = std::make_shared<ScriptedBackupClient>();
    FileStorage storage(Config(), client, "127.0.0.1:17990");
    ASSERT_TRUE(InitBackend(storage).has_value());
    ConfigureBackup(storage, 8);
    std::vector<std::pair<uint64_t, size_t>> acks;  // batch id, task count
    client->notify = [&](uint64_t id, const auto& tasks, const auto&) {
        acks.emplace_back(id, tasks.size());
        return tl::expected<std::vector<uint32_t>, ErrorCode>{};
    };
    client->heartbeat = [&] {
        SetDrainingFlag(storage, true);
        BackupHeartbeatResponse r;
        for (uint64_t id = 11; id <= 13; ++id) {
            BackupBatch b;
            b.batch_id = id;
            b.tasks.push_back(BackupTaskItem{.tenant_id = "default",
                                             .key = "k" + std::to_string(id),
                                             .size = 64,
                                             .version = 1});
            r.batches.push_back(std::move(b));
        }
        return r;
    };
    RunBackupTick(storage);
    ASSERT_EQ(acks.size(), 3u);
    for (size_t i = 0; i < acks.size(); ++i) {
        EXPECT_EQ(acks[i].first, 11 + i);
        EXPECT_EQ(acks[i].second, 0u);  // empty: nothing registered
    }
    EXPECT_EQ(client->residency_calls.load(), 0);
    EXPECT_EQ(Backend(storage)->GetBackupUsage().total_bytes, 0);
    SetDrainingFlag(storage, false);
}
// R4-NEW-END

// R3-B1: once a drain is observed while a scan's residency query is in
// flight, the scan issues no further query chunk. Six buckets with 200
// sampled keys each need two calls (1000 + 200; at most 1000 per call).
TEST_F(FileStorageBackupTest, ScanStopsBetweenChunksOnDrain) {
    auto client = std::make_shared<ScriptedBackupClient>();
    FileStorage storage(Config(), client, "127.0.0.1:17990");
    ASSERT_TRUE(InitBackend(storage).has_value());
    std::vector<std::string> data;
    for (int b = 0; b < 6; ++b) {
        WritePinnedBackupBucket(*Backend(storage),
                                "s" + std::to_string(b) + "_", 200, data);
    }
    ConfigureBackup(storage, 200);
    RunScan(storage);  // registers the buckets; their check is due now
    ASSERT_EQ(ScanStateEntries(storage), 6u);
    // Control: without a drain a due scan needs two calls.
    client->residency_calls = 0;
    RunScan(storage);
    ASSERT_EQ(client->residency_calls.load(), 2);
    // Make both due again, then latch a drain during the first call.
    ResetScanSchedule(storage);
    client->residency_calls = 0;
    client->residency_hook = [&] { SetDrainingFlag(storage, true); };
    RunScan(storage);
    EXPECT_EQ(client->residency_calls.load(), 1);
    client->residency_hook = nullptr;
    SetDrainingFlag(storage, false);
}

// R3-NEW-BEGIN
// R1-Q1: a drain while a backup write waits in pacing ends the wait; the
// batch is not written, nothing is registered, and its reservation is
// released by an empty ACK.
TEST_F(FileStorageBackupMasterTest, DrainCancelsPacedWrite) {
    const BackupBatch b1 = Put(a_, "seg_a", "paced1");
    const BackupBatch b2 = Put(a_, "seg_a", "paced2");
    ASSERT_EQ(b1.tasks.size(), 1u);
    ASSERT_EQ(b2.tasks.size(), 1u);
    int empty_acks = 0;
    client_->notify = [&](uint64_t id, const auto& tasks, const auto& metas) {
        if (tasks.empty()) ++empty_acks;
        return master_->NotifyBackupDone(h_, id, tasks, metas);
    };
    SetWriteRate(*storage_, 1);  // 1 B/s: the second write waits ~4096 s
    std::string one(kSize, '1'), two(kSize, '2');
    WriteBackup(*storage_, b1, {{b1.tasks[0], &one}});
    ASSERT_EQ(Read("paced1"), one);
    auto second = std::async(std::launch::async, [&] {
        WriteBackup(*storage_, b2, {{b2.tasks[0], &two}});
    });
    EXPECT_EQ(second.wait_for(std::chrono::milliseconds(300)),
              std::future_status::timeout);  // paced
    StartDrainAndWake(*storage_);
    ASSERT_EQ(second.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    EXPECT_EQ(Read("paced2"), "");
    EXPECT_EQ(MasterBackupsOnH("paced2"), 0u);
    EXPECT_EQ(empty_acks, 1);
    EXPECT_EQ(Read("paced1"), one);  // the earlier backup is untouched
}
// R3-NEW-END

// A definite refusal before anything was registered still rolls back.
TEST_F(FileStorageBackupMasterTest, RefusedRegistrationIsRolledBack) {
    const BackupBatch b = Put(a_, "seg_a", "refused");
    ASSERT_EQ(b.tasks.size(), 1u);
    client_->notify =
        [](uint64_t, const auto& tasks,
           const auto&) -> tl::expected<std::vector<uint32_t>, ErrorCode> {
        if (tasks.empty()) return std::vector<uint32_t>{};
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    };
    std::string bytes(kSize, 'R');
    WriteBackup(*storage_, b, {{b.tasks[0], &bytes}});
    EXPECT_EQ(MasterBackupsOnH("refused"), 0u);
    EXPECT_EQ(Read("refused"), "");
}

// R1-F08: the worker accepts the same MC_SSD_BACKUP_ENABLE values as the
// master (SsdBackupConfigTest.EnableParsedLikeWorker covers the master).
TEST_F(FileStorageBackupTest, EnableParsedLikeMaster) {
    for (const char* v : {"1", "true", "TRUE", "True", "on", "ON", "yes"}) {
        setenv("MC_SSD_BACKUP_ENABLE", v, 1);
        EXPECT_TRUE(BackupEnabledFromEnv()) << v;
    }
    for (const char* v : {"0", "false", "off", "", "bogus"}) {
        setenv("MC_SSD_BACKUP_ENABLE", v, 1);
        EXPECT_FALSE(BackupEnabledFromEnv()) << v;
    }
}

// R1-F09: a bucket's sample must fit one residency query (1000 keys), or
// the master refuses the scan of that bucket every round.
TEST_F(FileStorageBackupTest, SampleKeysCappedAtQueryLimit) {
    setenv("MC_SSD_BACKUP_SAMPLE_KEYS", "1001", 1);
    EXPECT_EQ(SampleKeysFromEnv(), 1000u);
    setenv("MC_SSD_BACKUP_SAMPLE_KEYS", "1000", 1);
    EXPECT_EQ(SampleKeysFromEnv(), 1000u);
    setenv("MC_SSD_BACKUP_SAMPLE_KEYS", "32", 1);
    EXPECT_EQ(SampleKeysFromEnv(), 32u);
}

// CFG-07: bad numbers fall back to the default; values that would spin,
// stall shutdown, overflow or break std::clamp are bounded.
TEST_F(FileStorageBackupTest, WorkerConfigNumericBoundaries) {
    const char* vars[] = {"MC_SSD_BACKUP_CAPACITY_BYTES",
                          "MC_SSD_BACKUP_STAGING_BYTES",
                          "MC_SSD_BACKUP_INTERVAL_MS",
                          "MC_SSD_BACKUP_SCAN_INTERVAL_MS",
                          "MC_SSD_BACKUP_MIN_BUCKETS_PER_SCAN",
                          "MC_SSD_BACKUP_MAX_BUCKETS_PER_SCAN"};
    for (const char* v : vars) unsetenv(v);
    const WorkerConfig d = WorkerConfigFromEnv();
    EXPECT_EQ(d.capacity_bytes, 2LL << 30);  // 2x the DRAM segment
    EXPECT_EQ(d.staging_bytes, 512LL << 20);
    EXPECT_EQ(d.interval_ms, 1000);
    for (const char* bad : {"-1", "abc", "12x", "99999999999999999999"}) {
        SCOPED_TRACE(bad);
        for (const char* v : vars) setenv(v, bad, 1);
        const WorkerConfig c = WorkerConfigFromEnv();
        EXPECT_EQ(c.capacity_bytes, d.capacity_bytes);
        EXPECT_EQ(c.staging_bytes, d.staging_bytes);
        EXPECT_EQ(c.interval_ms, d.interval_ms);
        EXPECT_EQ(c.scan_interval_ms, d.scan_interval_ms);
        EXPECT_EQ(c.min_buckets, d.min_buckets);
        EXPECT_EQ(c.max_buckets, d.max_buckets);
    }
    for (const char* v : vars) unsetenv(v);
    setenv("MC_SSD_BACKUP_INTERVAL_MS", "0", 1);  // would spin
    EXPECT_EQ(WorkerConfigFromEnv().interval_ms, 10);
    setenv("MC_SSD_BACKUP_INTERVAL_MS", "86400000000", 1);  // stalls shutdown
    EXPECT_EQ(WorkerConfigFromEnv().interval_ms, 60000);
    unsetenv("MC_SSD_BACKUP_INTERVAL_MS");
    setenv("MC_SSD_BACKUP_SCAN_INTERVAL_MS", "9000000000000", 1);
    EXPECT_EQ(WorkerConfigFromEnv().scan_interval_ms, 24LL * 3600 * 1000);
    unsetenv("MC_SSD_BACKUP_SCAN_INTERVAL_MS");
    setenv("MC_SSD_BACKUP_MIN_BUCKETS_PER_SCAN", "700", 1);
    setenv("MC_SSD_BACKUP_MAX_BUCKETS_PER_SCAN", "600", 1);
    const WorkerConfig mm = WorkerConfigFromEnv();
    EXPECT_LE(mm.min_buckets, mm.max_buckets);
    EXPECT_EQ(mm.max_buckets, 600u);
    for (const char* v : vars) unsetenv(v);
}

// R1-F02: staging that may still receive a late DMA is never handed out
// again, and is not freed when the FileStorage goes away.
TEST_F(FileStorageBackupTest, QuarantinedStagingNeverFreed) {
    constexpr size_t kStaging = 1 << 20;
    std::weak_ptr<ClientBufferAllocator> staging_alive;
    {
        FileStorage storage(Config(), nullptr, "localhost:9101");
        auto staging = AlignedClientBufferAllocator::create(kStaging);
        ASSERT_TRUE(staging);
        staging_alive = staging;
        SetStaging(storage, staging);
        staging.reset();
        auto batch = AllocateStaging(storage, {"k"}, {kStaging / 2});
        ASSERT_TRUE(batch.has_value());
        Quarantine(storage, std::move(batch.value()));
        // The quarantined half is not reusable.
        EXPECT_FALSE(AllocateStaging(storage, {"k2"}, {kStaging}).has_value());
    }
    // Destruction leaked the quarantined staging instead of freeing it.
    EXPECT_FALSE(staging_alive.expired());
}

// C1 (user decision, loop 2 round 1): quarantine is timed. Quarantined
// staging stays out of use for MC_SSD_BACKUP_STAGING_QUARANTINE_MS (default
// 600000) on the backup clock, then goes back to the allocator; the
// quarantined-bytes gauge and the cumulative count follow it. Once nothing
// is quarantined any more, destruction frees the staging normally.
TEST_F(FileStorageBackupTest, QuarantinedStagingReturnsAfterItsTime) {
    constexpr size_t kStaging = 1 << 20;
    SsdMetric metric;
    std::weak_ptr<ClientBufferAllocator> staging_alive;
    auto now = std::make_shared<Clock::time_point>(Clock::now());
    {
        FileStorage storage(Config(), nullptr, "localhost:9102", &metric);
        auto staging = AlignedClientBufferAllocator::create(kStaging);
        ASSERT_TRUE(staging);
        staging_alive = staging;
        SetStaging(storage, staging);
        staging.reset();
        SetBackupClock(storage, [now] { return *now; });
        SetStagingTimes(storage, 600000, 0);
        auto batch = AllocateStaging(storage, {"k"}, {kStaging / 2});
        ASSERT_TRUE(batch.has_value());
        Quarantine(storage, std::move(batch.value()));
        EXPECT_EQ(StagingStats(storage).quarantined_batches, 1u);
        EXPECT_EQ(StagingStats(storage).quarantined_bytes,
                  static_cast<int64_t>(kStaging / 2));
        EXPECT_EQ(StagingStats(storage).quarantined_total, 1u);
        // R11-GREEN-ONLY-BEGIN (the metrics are new)
        EXPECT_EQ(metric.ssd_backup_staging_quarantined_bytes.value(),
                  static_cast<int64_t>(kStaging / 2));
        EXPECT_EQ(metric.ssd_backup_staging_quarantined_total.value(), 1);
        // R11-GREEN-ONLY-END
        // One millisecond before its time: still out of use.
        *now += std::chrono::milliseconds(599999);
        ReleaseExpiredQuarantine(storage);
        EXPECT_FALSE(
            AllocateStaging(storage, {"k2"}, {kStaging * 3 / 4}).has_value());
        // Its time is up: the memory is allocatable again.
        *now += std::chrono::milliseconds(1);
        ReleaseExpiredQuarantine(storage);
        auto again = AllocateStaging(storage, {"k2"}, {kStaging * 3 / 4});
        EXPECT_TRUE(again.has_value()) << "quarantined staging never returned";
        if (again) again.value().reset();  // back to the allocator
        const auto s = StagingStats(storage);
        EXPECT_EQ(s.quarantined_batches, 0u);
        EXPECT_EQ(s.quarantined_bytes, 0);
        EXPECT_EQ(s.quarantined_total, 1u);  // cumulative
        EXPECT_EQ(s.released, 1u);
        // R11-GREEN-ONLY-BEGIN
        EXPECT_EQ(metric.ssd_backup_staging_quarantined_bytes.value(), 0);
        EXPECT_EQ(metric.ssd_backup_staging_quarantined_total.value(), 1);
        // R11-GREEN-ONLY-END
    }
    EXPECT_TRUE(staging_alive.expired()) << "staging leaked after release";
}

// C1: MC_SSD_BACKUP_STAGING_QUARANTINE_MS / MC_SSD_BACKUP_STAGING_WAIT_MS
// parsing: defaults 600000 / 60000, values taken as given in [0, 24 h],
// negative or unparsable values fall back to the default, larger ones are
// clamped to 24 h. (Not on the previous commit: the settings are new.)
// R11-GREEN-ONLY-BEGIN
TEST_F(FileStorageBackupTest, StagingQuarantineAndWaitConfig) {
    const auto read = [] { return StagingTimesFromEnv(); };
    unsetenv("MC_SSD_BACKUP_STAGING_QUARANTINE_MS");
    unsetenv("MC_SSD_BACKUP_STAGING_WAIT_MS");
    EXPECT_EQ(read(), std::make_pair(int64_t{600000}, int64_t{60000}));
    struct Case {
        const char* value;
        int64_t want_quarantine, want_wait;
    };
    for (const Case& c : {Case{"0", 0, 0}, Case{"1234", 1234, 1234},
                          Case{"-5", 600000, 60000}, Case{"abc", 600000, 60000},
                          Case{"999999999999", 86400000, 86400000}}) {
        SCOPED_TRACE(c.value);
        setenv("MC_SSD_BACKUP_STAGING_QUARANTINE_MS", c.value, 1);
        setenv("MC_SSD_BACKUP_STAGING_WAIT_MS", c.value, 1);
        EXPECT_EQ(read(), std::make_pair(c.want_quarantine, c.want_wait));
    }
    unsetenv("MC_SSD_BACKUP_STAGING_QUARANTINE_MS");
    unsetenv("MC_SSD_BACKUP_STAGING_WAIT_MS");
}
// R11-GREEN-ONLY-END

// P2-03 (review 2026-11-25): a pinned backup bucket whose keys all map to a
// newer backup bucket protects nothing. The scanner demotes it without a
// residency query for its (historical) keys, which the master would answer
// with the newer copy's state (here: always in DRAM, so it stayed "hot" and
// pinned forever). A bucket that still holds some live keys stays pinned.
TEST_F(FileStorageBackupTest, ScanDemotesBucketsNoKeyMapsTo) {
    auto client = std::make_shared<ScriptedBackupClient>();
    FileStorage storage(Config(), client, "127.0.0.1:17990");
    ASSERT_TRUE(InitBackend(storage).has_value());
    auto backend = Backend(storage);
    std::vector<std::string> data;
    const auto pinned_ids = [&] {
        std::set<int64_t> ids;
        for (const auto& b : backend->ListPinnedBackupBuckets(0)) {
            ids.insert(b.bucket_id);
        }
        return ids;
    };
    const auto write = [&](const std::string& prefix, int n) {
        const auto before = pinned_ids();
        WritePinnedBackupBucket(*backend, prefix, n, data);
        int64_t id = -1;
        for (int64_t b : pinned_ids()) {
            if (!before.count(b)) id = b;
        }
        return id;
    };
    const int64_t old_all = write("ux_", 4);
    const int64_t new_all = write("ux_", 4);  // replaces all of old_all
    const int64_t partly = write("pz_", 4);
    const int64_t one = write("pz_", 1);  // replaces pz_0 only
    ASSERT_EQ(pinned_ids(), (std::set<int64_t>{old_all, new_all, partly, one}));
    ConfigureBackup(storage, 32);
    RunScan(storage);  // first sighting
    ResetScanSchedule(storage);
    RunScan(storage);  // every bucket due; residency says "in DRAM"
    EXPECT_EQ(pinned_ids(), (std::set<int64_t>{new_all, partly, one}))
        << "the bucket no key maps to is still pinned";
    EXPECT_EQ(DemotedUnmapped(storage), 1u);
    EXPECT_EQ(backend->GetBackupUsage().pinned_buckets, 3u);
}

// A bucket's first residency check is due one scan interval after it was
// written, not one interval after the scan that first sees it: a bucket
// already older than the interval is checked by its first scan, a fresh one
// is not.
TEST_F(FileStorageBackupTest, FirstCheckDueOneIntervalAfterWrite) {
    auto client = std::make_shared<ScriptedBackupClient>();
    FileStorage storage(Config(), client, "127.0.0.1:17990");
    ASSERT_TRUE(InitBackend(storage).has_value());
    auto backend = Backend(storage);
    std::vector<std::string> data;
    WritePinnedBackupBucket(*backend, "old_", 4, data);
    ConfigureBackup(storage, 4);
    SetScanInterval(storage, std::chrono::milliseconds(300));
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    WritePinnedBackupBucket(*backend, "new_", 4, data);
    RunScan(storage);  // first sighting of both
    EXPECT_EQ(storage.GetBackupScanStats().buckets_checked, 1u)
        << "only the bucket older than the interval is due";
    EXPECT_EQ(client->residency_calls.load(), 1);
    RunScan(storage);  // the old one was kept (in DRAM), the new not yet due
    EXPECT_EQ(storage.GetBackupScanStats().buckets_checked, 1u);
}

// Pin watermarks: at or above the high mark (95% of the capacity) pinned
// buckets are demoted, oldest first among equals, until below the low mark
// (90%); between the marks nothing is demoted. The heartbeat that follows in
// the same tick reports the lowered usage, below the master's 98% limit.
TEST_F(FileStorageBackupTest, PinWatermarksDemoteFromHighToLow) {
    auto client = std::make_shared<ScriptedBackupClient>();
    FileStorage storage(Config(), client, "127.0.0.1:17990");
    ASSERT_TRUE(InitBackend(storage).has_value());
    auto backend = Backend(storage);
    std::vector<std::string> data;
    std::vector<int64_t> order;  // bucket ids, oldest first
    for (int i = 0; i < 20; ++i) {
        std::set<int64_t> before;
        for (const auto& b : backend->ListPinnedBackupBuckets(0)) {
            before.insert(b.bucket_id);
        }
        // Same-length prefixes: every bucket has the same size.
        WritePinnedBackupBucket(*backend, "w" + std::to_string(10 + i) + "_", 4,
                                data);
        for (const auto& b : backend->ListPinnedBackupBuckets(0)) {
            if (!before.count(b.bucket_id)) order.push_back(b.bucket_id);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(order.size(), 20u);
    ConfigureBackup(storage, 4);
    const int64_t pinned = backend->GetBackupUsage().pinned_bytes;
    const int64_t bucket = pinned / 20;
    // 93% of the capacity: between the marks, nothing happens.
    SetCapacity(storage, pinned * 100 / 93);
    EnforceCap(storage);
    EXPECT_EQ(backend->GetBackupUsage().pinned_buckets, 20u);
    // Exactly at the high mark (rounded down): demotion starts.
    int64_t cap = static_cast<int64_t>(static_cast<double>(pinned) / 0.95);
    SetCapacity(storage, cap);
    EnforceCap(storage);
    auto usage = backend->GetBackupUsage();
    EXPECT_LT(usage.pinned_bytes, static_cast<int64_t>(cap * 0.90));
    // Only as many as needed: one fewer would still be at or above 90%.
    EXPECT_GE(usage.pinned_bytes + bucket, static_cast<int64_t>(cap * 0.90));
    std::set<int64_t> left;
    for (const auto& b : backend->ListPinnedBackupBuckets(0)) {
        left.insert(b.bucket_id);
    }
    const size_t n_demoted = 20 - left.size();
    ASSERT_GT(n_demoted, 0u);
    for (size_t i = 0; i < order.size(); ++i) {
        EXPECT_EQ(left.count(order[i]) == 0, i < n_demoted)
            << "bucket " << i << " (oldest first) demotion out of order";
    }
    // 99% through a real tick: demoted before the heartbeat, which reports
    // usage under the low mark (so under the master's 98% limit).
    cap = backend->GetBackupUsage().pinned_bytes * 100 / 99;
    SetCapacity(storage, cap);
    RunBackupTick(storage);
    EXPECT_EQ(client->heartbeats.load(), 1);
    EXPECT_EQ(client->last_capacity.load(), cap);
    EXPECT_LT(client->last_used.load(), static_cast<int64_t>(cap * 0.90));
    EXPECT_EQ(client->last_used.load(), backend->GetBackupUsage().pinned_bytes);
}

// A demotion asks for an immediate scan at most once per 30 s: under steady
// writes demotions come every few seconds and must not run the scanner each
// time.
TEST_F(FileStorageBackupTest, PinCapRescanIsRateLimited) {
    auto client = std::make_shared<ScriptedBackupClient>();
    FileStorage storage(Config(), client, "127.0.0.1:17990");
    ASSERT_TRUE(InitBackend(storage).has_value());
    auto backend = Backend(storage);
    std::vector<std::string> data;
    for (int i = 0; i < 4; ++i) {
        WritePinnedBackupBucket(*backend, "r" + std::to_string(i) + "_", 4,
                                data);
    }
    ConfigureBackup(storage, 4);
    const auto far = std::chrono::steady_clock::now() + std::chrono::hours(1);
    SetNextScan(storage, far);
    SetCapacity(storage, backend->GetBackupUsage().pinned_bytes);
    EnforceCap(storage);  // demotes; first rescan request goes through
    EXPECT_LT(NextScan(storage), far);
    for (int i = 0; i < 4; ++i) {
        WritePinnedBackupBucket(*backend, "s" + std::to_string(i) + "_", 4,
                                data);
    }
    SetNextScan(storage, far);
    SetCapacity(storage, backend->GetBackupUsage().pinned_bytes);
    const auto demoted = storage.GetBackupScanStats().demoted_cap;
    EnforceCap(storage);  // demotes again within 30 s: no new rescan
    EXPECT_GT(storage.GetBackupScanStats().demoted_cap, demoted);
    EXPECT_EQ(NextScan(storage), far);
}

// The watermarks come from the environment only as a valid pair
// (0 < low < high < 0.98); otherwise both stay at their defaults.
TEST_F(FileStorageBackupTest, PinWatermarkEnvValidation) {
    const auto with = [](const char* high, const char* low) {
        if (high) setenv("MC_SSD_BACKUP_PIN_HIGH_RATIO", high, 1);
        if (low) setenv("MC_SSD_BACKUP_PIN_LOW_RATIO", low, 1);
        auto r = PinRatiosFromEnv();
        unsetenv("MC_SSD_BACKUP_PIN_HIGH_RATIO");
        unsetenv("MC_SSD_BACKUP_PIN_LOW_RATIO");
        return r;
    };
    const std::pair<double, double> dflt{0.95, 0.90};
    EXPECT_EQ(with(nullptr, nullptr), dflt);
    EXPECT_EQ(with("0.97", "0.93"), std::make_pair(0.97, 0.93));
    EXPECT_EQ(with("0.92", nullptr), std::make_pair(0.92, 0.90));
    EXPECT_EQ(with("0.98", "0.9"), dflt);    // not below the master's limit
    EXPECT_EQ(with("0.9", "0.9"), dflt);     // low not below high
    EXPECT_EQ(with(nullptr, "0.96"), dflt);  // low above the default high
    EXPECT_EQ(with("0.95", "0"), dflt);
    EXPECT_EQ(with("0.95", "-0.1"), dflt);
    EXPECT_EQ(with("abc", "0.9"), dflt);
}

// P2-03: over the pin cap, a bucket that protects nothing is demoted before
// any bucket with live keys, even an older one (the cap otherwise demotes
// the oldest among equal hotness and would drop a valid backup first).
TEST_F(FileStorageBackupTest, PinCapDemotesUnmappedBucketFirst) {
    auto client = std::make_shared<ScriptedBackupClient>();
    FileStorage storage(Config(), client, "127.0.0.1:17990");
    ASSERT_TRUE(InitBackend(storage).has_value());
    auto backend = Backend(storage);
    std::vector<std::string> data;
    const auto pinned_ids = [&] {
        std::set<int64_t> ids;
        for (const auto& b : backend->ListPinnedBackupBuckets(0)) {
            ids.insert(b.bucket_id);
        }
        return ids;
    };
    const auto write = [&](const std::string& prefix) {
        const auto before = pinned_ids();
        WritePinnedBackupBucket(*backend, prefix, 4, data);
        int64_t id = -1;
        for (int64_t b : pinned_ids()) {
            if (!before.count(b)) id = b;
        }
        return id;
    };
    const int64_t valid_oldest = write("vv_");
    const int64_t unmapped = write("oo_");
    const int64_t newer = write("oo_");  // takes all of unmapped's keys
    ConfigureBackup(storage, 32);
    // At the cap (over the 95% high mark): one demotion brings pinned bytes
    // under the 90% low mark.
    SetCapacity(storage, backend->GetBackupUsage().pinned_bytes);
    EnforceCap(storage);
    EXPECT_EQ(pinned_ids(), (std::set<int64_t>{valid_oldest, newer}))
        << "cap demoted a bucket with live keys before the unmapped one";
    EXPECT_FALSE(pinned_ids().count(unmapped));
}

// R1-F05: a registration whose reply was lost may have been applied by the
// master. It is retried; if the outcome stays unknown the bucket is kept
// (outcome_unknown), never rolled back. A refusal is still a failure.
TEST_F(FileStorageBackupTest, LostAckIsRetriedAndNeverRolledBack) {
    using Res = AckRes;
    bool unknown = true;
    int calls = 0;
    // Reply lost once, then a definite answer (master says key 1 is stale).
    auto res = Ack(
        [&]() -> Res {
            if (++calls == 1)
                return tl::make_unexpected(ErrorCode::RPC_TIMEOUT);
            return std::vector<uint32_t>{1};
        },
        &unknown);
    ASSERT_TRUE(res.has_value());
    EXPECT_FALSE(unknown);
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(res.value(), std::vector<uint32_t>{1});
    // Never answered: keep the bucket.
    calls = 0;
    res = Ack(
        [&]() -> Res {
            ++calls;
            return tl::make_unexpected(ErrorCode::RPC_FAIL);
        },
        &unknown);
    EXPECT_TRUE(res.has_value());
    EXPECT_TRUE(unknown);
    EXPECT_EQ(calls, 3);
    // Refused by the master: no retry, roll back.
    calls = 0;
    res = Ack(
        [&]() -> Res {
            ++calls;
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        },
        &unknown);
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_FALSE(unknown);
    EXPECT_EQ(calls, 1);
}

// A real TCP Client of an in-process master (offload on), with a DRAM
// segment and a registered buffer for Put, plus a FileStorage for it that is
// brought up with the full Init().
// R13-GREEN-ONLY-BEGIN (needs the RealClient test friend)
// T7 / R8-M1 (round 2): real readers. Three RealClients against an
// in-process master with SSD backup on: a source A and a backup holder H,
// both with real SSD offload (FileStorage, bucket backend, offload RPC
// server, backup worker), and a reader R without offload. A's objects get a
// real primary SSD copy written by A's own FileStorage and a real backup
// written by H's worker. R reads through the two promised batch APIs
// (batch_get_into, batch_get_into_multi_buffers): from A's DRAM, from A's
// primary SSD after the DRAM copy is evicted (which also sends H a
// tenant-scoped refresh of the backup bucket), and, with read fallback on,
// from H's backup once A's SSD RPC endpoint stops answering (the in-call
// fallback: a transport error, then the backup, in the same call). Runs in
// death-test children: MC_SSD_BACKUP_READ_FALLBACK is read once per
// process.
class FileStorageBackupReaderTest : public FileStorageBackupTest {
   protected:
    static constexpr size_t kSeg = 32 << 20;
    void SetUp() override {
        FileStorageBackupTest::SetUp();
        for (const auto& [k, v] : Env()) setenv(k, v, 1);
        // Short lease: the DRAM copy can be evicted soon after a read.
        auto builder = InProcMasterConfigBuilder()
                           .set_enable_offload(true)
                           .set_default_kv_lease_ttl(LeaseTtlMs())
                           .set_root_fs_dir("");
        if (MultiTenant()) {
            // Registered tenants "default" and kTenantA (file policy).
            const auto policy_path = dir_ / "tenants.yaml";
            TenantQuotaPolicySnapshot policy;
            policy.tenant_quotas = {
                {std::string(TenantId::kDefaultValue), 256 * 1024 * 1024},
                {kTenantA, 256 * 1024 * 1024},
            };
            std::ofstream out(policy_path);
            ASSERT_TRUE(out.is_open());
            out << FormatTenantQuotaPolicyYaml(policy);
            out.close();
            builder.set_enable_multi_tenants(true)
                .set_tenant_quota_connector_uri(policy_path.string());
        }
        ASSERT_TRUE(master_.Start(builder.build()));
    }
    static constexpr char kTenantA[] = "tenant-a";
    virtual bool MultiTenant() const { return false; }
    virtual uint64_t LeaseTtlMs() const { return 300; }
    // Watchdog end: all thread stacks to $MC_TEST_TRACE_DIR (gdb, bounded),
    // then exit 9.
    static void DumpStacksAndExit(const char* what) {
        std::fprintf(stderr, "watchdog: %s\n", what);
        const char* dir = std::getenv("MC_TEST_TRACE_DIR");
        const std::string out = std::string(dir ? dir : "/tmp") + "/reader-" +
                                std::to_string(::getpid()) + ".stacks";
        const std::string cmd =
            "timeout 30 gdb -p " + std::to_string(::getpid()) +
            " -batch -ex 'thread apply all bt' > " + out + " 2>&1";
        const int rc = std::system(cmd.c_str());
        std::fprintf(stderr, "thread stacks (gdb rc=%d): %s\n", rc,
                     out.c_str());
        std::_Exit(9);
    }
    void TearDown() override {
        for (auto* c : {&r_, &h_, &a_}) {
            if (*c) (*c)->tearDownAll();
            c->reset();
        }
        master_.Stop();
        for (const auto& [k, v] : Env()) unsetenv(k);
        FileStorageBackupTest::TearDown();
    }
    static std::vector<std::pair<const char*, const char*>> Env() {
        return {{"MC_SSD_BACKUP_ENABLE", "1"},
                {"MC_SSD_BACKUP_BATCH_KEYS", "1"},
                {"MC_SSD_BACKUP_MAX_INFLIGHT_BYTES", "67108864"},
                {"MC_SSD_BACKUP_INTERVAL_MS", "50"},
                {"MC_SSD_BACKUP_STAGING_BYTES", "8388608"},
                {"MC_SSD_BACKUP_READ_FALLBACK", "1"},
                {"MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "1"},
                {"MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES", "16777216"},
                {"MC_STORE_LOCAL_HOT_CACHE_SIZE", "0"}};
    }
    std::shared_ptr<RealClient> Make(const std::string& host, bool offload,
                                     const std::string& sub,
                                     const std::string& tenant = "default") {
        setenv("MOONCAKE_HOST_ID", host.c_str(), 1);
        auto rc = RealClient::create();
        const auto path = (dir_ / sub).string();
        if (offload) fs::create_directories(path);
        const int rc_setup = rc->setup_real(
            UniqueLocalEndpoint(), master_.metadata_url(), kSeg, 16 << 20,
            "tcp", "", master_.master_address(), nullptr, "", offload,
            offload ? path : std::string(), tenant);
        unsetenv("MOONCAKE_HOST_ID");
        return rc_setup == 0 ? rc : nullptr;
    }
    MasterService& Ms() { return master_.service()->GetMasterServiceForTest(); }
    static UUID Id(RealClient& c) { return c.client_->getClientId(); }
    static std::string Tag(const UUID& id) {
        return std::to_string(id.first) + "-" + std::to_string(id.second);
    }
    std::string Record(const std::string& key) {
        return Ms().DescribeSsdRecordsForTest({key}, TenantId::Default()).at(0);
    }
    static bool WaitFor(const std::function<bool()>& done,
                        std::chrono::seconds limit) {
        const auto until = std::chrono::steady_clock::now() + limit;
        while (!done()) {
            if (std::chrono::steady_clock::now() >= until) return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return true;
    }
    static void StopOffloadServer(RealClient& c) {
        if (c.offload_rpc_server_) c.offload_rpc_server_->stop();
    }
    static uint64_t FallbackOk(RealClient& c) {
        return c.backup_fallback_ok_.load();
    }
    static uint64_t FallbackAttempts(RealClient& c) {
        return c.backup_fallback_attempts_.load();
    }
    static const std::string& Hostname(RealClient& c) {
        return c.local_hostname;
    }
    static int Fail(int code, const std::string& why) {
        std::fprintf(stderr, "check %d failed: %s\n", code, why.c_str());
        return code;
    }

    // A's offload RPC server again, on the port the master knows for it.
    static bool RestartOffloadServer(RealClient& c) {
        const int port = c.offload_rpc_port_;
        if (c.offload_rpc_server_) c.offload_rpc_server_->stop();
        c.offload_rpc_server_ =
            std::make_unique<coro_rpc::coro_rpc_server>(1, port, "0.0.0.0");
        auto& s = *c.offload_rpc_server_;
        s.register_handler<&RealClient::batch_get_offload_object>(&c);
        s.register_handler<&RealClient::batch_get_offload_object_ranges>(&c);
        s.register_handler<&RealClient::release_offload_buffer>(&c);
        s.register_handler<&RealClient::verify_disk_replica>(&c);
        s.register_handler<&RealClient::prefetch_offload_object>(&c);
        s.async_start();
        return !s.get_errc() && static_cast<int>(s.port()) == port;
    }

    // Per-key record of what the reader did (RealClient's test observer).
    // Canon gives one line per observation, phase[#call]|key|role|endpoint|
    // result, with endpoints replaced by tags (A, H, ...) and results by the
    // byte count, "err", or "ok" for a query that returned. Sorted, so the
    // comparison does not depend on key order inside one step.
    struct ObsLog {
        std::mutex mu;
        std::vector<RealClient::ReadObservation> v;
    };
    void StartObserving(ObsLog& log) {
        r_->read_observer_for_test_ =
            [&log](const RealClient::ReadObservation& o) {
                std::lock_guard<std::mutex> l(log.mu);
                log.v.push_back(o);
            };
    }
    void StopObserving() { r_->read_observer_for_test_ = nullptr; }
    static std::vector<std::string> Canon(
        ObsLog& log, const std::map<std::string, std::string>& tags) {
        std::lock_guard<std::mutex> l(log.mu);
        std::vector<std::string> out;
        for (const auto& o : log.v) {
            const auto t = tags.find(o.endpoint);
            const std::string ep = o.endpoint.empty() ? "-"
                                   : t != tags.end()  ? t->second
                                                      : "?" + o.endpoint;
            std::string res;
            if (o.phase == "read") {
                res = o.result > 0 ? std::to_string(o.result)
                                   : "e" + std::to_string(o.result);
            } else if (o.phase != "mark_suspect") {
                res = o.result == 0 ? "ok" : "e" + std::to_string(o.result);
            }
            out.push_back(o.phase +
                          (o.phase == "read" ? "#" + std::to_string(o.call)
                                             : std::string()) +
                          "|" + o.key + "|" + o.role + "|" + ep + "|" + res);
        }
        std::sort(out.begin(), out.end());
        return out;
    }
    static std::string Join(const std::vector<std::string>& v) {
        std::string s;
        for (const auto& x : v) s += x + "; ";
        return s;
    }
    // The observed per-key log must equal `want` exactly. Two wrong
    // expectations built from `want` (one more line, `extra`; and the first
    // read from A moved to H, or from H to A) must differ from what was
    // observed, so the comparison cannot pass vacuously. 0 or `code`.
    int CheckLog(ObsLog& log, const std::map<std::string, std::string>& tags,
                 std::vector<std::string> want, const std::string& extra,
                 int code, const std::string& what) {
        std::sort(want.begin(), want.end());
        const auto got = Canon(log, tags);
        if (got != want) {
            return Fail(code, what + ": per-key log " + Join(got) + " want " +
                                  Join(want));
        }
        auto more = want;
        more.push_back(extra);
        std::sort(more.begin(), more.end());
        auto moved = want;
        for (auto& line : moved) {
            if (line.rfind("read#", 0) != 0) continue;
            const auto a = line.find("|A|"), h = line.find("|H|"),
                       m = line.find("|Hmem|");
            if (a != std::string::npos) {
                line.replace(a, 3, "|H|");
                break;
            }
            if (h != std::string::npos) {
                line.replace(h, 3, "|A|");
                break;
            }
            if (m != std::string::npos) {
                line.replace(m, 6, "|A|");
                break;
            }
        }
        std::sort(moved.begin(), moved.end());
        if (moved == want || got == more || got == moved) {
            return Fail(code, what + ": negative control not rejected");
        }
        Trace(what + ": " + Join(got) + "(2 wrong expectations rejected)");
        return 0;
    }
    // "e<code>" as written by Canon / TCanon for an error result.
    static std::string E(ErrorCode c) {
        return "e" + std::to_string(static_cast<int64_t>(c));
    }

    // Execution-site log (RealClient::transfer_observer_for_test_): what
    // the readers actually submitted and what came back. TCanon line:
    // site|key|endpoint tag|slice lengths|result, with result the bytes,
    // "ok" for a successful restore, or "e<code>". Sorted.
    struct TLog {
        std::mutex mu;
        std::vector<RealClient::TransferObservation> v;
    };
    void StartTransferObserving(TLog& log) {
        r_->transfer_observer_for_test_ =
            [&log](const RealClient::TransferObservation& o) {
                std::lock_guard<std::mutex> l(log.mu);
                log.v.push_back(o);
            };
    }
    void StopTransferObserving() { r_->transfer_observer_for_test_ = nullptr; }
    static std::string Lens(const std::vector<size_t>& lens) {
        std::string s;
        for (size_t i = 0; i < lens.size(); ++i) {
            s += (i ? "," : "") + std::to_string(lens[i]);
        }
        return s;
    }
    static std::vector<std::string> TCanon(
        const std::vector<RealClient::TransferObservation>& events,
        const std::map<std::string, std::string>& tags, bool with_lens = true) {
        std::vector<std::string> out;
        for (const auto& o : events) {
            // Selections are checked by Correlation (one per restore).
            if (o.site == "ssd_select") continue;
            const auto t = tags.find(o.endpoint);
            const std::string ep =
                t != tags.end() ? t->second : "?" + o.endpoint;
            std::string res;
            if (o.result < 0) {
                res = "e" + std::to_string(o.result);
            } else if (o.site == "ssd_restore") {
                res = o.result == 0 ? "ok" : "?" + std::to_string(o.result);
            } else {
                res = std::to_string(o.result);
            }
            out.push_back(o.site + "#" + std::to_string(o.attempt) + "|" +
                          o.key + "|" + ep + "|" +
                          (with_lens ? Lens(o.slice_lens) + "|" : "") + res);
        }
        std::sort(out.begin(), out.end());
        return out;
    }
    // The recorded execution-site log equals `want` exactly. The checker is
    // also run on two altered copies of the recorded events, which it must
    // reject: one extra submission (a memory transfer for `extra_key`), and
    // the first transfer from H reported as coming from A (or the reverse).
    // Then the typed side fields: every memory transfer had lease time left
    // and the replica id the reader's own query showed (`mem_ids`); every
    // successful restore has a nonzero batch id and gc TTL.
    int CheckTransfers(TLog& log,
                       const std::map<std::string, std::string>& tags,
                       std::vector<std::string> want,
                       const std::string& extra_key,
                       const std::map<std::string, uint64_t>& mem_ids, int code,
                       const std::string& what, bool with_lens = true) {
        std::vector<RealClient::TransferObservation> got_events;
        {
            std::lock_guard<std::mutex> l(log.mu);
            got_events = log.v;
        }
        std::sort(want.begin(), want.end());
        const auto got = TCanon(got_events, tags, with_lens);
        if (got != want) {
            return Fail(code, what + ": execution-site log " + Join(got) +
                                  " want " + Join(want));
        }
        auto extra = got_events;
        RealClient::TransferObservation fake;
        fake.site = "memory_transfer";
        fake.key = extra_key;
        extra.push_back(fake);
        auto moved = got_events;
        std::string ep_a, ep_h;
        for (const auto& [ep, tag] : tags) {
            if (tag == "A") ep_a = ep;
            if (tag == "H") ep_h = ep;
        }
        bool changed = false;
        for (auto& o : moved) {
            if (o.site != "ssd_transfer") continue;
            o.endpoint = o.endpoint == ep_h ? ep_a : ep_h;
            changed = true;
            break;
        }
        for (auto& o : moved) {  // no SSD transfer: move a memory one
            if (changed || o.site != "memory_transfer") continue;
            o.endpoint = ep_a;
            changed = true;
        }
        if (TCanon(extra, tags, with_lens) == want || !changed ||
            TCanon(moved, tags, with_lens) == want) {
            return Fail(code, what + ": checker accepted an altered log");
        }
        for (const auto& o : got_events) {
            if (o.site == "memory_transfer" && !mem_ids.empty()) {
                const auto id = mem_ids.find(o.key);
                if (o.lease_ms <= 0 || id == mem_ids.end() ||
                    id->second != o.replica_id) {
                    return Fail(code,
                                what + ": memory transfer of " + o.key +
                                    " lease_ms=" + std::to_string(o.lease_ms) +
                                    " replica=" + std::to_string(o.replica_id));
                }
            }
            if (o.site == "ssd_restore" && o.result == 0 &&
                (o.batch_id == 0 || o.gc_ttl_ms <= 0)) {
                return Fail(code, what + ": restore of " + o.key +
                                      " batch=" + std::to_string(o.batch_id) +
                                      " gc_ttl=" + std::to_string(o.gc_ttl_ms));
            }
        }
        Trace(what + " (execution sites): " + Join(got) +
              "(extra submission and moved source rejected)");
        return 0;
    }

    // Destination slices by input index, as handed to the reader: (address,
    // length) pairs; length 0 stands for "the object size of the query the
    // attempt consumed" (batch_get_into slices the destination by the
    // replica it reads).
    using DestMap = std::map<size_t, std::vector<std::pair<uintptr_t, size_t>>>;
    // Each transfer event against the read attempt it belongs to. An
    // ssd_restore / ssd_transfer belongs to the one ssd_select of its call,
    // attempt and key (same endpoint and destination slices); a
    // memory_transfer or ssd_select to the read event of its call, attempt,
    // key and input index. Against that read event the event must carry
    // the same endpoint, replica id, replica type, backup flag, lease
    // deadline (the identity of the query result consumed) and resolved
    // tenant; every event runs in the reader's tenant and writes exactly
    // the destination (address, length) pairs (`dests`); a successful SSD
    // restore names its owner's transfer engine (`te_tags`, tag equal to
    // the endpoint's tag) and nonzero source pointers; every selection has
    // exactly one restore. "" when all hold.
    static std::string Correlation(
        const std::vector<RealClient::ReadObservation>& reads,
        const std::vector<RealClient::TransferObservation>& xfers,
        const std::map<std::string, std::string>& tags,
        const std::map<std::string, std::string>& te_tags, const DestMap& dests,
        const std::string& tenant) {
        const auto same_step = [](const RealClient::TransferObservation& a,
                                  const RealClient::TransferObservation& b) {
            return a.call_id == b.call_id && a.attempt == b.attempt &&
                   a.key == b.key;
        };
        for (const auto& t : xfers) {
            const std::string id =
                t.site + "|" + t.key + "|attempt " + std::to_string(t.attempt);
            if (t.call_id == 0) return id + ": no read call";
            if (t.tenant != tenant) return id + ": tenant " + t.tenant;
            const bool ssd_io =
                t.site == "ssd_restore" || t.site == "ssd_transfer";
            const RealClient::TransferObservation* sel = nullptr;
            if (ssd_io) {
                int n = 0;
                for (const auto& x : xfers) {
                    if (x.site == "ssd_select" && same_step(x, t)) {
                        sel = &x;
                        ++n;
                    }
                }
                if (n != 1) {
                    return id + ": " + std::to_string(n) + " selections";
                }
                if (sel->endpoint != t.endpoint) {
                    return id + ": endpoint differs from its selection";
                }
                if (sel->slice_addrs != t.slice_addrs ||
                    sel->slice_lens != t.slice_lens) {
                    return id + ": slices differ from its selection";
                }
            }
            const auto& ident = sel != nullptr ? *sel : t;
            const RealClient::ReadObservation* r = nullptr;
            for (const auto& o : reads) {
                if (o.phase == "read" && o.call_id == t.call_id &&
                    o.call == t.attempt && o.key == t.key &&
                    o.index == ident.index) {
                    r = &o;
                }
            }
            if (r == nullptr) {
                return id + ": no read attempt at index " +
                       std::to_string(ident.index);
            }
            // A key is transferred once per attempt, at its first occurrence
            // in the input (duplicates get those bytes copied locally).
            for (const auto& o : reads) {
                if (o.phase == "read" && o.call_id == t.call_id &&
                    o.call == t.attempt && o.key == t.key &&
                    o.index < ident.index) {
                    return id + ": transferred at index " +
                           std::to_string(ident.index) +
                           ", not at its first occurrence " +
                           std::to_string(o.index);
                }
            }
            if (r->endpoint != t.endpoint) {
                return id + ": endpoint differs from the attempt's query";
            }
            if (r->tenant != tenant) return id + ": read tenant " + r->tenant;
            if (ssd_io) {
                // The owner is asked for the key in the reader's tenant
                // scope, which is the scope the master resolved.
                if (t.storage_key != TenantId(tenant).MakeScopedKey(t.key)) {
                    return id + ": storage key '" + t.storage_key + "'";
                }
                if (!sel->resolved_tenant.empty() &&
                    sel->resolved_tenant != tenant) {
                    return id + ": scope " + tenant + " but resolved " +
                           sel->resolved_tenant;
                }
            }
            if (ident.replica_id != r->replica_id) {
                return id + ": replica " + std::to_string(ident.replica_id) +
                       " but the query gave " + std::to_string(r->replica_id);
            }
            if (ident.role != r->role) {
                return id + ": type " + ident.role + " but the query's " +
                       r->role;
            }
            if (ident.backup != r->backup) return id + ": backup flag differs";
            if (ident.lease_deadline_ns != r->lease_deadline_ns) {
                return id + ": lease deadline differs (another query result)";
            }
            if (ident.resolved_tenant != r->resolved_tenant) {
                return id + ": resolved tenant " + ident.resolved_tenant +
                       " but the query's " + r->resolved_tenant;
            }
            if (auto d = dests.find(ident.index); d != dests.end()) {
                const auto& want = d->second;
                if (want.size() != t.slice_addrs.size() ||
                    t.slice_lens.size() != t.slice_addrs.size()) {
                    return id + ": destination slice count differs";
                }
                for (size_t j = 0; j < want.size(); ++j) {
                    if (want[j].first != t.slice_addrs[j]) {
                        return id + ": destination address differs";
                    }
                    const size_t len =
                        want[j].second != 0 ? want[j].second : r->object_size;
                    if (t.slice_lens[j] != len) {
                        return id + ": destination length " +
                               std::to_string(t.slice_lens[j]) + " want " +
                               std::to_string(len);
                    }
                }
            }
            if (ssd_io && t.batch_id != 0) {
                const auto te = te_tags.find(t.te_endpoint);
                const auto ep = tags.find(t.endpoint);
                if (te == te_tags.end() || ep == tags.end() ||
                    te->second != ep->second) {
                    return id + ": transfer engine " + t.te_endpoint;
                }
                if (t.pointers.size() != 1 || t.pointers[0] == 0) {
                    return id + ": not one nonzero restored pointer";
                }
            }
            // The transfer reads the buffer its restore returned: same
            // batch and the same source pointer.
            if (t.site == "ssd_transfer") {
                const RealClient::TransferObservation* rs = nullptr;
                for (const auto& x : xfers) {
                    if (x.site == "ssd_restore" && same_step(x, t)) rs = &x;
                }
                if (rs == nullptr || rs->batch_id != t.batch_id ||
                    rs->pointers != t.pointers ||
                    rs->te_endpoint != t.te_endpoint ||
                    rs->storage_key != t.storage_key) {
                    return id + ": not the buffer of its restore";
                }
            }
        }
        // Within one restore batch every key has its own source pointer.
        std::map<std::pair<std::string, uint64_t>, std::set<uintptr_t>>
            batch_ptrs;
        std::map<std::pair<std::string, uint64_t>, size_t> batch_keys;
        for (const auto& x : xfers) {
            if (x.site != "ssd_restore" || x.batch_id == 0) continue;
            const auto b = std::make_pair(x.te_endpoint, x.batch_id);
            batch_ptrs[b].insert(x.pointers.empty() ? 0 : x.pointers[0]);
            ++batch_keys[b];
        }
        for (const auto& [b, n] : batch_keys) {
            if (batch_ptrs[b].size() != n) {
                return "restore batch " + std::to_string(b.second) +
                       ": keys share a source pointer";
            }
        }
        // Every read attempt consumed a query result that was observed:
        // the same key and input index, and the same lease deadline as a
        // query, backup query or fresh query of its call.
        for (const auto& o : reads) {
            if (o.phase != "read" || o.role == "no_query") continue;
            bool found = false;
            for (const auto& q : reads) {
                if ((q.phase == "query" || q.phase == "query_backup" ||
                     q.phase == "requery") &&
                    q.call_id == o.call_id && q.key == o.key &&
                    q.index == o.index &&
                    q.lease_deadline_ns == o.lease_deadline_ns) {
                    found = true;
                }
            }
            if (!found) {
                return "read#" + std::to_string(o.call) + "|" + o.key +
                       "|index " + std::to_string(o.index) +
                       ": consumed no observed query result";
            }
        }
        for (const auto& s : xfers) {
            if (s.site != "ssd_select") continue;
            int n = 0;
            for (const auto& x : xfers) {
                if (x.site == "ssd_restore" && same_step(x, s)) ++n;
            }
            if (n != 1) {
                return "ssd_select|" + s.key + "|attempt " +
                       std::to_string(s.attempt) + ": " + std::to_string(n) +
                       " restores";
            }
        }
        return "";
    }
    // Correlation on the recorded logs, then on altered copies of the
    // recorded transfer events, one field changed in one event each, which
    // must all be rejected: attempt, endpoint, replica id, replica type,
    // backup flag, lease deadline, input index, requested and resolved
    // tenant, first destination address and length. A field no event can
    // carry in this step (e.g. no SSD selection) is skipped; at least
    // `min_altered` must apply. 0 or `code`.
    int CheckCorrelation(ObsLog& obs, TLog& tlog,
                         const std::map<std::string, std::string>& tags,
                         const std::map<std::string, std::string>& te_tags,
                         const DestMap& dests, int code,
                         const std::string& what, size_t min_altered = 9) {
        std::vector<RealClient::ReadObservation> reads;
        std::vector<RealClient::TransferObservation> xfers;
        {
            std::lock_guard<std::mutex> l(obs.mu);
            reads = obs.v;
        }
        {
            std::lock_guard<std::mutex> l(tlog.mu);
            xfers = tlog.v;
        }
        const std::string tenant = r_->client_->tenant_id();
        const std::string why =
            Correlation(reads, xfers, tags, te_tags, dests, tenant);
        if (!why.empty()) return Fail(code, what + ": " + why);
        int altered = 0, rejected = 0;
        const auto try_altered = [&](auto change) {
            auto copy = xfers;
            for (auto& t : copy) {
                if (change(t)) {
                    ++altered;
                    if (!Correlation(reads, copy, tags, te_tags, dests, tenant)
                             .empty()) {
                        ++rejected;
                    }
                    return;
                }
            }
        };
        // The attempt change is applied to an event whose key was read only
        // once in its call: two attempts of one key may legitimately carry
        // the same identity (same endpoint, a reused replica id/address).
        try_altered([&](RealClient::TransferObservation& t) {
            for (const auto& o : reads) {
                if (o.phase == "read" && o.call_id == t.call_id &&
                    o.key == t.key && o.call != t.attempt) {
                    return false;
                }
            }
            t.attempt ^= 1;
            return true;
        });
        try_altered([&](RealClient::TransferObservation& t) {
            for (const auto& [ep, tag] : tags) {
                if (ep != t.endpoint) {
                    t.endpoint = ep;
                    return true;
                }
            }
            return false;
        });
        // Identity fields live on memory transfers and SSD selections.
        const auto identity = [](const RealClient::TransferObservation& t) {
            return t.site == "memory_transfer" || t.site == "ssd_select";
        };
        try_altered([&](RealClient::TransferObservation& t) {
            if (!identity(t)) return false;
            ++t.replica_id;
            return true;
        });
        try_altered([&](RealClient::TransferObservation& t) {
            if (!identity(t)) return false;
            t.role = t.role == "memory" ? "local_disk" : "memory";
            return true;
        });
        try_altered([&](RealClient::TransferObservation& t) {
            if (!identity(t)) return false;
            t.backup = !t.backup;
            return true;
        });
        try_altered([&](RealClient::TransferObservation& t) {
            if (!identity(t)) return false;
            ++t.lease_deadline_ns;
            return true;
        });
        try_altered([&](RealClient::TransferObservation& t) {
            if (!identity(t) || t.index == SIZE_MAX) return false;
            t.index ^= 1;
            return true;
        });
        try_altered([](RealClient::TransferObservation& t) {
            t.tenant += "-other";
            return true;
        });
        try_altered([&](RealClient::TransferObservation& t) {
            if (!identity(t)) return false;
            t.resolved_tenant += "-other";
            return true;
        });
        // Restore-boundary identity (SSD reads only).
        try_altered([](RealClient::TransferObservation& t) {
            if (t.storage_key.empty()) return false;
            t.storage_key += "-other";
            return true;
        });
        try_altered([](RealClient::TransferObservation& t) {
            if (t.te_endpoint.empty()) return false;
            t.te_endpoint += "-other";
            return true;
        });
        try_altered([](RealClient::TransferObservation& t) {
            if (t.pointers.empty()) return false;
            t.pointers[0] += 64;
            return true;
        });
        try_altered([](RealClient::TransferObservation& t) {
            if (t.batch_id == 0) return false;
            ++t.batch_id;
            return true;
        });
        try_altered([](RealClient::TransferObservation& t) {
            if (t.slice_addrs.empty()) return false;
            ++t.slice_addrs[0];
            return true;
        });
        try_altered([](RealClient::TransferObservation& t) {
            if (t.slice_lens.empty()) return false;
            ++t.slice_lens[0];
            return true;
        });
        if (rejected != altered || static_cast<size_t>(altered) < min_altered) {
            return Fail(code, what +
                                  ": correlation accepted an altered event (" +
                                  std::to_string(rejected) + "/" +
                                  std::to_string(altered) + " rejected)");
        }
        Trace(what + " (correlation): " + std::to_string(xfers.size()) +
              " transfer events match their attempts' query results (index, "
              "replica, type, backup flag, lease deadline, resolved tenant), "
              "destinations and tenant; " +
              std::to_string(altered) + " altered copies rejected");
        return 0;
    }
    static uint64_t MemoryReplicaId(
        const tl::expected<QueryResult, ErrorCode>& q) {
        if (q) {
            for (const auto& d : q->replicas) {
                if (d.is_memory_replica()) return d.id;
            }
        }
        return 0;
    }

    static std::string DiskEndpoint(
        const tl::expected<QueryResult, ErrorCode>& q) {
        if (q) {
            for (const auto& d : q->replicas) {
                if (d.is_local_disk_replica()) {
                    return d.get_local_disk_descriptor().transport_endpoint;
                }
            }
        }
        return {};
    }
    static std::string MemoryEndpoint(
        const tl::expected<QueryResult, ErrorCode>& q) {
        if (q) {
            for (const auto& d : q->replicas) {
                if (d.is_memory_replica()) {
                    return d.get_memory_descriptor()
                        .buffer_descriptor.transport_endpoint_;
                }
            }
        }
        return {};
    }

    // Reads `keys` with batch_get_into into fresh slots of the registered
    // buffer; returns per key the result and the bytes read.
    struct Got {
        int64_t result = 0;
        std::string bytes;
    };
    std::vector<Got> GetInto(const std::vector<std::string>& keys,
                             const std::vector<size_t>& sizes) {
        std::vector<void*> bufs;
        size_t off = 0;
        for (size_t s : sizes) {
            bufs.push_back(buf_.data() + off);
            off += s;
        }
        std::memset(buf_.data(), 0, off);
        auto res = r_->batch_get_into(keys, bufs, sizes);
        std::vector<Got> out;
        for (size_t i = 0; i < keys.size(); ++i) {
            Got g;
            g.result = i < res.size() ? res[i] : -9999;
            if (g.result > 0) {
                g.bytes.assign(static_cast<char*>(bufs[i]),
                               static_cast<size_t>(g.result));
            }
            out.push_back(std::move(g));
        }
        return out;
    }
    // Same through batch_get_into_multi_buffers, each key split into two
    // slices (first `split` bytes, then the rest).
    std::vector<Got> GetIntoMulti(const std::vector<std::string>& keys,
                                  const std::vector<size_t>& sizes,
                                  size_t split) {
        std::vector<std::vector<void*>> bufs;
        std::vector<std::vector<size_t>> lens;
        size_t off = 0;
        for (size_t s : sizes) {
            const size_t first = std::min(split, s);
            bufs.push_back({buf_.data() + off, buf_.data() + off + first});
            lens.push_back({first, s - first});
            off += s;
        }
        std::memset(buf_.data(), 0, off);
        auto res = r_->batch_get_into_multi_buffers(keys, bufs, lens, false);
        std::vector<Got> out;
        for (size_t i = 0; i < keys.size(); ++i) {
            Got g;
            g.result = i < res.size() ? res[i] : -9999;
            if (g.result > 0) {
                g.bytes.assign(static_cast<char*>(bufs[i][0]),
                               static_cast<size_t>(g.result));
            }
            out.push_back(std::move(g));
        }
        return out;
    }

    // Destination layout for one batch: every slice at its own place in the
    // registered buffer, with kGuard guard bytes (kGuardByte) before, between
    // and after the slices. Each key gets `parts` slices (multi-buffer API)
    // or one.
    static constexpr size_t kGuard = 256;
    static constexpr char kGuardByte = static_cast<char>(0xEE);
    struct Layout {
        std::vector<std::vector<char*>> ptrs;
        std::vector<std::vector<size_t>> lens;
        size_t used = 0;
    };
    Layout MakeLayout(const std::vector<size_t>& sizes, size_t parts) {
        Layout l;
        size_t off = kGuard;
        for (size_t s : sizes) {
            std::vector<char*> p;
            std::vector<size_t> n;
            size_t left = s;
            for (size_t j = 0; j < parts; ++j) {
                // Unequal parts (a third, then the rest), so a reader that
                // swapped or repeated slices would not reproduce the bytes.
                const size_t len = j + 1 == parts ? left : left / 3;
                p.push_back(buf_.data() + off);
                n.push_back(len);
                off += len + kGuard;
                left -= len;
            }
            l.ptrs.push_back(std::move(p));
            l.lens.push_back(std::move(n));
        }
        l.used = off;
        std::memset(buf_.data(), kGuardByte, l.used);
        return l;
    }
    // Bytes of key i reassembled from its slices; and whether every guard
    // byte is intact (nothing written outside the slices).
    static std::string Assemble(const Layout& l, size_t i) {
        std::string out;
        for (size_t j = 0; j < l.ptrs[i].size(); ++j) {
            out.append(l.ptrs[i][j], l.lens[i][j]);
        }
        return out;
    }
    // Destination slices by input index, as handed to the reader (DestMap):
    // the caller's (address, length) pairs for the multi-buffer API; the
    // one destination address with length 0 ("object size of the attempt's
    // query") for batch_get_into, which slices by the replica it reads.
    static DestMap Dests(const std::vector<std::string>& keys,
                         const Layout& l) {
        DestMap d;
        for (size_t i = 0; i < keys.size() && i < l.ptrs.size(); ++i) {
            const bool single = l.ptrs[i].size() == 1;
            for (size_t j = 0; j < l.ptrs[i].size(); ++j) {
                d[i].push_back({reinterpret_cast<uintptr_t>(l.ptrs[i][j]),
                                single ? 0 : l.lens[i][j]});
            }
        }
        return d;
    }
    // Deterministic payload that differs per key, generation and byte offset.
    static std::string Payload(int key_seed, int gen, size_t n) {
        std::string s(n, '\0');
        for (size_t i = 0; i < n; ++i) {
            s[i] = static_cast<char>(
                (key_seed * 131 + gen * 17 + i * 7 + (i >> 8) * 3) & 0xff);
        }
        return s;
    }
    // Key i's slices were never written (still all guard bytes).
    static bool Untouched(const Layout& l, size_t i) {
        for (size_t j = 0; j < l.ptrs[i].size(); ++j) {
            for (size_t k = 0; k < l.lens[i][j]; ++k) {
                if (l.ptrs[i][j][k] != kGuardByte) return false;
            }
        }
        return true;
    }
    bool GuardsIntact(const Layout& l) const {
        size_t off = 0;
        auto check = [&](size_t from, size_t to) {
            for (size_t k = from; k < to; ++k) {
                if (buf_[k] != kGuardByte) return false;
            }
            return true;
        };
        for (size_t i = 0; i < l.ptrs.size(); ++i) {
            for (size_t j = 0; j < l.ptrs[i].size(); ++j) {
                const size_t start =
                    static_cast<size_t>(l.ptrs[i][j] - buf_.data());
                if (!check(off, start)) return false;
                off = start + l.lens[i][j];
            }
        }
        return check(off, l.used);
    }

    // T7 step 2: each promised API as the first reader after the primary
    // SSD RPC fails (fresh reader, no suspect endpoint yet), in one mixed
    // batch: k1 (4096) and k2 (12288) affected (primary SSD on A, backup on
    // H), "ok" (8192, a DRAM copy on H) unaffected, and a missing key.
    // Required: exactly the two affected keys are retried once each from
    // the backup (attempts == ok == 2), every result in its own position
    // with its exact length and bytes, the unaffected key read once from
    // its own copy, the missing key failing alone, and no byte written
    // outside the destination slices (multi-buffer: two separated slices
    // per key). api 0: batch_get_into, 1: batch_get_into_multi_buffers.
    int FirstReaderAfterPrimaryFailureChild(int api) {
        // Read once per process (on the first suspect mark), and this child
        // is a fresh process: the recovery step can wait for expiry.
        setenv("MC_SSD_BACKUP_SUSPECT_TTL_MS", "5000", 1);
        std::thread([] {
            std::this_thread::sleep_for(std::chrono::seconds(120));
            DumpStacksAndExit("first reader did not finish");
        }).detach();
        a_ = Make("rd-src", true, "a");
        h_ = Make("rd-holder", true, "h");
        r_ = Make("rd-reader", false, "");
        if (!a_ || !h_ || !r_) return Fail(10, "client setup");
        const UUID a = Id(*a_), h = Id(*h_);
        if (!WaitFor(
                [&] {
                    return Ms().GetSsdBackupClientSnapshotForTest(h)
                               .has_value() &&
                           Ms().GetSsdBackupClientSnapshotForTest(a)
                               .has_value();
                },
                std::chrono::seconds(20))) {
            return Fail(11, "backup candidates missing");
        }
        const std::string v1 = Payload(1, 1, 4096), v2 = Payload(2, 1, 12288),
                          vok = Payload(3, 1, 8192);
        ReplicateConfig on_a, on_h;
        on_a.replica_num = on_h.replica_num = 1;
        on_a.preferred_segment = Hostname(*a_);
        on_h.preferred_segment = Hostname(*h_);
        if (a_->put("k1", std::span<const char>(v1.data(), v1.size()), on_a) ||
            a_->put("k2", std::span<const char>(v2.data(), v2.size()), on_a)) {
            return Fail(12, "put");
        }
        const std::string primary = "P:" + Tag(a) + ":",
                          backup = "B:" + Tag(h) + ":";
        if (!WaitFor(
                [&] {
                    for (const char* k : {"k1", "k2"}) {
                        const auto r = Record(k);
                        if (r.find(primary) == std::string::npos ||
                            r.find(backup) == std::string::npos) {
                            return false;
                        }
                    }
                    return true;
                },
                std::chrono::seconds(40))) {
            return Fail(13, "no primary SSD + backup");
        }
        // k1/k2 lose their DRAM copy (evicted after their write leases
        // expired). "ok" is written only afterwards, so its DRAM copy on H
        // is the replica the reader selects.
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        test::MasterServiceTestPeer(Ms()).RunBatchEvictForTesting(1.0, 1.0);
        for (const char* k : {"k1", "k2"}) {
            auto q = r_->client_->Query(k);
            if (!q) return Fail(14, std::string("query ") + k);
            for (const auto& d : q->replicas) {
                if (d.is_memory_replica()) {
                    return Fail(14, std::string(k) + " still in DRAM");
                }
            }
        }
        if (h_->put("ok", std::span<const char>(vok.data(), vok.size()),
                    on_h)) {
            return Fail(12, "put ok");
        }
        // Endpoint tags for the per-key log: A's and H's SSD RPC endpoints
        // (primary and backup of k1/k2) and H's DRAM endpoint ("ok").
        const auto qk1 = r_->client_->Query("k1"),
                   qk2 = r_->client_->Query("k2");
        const auto bk = r_->client_->BatchQueryBackup({"k1", "k2"});
        const std::string ep_a = DiskEndpoint(qk1);
        const std::string ep_h = bk.size() == 2 ? DiskEndpoint(bk[0]) : "";
        const std::string ep_hmem = MemoryEndpoint(r_->client_->Query("ok"));
        if (ep_a.empty() || ep_h.empty() || ep_hmem.empty() || ep_a == ep_h ||
            ep_hmem == ep_a || ep_hmem == ep_h || DiskEndpoint(qk2) != ep_a ||
            DiskEndpoint(bk[1]) != ep_h) {
            return Fail(
                25, "endpoints A=" + ep_a + " H=" + ep_h + " Hmem=" + ep_hmem);
        }
        const std::map<std::string, std::string> tags{
            {ep_a, "A"}, {ep_h, "H"}, {ep_hmem, "Hmem"}};
        // The SSD owners' transfer engine addresses (what a restore answers).
        const std::map<std::string, std::string> te_tags{
            {a_->client_->GetSegmentEndpoint(), "A"},
            {h_->client_->GetSegmentEndpoint(), "H"}};
        const auto records_before =
            Ms().DescribeSsdRecordsForTest({"k1", "k2"}, TenantId::Default());
        buf_.assign(1 << 20, 0);
        if (r_->register_buffer(buf_.data(), buf_.size()) != 0) {
            return Fail(15, "register reader buffer");
        }
        StopOffloadServer(*a_);  // A stays registered with the master
        if (r_->SuspectEndpointCountForTest() != 0 || FallbackAttempts(*r_)) {
            return Fail(16, "reader not fresh");
        }
        const std::vector<std::string> keys{"k1", "ok", "k2", "nokey"};
        const std::vector<size_t> sizes{v1.size(), vok.size(), v2.size(), 4096};
        const std::vector<const std::string*> want{&v1, &vok, &v2, nullptr};
        const auto read_with = [&](const std::vector<std::string>& ks,
                                   const std::vector<size_t>& ss,
                                   const Layout& lay) {
            std::vector<int64_t> out;
            if (api == 0) {
                std::vector<void*> bufs;
                for (const auto& p : lay.ptrs) bufs.push_back(p[0]);
                out = r_->batch_get_into(ks, bufs, ss);
            } else {
                std::vector<std::vector<void*>> bufs;
                for (const auto& p : lay.ptrs) bufs.push_back({p[0], p[1]});
                auto r =
                    r_->batch_get_into_multi_buffers(ks, bufs, lay.lens, false);
                out.assign(r.begin(), r.end());
            }
            return out;
        };
        const Layout l = MakeLayout(sizes, api == 0 ? 1 : 2);
        // Destination slices the reader must hand to the transfer: one
        // slice per key for batch_get_into, the caller's two for the
        // multi-buffer API.
        const auto lens_of = [&](const Layout& lay, size_t i) {
            size_t total = 0;
            for (size_t n : lay.lens[i]) total += n;
            return api == 0 ? std::to_string(total) : Lens(lay.lens[i]);
        };
        const std::map<std::string, uint64_t> mem_ids{
            {"ok", MemoryReplicaId(r_->client_->Query("ok"))}};
        ObsLog obs;
        TLog tobs;
        StartObserving(obs);
        StartTransferObserving(tobs);
        const std::vector<int64_t> res = read_with(keys, sizes, l);
        StopTransferObserving();
        StopObserving();
        std::string results;
        for (auto v : res) results += std::to_string(v) + " ";
        if (res.size() != keys.size()) return Fail(17, "result count");
        for (size_t i = 0; i < keys.size(); ++i) {
            if (want[i] == nullptr) {
                if (res[i] >= 0)
                    return Fail(18, "missing key read: " + results);
                if (!Untouched(l, i)) {
                    return Fail(24, "missing key's destination was written");
                }
                continue;
            }
            if (res[i] != static_cast<int64_t>(want[i]->size()) ||
                Assemble(l, i) != *want[i]) {
                return Fail(19, keys[i] + " wrong (results " + results + ")");
            }
        }
        if (!GuardsIntact(l)) return Fail(20, "bytes written outside slices");
        if (FallbackAttempts(*r_) != 2 || FallbackOk(*r_) != 2) {
            return Fail(21, "fallback attempts=" +
                                std::to_string(FallbackAttempts(*r_)) +
                                " ok=" + std::to_string(FallbackOk(*r_)));
        }
        if (r_->SuspectEndpointCountForTest() != 1) {
            return Fail(22,
                        "suspect endpoints=" +
                            std::to_string(r_->SuspectEndpointCountForTest()));
        }
        // Per key: k1/k2 queried once (primary SSD on A), read once from A
        // (transport error), A marked suspect once per failed key, one
        // backup query each (H), one retry each from H with full length;
        // "ok" queried and read once from H's DRAM, never retried; the
        // missing key has no query result, takes no transfer and no retry.
        const std::string len1 = std::to_string(v1.size()),
                          len2 = std::to_string(v2.size()),
                          lenok = std::to_string(vok.size());
        const std::string rpc_fail = E(ErrorCode::RPC_FAIL),
                          not_found = E(ErrorCode::OBJECT_NOT_FOUND);
        if (int c = CheckLog(
                obs, tags,
                {"query|k1|local_disk|A|ok", "query|k2|local_disk|A|ok",
                 "query|ok|memory|Hmem|ok",
                 "query|nokey|no_query|-|" + not_found,
                 "read#0|k1|local_disk|A|" + rpc_fail,
                 "read#0|k2|local_disk|A|" + rpc_fail,
                 "read#0|ok|memory|Hmem|" + lenok,
                 "read#0|nokey|no_query|-|" + not_found, "mark_suspect|||A|",
                 "mark_suspect|||A|", "query_backup|k1|local_disk|H|ok",
                 "query_backup|k2|local_disk|H|ok",
                 "read#1|k1|local_disk|H|" + len1,
                 "read#1|k2|local_disk|H|" + len2},
                "read#1|ok|memory|Hmem|" + lenok, 26, "first reader")) {
            return c;
        }
        // What actually moved: one memory transfer for "ok" from H's DRAM;
        // one restore RPC to A for k1/k2 that fails with RPC_FAIL and no
        // transfer from A; one restore and one transfer each from H on the
        // retry; nothing at all for the missing key (index 3).
        if (int c = CheckTransfers(
                tobs, tags,
                {"memory_transfer#0|ok|Hmem|" + lens_of(l, 1) + "|" + lenok,
                 "ssd_restore#0|k1|A|" + lens_of(l, 0) + "|" + rpc_fail,
                 "ssd_restore#0|k2|A|" + lens_of(l, 2) + "|" + rpc_fail,
                 "ssd_restore#1|k1|H|" + lens_of(l, 0) + "|ok",
                 "ssd_restore#1|k2|H|" + lens_of(l, 2) + "|ok",
                 "ssd_transfer#1|k1|H|" + lens_of(l, 0) + "|" + len1,
                 "ssd_transfer#1|k2|H|" + lens_of(l, 2) + "|" + len2},
                "nokey", mem_ids, 34, "first reader")) {
            return c;
        }
        if (int c = CheckCorrelation(obs, tobs, tags, te_tags, Dests(keys, l),
                                     34, "first reader")) {
            return c;
        }
        const auto records_after =
            Ms().DescribeSsdRecordsForTest({"k1", "k2"}, TenantId::Default());
        if (records_after != records_before) {
            return Fail(23, "SSD records changed: " + records_after[0] + " " +
                                records_after[1]);
        }
        Trace("api=" + std::to_string(api) + " results " + results +
              "fallback attempts=2 ok=2 suspect=1 guards intact, records "
              "unchanged: " +
              records_after[0] + " " + records_after[1]);

        // T7 step 3: A answers again. While A is still suspect, k1/k2 go
        // straight to H; once the suspect time (MC_SSD_BACKUP_SUSPECT_TTL_MS,
        // 5000 ms here) is over, they are read from A's primary SSD again,
        // with no fallback.
        if (!RestartOffloadServer(*a_)) return Fail(27, "restart A's server");
        const std::vector<std::string> rk{"k1", "k2"};
        const std::vector<size_t> rs{v1.size(), v2.size()};
        // Both logs for one read of k1/k2: `want` per key (orchestration),
        // `from` the SSD owner tag every restore and transfer must name.
        const auto read_and_check = [&](int code, const std::string& what,
                                        const std::vector<std::string>& want,
                                        const std::string& from) -> int {
            const Layout lay = MakeLayout(rs, api == 0 ? 1 : 2);
            ObsLog o;
            TLog t;
            StartObserving(o);
            StartTransferObserving(t);
            const auto got = read_with(rk, rs, lay);
            StopTransferObserving();
            StopObserving();
            if (got.size() != 2 || got[0] != static_cast<int64_t>(v1.size()) ||
                got[1] != static_cast<int64_t>(v2.size()) ||
                Assemble(lay, 0) != v1 || Assemble(lay, 1) != v2 ||
                !GuardsIntact(lay)) {
                return Fail(code, what + ": wrong bytes or results");
            }
            if (int c =
                    CheckLog(o, tags, want, "read#1|k1|local_disk|A|" + len1,
                             code, what)) {
                return c;
            }
            const int c = CheckTransfers(
                t, tags,
                {"ssd_restore#0|k1|" + from + "|" + lens_of(lay, 0) + "|ok",
                 "ssd_restore#0|k2|" + from + "|" + lens_of(lay, 1) + "|ok",
                 "ssd_transfer#0|k1|" + from + "|" + lens_of(lay, 0) + "|" +
                     len1,
                 "ssd_transfer#0|k2|" + from + "|" + lens_of(lay, 1) + "|" +
                     len2},
                "nokey", {}, code, what);
            if (c != 0) return c;
            return CheckCorrelation(o, t, tags, te_tags, Dests(rk, lay), code,
                                    what);
        };
        if (!r_->IsSuspectEndpoint(ep_a)) {
            return Fail(28, "A not suspect right after the failure");
        }
        if (int c = read_and_check(
                29, "while A suspect",
                {"query|k1|local_disk|A|ok", "query|k2|local_disk|A|ok",
                 "query_backup|k1|local_disk|H|ok",
                 "query_backup|k2|local_disk|H|ok",
                 "read#0|k1|local_disk|H|" + len1,
                 "read#0|k2|local_disk|H|" + len2},
                "H")) {
            return c;
        }
        if (!WaitFor([&] { return !r_->IsSuspectEndpoint(ep_a); },
                     std::chrono::seconds(15))) {
            return Fail(30, "A still suspect after its TTL");
        }
        const auto attempts_before = FallbackAttempts(*r_);
        if (int c = read_and_check(
                31, "after suspect TTL",
                {"query|k1|local_disk|A|ok", "query|k2|local_disk|A|ok",
                 "read#0|k1|local_disk|A|" + len1,
                 "read#0|k2|local_disk|A|" + len2},
                "A")) {
            return c;
        }
        if (FallbackAttempts(*r_) != attempts_before) {
            return Fail(32, "fallback used after A recovered");
        }
        if (Ms().DescribeSsdRecordsForTest({"k1", "k2"}, TenantId::Default()) !=
            records_before) {
            return Fail(33, "SSD records changed during recovery");
        }
        Trace("api=" + std::to_string(api) +
              " recovery: suspect reads from H, after TTL from A, no fallback");

        // T7 step 3b: A has recovered but is still suspect (the entry is
        // held live by marking it with a clock reading an hour ahead, so the
        // schedule cannot outlive it). H's SSD restore handler fails the
        // backup read with FILE_READ_FAIL before reading or transferring
        // anything, while "ok" is read from H's DRAM in the same batch.
        // Required: one fresh normal query for k1/k2, one retry each from
        // A with that metadata, no third read, ok and the missing key not
        // retried, bytes / positions / guards and SSD records unchanged.
        r_->MarkSuspectEndpoint(
            ep_a, std::chrono::steady_clock::now() + std::chrono::hours(1));
        if (!r_->IsSuspectEndpoint(ep_a)) return Fail(35, "A not held suspect");
        std::atomic<int> faults{0};
        h_->offload_serve_fault_for_test_ =
            [&faults](
                const std::vector<std::string>&) -> std::optional<ErrorCode> {
            ++faults;
            return ErrorCode::FILE_READ_FAIL;
        };  // STEP3B-FAULT
        const auto attempts0 = FallbackAttempts(*r_), ok0 = FallbackOk(*r_);
        const Layout l3 = MakeLayout(sizes, api == 0 ? 1 : 2);
        ObsLog obs3;
        TLog tobs3;
        StartObserving(obs3);
        StartTransferObserving(tobs3);
        const std::vector<int64_t> res3 = read_with(keys, sizes, l3);
        StopTransferObserving();
        StopObserving();
        h_->offload_serve_fault_for_test_ = nullptr;
        std::string results3;
        for (auto v : res3) results3 += std::to_string(v) + " ";
        if (res3.size() != keys.size()) return Fail(36, "step 3b result count");
        for (size_t i = 0; i < keys.size(); ++i) {
            if (want[i] == nullptr) {
                if (res3[i] !=
                        static_cast<int64_t>(ErrorCode::OBJECT_NOT_FOUND) ||
                    !Untouched(l3, i)) {
                    return Fail(36, "step 3b missing key: " + results3);
                }
                continue;
            }
            if (res3[i] != static_cast<int64_t>(want[i]->size()) ||
                Assemble(l3, i) != *want[i]) {
                return Fail(36, "step 3b " + keys[i] + " wrong: " + results3);
            }
        }
        if (!GuardsIntact(l3)) return Fail(36, "step 3b guards");
        if (faults.load() != 1) {
            return Fail(37, "H's restore handler ran " +
                                std::to_string(faults.load()) + " times");
        }
        const std::string read_fail = E(ErrorCode::FILE_READ_FAIL);
        if (int c = CheckLog(
                obs3, tags,
                {"query|k1|local_disk|A|ok", "query|k2|local_disk|A|ok",
                 "query|ok|memory|Hmem|ok",
                 "query|nokey|no_query|-|" + not_found,
                 "query_backup|k1|local_disk|H|ok",
                 "query_backup|k2|local_disk|H|ok",
                 "read#0|k1|local_disk|H|" + read_fail,
                 "read#0|k2|local_disk|H|" + read_fail,
                 "read#0|ok|memory|Hmem|" + lenok,
                 "read#0|nokey|no_query|-|" + not_found,
                 "requery|k1|local_disk|A|ok", "requery|k2|local_disk|A|ok",
                 "read#1|k1|local_disk|A|" + len1,
                 "read#1|k2|local_disk|A|" + len2},
                "read#2|k1|local_disk|A|" + len1, 38, "step 3b")) {
            return c;
        }
        if (int c = CheckTransfers(
                tobs3, tags,
                {"memory_transfer#0|ok|Hmem|" + lens_of(l3, 1) + "|" + lenok,
                 "ssd_restore#0|k1|H|" + lens_of(l3, 0) + "|" + read_fail,
                 "ssd_restore#0|k2|H|" + lens_of(l3, 2) + "|" + read_fail,
                 "ssd_restore#1|k1|A|" + lens_of(l3, 0) + "|ok",
                 "ssd_restore#1|k2|A|" + lens_of(l3, 2) + "|ok",
                 "ssd_transfer#1|k1|A|" + lens_of(l3, 0) + "|" + len1,
                 "ssd_transfer#1|k2|A|" + lens_of(l3, 2) + "|" + len2},
                "nokey", mem_ids, 39, "step 3b")) {
            return c;
        }
        if (int c = CheckCorrelation(obs3, tobs3, tags, te_tags,
                                     Dests(keys, l3), 39, "step 3b")) {
            return c;
        }
        if (FallbackAttempts(*r_) != attempts0 + 2 ||
            FallbackOk(*r_) != ok0 + 2) {
            return Fail(40,
                        "step 3b fallback attempts +" +
                            std::to_string(FallbackAttempts(*r_) - attempts0) +
                            " ok +" + std::to_string(FallbackOk(*r_) - ok0));
        }
        if (!r_->IsSuspectEndpoint(ep_a))
            return Fail(41, "A no longer suspect");
        if (Ms().DescribeSsdRecordsForTest({"k1", "k2"}, TenantId::Default()) !=
            records_before) {
            return Fail(42, "SSD records changed in step 3b");
        }
        Trace("api=" + std::to_string(api) + " step 3b: H restore failed (" +
              read_fail + "), one requery, retry from A, results " + results3);
        r_->unregister_buffer(buf_.data());
        return 0;
    }

    // Child body: 0 on success, the failed check's code otherwise, 9 from
    // the watchdog.
    int RealReaderChild() {
        std::thread([] {
            std::this_thread::sleep_for(std::chrono::seconds(120));
            DumpStacksAndExit("real reader did not finish");
        }).detach();
        a_ = Make("rd-src", true, "a");
        h_ = Make("rd-holder", true, "h");
        r_ = Make("rd-reader", false, "");
        if (!a_ || !h_ || !r_) return Fail(10, "client setup");
        const UUID a = Id(*a_), h = Id(*h_);
        if (!WaitFor(
                [&] {
                    return Ms()
                        .GetSsdBackupClientSnapshotForTest(h)
                        .has_value();
                },
                std::chrono::seconds(20))) {
            return Fail(11, "holder never became a backup candidate");
        }
        const std::string v1(4096, 'a'), v2(12288, 'b');
        ReplicateConfig rc;
        rc.replica_num = 1;
        rc.preferred_segment = Hostname(*a_);
        if (a_->put("k1", std::span<const char>(v1.data(), v1.size()), rc) !=
                0 ||
            a_->put("k2", std::span<const char>(v2.data(), v2.size()), rc) !=
                0) {
            return Fail(12, "put");
        }
        // A real primary SSD copy on A (A's FileStorage offload) and a real
        // backup on H (H's worker), for both keys.
        const std::string primary = "P:" + Tag(a) + ":",
                          backup = "B:" + Tag(h) + ":";
        if (!WaitFor(
                [&] {
                    for (const char* k : {"k1", "k2"}) {
                        const auto r = Record(k);
                        if (r.find(primary) == std::string::npos ||
                            r.find(backup) == std::string::npos) {
                            return false;
                        }
                    }
                    return true;
                },
                std::chrono::seconds(40))) {
            return Fail(13, "no primary SSD + backup: k1=" + Record("k1") +
                                " k2=" + Record("k2"));
        }
        buf_.assign(1 << 20, 0);
        if (r_->register_buffer(buf_.data(), buf_.size()) != 0) {
            return Fail(14, "register reader buffer");
        }
        const std::vector<std::string> keys{"k1", "k2", "nokey"};
        const std::vector<size_t> sizes{v1.size(), v2.size(), 4096};
        const auto check = [&](const std::vector<Got>& g, const char* what,
                               int code) -> int {
            if (g.size() != 3 || g[0].result != 4096 || g[0].bytes != v1 ||
                g[1].result != 12288 || g[1].bytes != v2 || g[2].result >= 0) {
                return Fail(
                    code,
                    std::string(what) + ": results " +
                        std::to_string(g.size() > 0 ? g[0].result : 0) + "/" +
                        std::to_string(g.size() > 1 ? g[1].result : 0) + "/" +
                        std::to_string(g.size() > 2 ? g[2].result : 0));
            }
            return 0;
        };
        // 1. From A's DRAM, both APIs (the missing key fails on its own).
        if (int c = check(GetInto(keys, sizes), "dram batch_get_into", 20))
            return c;
        if (int c = check(GetIntoMulti(keys, sizes, 1000), "dram multi", 21))
            return c;
        // 2. Evict the DRAM copies (they have a primary SSD copy and a
        // backup; the read leases must have expired first).
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        test::MasterServiceTestPeer(Ms()).RunBatchEvictForTesting(1.0, 1.0);
        const auto has_memory = [&](const std::string& k) {
            auto q = r_->client_->Query(k);
            if (!q) return true;
            for (const auto& d : q->replicas) {
                if (d.is_memory_replica()) return true;
            }
            return false;
        };
        if (has_memory("k1") || has_memory("k2")) {
            return Fail(22, "DRAM copies not evicted");
        }
        const auto refreshed0 =
            h_->file_storage_->GetBackupScanStats().refreshed;
        // 3. From A's primary SSD, both APIs; H gets a refresh of the backup.
        if (int c =
                check(GetInto(keys, sizes), "primary ssd batch_get_into", 23)) {
            return c;
        }
        if (int c = check(GetIntoMulti(keys, sizes, 1000), "primary ssd multi",
                          24)) {
            return c;
        }
        if (FallbackAttempts(*r_) != 0) {
            return Fail(25, "fallback used while the primary SSD answers");
        }
        if (!WaitFor(
                [&] {
                    return h_->file_storage_->GetBackupScanStats().refreshed >
                           refreshed0;
                },
                std::chrono::seconds(10))) {
            return Fail(26, "holder never applied a refresh");
        }
        // 4. A's SSD RPC endpoint stops answering (A stays registered with
        // the master): the read fails with a transport error and is
        // re-read from H's backup in the same call.
        const auto records_before_stop =
            Ms().DescribeSsdRecordsForTest({"k1", "k2"}, TenantId::Default());
        StopOffloadServer(*a_);
        const auto ok0 = FallbackOk(*r_);
        if (int c =
                check(GetInto(keys, sizes), "fallback batch_get_into", 27)) {
            return c;
        }
        if (FallbackOk(*r_) < ok0 + 2) {
            return Fail(28, "batch_get_into: fallback ok " +
                                std::to_string(FallbackOk(*r_) - ok0));
        }
        const auto ok1 = FallbackOk(*r_);
        Trace("records k1=" + Record("k1") + " k2=" + Record("k2"));
        Trace(
            "dram+primary-ssd reads ok, fallback attempts before stop=0, "
            "holder refreshed " +
            std::to_string(refreshed0) + "->" +
            std::to_string(h_->file_storage_->GetBackupScanStats().refreshed) +
            ", batch_get_into after stop: fallback ok +" +
            std::to_string(ok1 - ok0) + " suspect endpoints=" +
            std::to_string(r_->SuspectEndpointCountForTest()));
        // The endpoint is suspect now; the multi-buffer read goes to the
        // backup too, with each object split over two slices.
        if (int c =
                check(GetIntoMulti(keys, sizes, 1000), "fallback multi", 29)) {
            return c;
        }
        Trace("multi after stop: fallback ok +" +
              std::to_string(FallbackOk(*r_) - ok1) + " attempts total " +
              std::to_string(FallbackAttempts(*r_)));
        // The SSD records (version, primary and backup, sizes, status) are
        // exactly as before the endpoint stopped.
        if (Ms().DescribeSsdRecordsForTest({"k1", "k2"}, TenantId::Default()) !=
            records_before_stop) {
            return Fail(30, "SSD records changed");
        }
        r_->unregister_buffer(buf_.data());
        return 0;
    }

    mooncake::testing::InProcMaster master_;
    std::shared_ptr<RealClient> a_, h_, r_;
    std::vector<char> buf_;
};

TEST_F(FileStorageBackupReaderTest, RealReaderPrimarySsdAndBackupFallback) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(RealReaderChild()), ::testing::ExitedWithCode(0),
                "");
}

TEST_F(FileStorageBackupReaderTest, BatchGetIntoFirstReaderFallsBack) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(FirstReaderAfterPrimaryFailureChild(0)),
                ::testing::ExitedWithCode(0), "");
}

TEST_F(FileStorageBackupReaderTest, MultiBuffersFirstReaderFallsBack) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(FirstReaderAfterPrimaryFailureChild(1)),
                ::testing::ExitedWithCode(0), "");
}

// T7 steps 4 and 5: real reads held inside the reader after their leased
// metadata (or their SSD restore) came back, while the test removes,
// replaces or evicts the object, before and after the lease runs out. The
// read thread parks in the test observer (no RealClient lock held there),
// with gates installed before it starts and a scoped release + join guard
// declared after every capture, so every exit releases and joins before
// buffers, registrations or captures go. Master read lease 2000 ms, SSD
// restore-buffer TTL 1500 ms.
class FileStorageBackupHeldReadTest : public FileStorageBackupReaderTest {
   protected:
    static constexpr int64_t kLeaseMs = 2000;
    static constexpr int64_t kGcTtlMs = 1500;
    uint64_t LeaseTtlMs() const override { return kLeaseMs; }

    struct HGate {
        std::mutex mu;
        std::condition_variable cv;
        bool open = false;
        void Open() {
            {
                std::lock_guard<std::mutex> l(mu);
                open = true;
            }
            cv.notify_all();
        }
        bool Wait(std::chrono::milliseconds d) {
            std::unique_lock<std::mutex> l(mu);
            return cv.wait_for(l, d, [&] { return open; });
        }
    };
    struct HGuard {
        std::function<void()> fn;
        ~HGuard() {
            if (fn) fn();
        }
    };
    // Where the read parks: an orchestration phase ("query") or a transfer
    // site ("ssd_restore"), for `key`, first occurrence only.
    struct Park {
        bool transfer = false;
        std::string what;
        std::string key;
    };
    std::vector<int64_t> ReadWith(int api, const std::vector<std::string>& ks,
                                  const Layout& lay) {
        std::vector<size_t> caps;
        for (const auto& n : lay.lens) {
            size_t t = 0;
            for (size_t x : n) t += x;
            caps.push_back(t);
        }
        std::vector<int64_t> out;
        if (api == 0) {
            std::vector<void*> bufs;
            for (const auto& p : lay.ptrs) bufs.push_back(p[0]);
            out = r_->batch_get_into(ks, bufs, caps);
        } else {
            std::vector<std::vector<void*>> bufs;
            for (const auto& p : lay.ptrs) bufs.push_back({p[0], p[1]});
            auto r =
                r_->batch_get_into_multi_buffers(ks, bufs, lay.lens, false);
            out.assign(r.begin(), r.end());
        }
        return out;
    }
    // One read of `ks` into `lay`, parked at each of `parks` in turn; at
    // park p the test runs at_park(p) (0 = go on), then releases it.
    // Results in `res`; both logs recorded. 0, or the failing code.
    int HeldRead(int api, const std::vector<std::string>& ks, const Layout& lay,
                 const std::vector<Park>& parks,
                 const std::function<int(size_t)>& at_park, ObsLog& obs,
                 TLog& tlog, std::vector<int64_t>& res, int code) {
        std::vector<std::unique_ptr<HGate>> parked, release;
        for (size_t p = 0; p < parks.size(); ++p) {
            parked.push_back(std::make_unique<HGate>());
            release.push_back(std::make_unique<HGate>());
        }
        std::atomic<size_t> next{0};
        const auto maybe_park = [&](bool transfer, const std::string& what,
                                    const std::string& key) {
            const size_t p = next.load();
            if (p >= parks.size() || parks[p].transfer != transfer ||
                parks[p].what != what || parks[p].key != key) {
                return;
            }
            next = p + 1;
            parked[p]->Open();
            release[p]->Wait(std::chrono::seconds(30));
        };
        r_->read_observer_for_test_ =
            [&](const RealClient::ReadObservation& o) {
                {
                    std::lock_guard<std::mutex> l(obs.mu);
                    obs.v.push_back(o);
                }
                maybe_park(false, o.phase, o.key);
            };
        r_->transfer_observer_for_test_ =
            [&](const RealClient::TransferObservation& o) {
                {
                    std::lock_guard<std::mutex> l(tlog.mu);
                    tlog.v.push_back(o);
                }
                maybe_park(true, o.site, o.key);
            };
        std::thread reader;
        HGuard guard{[&] {
            for (auto& g : release) g->Open();
            if (reader.joinable()) reader.join();
            StopTransferObserving();
            StopObserving();
        }};
        reader = std::thread([&] { res = ReadWith(api, ks, lay); });
        for (size_t p = 0; p < parks.size(); ++p) {
            if (!parked[p]->Wait(std::chrono::seconds(20))) {
                return Fail(code, "read never reached park " +
                                      std::to_string(p) + " (" + parks[p].what +
                                      " " + parks[p].key + ")");
            }
            if (int c = at_park(p)) return c;
            release[p]->Open();
        }
        guard.fn();
        guard.fn = nullptr;
        return 0;
    }
    static void SleepPast(int64_t ms) {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms + 400));
    }
    static int Code(ErrorCode e) { return static_cast<int>(e); }
    static std::string Show(const std::vector<int64_t>& res) {
        std::string s;
        for (auto v : res) s += std::to_string(v) + " ";
        return s;
    }
    // Results plus both logs, for failure messages.
    static std::string Show(const std::vector<int64_t>& res, ObsLog& obs,
                            TLog& t,
                            const std::map<std::string, std::string>& tags) {
        std::vector<RealClient::TransferObservation> x;
        {
            std::lock_guard<std::mutex> l(t.mu);
            x = t.v;
        }
        std::string leases;
        {
            std::lock_guard<std::mutex> l(obs.mu);
            for (const auto& o : obs.v) {
                if (o.phase == "query" || o.phase == "requery") {
                    leases += o.phase + "|" + o.key +
                              " lease_ms=" + std::to_string(o.lease_ms) + "; ";
                }
            }
        }
        std::string xl;
        for (const auto& o : x) {
            if (o.site == "memory_transfer") {
                xl += "memory_transfer#" + std::to_string(o.attempt) + "|" +
                      o.key + " lease_ms=" + std::to_string(o.lease_ms) + "; ";
            }
        }
        return Show(res) + "| reads " + Join(Canon(obs, tags)) +
               "| transfers " + Join(TCanon(x, tags, false)) + "| " + leases +
               xl;
    }
    // Key i of `lay` holds exactly `want` (and nothing after it within its
    // capacity was written).
    static bool Holds(const Layout& lay, size_t i, const std::string& want) {
        const std::string got = Assemble(lay, i);
        if (got.size() < want.size() ||
            got.compare(0, want.size(), want) != 0) {
            return false;
        }
        for (size_t k = want.size(); k < got.size(); ++k) {
            if (got[k] != kGuardByte) return false;
        }
        return true;
    }

    struct World {
        std::string ep_a, ep_h, ep_hmem;
        std::map<std::string, std::string> tags, te_tags;
    };
    // Restore-buffer TTL of the owners: kGcTtlMs (below the lease) where a
    // test needs the restore-lease check to fire, kLongGcTtlMs where a read
    // is held after its restore past the master lease.
    static constexpr int64_t kLongGcTtlMs = 8000;
    int Setup(World& w, int64_t gc_ttl_ms = kGcTtlMs) {
        setenv("MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS",
               std::to_string(gc_ttl_ms).c_str(), 1);
        a_ = Make("rd-src", true, "a");
        h_ = Make("rd-holder", true, "h");
        r_ = Make("rd-reader", false, "");
        if (!a_ || !h_ || !r_) return Fail(10, "client setup");
        const UUID a = Id(*a_), h = Id(*h_);
        if (!WaitFor(
                [&] {
                    return Ms().GetSsdBackupClientSnapshotForTest(h)
                               .has_value() &&
                           Ms().GetSsdBackupClientSnapshotForTest(a)
                               .has_value();
                },
                std::chrono::seconds(20))) {
            return Fail(11, "backup candidates missing");
        }
        buf_.assign(1 << 20, 0);
        if (r_->register_buffer(buf_.data(), buf_.size()) != 0) {
            return Fail(15, "register reader buffer");
        }
        w.te_tags = {{a_->client_->GetSegmentEndpoint(), "A"},
                     {h_->client_->GetSegmentEndpoint(), "H"}};
        return 0;
    }
    // Put `v` under `key` on `owner` (A or H) and wait for its SSD primary
    // there and its backup on the other.
    int PutWithSsd(RealClient& owner, RealClient& other, const std::string& key,
                   const std::string& v, int code) {
        ReplicateConfig rc;
        rc.replica_num = 1;
        rc.preferred_segment = Hostname(owner);
        if (owner.put(key, std::span<const char>(v.data(), v.size()), rc)) {
            return Fail(code, "put " + key);
        }
        const std::string primary = "P:" + Tag(Id(owner)) + ":",
                          backup = "B:" + Tag(Id(other)) + ":";
        if (!WaitFor(
                [&] {
                    const auto r = Record(key);
                    return r.find(primary) != std::string::npos &&
                           r.find(backup) != std::string::npos;
                },
                std::chrono::seconds(40))) {
            return Fail(code, key + " no SSD primary + backup: " + Record(key));
        }
        return 0;
    }
    int PutMemory(RealClient& owner, const std::string& key,
                  const std::string& v, int code) {
        ReplicateConfig rc;
        rc.replica_num = 1;
        rc.preferred_segment = Hostname(owner);
        if (owner.put(key, std::span<const char>(v.data(), v.size()), rc)) {
            return Fail(code, "put " + key);
        }
        return 0;
    }
    int Upsert(RealClient& owner, const std::string& key,
               const std::string& v) {
        ReplicateConfig rc;
        rc.replica_num = 1;
        rc.preferred_segment = Hostname(owner);
        return owner.upsert(key, std::span<const char>(v.data(), v.size()), rc);
    }
    // Endpoint tags from the reader's own (lease-granting) queries, before
    // any held read: A/H SSD RPC endpoints and H's DRAM endpoint.
    int Tags(World& w, const std::string& ssd_on_a, const std::string& mem_on_h,
             int code) {
        w.ep_a = DiskEndpoint(r_->client_->Query(ssd_on_a));
        const auto b = r_->client_->BatchQueryBackup({ssd_on_a});
        w.ep_h = b.size() == 1 ? DiskEndpoint(b[0]) : "";
        w.ep_hmem = MemoryEndpoint(r_->client_->Query(mem_on_h));
        if (w.ep_a.empty() || w.ep_h.empty() || w.ep_hmem.empty() ||
            w.ep_a == w.ep_h) {
            return Fail(code, "endpoints A=" + w.ep_a + " H=" + w.ep_h +
                                  " Hmem=" + w.ep_hmem);
        }
        w.tags = {{w.ep_a, "A"}, {w.ep_h, "H"}, {w.ep_hmem, "Hmem"}};
        return 0;
    }

    // T7 step 4. Child body (api 0: batch_get_into, 1: multi-buffer).
    int HeldMutationsChild(int api) {
        std::thread([] {
            std::this_thread::sleep_for(std::chrono::seconds(240));
            DumpStacksAndExit("held-read mutations did not finish");
        }).detach();
        World w;
        if (int c = Setup(w, kLongGcTtlMs)) return c;
        const size_t n = 4096, n2 = 8192;
        const int parts = api == 0 ? 1 : 2;
        // Slice lengths an event carries: batch_get_into slices by the
        // replica read (`replica` bytes), the multi-buffer API uses the
        // caller's two slices of the `cap`-byte destination.
        const auto L = [&](size_t replica, size_t cap) {
            return parts == 1 ? std::to_string(replica)
                              : Lens({cap / 3, cap - cap / 3});
        };
        // SSD keys: primary on A, backup on H, DRAM evicted.
        for (const char* k : {"sb", "sr", "su"}) {
            if (int c = PutWithSsd(*a_, *h_, k, Payload(k[1], 1, n), 12))
                return c;
        }
        SleepPast(kLeaseMs);  // the puts' own leases
        test::MasterServiceTestPeer(Ms()).RunBatchEvictForTesting(1.0, 1.0);
        // Memory keys on H (after that eviction): mb also gets its SSD
        // primary on H and backup on A (needed for the eviction cases).
        if (int c = PutWithSsd(*h_, *a_, "mb", Payload('b', 1, n), 13))
            return c;
        SleepPast(kLeaseMs);
        if (int c = Tags(w, "sb", "mb", 14)) return c;
        SleepPast(kLeaseMs);  // the tag queries' leases
        const std::string hl = E(ErrorCode::OBJECT_HAS_LEASE),
                          le = E(ErrorCode::LEASE_EXPIRED),
                          nf = E(ErrorCode::OBJECT_NOT_FOUND);
        const int ohl = Code(ErrorCode::OBJECT_HAS_LEASE);
        std::string summary;

        // One held single-key read; `mutate` runs at the park.
        const auto one = [&](const std::string& key, size_t cap, int code,
                             const std::function<int()>& mutate,
                             std::vector<int64_t>& res, ObsLog& obs, TLog& t,
                             Layout& lay) -> int {
            lay = MakeLayout({cap}, parts);
            return HeldRead(
                api, {key}, lay, {Park{false, "query", key}},
                [&](size_t) { return mutate(); }, obs, t, res, code);
        };
        // Eviction only for the DRAM case (an SSD-only key has nothing to
        // evict, and a global eviction then would drop mb's DRAM copy).
        const auto refused_all = [&](const std::string& key, RealClient& owner,
                                     bool evict, int code) -> int {
            const std::string before = Record(key);
            if (int r = owner.remove(key); r != ohl) {
                return Fail(code, key + ": remove while leased gave " +
                                      std::to_string(r));
            }
            if (int r = Upsert(owner, key, Payload('z', 2, n)); r != ohl) {
                return Fail(code, key +
                                      ": same-size upsert while leased gave " +
                                      std::to_string(r));
            }
            if (int r = Upsert(owner, key, Payload('z', 2, n2)); r != ohl) {
                return Fail(code, key + ": bigger upsert while leased gave " +
                                      std::to_string(r));
            }
            if (evict) {
                test::MasterServiceTestPeer(Ms()).RunBatchEvictForTesting(1.0,
                                                                          1.0);
            }
            if (Record(key) != before) {
                return Fail(code, key + ": SSD records changed while leased");
            }
            return 0;
        };

        // 4a. SSD primary, before expiry: all refused, read returns gen 1.
        {
            std::vector<int64_t> res;
            ObsLog obs;
            TLog t;
            Layout lay;
            if (int c = one(
                    "sb", n, 20,
                    [&] { return refused_all("sb", *a_, false, 20); }, res, obs,
                    t, lay)) {
                return c;
            }
            if (res.size() != 1 || res[0] != static_cast<int64_t>(n) ||
                !Holds(lay, 0, Payload('b', 1, n)) || !GuardsIntact(lay)) {
                return Fail(21, "sb: " + Show(res, obs, t, w.tags));
            }
            if (int c = CheckLog(
                    obs, w.tags,
                    {"query|sb|local_disk|A|ok",
                     "read#0|sb|local_disk|A|" + std::to_string(n)},
                    "read#1|sb|local_disk|A|4096", 22, "4a ssd before")) {
                return c;
            }
            if (int c =
                    CheckTransfers(t, w.tags,
                                   {"ssd_restore#0|sb|A|" + L(n, n) + "|ok",
                                    "ssd_transfer#0|sb|A|" + L(n, n) + "|4096"},
                                   "nokey", {}, 22, "4a ssd before")) {
                return c;
            }
            if (int c =
                    CheckCorrelation(obs, t, w.tags, w.te_tags,
                                     Dests({"sb"}, lay), 22, "4a ssd before")) {
                return c;
            }
            summary +=
                "4a ssd-before: remove/upsert/upsert-bigger refused "
                "(OBJECT_HAS_LEASE), evict no change, gen1 read; ";
        }
        // 4b. Memory (H DRAM), before expiry: all refused, eviction keeps the
        // leased DRAM copy; the read is served from that DRAM, gen 1.
        {
            std::vector<int64_t> res;
            ObsLog obs;
            TLog t;
            Layout lay;
            if (int c = one(
                    "mb", n, 23,
                    [&] { return refused_all("mb", *h_, true, 23); }, res, obs,
                    t, lay)) {
                return c;
            }
            if (res.size() != 1 || res[0] != static_cast<int64_t>(n) ||
                !Holds(lay, 0, Payload('b', 1, n)) || !GuardsIntact(lay)) {
                return Fail(24, "mb: " + Show(res, obs, t, w.tags));
            }
            if (int c = CheckLog(
                    obs, w.tags,
                    {"query|mb|memory|Hmem|ok", "read#0|mb|memory|Hmem|4096"},
                    "read#1|mb|memory|Hmem|4096", 25, "4b memory before")) {
                return c;
            }
            if (int c = CheckTransfers(
                    t, w.tags,
                    {"memory_transfer#0|mb|Hmem|" + L(n, n) + "|4096"}, "nokey",
                    {}, 25, "4b memory before")) {
                return c;
            }
            if (int c = CheckCorrelation(obs, t, w.tags, w.te_tags,
                                         Dests({"mb"}, lay), 25,
                                         "4b memory before")) {
                return c;
            }
            summary +=
                "4b memory-before: refused, leased DRAM kept by eviction, "
                "gen1 read from DRAM; ";
        }
        // mr and mu only now: H offloads its keys to its SSD in the
        // background, so 4b's eviction would have dropped their DRAM
        // copies, and the reads below are about DRAM copies.
        for (const char* k : {"mr", "mu"}) {
            if (int c = PutMemory(*h_, k, Payload(k[1], 1, n), 13)) return c;
        }
        SleepPast(kLeaseMs);  // the puts' own leases
        // 4c. Memory, after expiry: Remove succeeds while the read is held;
        // the released read gets LEASE_EXPIRED from the transfer's own
        // check, re-queries once (not found), and fails: no old bytes are
        // reported as a successful read.
        {
            std::vector<int64_t> res;
            ObsLog obs;
            TLog t;
            Layout lay;
            if (int c = one(
                    "mr", n, 26,
                    [&] {
                        SleepPast(kLeaseMs);
                        if (int r = h_->remove("mr"); r != 0) {
                            return Fail(26, "mr: remove after expiry gave " +
                                                std::to_string(r));
                        }
                        return 0;
                    },
                    res, obs, t, lay)) {
                return c;
            }
            if (res.size() != 1 || res[0] != Code(ErrorCode::LEASE_EXPIRED)) {
                return Fail(27, "mr: " + Show(res, obs, t, w.tags));
            }
            if (int c = CheckLog(
                    obs, w.tags,
                    {"query|mr|memory|Hmem|ok", "read#0|mr|memory|Hmem|" + le,
                     "requery|mr|no_query|-|" + nf},
                    "read#1|mr|memory|Hmem|4096", 28, "4c memory remove")) {
                return c;
            }
            if (int c = CheckTransfers(
                    t, w.tags,
                    {"memory_transfer#0|mr|Hmem|" + L(n, n) + "|" + le},
                    "nokey", {}, 28, "4c memory remove")) {
                return c;
            }
            if (int c = CheckCorrelation(obs, t, w.tags, w.te_tags,
                                         Dests({"mr"}, lay), 28,
                                         "4c memory remove")) {
                return c;
            }
            summary +=
                "4c memory-after-remove: LEASE_EXPIRED, one requery "
                "(not found), no retry, result LEASE_EXPIRED; ";
        }
        // 4d. Memory, after expiry: an Upsert (gen 2) succeeds while held;
        // the released read gets LEASE_EXPIRED, re-queries once and reads
        // the new gen-2 value with the fresh descriptor. batch_get_into:
        // bigger value (8192) into an 8192-byte destination. The
        // multi-buffer API requires destination slices of exactly the
        // object's size for memory replicas (larger ones fail the transfer,
        // existing behaviour), so there the replacement keeps the size.
        const size_t nu = api == 0 ? n2 : n;
        const std::string lu = std::to_string(nu);
        {
            std::vector<int64_t> res;
            ObsLog obs;
            TLog t;
            Layout lay;
            if (int c = one(
                    "mu", nu, 29,
                    [&] {
                        SleepPast(kLeaseMs);
                        if (int r = Upsert(*h_, "mu", Payload('u', 2, nu));
                            r != 0) {
                            return Fail(29, "mu: upsert after expiry gave " +
                                                std::to_string(r));
                        }
                        return 0;
                    },
                    res, obs, t, lay)) {
                return c;
            }
            if (res.size() != 1 || res[0] != static_cast<int64_t>(nu) ||
                !Holds(lay, 0, Payload('u', 2, nu)) || !GuardsIntact(lay)) {
                return Fail(30, "mu: " + Show(res, obs, t, w.tags));
            }
            if (int c = CheckLog(
                    obs, w.tags,
                    {"query|mu|memory|Hmem|ok", "read#0|mu|memory|Hmem|" + le,
                     "requery|mu|memory|Hmem|ok",
                     "read#1|mu|memory|Hmem|" + lu},
                    "read#2|mu|memory|Hmem|" + lu, 31, "4d memory upsert")) {
                return c;
            }
            if (int c = CheckTransfers(
                    t, w.tags,
                    {"memory_transfer#0|mu|Hmem|" + L(n, nu) + "|" + le,
                     "memory_transfer#1|mu|Hmem|" + L(nu, nu) + "|" + lu},
                    "nokey", {}, 31, "4d memory upsert")) {
                return c;
            }
            if (int c = CheckCorrelation(obs, t, w.tags, w.te_tags,
                                         Dests({"mu"}, lay), 31,
                                         "4d memory upsert")) {
                return c;
            }
            summary +=
                "4d memory-after-upsert: LEASE_EXPIRED, one requery, "
                "retry reads gen2 " +
                lu + " bytes with the requery's replica; ";
        }
        // 4e. Memory with SSD copies, after expiry: eviction drops the DRAM
        // copy while held; the released read gets LEASE_EXPIRED, re-queries
        // once and reads gen 1 from the SSD primary on H.
        {
            std::vector<int64_t> res;
            ObsLog obs;
            TLog t;
            Layout lay;
            if (int c = one(
                    "mb", n, 32,
                    [&] {
                        SleepPast(kLeaseMs);
                        test::MasterServiceTestPeer(Ms())
                            .RunBatchEvictForTesting(1.0, 1.0);
                        return 0;
                    },
                    res, obs, t, lay)) {
                return c;
            }
            if (res.size() != 1 || res[0] != static_cast<int64_t>(n) ||
                !Holds(lay, 0, Payload('b', 1, n)) || !GuardsIntact(lay)) {
                return Fail(33,
                            "mb after eviction: " + Show(res, obs, t, w.tags));
            }
            if (int c = CheckLog(
                    obs, w.tags,
                    {"query|mb|memory|Hmem|ok", "read#0|mb|memory|Hmem|" + le,
                     "requery|mb|local_disk|H|ok",
                     "read#1|mb|local_disk|H|4096"},
                    "read#2|mb|local_disk|H|4096", 34, "4e memory evict")) {
                return c;
            }
            if (int c = CheckTransfers(
                    t, w.tags,
                    {"memory_transfer#0|mb|Hmem|" + L(n, n) + "|" + le,
                     "ssd_restore#1|mb|H|" + L(n, n) + "|ok",
                     "ssd_transfer#1|mb|H|" + L(n, n) + "|4096"},
                    "nokey", {}, 34, "4e memory evict")) {
                return c;
            }
            if (int c = CheckCorrelation(obs, t, w.tags, w.te_tags,
                                         Dests({"mb"}, lay), 34,
                                         "4e memory evict")) {
                return c;
            }
            summary +=
                "4e memory-after-evict: LEASE_EXPIRED, one requery, gen1 "
                "from H's SSD primary; ";
        }
        // 4f/4g. SSD primary, held after its restore response (the restored
        // buffer on A holds gen 1; its TTL, kLongGcTtlMs, outlasts the hold)
        // while the master lease runs out; then Remove (sr) or a bigger
        // Upsert (su) succeeds. The transfer from the restored buffer checks
        // only the restore lease, not the master object, so the released
        // read returns the complete gen-1 value under its original query.
        // Inherited ordinary-SSD behaviour: the base tree (b5d79e69) does
        // the same (held_ssd_restore_test, run on both trees). A fresh read
        // afterwards reports not found, or the gen-2 value.
        for (const auto& kv : std::vector<std::pair<std::string, bool>>{
                 {"sr", false}, {"su", true}}) {
            const std::string key = kv.first;
            const bool upsert = kv.second;
            const std::string step = upsert ? "4g ssd upsert" : "4f ssd remove";
            const int code = upsert ? 40 : 35;
            std::vector<int64_t> res;
            ObsLog obs;
            TLog t;
            const Layout lay = MakeLayout({n}, parts);
            if (int c = HeldRead(
                    api, {key}, lay, {Park{true, "ssd_restore", key}},
                    [&](size_t) {
                        SleepPast(kLeaseMs);
                        const int r =
                            upsert ? Upsert(*a_, key, Payload('s', 2, n2))
                                   : a_->remove(key);
                        if (r != 0) {
                            return Fail(code,
                                        key + ": mutation after expiry gave " +
                                            std::to_string(r));
                        }
                        return 0;
                    },
                    obs, t, res, code)) {
                return c;
            }
            const std::string old = Payload(key[1], 1, n);
            if (res.size() != 1 || res[0] != static_cast<int64_t>(n) ||
                !Holds(lay, 0, old) || !GuardsIntact(lay)) {
                return Fail(code + 1, key + ": " + Show(res, obs, t, w.tags));
            }
            if (int c = CheckLog(obs, w.tags,
                                 {"query|" + key + "|local_disk|A|ok",
                                  "read#0|" + key + "|local_disk|A|4096"},
                                 "read#1|" + key + "|local_disk|A|4096",
                                 code + 2, step)) {
                return c;
            }
            if (int c = CheckTransfers(
                    t, w.tags,
                    {"ssd_restore#0|" + key + "|A|" + L(n, n) + "|ok",
                     "ssd_transfer#0|" + key + "|A|" + L(n, n) + "|4096"},
                    "nokey", {}, code + 2, step)) {
                return c;
            }
            if (int c = CheckCorrelation(obs, t, w.tags, w.te_tags,
                                         Dests({key}, lay), code + 2, step)) {
                return c;
            }
            // The one query of the read granted the lease its transfer
            // used: the restore and transfer belong to attempt 0 of that
            // call (checked above); there was no requery.
            const Layout fresh = MakeLayout({upsert ? n2 : n}, parts);
            const auto r2 = ReadWith(api, {key}, fresh);
            if (upsert ? (r2.size() != 1 || r2[0] != static_cast<int64_t>(n2) ||
                          !Holds(fresh, 0, Payload('s', 2, n2)) ||
                          !GuardsIntact(fresh))
                       : (r2.size() != 1 ||
                          r2[0] != Code(ErrorCode::OBJECT_NOT_FOUND) ||
                          !Untouched(fresh, 0) || !GuardsIntact(fresh))) {
                return Fail(code + 3, key + ": fresh read " + Show(r2));
            }
            summary +=
                std::string(upsert ? "4g ssd-after-upsert"
                                   : "4f ssd-after-remove") +
                ": read held after its restore returned the complete gen1 "
                "value (inherited), fresh read " +
                (upsert ? "gen2 8192 bytes" : "OBJECT_NOT_FOUND") + "; ";
        }
        Trace("api=" + std::to_string(api) + " step 4: " + summary);
        r_->unregister_buffer(buf_.data());
        return 0;
    }

    // T7 step 5, mixed batch: xm (DRAM on H, 4096 gen 1) is replaced by an
    // 8192-byte gen-2 value after the read's lease ran out, so the
    // transfer's own lease check gives LEASE_EXPIRED; xs (SSD on A) is held
    // after its restore past the restore-buffer TTL, so the restore-lease
    // check gives OBJECT_HAS_LEASE; xu (SSD on H) and a missing key are
    // unaffected. Required: one requery for xm and xs, one retry each with
    // the requery's descriptor, no third attempt, xu not retried.
    int RealExpiryChild(int api) {
        std::thread([] {
            std::this_thread::sleep_for(std::chrono::seconds(180));
            DumpStacksAndExit("real expiry did not finish");
        }).detach();
        World w;
        if (int c = Setup(w)) return c;
        const size_t n = 4096, n2 = 8192, ns = 12288;
        const int parts = api == 0 ? 1 : 2;
        if (int c = PutWithSsd(*a_, *h_, "xs", Payload('s', 1, ns), 12))
            return c;
        if (int c = PutWithSsd(*h_, *a_, "xu", Payload('u', 1, n), 12))
            return c;
        SleepPast(kLeaseMs);
        test::MasterServiceTestPeer(Ms()).RunBatchEvictForTesting(1.0, 1.0);
        if (int c = PutMemory(*h_, "xm", Payload('m', 1, n), 13)) return c;
        SleepPast(kLeaseMs);
        if (int c = Tags(w, "xs", "xm", 14)) return c;
        SleepPast(kLeaseMs);
        // xm's replacement: 8192 bytes into an 8192-byte destination for
        // batch_get_into (size-changing); the multi-buffer API needs memory
        // destinations of exactly the object's size, so there it keeps 4096.
        const size_t nm = api == 0 ? n2 : n;
        const std::string lm = std::to_string(nm);
        const std::vector<std::string> keys{"xm", "xs", "xu", "nokey"};
        const Layout lay = MakeLayout({nm, ns, n, n}, parts);
        std::vector<int64_t> res;
        ObsLog obs;
        TLog t;
        const auto attempts0 = FallbackAttempts(*r_), ok0 = FallbackOk(*r_);
        if (int c = HeldRead(
                api, keys, lay,
                {Park{false, "query", "xm"}, Park{true, "ssd_restore", "xs"}},
                [&](size_t p) {
                    if (p == 0) {
                        SleepPast(kLeaseMs);
                        if (int r = Upsert(*h_, "xm", Payload('m', 2, nm));
                            r != 0) {
                            return Fail(20,
                                        "xm upsert gave " + std::to_string(r));
                        }
                    } else {
                        SleepPast(kGcTtlMs);
                    }
                    return 0;
                },
                obs, t, res, 20)) {
            return c;
        }
        if (res.size() != 4 || res[0] != static_cast<int64_t>(nm) ||
            !Holds(lay, 0, Payload('m', 2, nm)) ||
            res[1] != static_cast<int64_t>(ns) ||
            !Holds(lay, 1, Payload('s', 1, ns)) ||
            res[2] != static_cast<int64_t>(n) ||
            !Holds(lay, 2, Payload('u', 1, n)) ||
            res[3] != Code(ErrorCode::OBJECT_NOT_FOUND) || !Untouched(lay, 3) ||
            !GuardsIntact(lay)) {
            return Fail(21, "results " + Show(res, obs, t, w.tags));
        }
        const std::string le = E(ErrorCode::LEASE_EXPIRED),
                          hl = E(ErrorCode::OBJECT_HAS_LEASE),
                          nf = E(ErrorCode::OBJECT_NOT_FOUND);
        if (int c = CheckLog(
                obs, w.tags,
                {"query|xm|memory|Hmem|ok", "query|xs|local_disk|A|ok",
                 "query|xu|local_disk|H|ok", "query|nokey|no_query|-|" + nf,
                 "read#0|xm|memory|Hmem|" + le, "read#0|xs|local_disk|A|" + hl,
                 "read#0|xu|local_disk|H|4096", "read#0|nokey|no_query|-|" + nf,
                 "requery|xm|memory|Hmem|ok", "requery|xs|local_disk|A|ok",
                 "read#1|xm|memory|Hmem|" + lm, "read#1|xs|local_disk|A|12288"},
                "read#2|xm|memory|Hmem|" + lm, 22, "step 5")) {
            return c;
        }
        // Slice lengths: by the replica read (batch_get_into) or the
        // caller's two slices of the destination (multi-buffer).
        const auto L = [&](size_t replica, size_t cap) {
            return parts == 1 ? std::to_string(replica)
                              : Lens({cap / 3, cap - cap / 3});
        };
        const std::string lxm0 = L(n, nm), lxm1 = L(nm, nm), lxs = L(ns, ns),
                          lxu = L(n, n);
        if (int c =
                CheckTransfers(t, w.tags,
                               {"memory_transfer#0|xm|Hmem|" + lxm0 + "|" + le,
                                "ssd_restore#0|xs|A|" + lxs + "|ok",
                                "ssd_transfer#0|xs|A|" + lxs + "|" + hl,
                                "ssd_restore#0|xu|H|" + lxu + "|ok",
                                "ssd_transfer#0|xu|H|" + lxu + "|4096",
                                "memory_transfer#1|xm|Hmem|" + lxm1 + "|" + lm,
                                "ssd_restore#1|xs|A|" + lxs + "|ok",
                                "ssd_transfer#1|xs|A|" + lxs + "|12288"},
                               "nokey", {}, 23, "step 5")) {
            return c;
        }
        if (int c = CheckCorrelation(obs, t, w.tags, w.te_tags,
                                     Dests(keys, lay), 24, "step 5")) {
            return c;
        }
        if (FallbackAttempts(*r_) != attempts0 + 2 ||
            FallbackOk(*r_) != ok0 + 2) {
            return Fail(25,
                        "retries +" +
                            std::to_string(FallbackAttempts(*r_) - attempts0) +
                            " ok +" + std::to_string(FallbackOk(*r_) - ok0));
        }
        // The xm retry used the requery's replica, the first attempt the
        // query's (checked per event by the correlation); print both ids.
        uint64_t q_id = 0, rq_id = 0;
        {
            std::lock_guard<std::mutex> l(obs.mu);
            for (const auto& o : obs.v) {
                if (o.key != "xm") continue;
                if (o.phase == "query") q_id = o.replica_id;
                if (o.phase == "requery") rq_id = o.replica_id;
            }
        }
        Trace("api=" + std::to_string(api) + " step 5: results " + Show(res) +
              "xm LEASE_EXPIRED then gen2 " + lm + " bytes (replica " +
              std::to_string(q_id) + " -> " + std::to_string(rq_id) +
              "), xs OBJECT_HAS_LEASE then 12288 bytes, xu and nokey not "
              "retried");
        r_->unregister_buffer(buf_.data());
        return 0;
    }

    // T7 held reads whose source is an SSD backup (R7-M2 / MG-2). Keys
    // b* have their SSD primary on A and their backup on H, DRAM evicted;
    // A's SSD endpoint is held suspect, so each read is sent to H's backup
    // (query, query_backup, read#0 from H). "hu" (SSD primary on H) is the
    // unaffected member of every batch. Each read is held:
    //  - B1, before expiry, after the leased backup query returned
    //    (query_backup): Remove, same-size and bigger Upsert are refused
    //    (OBJECT_HAS_LEASE), a memory eviction changes nothing (no DRAM
    //    copy), records unchanged; the released read returns gen 1;
    //  - B2-B5, after the backup restore response (ssd_restore at H, the
    //    restored buffer holds gen 1) while the master lease runs out; then
    //    Remove, same-size Upsert, bigger Upsert or a memory eviction
    //    succeeds; the released read returns the complete gen-1 value from
    //    the backup, as an ordinary SSD read held after its restore does
    //    (4f/4g); a fresh read then sees the mutation.
    // Every step: exact orchestration, execution-site and correlation
    // checks, the backup flag of the selected descriptor (b*: backup, hu:
    // primary), hu's bytes. Child body (api 0 / 1).
    int HeldBackupChild(int api) {
        std::thread([] {
            std::this_thread::sleep_for(std::chrono::seconds(240));
            DumpStacksAndExit("held backup reads did not finish");
        }).detach();
        World w;
        if (int c = Setup(w, kLongGcTtlMs)) return c;
        const size_t n = 4096, n2 = 8192;
        const int parts = api == 0 ? 1 : 2;
        const auto L = [&](size_t replica, size_t cap) {
            return parts == 1 ? std::to_string(replica)
                              : Lens({cap / 3, cap - cap / 3});
        };
        const std::vector<std::string> bkeys{"bb", "br", "bs", "bv", "be"};
        for (const auto& k : bkeys) {
            if (int c = PutWithSsd(*a_, *h_, k, Payload(k[1], 1, n), 12))
                return c;
        }
        if (int c = PutWithSsd(*h_, *a_, "hu", Payload('h', 1, n), 12))
            return c;
        SleepPast(kLeaseMs);
        test::MasterServiceTestPeer(Ms()).RunBatchEvictForTesting(1.0, 1.0);
        SleepPast(kLeaseMs);
        w.ep_a = DiskEndpoint(r_->client_->Query("bb"));
        w.ep_h = DiskEndpoint(r_->client_->Query("hu"));
        const auto bq = r_->client_->BatchQueryBackup({"bb"});
        if (w.ep_a.empty() || w.ep_h.empty() || w.ep_a == w.ep_h ||
            bq.size() != 1 || DiskEndpoint(bq[0]) != w.ep_h) {
            return Fail(14, "endpoints A=" + w.ep_a + " H=" + w.ep_h);
        }
        w.tags = {{w.ep_a, "A"}, {w.ep_h, "H"}};
        SleepPast(kLeaseMs);  // the tag queries' leases
        r_->MarkSuspectEndpoint(
            w.ep_a, std::chrono::steady_clock::now() + std::chrono::hours(1));
        const int ohl = Code(ErrorCode::OBJECT_HAS_LEASE);
        std::string summary;

        // One held read of {key, hu}; checks common to every step.
        const auto held = [&](const std::string& key, const Park& park,
                              const std::function<int()>& mutate, int code,
                              const std::string& step) -> int {
            const Layout lay = MakeLayout({n, n}, parts);
            std::vector<int64_t> res;
            ObsLog obs;
            TLog t;
            const std::vector<std::string> keys{key, "hu"};
            if (int c = HeldRead(
                    api, keys, lay, {park}, [&](size_t) { return mutate(); },
                    obs, t, res, code)) {
                return c;
            }
            if (res.size() != 2 || res[0] != static_cast<int64_t>(n) ||
                !Holds(lay, 0, Payload(key[1], 1, n)) ||
                res[1] != static_cast<int64_t>(n) ||
                !Holds(lay, 1, Payload('h', 1, n)) || !GuardsIntact(lay)) {
                return Fail(code + 1, step + ": " + Show(res, obs, t, w.tags));
            }
            const std::string ln = std::to_string(n);
            if (int c = CheckLog(obs, w.tags,
                                 {"query|" + key + "|local_disk|A|ok",
                                  "query_backup|" + key + "|local_disk|H|ok",
                                  "read#0|" + key + "|local_disk|H|" + ln,
                                  "query|hu|local_disk|H|ok",
                                  "read#0|hu|local_disk|H|" + ln},
                                 "read#1|" + key + "|local_disk|A|" + ln,
                                 code + 2, step)) {
                return c;
            }
            if (int c = CheckTransfers(
                    t, w.tags,
                    {"ssd_restore#0|" + key + "|H|" + L(n, n) + "|ok",
                     "ssd_transfer#0|" + key + "|H|" + L(n, n) + "|" + ln,
                     "ssd_restore#0|hu|H|" + L(n, n) + "|ok",
                     "ssd_transfer#0|hu|H|" + L(n, n) + "|" + ln},
                    "nokey", {}, code + 2, step)) {
                return c;
            }
            if (int c = CheckCorrelation(obs, t, w.tags, w.te_tags,
                                         Dests(keys, lay), code + 2, step)) {
                return c;
            }
            // The selected descriptors: the backup for `key`, the primary
            // for hu, each from the query its attempt consumed.
            std::map<std::string, bool> backup;
            {
                std::lock_guard<std::mutex> l(t.mu);
                for (const auto& o : t.v) {
                    if (o.site == "ssd_select") backup[o.key] = o.backup;
                }
            }
            if (backup !=
                std::map<std::string, bool>{{key, true}, {"hu", false}}) {
                return Fail(code + 3, step + ": selected backup flags wrong");
            }
            return 0;
        };

        // B1: before expiry, held after the backup query.
        {
            const std::string before = Record("bb");
            if (int c = held(
                    "bb", Park{false, "query_backup", "bb"},
                    [&] {
                        if (int r = a_->remove("bb"); r != ohl) {
                            return Fail(20,
                                        "bb: remove gave " + std::to_string(r));
                        }
                        if (int r = Upsert(*a_, "bb", Payload('z', 2, n));
                            r != ohl) {
                            return Fail(20, "bb: same-size upsert gave " +
                                                std::to_string(r));
                        }
                        if (int r = Upsert(*a_, "bb", Payload('z', 2, n2));
                            r != ohl) {
                            return Fail(20, "bb: bigger upsert gave " +
                                                std::to_string(r));
                        }
                        test::MasterServiceTestPeer(Ms())
                            .RunBatchEvictForTesting(1.0, 1.0);
                        if (Record("bb") != before) {
                            return Fail(20, "bb: records changed while leased");
                        }
                        return 0;
                    },
                    20, "B1 backup before")) {
                return c;
            }
            if (Record("bb") != before) return Fail(24, "bb: records changed");
            summary +=
                "B1 backup-before (held after query_backup): remove/"
                "upsert/upsert-bigger refused (OBJECT_HAS_LEASE), evict "
                "no change, gen1 from H's backup; ";
        }
        // B2-B5: after expiry, held after the backup restore.
        struct After {
            std::string key, what;
            int code;
            std::function<int()> mutate;
            size_t fresh_size;  // 0: not found afterwards
            int fresh_gen;
            // Same-size Upsert of an SSD-only object: current behaviour,
            // recorded exactly (R8-Q1, accepted limitation, see below).
            bool same_size = false;
        };
        std::string same_size_records;
        const std::vector<After> after{
            {"br", "remove", 30, [&] { return a_->remove("br"); }, 0, 0},
            {"bs", "same-size upsert", 40,
             [&] {
                 const int r = Upsert(*a_, "bs", Payload('s', 2, n));
                 same_size_records = Record("bs");
                 // NO_AVAILABLE_HANDLE is the expected (current) result.
                 return r == Code(ErrorCode::NO_AVAILABLE_HANDLE) ? 0 : r;
             },
             n, 2, true},
            {"bv", "bigger upsert", 50,
             [&] { return Upsert(*a_, "bv", Payload('v', 2, n2)); }, n2, 2},
            {"be", "evict", 60,
             [&] {
                 test::MasterServiceTestPeer(Ms()).RunBatchEvictForTesting(1.0,
                                                                           1.0);
                 return 0;
             },
             n, 1},
        };
        for (const auto& s : after) {
            const std::string records = Record(s.key);
            if (int c = held(
                    s.key, Park{true, "ssd_restore", s.key},
                    [&] {
                        SleepPast(kLeaseMs);
                        if (int r = s.mutate(); r != 0) {
                            return Fail(s.code, s.key + ": " + s.what +
                                                    " after expiry gave " +
                                                    std::to_string(r));
                        }
                        return 0;
                    },
                    s.code, "B " + s.key + " backup " + s.what)) {
                return c;
            }
            if (s.what == "evict" && Record(s.key) != records) {
                return Fail(s.code + 4,
                            s.key + ": eviction changed SSD records");
            }
            if (s.same_size) {
                // R8-Q1 (accepted limitation, not fixed: no production
                // caller uses Upsert): the same-size Upsert of an SSD-only
                // object
                // takes the master's in-place path, which drops the backup
                // and marks the SSD primary PROCESSING; with no memory
                // replica to write, the client revokes (memory/NoF/DFS
                // only) and fails with NO_AVAILABLE_HANDLE. Left behind: no
                // backup record, the primary PROCESSING (status 2), reads
                // REPLICA_IS_NOT_READY. The primary part is inherited (the
                // base tree does the same, held_ssd_restore_test); losing
                // the backup is the feature's. Recorded exactly.
                const std::string primary = "P:" + Tag(Id(*a_)) + ":4096:2;";
                if (same_size_records.find(primary) == std::string::npos ||
                    same_size_records.find(";B:") != std::string::npos) {
                    return Fail(s.code + 6, "bs records after the upsert: " +
                                                same_size_records);
                }
                const Layout fl = MakeLayout({n}, parts);
                const auto r2 = ReadWith(api, {s.key}, fl);
                if (r2.size() != 1 ||
                    r2[0] != Code(ErrorCode::REPLICA_IS_NOT_READY) ||
                    !Untouched(fl, 0) || !GuardsIntact(fl)) {
                    return Fail(s.code + 5,
                                "bs read after the upsert " + Show(r2));
                }
                summary +=
                    "B bs backup-after-same-size-upsert (held after H's "
                    "restore): complete gen1 from the backup; the upsert "
                    "gave NO_AVAILABLE_HANDLE, dropped the backup record, "
                    "left the primary PROCESSING, reads REPLICA_IS_NOT_READY "
                    "(R8-Q1, accepted limitation); ";
                continue;
            }
            // A fresh read (the backup still serves it while A is suspect).
            const Layout fresh =
                MakeLayout({s.fresh_size ? s.fresh_size : n}, parts);
            const auto r2 = ReadWith(api, {s.key}, fresh);
            const bool ok =
                s.fresh_size == 0
                    ? r2.size() == 1 &&
                          r2[0] == Code(ErrorCode::OBJECT_NOT_FOUND) &&
                          Untouched(fresh, 0)
                    : r2.size() == 1 &&
                          r2[0] == static_cast<int64_t>(s.fresh_size) &&
                          Holds(fresh, 0,
                                Payload(s.key[1], s.fresh_gen, s.fresh_size));
            if (!ok || !GuardsIntact(fresh)) {
                return Fail(s.code + 5, s.key + ": fresh read after " + s.what +
                                            " " + Show(r2));
            }
            summary +=
                "B " + s.key + " backup-after-" + s.what +
                " (held after H's restore): complete gen1 from the backup, "
                "fresh read " +
                (s.fresh_size == 0 ? std::string("OBJECT_NOT_FOUND")
                                   : "gen" + std::to_string(s.fresh_gen) + " " +
                                         std::to_string(s.fresh_size)) +
                "; ";
        }
        Trace("api=" + std::to_string(api) +
              " backup-source held reads: " + summary);
        r_->unregister_buffer(buf_.data());
        return 0;
    }
};

TEST_F(FileStorageBackupHeldReadTest, BatchGetIntoHeldBackupReads) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeldBackupChild(0)), ::testing::ExitedWithCode(0),
                "");
}

TEST_F(FileStorageBackupHeldReadTest, MultiBuffersHeldBackupReads) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeldBackupChild(1)), ::testing::ExitedWithCode(0),
                "");
}

// T7 step 6 (MG-4): two registered tenants ("default" and kTenantA) on a
// multi-tenant master. A (default, SSD offload) owns the DRAM and SSD
// primary of both tenants' objects (the tenant-a writer RT puts into A's
// segment), H (default, offload) holds their backups; R reads as default,
// RT as tenant-a. The same bare keys exist in both tenants: "eq" with
// equal lengths (4096 / 4096), "ne" with unequal ones (4096 / 12288),
// tenant-distinct payloads, DRAM evicted.
class FileStorageBackupTenantReadTest : public FileStorageBackupHeldReadTest {
   protected:
    bool MultiTenant() const override { return true; }
    std::string RecordIn(const std::string& tenant, const std::string& key) {
        return Ms().DescribeSsdRecordsForTest({key}, TenantId(tenant)).at(0);
    }
    // Last access time of the holder's pinned backup bucket of each
    // storage key.
    std::map<std::string, int64_t> BucketAccess() {
        std::map<std::string, int64_t> m;
        auto backend = Backend(*h_->file_storage_);
        if (!backend) return m;
        for (const auto& b : backend->ListPinnedBackupBuckets(1000)) {
            for (const auto& k : b.sample_keys) m[k] = b.last_access_ns;
        }
        return m;
    }
    // One read of `ks` by `reader` (made r_ for the read), both logs
    // recorded.
    std::vector<int64_t> ReadLogged(std::shared_ptr<RealClient>& reader,
                                    int api, const std::vector<std::string>& ks,
                                    const Layout& lay, ObsLog& obs, TLog& t) {
        std::swap(r_, reader);
        StartObserving(obs);
        StartTransferObserving(t);
        auto res = ReadWith(api, ks, lay);
        StopTransferObserving();
        StopObserving();
        std::swap(r_, reader);
        return res;
    }

    int TenantsChild(int api) {
        std::thread([] {
            std::this_thread::sleep_for(std::chrono::seconds(240));
            DumpStacksAndExit("two-tenant reads did not finish");
        }).detach();
        const std::string tn = kTenantA;
        a_ = Make("t6-src", true, "a");
        h_ = Make("t6-holder", true, "h");
        r_ = Make("t6-reader", false, "");
        auto rt = Make("t6-reader-t", false, "", tn);
        if (!a_ || !h_ || !r_ || !rt) return Fail(10, "client setup");
        if (rt->client_->tenant_id() != tn) return Fail(10, "tenant client");
        const UUID a = Id(*a_), h = Id(*h_);
        if (!WaitFor(
                [&] {
                    return Ms().GetSsdBackupClientSnapshotForTest(h)
                               .has_value() &&
                           Ms().GetSsdBackupClientSnapshotForTest(a)
                               .has_value();
                },
                std::chrono::seconds(20))) {
            return Fail(11, "backup candidates missing");
        }
        buf_.assign(1 << 20, 0);
        if (r_->register_buffer(buf_.data(), buf_.size()) != 0 ||
            rt->register_buffer(buf_.data(), buf_.size()) != 0) {
            return Fail(15, "register reader buffers");
        }
        const std::map<std::string, std::string> te_tags{
            {a_->client_->GetSegmentEndpoint(), "A"},
            {h_->client_->GetSegmentEndpoint(), "H"}};
        const size_t n = 4096, nt = 12288;
        // Payload per tenant (seed 'd' / 't'), key and generation.
        const auto value = [&](const std::string& tenant,
                               const std::string& key, int gen, size_t size) {
            return Payload((tenant == tn ? 't' : 'd') * 3 + key[0] + key[1],
                           gen, size);
        };
        const std::map<std::string, size_t> size_d{{"eq", n}, {"ne", n}},
            size_t_{{"eq", n}, {"ne", nt}};
        ReplicateConfig on_a;
        on_a.replica_num = 1;
        on_a.preferred_segment = Hostname(*a_);
        const auto put = [&](RealClient& w, const std::string& tenant,
                             const std::string& key, int gen, size_t size) {
            const std::string v = value(tenant, key, gen, size);
            return w.put(key, std::span<const char>(v.data(), v.size()), on_a);
        };
        for (const char* k : {"eq", "ne"}) {
            if (put(*a_, "default", k, 1, size_d.at(k)) ||
                put(*rt, tn, k, 1, size_t_.at(k))) {
                return Fail(12, std::string("put ") + k);
            }
        }
        const std::string primary = "P:" + Tag(a) + ":",
                          backup = "B:" + Tag(h) + ":";
        if (!WaitFor(
                [&] {
                    for (const std::string tenant :
                         {std::string("default"), tn}) {
                        for (const char* k : {"eq", "ne"}) {
                            const auto r = RecordIn(tenant, k);
                            if (r.find(primary) == std::string::npos ||
                                r.find(backup) == std::string::npos) {
                                return false;
                            }
                        }
                    }
                    return true;
                },
                std::chrono::seconds(60))) {
            return Fail(13, "no SSD primary + backup in both tenants: " +
                                RecordIn("default", "ne") + " / " +
                                RecordIn(tn, "ne"));
        }
        SleepPast(kLeaseMs);
        test::MasterServiceTestPeer(Ms()).RunBatchEvictForTesting(1.0, 1.0);
        SleepPast(kLeaseMs);
        const std::string ep_a = DiskEndpoint(r_->client_->Query("eq"));
        if (ep_a.empty()) return Fail(14, "no SSD endpoint for eq");
        const std::map<std::string, std::string> tags{{ep_a, "A"}};
        SleepPast(kLeaseMs);

        // Storage keys A's SSD server is asked for (the tenant scope the
        // reader used, as the owner receives it).
        std::mutex served_mu;
        std::set<std::string> served;
        a_->offload_serve_fault_for_test_ =
            [&](const std::vector<std::string>& keys)
            -> std::optional<ErrorCode> {
            std::lock_guard<std::mutex> l(served_mu);
            served.insert(keys.begin(), keys.end());
            return std::nullopt;
        };
        const auto scoped = [](const std::string& tenant,
                               const std::string& key) {
            return TenantId(tenant).MakeScopedKey(key);
        };
        // eq twice (input 0 and 2): transferred once, at its first
        // occurrence, and copied locally into the duplicate's destination.
        const std::vector<std::string> keys{"eq", "ne", "eq"};
        std::string summary;

        // One tenant's read of eq and ne from the SSD primaries: its own
        // bytes, exact logs, correlation in its tenant, resolved tenant at
        // the query and transfer events, its scoped storage keys at A.
        const auto read_tenant = [&](std::shared_ptr<RealClient>& reader,
                                     const std::string& tenant,
                                     const std::map<std::string, size_t>& sizes,
                                     int gen_eq, int code) -> int {
            const Layout lay =
                MakeLayout({sizes.at("eq"), sizes.at("ne"), sizes.at("eq")},
                           api == 0 ? 1 : 2);
            ObsLog obs;
            TLog t;
            {
                std::lock_guard<std::mutex> l(served_mu);
                served.clear();
            }
            const auto res = ReadLogged(reader, api, keys, lay, obs, t);
            const std::string le = std::to_string(sizes.at("eq")),
                              ln = std::to_string(sizes.at("ne"));
            if (res.size() != 3 ||
                res[0] != static_cast<int64_t>(sizes.at("eq")) ||
                !Holds(lay, 0, value(tenant, "eq", gen_eq, sizes.at("eq"))) ||
                res[1] != static_cast<int64_t>(sizes.at("ne")) ||
                !Holds(lay, 1, value(tenant, "ne", 1, sizes.at("ne"))) ||
                res[2] != static_cast<int64_t>(sizes.at("eq")) ||
                !Holds(lay, 2, value(tenant, "eq", gen_eq, sizes.at("eq"))) ||
                !GuardsIntact(lay)) {
                return Fail(code, tenant + " read: " + Show(res, obs, t, tags));
            }
            if (int c = CheckLog(
                    obs, tags,
                    {"query|eq|local_disk|A|ok", "query|ne|local_disk|A|ok",
                     "query|eq|local_disk|A|ok", "read#0|eq|local_disk|A|" + le,
                     "read#0|ne|local_disk|A|" + ln,
                     "read#0|eq|local_disk|A|" + le},
                    "read#1|eq|local_disk|A|" + le, code + 1,
                    "tenant " + tenant)) {
                return c;
            }
            const auto L = [&](size_t s) {
                return api == 0 ? std::to_string(s) : Lens({s / 3, s - s / 3});
            };
            if (int c = CheckTransfers(
                    t, tags,
                    {"ssd_restore#0|eq|A|" + L(sizes.at("eq")) + "|ok",
                     "ssd_restore#0|ne|A|" + L(sizes.at("ne")) + "|ok",
                     "ssd_transfer#0|eq|A|" + L(sizes.at("eq")) + "|" + le,
                     "ssd_transfer#0|ne|A|" + L(sizes.at("ne")) + "|" + ln},
                    "nokey", {}, code + 1, "tenant " + tenant)) {
                return c;
            }
            std::swap(r_, reader);  // correlation in the reader's tenant
            const int cc =
                CheckCorrelation(obs, t, tags, te_tags, Dests(keys, lay),
                                 code + 1, "tenant " + tenant);
            std::swap(r_, reader);
            if (cc) return cc;
            {
                std::lock_guard<std::mutex> l(obs.mu);
                for (const auto& o : obs.v) {
                    if (o.tenant != tenant || o.resolved_tenant != tenant) {
                        return Fail(code + 2, o.phase + "|" + o.key +
                                                  ": tenant " + o.tenant +
                                                  " resolved " +
                                                  o.resolved_tenant);
                    }
                }
            }
            {
                std::lock_guard<std::mutex> l(t.mu);
                for (const auto& o : t.v) {
                    if (o.tenant != tenant || (o.site == "ssd_select" &&
                                               o.resolved_tenant != tenant)) {
                        return Fail(code + 2, o.site + "|" + o.key +
                                                  ": tenant " + o.tenant +
                                                  " resolved " +
                                                  o.resolved_tenant);
                    }
                }
            }
            {
                std::lock_guard<std::mutex> l(served_mu);
                const std::set<std::string> want{scoped(tenant, "eq"),
                                                 scoped(tenant, "ne")};
                std::string got;
                for (const auto& s : served) got += "'" + s + "' ";
                if (served != want) {
                    return Fail(code + 3, tenant + ": A served " + got);
                }
                std::string restored;
                {
                    std::lock_guard<std::mutex> l(t.mu);
                    for (const auto& o : t.v) {
                        if (o.site == "ssd_restore") {
                            restored += o.key + "@" + std::to_string(o.index) +
                                        "->'" + o.storage_key + "' ";
                        }
                        if (o.site == "ssd_select" && o.key == "eq" &&
                            o.index != 0) {
                            return Fail(code + 3,
                                        tenant + ": eq selected at index " +
                                            std::to_string(o.index));
                        }
                    }
                }
                Trace("tenant " + tenant +
                      ": every query, read, memory/SSD "
                      "event in tenant " +
                      tenant + " with resolved tenant " + tenant +
                      "; A's SSD server was asked for " + got + "; restores " +
                      restored +
                      "; duplicate eq (index 2) not transferred, its bytes "
                      "copied from index 0");
            }
            return 0;
        };
        // The refresh a primary-SSD read queues for the backup holder
        // reaches only the read tenant's buckets on H.
        const auto refreshed_only =
            [&](const std::string& tenant, const std::string& other,
                const std::map<std::string, int64_t>& before, int code) -> int {
            for (const char* k : {"eq", "ne"}) {
                if (!before.count(scoped(tenant, k)) ||
                    !before.count(scoped(other, k))) {
                    return Fail(
                        code, std::string("no pinned backup bucket for ") + k);
                }
            }
            if (!WaitFor(
                    [&] {
                        const auto now = BucketAccess();
                        for (const char* k : {"eq", "ne"}) {
                            const auto it = now.find(scoped(tenant, k));
                            if (it == now.end() ||
                                it->second <= before.at(scoped(tenant, k))) {
                                return false;
                            }
                        }
                        return true;
                    },
                    std::chrono::seconds(15))) {
                return Fail(code,
                            tenant + ": its backup buckets not refreshed");
            }
            const auto now = BucketAccess();
            std::string shown;
            for (const char* k : {"eq", "ne"}) {
                if (now.at(scoped(other, k)) != before.at(scoped(other, k))) {
                    return Fail(code + 1, tenant + "'s read refreshed " +
                                              other + "'s bucket of " + k);
                }
                for (const auto& t : {tenant, other}) {
                    shown += "'" + scoped(t, k) + "' " +
                             std::to_string(before.at(scoped(t, k))) + "->" +
                             std::to_string(now.at(scoped(t, k))) + "; ";
                }
            }
            Trace("refresh after the " + tenant +
                  " read (H's backup bucket "
                  "last access, ns): " +
                  shown);
            return 0;
        };

        const auto b0 = BucketAccess();
        if (int c = read_tenant(rt, tn, size_t_, 1, 20)) return c;
        if (int c = refreshed_only(tn, "default", b0, 24)) return c;
        const auto b1 = BucketAccess();
        if (int c = read_tenant(r_, "default", size_d, 1, 30)) return c;
        if (int c = refreshed_only("default", tn, b1, 34)) return c;
        summary +=
            "eq/ne read by each tenant from A's SSD primary: own bytes, "
            "tenant and resolved tenant at query and transfer events, own "
            "scoped storage keys at A; refresh reached only the read "
            "tenant's buckets on H; ";

        // Delete tenant-a's eq, then replace it at the same and a different
        // size; default's eq keeps its bytes and records after each step.
        SleepPast(kLeaseMs);
        const std::string rec_d = RecordIn("default", "eq");
        const auto default_unchanged = [&](int code,
                                           const std::string& after) -> int {
            if (RecordIn("default", "eq") != rec_d) {
                return Fail(code, "default eq records changed after " + after);
            }
            const Layout lay = MakeLayout({n}, api == 0 ? 1 : 2);
            const auto r = ReadWith(api, {"eq"}, lay);
            if (r.size() != 1 || r[0] != static_cast<int64_t>(n) ||
                !Holds(lay, 0, value("default", "eq", 1, n)) ||
                !GuardsIntact(lay)) {
                return Fail(code, "default eq after " + after + ": " + Show(r));
            }
            if (RecordIn("default", "eq") != rec_d) {
                return Fail(code, "default eq records changed by its read");
            }
            return 0;
        };
        const auto tenant_read = [&](size_t cap) {
            const Layout lay = MakeLayout({cap}, api == 0 ? 1 : 2);
            std::swap(r_, rt);
            auto r = ReadWith(api, {"eq"}, lay);
            std::swap(r_, rt);
            return std::make_pair(r, lay);
        };
        if (int r = rt->remove("eq"); r != 0) {
            return Fail(40, "tenant-a remove eq gave " + std::to_string(r));
        }
        {
            auto [r, lay] = tenant_read(n);
            if (r.size() != 1 || r[0] != Code(ErrorCode::OBJECT_NOT_FOUND) ||
                !Untouched(lay, 0)) {
                return Fail(41, "tenant-a eq after remove: " + Show(r));
            }
            if (RecordIn(tn, "eq") != "missing") {
                return Fail(41, "tenant-a eq records after remove: " +
                                    RecordIn(tn, "eq"));
            }
        }
        if (int c = default_unchanged(42, "tenant-a remove")) return c;
        if (put(*rt, tn, "eq", 2, n)) return Fail(43, "tenant-a put eq gen2");
        {
            auto [r, lay] = tenant_read(n);
            if (r.size() != 1 || r[0] != static_cast<int64_t>(n) ||
                !Holds(lay, 0, value(tn, "eq", 2, n))) {
                return Fail(44, "tenant-a eq gen2: " + Show(r));
            }
        }
        if (int c = default_unchanged(45, "tenant-a same-size replace"))
            return c;
        SleepPast(kLeaseMs);
        {
            const std::string v = value(tn, "eq", 3, 8192);
            if (rt->upsert("eq", std::span<const char>(v.data(), v.size()),
                           on_a)) {
                return Fail(46, "tenant-a upsert eq gen3 8192");
            }
        }
        {
            auto [r, lay] = tenant_read(8192);
            if (r.size() != 1 || r[0] != 8192 ||
                !Holds(lay, 0, value(tn, "eq", 3, 8192))) {
                return Fail(47, "tenant-a eq gen3: " + Show(r));
            }
        }
        if (int c = default_unchanged(48, "tenant-a bigger replace")) return c;
        // tenant-a's ne was never touched.
        {
            const Layout lay = MakeLayout({nt}, api == 0 ? 1 : 2);
            std::swap(r_, rt);
            auto r = ReadWith(api, {"ne"}, lay);
            std::swap(r_, rt);
            if (r.size() != 1 || r[0] != static_cast<int64_t>(nt) ||
                !Holds(lay, 0, value(tn, "ne", 1, nt))) {
                return Fail(49, "tenant-a ne: " + Show(r));
            }
        }
        summary +=
            "tenant-a eq removed (its read not found), replaced at 4096 "
            "and 8192 (its read the new value); default eq kept its bytes "
            "and records after each step; tenant-a ne untouched";
        a_->offload_serve_fault_for_test_ = nullptr;
        Trace("api=" + std::to_string(api) + " step 6: " + summary);
        r_->unregister_buffer(buf_.data());
        rt->unregister_buffer(buf_.data());
        rt->tearDownAll();
        return 0;
    }
};

TEST_F(FileStorageBackupTenantReadTest, BatchGetIntoTwoTenants) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(TenantsChild(0)), ::testing::ExitedWithCode(0), "");
}

TEST_F(FileStorageBackupTenantReadTest, MultiBuffersTwoTenants) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(TenantsChild(1)), ::testing::ExitedWithCode(0), "");
}

TEST_F(FileStorageBackupHeldReadTest, BatchGetIntoHeldMutations) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeldMutationsChild(0)), ::testing::ExitedWithCode(0),
                "");
}

TEST_F(FileStorageBackupHeldReadTest, MultiBuffersHeldMutations) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeldMutationsChild(1)), ::testing::ExitedWithCode(0),
                "");
}

TEST_F(FileStorageBackupHeldReadTest, BatchGetIntoRealExpiry) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(RealExpiryChild(0)), ::testing::ExitedWithCode(0),
                "");
}

TEST_F(FileStorageBackupHeldReadTest, MultiBuffersRealExpiry) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(RealExpiryChild(1)), ::testing::ExitedWithCode(0),
                "");
}
// R13-GREEN-ONLY-END

class FileStorageBackupClientTest : public FileStorageBackupTest {
   protected:
    static constexpr size_t kSegment = 16 << 20;
    void SetUp() override {
        FileStorageBackupTest::SetUp();
        // A 0 ms lease would make every Get end as LEASE_EXPIRED.
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder()
                                      .set_enable_offload(true)
                                      .set_default_kv_lease_ttl(5000)
                                      .set_root_fs_dir("")
                                      .build()));
        NewClient();
    }
    // A FileStorage registers its own client buffer with the Client and
    // does not unregister it, so each FileStorage gets a fresh Client.
    void ReleaseClient() {
        if (client_) {
            (void)client_->UnmountSegment(segment_, kSegment);
            (void)client_->unregisterLocalMemory(allocator_->getBase());
        }
        client_.reset();
        allocator_.reset();
        if (segment_) free_memory("", segment_);
        segment_ = nullptr;
    }
    void NewClient() {
        ReleaseClient();
        endpoint_ = UniqueLocalEndpoint();
        auto created = Client::Create(endpoint_, master_.metadata_url(), "tcp",
                                      std::nullopt, master_.master_address());
        ASSERT_TRUE(created.has_value());
        client_ = *created;
        segment_ = allocate_buffer_allocator_memory(kSegment);
        ASSERT_NE(segment_, nullptr);
        ASSERT_TRUE(client_->MountSegment(segment_, kSegment, "tcp"));
        allocator_ = std::make_unique<SimpleAllocator>(kSegment);
        ASSERT_TRUE(client_->RegisterLocalMemory(
            allocator_->getBase(), kSegment, "cpu:0", false, false));
    }
    void TearDown() override {
        ReleaseClient();
        master_.Stop();
        FileStorageBackupTest::TearDown();
    }
    std::unique_ptr<FileStorage> MakeStorage(StorageBackendType backend,
                                             const std::string& sub) {
        auto c = Config();
        c.storage_backend_type = backend;
        c.storage_filepath = (dir_ / sub).string();
        SetHeartbeatInterval(c, 1);
        fs::create_directories(c.storage_filepath);
        return std::make_unique<FileStorage>(c, client_, endpoint_, &metric_);
    }
    bool PutBytes(const std::string& key, const std::string& bytes) {
        void* buf = allocator_->allocate(bytes.size());
        if (buf == nullptr) return false;
        std::memcpy(buf, bytes.data(), bytes.size());
        std::vector<Slice> slices{{buf, bytes.size()}};
        ReplicateConfig rc;
        rc.replica_num = 1;
        const bool ok = client_->Put(key, slices, rc).has_value();
        allocator_->deallocate(buf, bytes.size());
        return ok;
    }
    std::string GetBytes(const std::string& key, size_t n) {
        void* buf = allocator_->allocate(n);
        if (buf == nullptr) return "";
        std::vector<Slice> slices{{buf, n}};
        std::string out;
        if (client_->Get(key, slices).has_value()) {
            out.assign(static_cast<char*>(buf), n);
        }
        allocator_->deallocate(buf, n);
        return out;
    }
    bool HasLocalDiskReplica(const std::string& key) {
        auto q = client_->Query(key);
        if (!q) return false;
        for (const auto& r : q->replicas) {
            if (r.is_local_disk_replica()) return true;
        }
        return false;
    }
    // Primary bytes the storage backend serves for `key` ("" if none).
    static std::string BackendRead(FileStorage& fs, const std::string& key,
                                   size_t n) {
        std::string out(n, '\0');
        std::unordered_map<std::string, Slice> read{
            {TenantId::Default().MakeScopedKey(key), Slice{out.data(), n}}};
        auto& slice = read.begin()->second;
        if (!BackendOf(fs)->BatchLoad(read)) return "";
        if (slice.ptr != out.data()) {
            out.assign(static_cast<const char*>(slice.ptr), n);
        }
        return out;
    }
    static std::shared_ptr<StorageBackendInterface> BackendOf(FileStorage& fs) {
        return BackendPtr(fs);
    }

    mooncake::testing::InProcMaster master_;
    std::string endpoint_;
    std::shared_ptr<Client> client_;
    void* segment_ = nullptr;
    std::unique_ptr<SimpleAllocator> allocator_;
    SsdMetric metric_;
};

// CFG-01 (full): with both backup flags unset, the full FileStorage Init
// brings up the primary path only. Put/Get work, the ordinary SSD offload
// registers the key's disk copy with the right bytes, and the backup side
// stays idle (no staging, no thread, worker_up 0).
TEST_F(FileStorageBackupClientTest, DisabledBackupFullInitAndOffload) {
    unsetenv("MC_SSD_BACKUP_ENABLE");
    unsetenv("MC_SSD_BACKUP_READ_FALLBACK");
    auto storage = MakeStorage(StorageBackendType::kBucket, "cfg01");
    ASSERT_TRUE(storage->Init().has_value());
    EXPECT_FALSE(HasStaging(*storage));
    EXPECT_FALSE(WorkerRunning(*storage));
    EXPECT_EQ(metric_.ssd_backup_worker_up.value(), 0);
    const std::string bytes(4096, 'c');
    ASSERT_TRUE(PutBytes("cfg01_key", bytes));
    EXPECT_EQ(GetBytes("cfg01_key", bytes.size()), bytes);
    // The offload heartbeat picks the task up and writes it to SSD.
    bool offloaded = false;
    for (int i = 0; i < 50 && !offloaded; ++i) {
        ASSERT_TRUE(RunHeartbeat(*storage).has_value());
        offloaded = HasLocalDiskReplica("cfg01_key");
        if (!offloaded)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_TRUE(offloaded);
    EXPECT_EQ(BackendRead(*storage, "cfg01_key", bytes.size()), bytes);
    EXPECT_EQ(GetBytes("cfg01_key", bytes.size()), bytes);
    EXPECT_FALSE(HasStaging(*storage));
    EXPECT_FALSE(WorkerRunning(*storage));
}

// CFG-06: a real client with SSD offload disabled but MC_SSD_BACKUP_ENABLE=1
// builds no FileStorage (so no backup worker or candidate): its SSD
// directory is never created, and Put/Get return the bytes.
TEST_F(FileStorageBackupClientTest, OffloadDisabledRealClientHasNoBackup) {
    setenv("MC_SSD_BACKUP_ENABLE", "1", 1);
    const auto ssd = dir_ / "cfg06_ssd";
    auto rc = RealClient::create();
    ASSERT_EQ(rc->setup_real(UniqueLocalEndpoint(), master_.metadata_url(),
                             16 << 20, 16 << 20, "tcp", "",
                             master_.master_address(), nullptr, "",
                             /*enable_ssd_offload=*/false, ssd.string()),
              0);
    EXPECT_FALSE(fs::exists(ssd));
    std::string v(4096, 'r');
    ASSERT_EQ(rc->put("cfg06_key", std::span<const char>(v.data(), v.size())),
              0);
    std::vector<char> buf(v.size());
    ASSERT_EQ(rc->register_buffer(buf.data(), buf.size()), 0);
    EXPECT_EQ(rc->get_into("cfg06_key", buf.data(), buf.size()),
              static_cast<int64_t>(v.size()));
    EXPECT_EQ(std::string(buf.data(), buf.size()), v);
    EXPECT_FALSE(fs::exists(ssd));
    rc->tearDownAll();
    unsetenv("MC_SSD_BACKUP_ENABLE");
}

// CLI-15 (real drain and stop): a running worker (full Init, real Client)
// keeps working through DrainLocalDiskSegment, skips its ticks without
// spinning while drained, and the FileStorage is destroyed within bounds.
// Runs in a death-test child: exit 0 on success, 1-6 for a failed check,
// 9 from the watchdog if Init, drain or destruction hangs.
int FileStorageBackupClientTestDrainChild(
    const std::function<std::unique_ptr<FileStorage>()>& make,
    SsdMetric& metric, const std::function<bool(FileStorage&)>& running,
    const std::function<bool(FileStorage&)>& has_staging) {
    std::thread([] {
        std::this_thread::sleep_for(std::chrono::seconds(20));
        std::fprintf(stderr, "watchdog: drain/destroy did not finish\n");
        std::_Exit(9);
    }).detach();
    setenv("MC_SSD_BACKUP_ENABLE", "1", 1);
    setenv("MC_SSD_BACKUP_STAGING_BYTES", "8388608", 1);
    setenv("MC_SSD_BACKUP_INTERVAL_MS", "100", 1);
    auto storage = make();
    if (!storage->Init()) return 1;
    if (!running(*storage) || !has_staging(*storage)) return 2;
    if (metric.ssd_backup_worker_up.value() != 1) return 3;
    std::this_thread::sleep_for(std::chrono::milliseconds(300));  // ticks run
    if (!storage->DrainLocalDiskSegment(0)) return 4;
    // While drained the worker idles on its tick wait: process CPU over 1 s
    // (a spinning loop would use about one core).
    const auto cpu0 = std::clock();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (static_cast<double>(std::clock() - cpu0) / CLOCKS_PER_SEC >= 0.5) {
        return 5;
    }
    const auto t0 = std::chrono::steady_clock::now();
    storage.reset();
    if (std::chrono::steady_clock::now() - t0 >= std::chrono::seconds(5)) {
        return 6;
    }
    return 0;
}

TEST_F(FileStorageBackupClientTest, RealDrainThenDestroyIsBounded) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(FileStorageBackupClientTestDrainChild(
                    [this] {
                        return MakeStorage(StorageBackendType::kBucket,
                                           "drain");
                    },
                    metric_, [](FileStorage& s) { return WorkerRunning(s); },
                    [](FileStorage& s) { return HasStaging(s); })),
                ::testing::ExitedWithCode(0), "");
}

// R5-B2: StartBackupWorker registers its staging with the transfer engine.
// Destroying the FileStorage (no quarantine) must unregister it before the
// memory goes: the Client, and so its transfer engine, outlives the storage.
// The staging comes from an allocator the test keeps alive, so the same
// still-valid address and length can be registered again afterwards; a
// stale region left behind makes that fail with INVALID_PARAMS (overlap).
// Three start/destroy rounds, each on a fresh Client (a FileStorage also
// registers its own primary client buffer and never unregisters it; that
// baseline behaviour is not part of this test).
TEST_F(FileStorageBackupClientTest, BackupStagingUnregisteredOnDestroy) {
    setenv("MC_SSD_BACKUP_ENABLE", "1", 1);
    setenv("MC_SSD_BACKUP_STAGING_BYTES", "8388608", 1);
    constexpr size_t kStaging = 8 << 20;
    auto kept = AlignedClientBufferAllocator::create(kStaging, "tcp");
    ASSERT_TRUE(kept);
    for (int round = 0; round < 3; ++round) {
        SCOPED_TRACE(round);
        NewClient();
        auto storage = MakeStorage(StorageBackendType::kBucket,
                                   "unreg" + std::to_string(round));
        SetStagingAlloc(*storage, [kept](size_t size) {
            return size <= kStaging ? kept
                                    : std::shared_ptr<ClientBufferAllocator>();
        });
        ASSERT_TRUE(storage->Init().has_value());
        ASSERT_TRUE(WorkerRunning(*storage));
        ASSERT_EQ(StagingBase(*storage), kept->getBase());
        storage.reset();
        auto again = client_->RegisterLocalMemory(
            kept->getBase(), kStaging, kWildcardLocation, false, true);
        EXPECT_TRUE(again.has_value())
            << "staging still registered after destroy: "
            << toString(again.error());
        if (again) {
            EXPECT_TRUE(client_->unregisterLocalMemory(kept->getBase()));
        }
    }
    unsetenv("MC_SSD_BACKUP_STAGING_BYTES");
    unsetenv("MC_SSD_BACKUP_ENABLE");
}

// R6-NEW-BEGIN (uses the unregister seam of the fixed code)
// R5-B2: unregister happens while the staging memory is still allocated,
// then the memory is freed; a failed unregister (still registered) keeps the
// memory; NOT_REGISTERED frees it; a quarantined batch keeps both the
// registration and the memory (C1).
TEST_F(FileStorageBackupClientTest, BackupStagingReleaseOrderAndRetention) {
    setenv("MC_SSD_BACKUP_ENABLE", "1", 1);
    setenv("MC_SSD_BACKUP_STAGING_BYTES", "8388608", 1);
    constexpr size_t kStaging = 8 << 20;
    enum Mode { kOk, kFails, kNotRegistered, kQuarantined };
    for (Mode mode : {kOk, kFails, kNotRegistered, kQuarantined}) {
        SCOPED_TRACE(static_cast<int>(mode));
        NewClient();
        auto storage = MakeStorage(StorageBackendType::kBucket,
                                   "order" + std::to_string(mode));
        std::weak_ptr<ClientBufferAllocator> weak;
        void* base = nullptr;
        SetStagingAlloc(*storage, [&](size_t size) {
            auto a = AlignedClientBufferAllocator::create(size, "tcp");
            weak = a;
            base = a ? a->getBase() : nullptr;
            return a;
        });
        int calls = 0;
        bool alive_at_unregister = false;
        Client* client = client_.get();
        SetStagingUnregister(
            *storage, [&, client](void* addr) -> tl::expected<void, ErrorCode> {
                ++calls;
                alive_at_unregister = !weak.expired();
                if (mode == kFails)
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                auto r = client->unregisterLocalMemory(addr);
                if (mode == kNotRegistered) {
                    return tl::make_unexpected(ErrorCode::NOT_REGISTERED);
                }
                return r;
            });
        ASSERT_TRUE(storage->Init().has_value());
        ASSERT_TRUE(WorkerRunning(*storage));
        ASSERT_NE(base, nullptr);
        if (mode == kQuarantined) {
            auto batch = AllocateStaging(*storage, {"qk"}, {4096});
            ASSERT_TRUE(batch.has_value());
            Quarantine(*storage, std::move(batch.value()));
        }
        storage.reset();
        switch (mode) {
            case kOk:
            case kNotRegistered:
                EXPECT_EQ(calls, 1);
                EXPECT_TRUE(alive_at_unregister);  // unregister, then free
                EXPECT_TRUE(weak.expired());
                break;
            case kFails:
                EXPECT_EQ(calls, 1);
                EXPECT_FALSE(weak.expired());  // still registered: kept
                break;
            case kQuarantined:
                EXPECT_EQ(calls, 0);  // registration kept with the memory
                EXPECT_FALSE(weak.expired());
                break;
        }
        if (mode == kFails || mode == kQuarantined) {
            // The leaked staging is still registered; clean up for the next
            // round (its memory stays allocated).
            EXPECT_TRUE(client_->unregisterLocalMemory(base));
        }
    }
    unsetenv("MC_SSD_BACKUP_STAGING_BYTES");
    unsetenv("MC_SSD_BACKUP_ENABLE");
}
// R6-NEW-END

// R3-NEW-BEGIN
// CFG-08 (full Init): backup enabled but its worker cannot start. Init still
// succeeds, worker_up is 0, no staging stays allocated, no backup thread
// runs, and the primary storage writes and reads back. Covers the staging
// memory registration failure (real Client, only the backup registration
// fails), allocation failure, zero capacity, zero staging, and an
// unsupported backend.
TEST_F(FileStorageBackupClientTest, BackupStartFailureLeavesPrimaryUsable) {
    setenv("MC_SSD_BACKUP_ENABLE", "1", 1);
    struct Case {
        const char* name;
        StorageBackendType backend;
        const char* var;
        const char* value;
        int hook;  // 0 none, 1 registration fails, 2 allocation fails
    };
    const Case cases[] = {
        {"register_fails", StorageBackendType::kBucket, nullptr, nullptr, 1},
        {"alloc_fails", StorageBackendType::kBucket, nullptr, nullptr, 2},
        {"zero_capacity", StorageBackendType::kBucket,
         "MC_SSD_BACKUP_CAPACITY_BYTES", "0", 0},
        {"zero_staging", StorageBackendType::kBucket,
         "MC_SSD_BACKUP_STAGING_BYTES", "0", 0},
        {"file_per_key", StorageBackendType::kFilePerKey, nullptr, nullptr, 0},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(c.name);
        if (c.var) setenv(c.var, c.value, 1);
        NewClient();
        metric_.ssd_backup_worker_up.update(1);  // must be reset to 0
        auto storage = MakeStorage(c.backend, c.name);
        int register_calls = 0;
        if (c.hook == 1) FailStagingRegister(*storage, &register_calls);
        if (c.hook == 2) FailStagingAlloc(*storage);
        ASSERT_TRUE(storage->Init().has_value());
        EXPECT_EQ(metric_.ssd_backup_worker_up.value(), 0);
        EXPECT_FALSE(HasStaging(*storage));
        EXPECT_FALSE(WorkerRunning(*storage));
        if (c.hook == 1) EXPECT_EQ(register_calls, 1);
        // Primary write and read back through this storage's backend.
        std::string v(4096, 'w');
        const std::string key = std::string("cfg08_") + c.name;
        std::unordered_map<std::string, std::vector<Slice>> batch{
            {TenantId::Default().MakeScopedKey(key),
             {Slice{v.data(), v.size()}}}};
        ASSERT_TRUE(BackendOf(*storage)
                        ->BatchOffload(batch,
                                       [](const std::vector<std::string>&,
                                          std::vector<StorageObjectMetadata>&) {
                                           return ErrorCode::OK;
                                       })
                        .has_value());
        EXPECT_EQ(BackendRead(*storage, key, v.size()), v);
        // And the client path still works.
        const std::string bytes(1024, 'z');
        ASSERT_TRUE(PutBytes(key + "_put", bytes));
        EXPECT_EQ(GetBytes(key + "_put", bytes.size()), bytes);
        storage.reset();
        if (c.var) unsetenv(c.var);
    }
    unsetenv("MC_SSD_BACKUP_ENABLE");
}
// R3-NEW-END

// R1-F01: the backup worker reads the source through Client::BatchGet. The
// holder's local hot cache is keyed by the bare key and only checks the
// size, so a cached entry of another tenant or an older version with the
// same key and length must not be stored as this version's backup.
TEST_F(FileStorageBackupTest, BackupReadBypassesHotCache) {
    mooncake::testing::InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));
    const std::string host = "127.0.0.1";

    // Source: owns the DRAM copy.
    auto writer = Client::Create(host + ":17811", "P2PHANDSHAKE", "tcp",
                                 std::nullopt, master.master_address());
    ASSERT_TRUE(writer.has_value());
    constexpr size_t kSegment = 16 << 20;
    void* segment = allocate_buffer_allocator_memory(kSegment);
    ASSERT_NE(segment, nullptr);
    ASSERT_TRUE((*writer)->MountSegment(segment, kSegment).has_value());
    const std::string key = "f01_key";
    std::string current(4096, 'B');
    std::vector<Slice> put{Slice{current.data(), current.size()}};
    ReplicateConfig rc;
    rc.replica_num = 1;
    ASSERT_TRUE((*writer)->Put(key, put, rc).has_value());

    // Backup holder: hot cache on, holding other bytes under the same key.
    setenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "4194304", 1);
    setenv("MC_STORE_LOCAL_HOT_CACHE_SIZE", "8388608", 1);
    auto holder = Client::Create(host + ":17812", "P2PHANDSHAKE", "tcp",
                                 std::nullopt, master.master_address());
    unsetenv("MC_STORE_LOCAL_HOT_BLOCK_SIZE");
    unsetenv("MC_STORE_LOCAL_HOT_CACHE_SIZE");
    ASSERT_TRUE(holder.has_value());
    ASSERT_TRUE((*holder)->IsHotCacheEnabled());
    auto cache = (*holder)->GetHotCache();
    HotMemBlock* block = cache->GetFreeBlock();
    ASSERT_NE(block, nullptr);
    std::memset(block->addr, 'A', current.size());
    block->size = current.size();
    block->key_ = key;
    ASSERT_TRUE(cache->PutHotKey(block));
    ASSERT_TRUE(cache->HasHotKey(key));

    {
        FileStorage storage(Config(), *holder, host + ":17812");
        constexpr size_t kStaging = 4 << 20;
        auto staging = AlignedClientBufferAllocator::create(kStaging, "tcp");
        ASSERT_TRUE(staging);
        ASSERT_TRUE((*holder)
                        ->RegisterLocalMemory(staging->getBase(), kStaging,
                                              kWildcardLocation, false, true)
                        .has_value());
        SetStaging(storage, staging);
        BackupBatch batch;
        batch.batch_id = 1;
        batch.tasks.push_back(
            BackupTaskItem{.tenant_id = (*holder)->tenant_id(),
                           .key = key,
                           .size = static_cast<int64_t>(current.size()),
                           .version = 1});
        const auto storage_key =
            TenantId((*holder)->tenant_id()).MakeScopedKey(key);
        EXPECT_EQ(ReadBackupBytes(storage, batch, storage_key), current);
        (void)(*holder)->unregisterLocalMemory(staging->getBase());
    }
    (void)(*writer)->UnmountSegment(segment, kSegment);
    free_memory("", segment);
    master.Stop();
}

}  // namespace mooncake
