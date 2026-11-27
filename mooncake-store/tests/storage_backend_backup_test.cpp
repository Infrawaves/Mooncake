// Bucket backend support for SSD backup buckets: counted separately, hidden
// from the re-registration scan, deleted at restart.

#include <glog/logging.h>
#include <future>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <set>
#include <thread>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage_backend.h"

namespace mooncake::test {
namespace fs = std::filesystem;

class StorageBackendBackupTest : public ::testing::Test {
   protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("mc_backup_bucket_test_" + std::to_string(::getpid()));
        fs::remove_all(dir_);
        fs::create_directories(dir_);
        config_.storage_filepath = dir_.string();
    }
    void TearDown() override { fs::remove_all(dir_); }

    // Write one bucket with `n` keys; returns the bucket id.
    int64_t Write(BucketStorageBackend& backend, const std::string& prefix,
                  int n, bool backup) {
        data_.clear();
        std::unordered_map<std::string, std::vector<Slice>> batch;
        for (int i = 0; i < n; ++i) {
            data_.push_back(std::string(4096, static_cast<char>('a' + i)));
        }
        for (int i = 0; i < n; ++i) {
            batch[prefix + std::to_string(i)] = {
                Slice{data_[i].data(), data_[i].size()}};
        }
        auto handler = [](const std::vector<std::string>&,
                          std::vector<StorageObjectMetadata>&) {
            return ErrorCode::OK;
        };
        auto res = backup ? backend.BatchOffloadBackup(batch, handler)
                          : backend.BatchOffload(batch, handler);
        EXPECT_TRUE(res.has_value());
        return res.value_or(-1);
    }

    size_t ScanKeys(BucketStorageBackend& backend) {
        std::vector<std::string> keys;
        std::vector<StorageObjectMetadata> metas;
        std::vector<int64_t> buckets;
        auto r = backend.BucketScan(0, keys, metas, buckets, 100000);
        EXPECT_TRUE(r.has_value());
        return keys.size();
    }

    fs::path dir_;
    FileStorageConfig config_;
    BucketBackendConfig bucket_config_;
    std::vector<std::string> data_;
};

TEST_F(StorageBackendBackupTest, BackupBucketLifecycle) {
    int64_t backup_id = -1;
    {
        BucketStorageBackend backend(config_, bucket_config_);
        ASSERT_TRUE(backend.Init().has_value());
        Write(backend, "primary_", 3, false);
        EXPECT_EQ(backend.GetBackupUsedBytes(), 0);
        backup_id = Write(backend, "backup_", 2, true);
        ASSERT_GT(backup_id, 0);
        EXPECT_GE(backend.GetBackupUsedBytes(), 2 * 4096);
        EXPECT_EQ(backend.GetBackupUsage().pinned_buckets, 1u);
        EXPECT_EQ(backend.GetBackupUsage().pinned_bytes,
                  backend.GetBackupUsage().total_bytes);
        EXPECT_TRUE(fs::exists(dir_ / (std::to_string(backup_id) + ".backup")));

        // Backup keys are readable locally but not re-registered by a scan.
        auto exists = backend.IsExist("backup_0");
        ASSERT_TRUE(exists.has_value());
        EXPECT_TRUE(exists.value());
        EXPECT_EQ(ScanKeys(backend), 3u);
    }
    // Restart: the backup bucket and its marker are gone; primary survives.
    {
        BucketStorageBackend backend(config_, bucket_config_);
        ASSERT_TRUE(backend.Init().has_value());
        EXPECT_EQ(backend.GetBackupUsedBytes(), 0);
        EXPECT_EQ(ScanKeys(backend), 3u);
        auto exists = backend.IsExist("backup_0");
        ASSERT_TRUE(exists.has_value());
        EXPECT_FALSE(exists.value());
        EXPECT_FALSE(
            fs::exists(dir_ / (std::to_string(backup_id) + ".backup")));
        EXPECT_FALSE(fs::exists(dir_ / (std::to_string(backup_id) + ".meta")));
    }
}

TEST_F(StorageBackendBackupTest, PinnedSurvivesEvictionUntilDemoted) {
    bucket_config_.eviction_policy = BucketEvictionPolicy::LRU;
    // Room for about three 2-key buckets.
    bucket_config_.max_total_size = 3 * (2 * 4096 + 1024);
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    const int64_t backup_id = Write(backend, "bk_", 2, true);
    ASSERT_GT(backup_id, 0);
    for (int r = 0; r < 6; ++r) {
        Write(backend, "p" + std::to_string(r) + "_", 2, false);
    }
    // Primaries were evicted to make room; the pinned backup was not.
    EXPECT_TRUE(backend.IsExist("bk_0").value());
    EXPECT_FALSE(backend.IsExist("p0_0").value());
    auto pinned = backend.ListPinnedBackupBuckets(32);
    ASSERT_EQ(pinned.size(), 1u);
    EXPECT_EQ(pinned[0].bucket_id, backup_id);
    EXPECT_EQ(pinned[0].sample_keys.size(), 2u);

    // Refresh touches only backup buckets.
    EXPECT_EQ(backend.RefreshBackupKeys({"bk_0", "bk_1", "p5_0", "nope"}), 2u);

    // Demoted with the oldest possible time: the next write evicts it first.
    ASSERT_TRUE(backend.DemoteBackupBucket(backup_id, int64_t{0}));
    EXPECT_FALSE(backend.DemoteBackupBucket(backup_id));
    EXPECT_EQ(backend.GetBackupUsage().pinned_bytes, 0);
    EXPECT_GT(backend.GetBackupUsage().total_bytes, 0);
    Write(backend, "q_", 2, false);
    EXPECT_FALSE(backend.IsExist("bk_0").value());
    EXPECT_TRUE(backend.IsExist("p5_0").value());
    // Counters follow the eviction.
    EXPECT_EQ(backend.GetBackupUsage().total_bytes, 0);
    EXPECT_EQ(backend.GetBackupUsage().pinned_buckets, 0u);
}

TEST_F(StorageBackendBackupTest, FifoSkipsPinned) {
    bucket_config_.eviction_policy = BucketEvictionPolicy::FIFO;
    bucket_config_.max_total_size = 3 * (2 * 4096 + 1024);
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    Write(backend, "bk_", 2, true);  // oldest bucket, pinned
    for (int r = 0; r < 6; ++r) {
        Write(backend, "p" + std::to_string(r) + "_", 2, false);
    }
    EXPECT_TRUE(backend.IsExist("bk_0").value());
    EXPECT_FALSE(backend.IsExist("p0_0").value());
}

TEST_F(StorageBackendBackupTest, SampleRotatesWithSeed) {
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    Write(backend, "s_", 20, true);  // 20 keys, sample 5 -> strides of 4
    auto keys_for = [&](uint64_t seed) {
        auto pinned = backend.ListPinnedBackupBuckets(5, seed);
        EXPECT_EQ(pinned.size(), 1u);
        return pinned.at(0).sample_keys;
    };
    const auto a = keys_for(0), b = keys_for(1), c = keys_for(4);
    ASSERT_EQ(a.size(), 5u);
    EXPECT_NE(a, b);  // different offset within each stride
    EXPECT_EQ(a, c);  // offset wraps with the stride length
    std::set<std::string> all;
    for (uint64_t seed = 0; seed < 4; ++seed) {
        for (const auto& k : keys_for(seed)) all.insert(k);
    }
    EXPECT_EQ(all.size(), 20u);  // every key is sampled by some seed
}

// A copy the master refused as stale is unmapped, so a backup of the new
// version of the same key can be written afterwards instead of being skipped
// as a duplicate.
TEST_F(StorageBackendBackupTest, ForgetStaleBackupThenRewrite) {
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    Write(backend, "v_", 2, true);
    std::vector<std::string> committed;
    auto handler = [&](const std::vector<std::string>& keys,
                       std::vector<StorageObjectMetadata>&) {
        committed = keys;
        return ErrorCode::OK;
    };
    std::string v2(4096, 'z');
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"v_0", {Slice{v2.data(), v2.size()}}}};
    EXPECT_EQ(backend.ForgetBackupKeys({"v_0", "absent"}), 1u);
    EXPECT_FALSE(backend.IsExist("v_0").value());
    EXPECT_TRUE(backend.IsExist("v_1").value());
    ASSERT_TRUE(backend.BatchOffloadBackup(batch, handler).has_value());
    EXPECT_EQ(committed, std::vector<std::string>{"v_0"});
}

// R1-F04: Remove + Put of a key whose old backup is still mapped here. The
// new version's backup replaces the old mapping and is registered, instead of
// being skipped as a duplicate (which left the new version unprotected). A
// failed registration restores the old mapping.
TEST_F(StorageBackendBackupTest, NewVersionBackupReplacesOldBackupMapping) {
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    Write(backend, "nv_", 2, true);  // old backups: nv_0 = 'a', nv_1 = 'b'
    std::string v2(4096, 'z');
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"nv_0", {Slice{v2.data(), v2.size()}}}};
    auto read_first = [&](const std::string& key) {
        std::unordered_map<std::string, Slice> read{
            {key, Slice{std::malloc(4096), 4096}}};
        EXPECT_TRUE(backend.BatchLoad(read).has_value());
        const char c = static_cast<char*>(read[key].ptr)[0];
        std::free(read[key].ptr);
        return c;
    };
    // Registration refused: the old backup stays mapped and readable.
    auto refuse = [](const std::vector<std::string>&,
                     std::vector<StorageObjectMetadata>&) {
        return ErrorCode::INVALID_PARAMS;
    };
    EXPECT_FALSE(backend.BatchOffloadBackup(batch, refuse).has_value());
    EXPECT_EQ(read_first("nv_0"), 'a');
    // Registration accepted: the new version is registered and served.
    std::vector<std::string> committed;
    auto ok = [&](const std::vector<std::string>& keys,
                  std::vector<StorageObjectMetadata>&) {
        committed = keys;
        return ErrorCode::OK;
    };
    ASSERT_TRUE(backend.BatchOffloadBackup(batch, ok).has_value());
    EXPECT_EQ(committed, std::vector<std::string>{"nv_0"});
    EXPECT_EQ(read_first("nv_0"), 'z');
    EXPECT_EQ(read_first("nv_1"), 'b');  // the rest of the old bucket stays
    EXPECT_EQ(backend.GetBackupUsage().pinned_buckets, 2u);
}

// P2-03 (review 2026-11-25): the scanner samples only keys still mapped to a
// bucket. After a newer backup replaced a key, or a stale write was undone,
// the old (or refused) bucket must not offer that key: the master answers
// residency per key and would report the newer copy's state. Covers "old
// bucket keeps part of its keys", "old bucket keeps none", "all keys of a
// write refused as stale" and "refused write puts the old mapping back".
TEST_F(StorageBackendBackupTest, SampleOnlyKeysStillMappedToTheBucket) {
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    const int64_t old_id = Write(backend, "rk_", 2, true);  // rk_0, rk_1
    ASSERT_GT(old_id, 0);
    auto sample_of = [&](int64_t id) {
        std::set<std::string> keys;
        bool found = false;
        for (const auto& b : backend.ListPinnedBackupBuckets(32)) {
            if (b.bucket_id != id) continue;
            found = true;
            keys.insert(b.sample_keys.begin(), b.sample_keys.end());
        }
        EXPECT_TRUE(found) << "bucket " << id << " not pinned";
        return keys;
    };
    auto ok = [](const std::vector<std::string>&,
                 std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; };
    std::string v2(4096, 'z');
    std::unordered_map<std::string, std::vector<Slice>> one{
        {"rk_0", {Slice{v2.data(), v2.size()}}}};
    auto new_id = backend.BatchOffloadBackup(one, ok);
    ASSERT_TRUE(new_id.has_value());
    // Partly replaced: the old bucket offers only rk_1.
    EXPECT_EQ(sample_of(old_id), std::set<std::string>{"rk_1"});
    EXPECT_EQ(sample_of(*new_id), std::set<std::string>{"rk_0"});
    // Fully replaced: the old bucket offers nothing.
    std::unordered_map<std::string, std::vector<Slice>> other{
        {"rk_1", {Slice{v2.data(), v2.size()}}}};
    ASSERT_TRUE(backend.BatchOffloadBackup(other, ok).has_value());
    EXPECT_TRUE(sample_of(old_id).empty());

    // A write whose keys the master all refused as stale: nothing maps to
    // the new bucket; the displaced mapping (st_0 in its old bucket) is put
    // back and offered again by that bucket.
    const int64_t st_old = Write(backend, "st_", 1, true);
    std::vector<std::string> rejected;
    auto refuse_all = [&](const std::vector<std::string>& keys,
                          std::vector<StorageObjectMetadata>&) {
        rejected = keys;
        return ErrorCode::OK;
    };
    std::unordered_map<std::string, std::vector<Slice>> stale{
        {"st_0", {Slice{v2.data(), v2.size()}}}};
    auto st_new =
        backend.BatchOffloadBackup(stale, refuse_all, nullptr, &rejected);
    ASSERT_TRUE(st_new.has_value());
    ASSERT_EQ(rejected, std::vector<std::string>{"st_0"});
    EXPECT_TRUE(sample_of(*st_new).empty());
    EXPECT_EQ(sample_of(st_old), std::set<std::string>{"st_0"});
    // R11-GREEN-ONLY-BEGIN
    for (const auto& b : backend.ListPinnedBackupBuckets(0)) {
        const size_t want =
            b.bucket_id == old_id || b.bucket_id == *st_new ? 0u : 1u;
        EXPECT_EQ(b.live_keys, want) << "bucket " << b.bucket_id;
        EXPECT_TRUE(b.sample_keys.empty());  // sample_keys = 0
    }
    // R11-GREEN-ONLY-END
}

// R4 + review P1: a primary write over a key held here as a backup replaces
// the backup mapping only when the primary bucket commits. A failed
// registration restores the backup mapping; a successful one survives a
// restart while the rest of the backup bucket is dropped.
TEST_F(StorageBackendBackupTest, BackupKeyRewrittenAsPrimarySurvivesRestart) {
    std::string v(4096, 'p');
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"kp_0", {Slice{v.data(), v.size()}}}};
    {
        BucketStorageBackend backend(config_, bucket_config_);
        ASSERT_TRUE(backend.Init().has_value());
        Write(backend, "kp_", 2, true);  // backup bucket holding kp_0, kp_1
        // Grouping for a primary offload does not skip a backup-held key.
        std::vector<std::vector<std::string>> groups;
        std::vector<std::string> deferred;
        ASSERT_TRUE(
            backend
                .AllocateOffloadingBuckets({{"kp_0", 4096}}, groups, &deferred)
                .has_value());
        size_t grouped = deferred.size();
        for (const auto& g : groups) grouped += g.size();
        EXPECT_EQ(grouped, 1u);
        backend.ClearUngroupedOffloadingObjects();
        // Registration fails: the backup mapping is still there.
        auto fail = [](const std::vector<std::string>&,
                       std::vector<StorageObjectMetadata>&) {
            return ErrorCode::RPC_FAIL;
        };
        EXPECT_FALSE(backend.BatchOffload(batch, fail).has_value());
        EXPECT_TRUE(backend.IsExist("kp_0").value());
        EXPECT_EQ(ScanKeys(backend), 0u);  // still only a backup
        std::unordered_map<std::string, Slice> read{
            {"kp_0", Slice{std::malloc(4096), 4096}}};
        ASSERT_TRUE(backend.BatchLoad(read).has_value());
        EXPECT_EQ(static_cast<char*>(read["kp_0"].ptr)[0], 'a');  // backup data
        std::free(read["kp_0"].ptr);
        // Registration succeeds: now a primary.
        std::vector<std::string> committed;
        auto ok = [&](const std::vector<std::string>& keys,
                      std::vector<StorageObjectMetadata>&) {
            committed = keys;
            return ErrorCode::OK;
        };
        ASSERT_TRUE(backend.BatchOffload(batch, ok).has_value());
        EXPECT_EQ(committed, std::vector<std::string>{"kp_0"});
        EXPECT_EQ(ScanKeys(backend), 1u);
    }
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    EXPECT_TRUE(backend.IsExist("kp_0").value());   // primary kept
    EXPECT_FALSE(backend.IsExist("kp_1").value());  // backup dropped
}

// T1: a primary offload of a backup-held key that is deferred (too small to
// fill a bucket) leaves the backup mapping untouched and readable.
TEST_F(StorageBackendBackupTest, DeferredPrimaryKeepsBackupReadable) {
    bucket_config_.bucket_size_limit = 1LL << 30;  // nothing fills a bucket
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    Write(backend, "kd_", 2, true);
    std::vector<std::vector<std::string>> groups;
    std::vector<std::string> deferred;
    ASSERT_TRUE(
        backend.AllocateOffloadingBuckets({{"kd_0", 4096}}, groups, &deferred)
            .has_value());
    EXPECT_TRUE(groups.empty());
    EXPECT_EQ(deferred, std::vector<std::string>{"kd_0"});
    EXPECT_TRUE(backend.IsExist("kd_0").value());
    std::unordered_map<std::string, Slice> read{
        {"kd_0", Slice{std::malloc(4096), 4096}}};
    ASSERT_TRUE(backend.BatchLoad(read).has_value());
    EXPECT_EQ(static_cast<char*>(read["kd_0"].ptr)[0], 'a');
    std::free(read["kd_0"].ptr);
}

// Review P1 (rollback window): while a failed primary write is rolled back,
// a concurrent reader never sees the backup-held key as missing.
TEST_F(StorageBackendBackupTest, RollbackNeverHidesBackup) {
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    Write(backend, "kw_", 2, true);
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> missing{0}, checks{0};
    std::thread reader([&] {
        while (!stop.load()) {
            auto e = backend.IsExist("kw_0");
            if (!e.has_value() || !e.value()) ++missing;
            ++checks;
        }
    });
    std::string v(4096, 'p');
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"kw_0", {Slice{v.data(), v.size()}}}};
    auto fail = [](const std::vector<std::string>&,
                   std::vector<StorageObjectMetadata>&) {
        return ErrorCode::RPC_FAIL;
    };
    for (int i = 0; i < 200; ++i) {
        EXPECT_FALSE(backend.BatchOffload(batch, fail).has_value());
    }
    stop = true;
    reader.join();
    EXPECT_GT(checks.load(), 0u);
    EXPECT_EQ(missing.load(), 0u);
    EXPECT_TRUE(backend.IsExist("kw_0").value());
}

// Review P2: the old backup bucket is evicted while the key points at the
// new primary bucket (so that eviction does not report it), then the primary
// registration fails. The rollback must report the key as evicted so the
// master stops advertising a backup this client no longer has.
TEST_F(StorageBackendBackupTest, LostBackupReportedAfterFailedRewrite) {
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    const int64_t backup_id = Write(backend, "kl_", 2, true);
    std::string v(4096, 'p');
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"kl_0", {Slice{v.data(), v.size()}}}};
    auto fail_after_evicting_backup = [&](const std::vector<std::string>&,
                                          std::vector<StorageObjectMetadata>&) {
        EXPECT_TRUE(backend.DeleteBucket(backup_id).has_value());
        return ErrorCode::RPC_FAIL;
    };
    auto ok = [](const std::vector<std::string>&,
                 std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; };
    std::string nv(4096, 'n');
    std::unordered_map<std::string, std::vector<Slice>> rewrite{
        {"kl_0", {Slice{nv.data(), nv.size()}}}};
    std::vector<std::string> reported;
    auto on_evicted = [&](const std::vector<std::string>& keys)
        -> tl::expected<void, ErrorCode> {
        reported.insert(reported.end(), keys.begin(), keys.end());
        // Review P2 (follow-up): until this notification is done, a new
        // backup of the same key must not land here, or the notification
        // would drop its master record.
        EXPECT_FALSE(backend.BatchOffloadBackup(rewrite, ok).has_value());
        return {};
    };
    EXPECT_FALSE(
        backend.BatchOffload(batch, fail_after_evicting_backup, on_evicted)
            .has_value());
    EXPECT_FALSE(backend.IsExist("kl_0").value());
    EXPECT_EQ(reported, std::vector<std::string>{"kl_0"});
    // Reservation released afterwards: the key can be backed up again.
    EXPECT_TRUE(backend.BatchOffloadBackup(rewrite, ok).has_value());
    EXPECT_TRUE(backend.IsExist("kl_0").value());
}

// Review P2 (in-flight write): a new backup Q of the same key is admitted
// (and written) while the key still points at the unregistered primary P,
// and commits only after P's rollback reserved the key for the lost-backup
// report. Q must not be committed or registered, or that report would drop
// Q's master record.
TEST_F(StorageBackendBackupTest, InflightBackupNotCommittedDuringLostReport) {
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    const int64_t backup_id = Write(backend, "kq_", 2, true);
    std::string pv(4096, 'p'), qv(4096, 'q');
    std::unordered_map<std::string, std::vector<Slice>> primary{
        {"kq_0", {Slice{pv.data(), pv.size()}}}};
    std::unordered_map<std::string, std::vector<Slice>> q_batch{
        {"kq_0", {Slice{qv.data(), qv.size()}}}};

    std::promise<void> q_at_commit, release_q;
    auto release_q_future = release_q.get_future().share();
    std::vector<std::string> q_registered;
    std::future<tl::expected<int64_t, ErrorCode>> q_write;
    auto q_complete = [&](const std::vector<std::string>& keys,
                          std::vector<StorageObjectMetadata>&) {
        q_registered.insert(q_registered.end(), keys.begin(), keys.end());
        return ErrorCode::OK;
    };
    auto p_fails_after_q_admitted = [&](const std::vector<std::string>&,
                                        std::vector<StorageObjectMetadata>&) {
        // P is committed (key -> P), not registered. The old backup
        // bucket goes away, and Q gets admitted and written meanwhile.
        EXPECT_TRUE(backend.DeleteBucket(backup_id).has_value());
        backend.SetBeforeCommitHookForTest([&] {
            q_at_commit.set_value();
            release_q_future.wait();
        });
        q_write = std::async(std::launch::async, [&] {
            return backend.BatchOffloadBackup(q_batch, q_complete);
        });
        q_at_commit.get_future().wait();
        backend.SetBeforeCommitHookForTest(nullptr);
        return ErrorCode::RPC_FAIL;  // P's registration fails: rollback
    };
    std::vector<std::string> reported;
    auto on_evicted = [&](const std::vector<std::string>& keys)
        -> tl::expected<void, ErrorCode> {
        reported.insert(reported.end(), keys.begin(), keys.end());
        // The rollback has reserved the key; now let Q try to commit.
        release_q.set_value();
        EXPECT_EQ(q_write.wait_for(std::chrono::seconds(10)),
                  std::future_status::ready);
        return {};
    };
    EXPECT_FALSE(
        backend.BatchOffload(primary, p_fails_after_q_admitted, on_evicted)
            .has_value());
    ASSERT_TRUE(q_write.valid());
    (void)q_write.get();
    EXPECT_EQ(reported, std::vector<std::string>{"kq_0"});
    EXPECT_TRUE(q_registered.empty());
    EXPECT_FALSE(backend.IsExist("kq_0").value());
}

// Review F4: a primary bucket still waiting for its master registration is
// not evictable. Otherwise evicting it (here: the only unpinned bucket next
// to a pinned backup) races the rollback, which then cannot restore the
// backup mapping it replaced.
TEST_F(StorageBackendBackupTest, RegisteringPrimaryIsNotEvicted) {
    bucket_config_.eviction_policy = BucketEvictionPolicy::LRU;
    constexpr size_t kMax = 64 * 1024;
    bucket_config_.max_total_size = kMax;
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    Write(backend, "kr_", 1, true);  // pinned backup holding kr_0
    std::string pv(4096, 'p');
    // Fits next to the backup only once the 4 KiB primary is gone, so without
    // the fix the registering primary is the one bucket evicted for it.
    std::string xv(kMax - 2048, 'x');
    std::unordered_map<std::string, std::vector<Slice>> primary{
        {"kr_0", {Slice{pv.data(), pv.size()}}}};
    std::unordered_map<std::string, std::vector<Slice>> other{
        {"kx_0", {Slice{xv.data(), xv.size()}}}};
    auto ok = [](const std::vector<std::string>&,
                 std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; };
    auto fail_after_pressure = [&](const std::vector<std::string>&,
                                   std::vector<StorageObjectMetadata>&) {
        // The pinned backup and the registering primary are the only
        // buckets; neither may be evicted, so this write fails.
        EXPECT_FALSE(backend.BatchOffload(other, ok).has_value());
        EXPECT_TRUE(backend.IsExist("kr_0").value());
        return ErrorCode::RPC_FAIL;
    };
    EXPECT_FALSE(
        backend.BatchOffload(primary, fail_after_pressure).has_value());
    // The rollback put the backup mapping back.
    EXPECT_TRUE(backend.IsExist("kr_0").value());
}

// Review F5: when reporting a lost backup fails, the key stays reserved and
// is reported again by the next offload; only then can it be written here.
TEST_F(StorageBackendBackupTest, FailedLostReportIsRetried) {
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    const int64_t backup_id = Write(backend, "kf_", 2, true);
    std::string v(4096, 'p');
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"kf_0", {Slice{v.data(), v.size()}}}};
    auto ok = [](const std::vector<std::string>&,
                 std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; };
    auto fail_after_evicting_backup = [&](const std::vector<std::string>&,
                                          std::vector<StorageObjectMetadata>&) {
        EXPECT_TRUE(backend.DeleteBucket(backup_id).has_value());
        return ErrorCode::RPC_FAIL;
    };
    std::vector<std::vector<std::string>> reports;
    bool master_up = false;
    auto on_evicted = [&](const std::vector<std::string>& keys)
        -> tl::expected<void, ErrorCode> {
        reports.push_back(keys);
        if (!master_up) return tl::make_unexpected(ErrorCode::RPC_FAIL);
        return {};
    };
    EXPECT_FALSE(
        backend.BatchOffload(batch, fail_after_evicting_backup, on_evicted)
            .has_value());
    ASSERT_EQ(reports.size(), 1u);
    // Still reserved: a new copy of the key cannot land here yet.
    std::string nv(4096, 'n');
    std::unordered_map<std::string, std::vector<Slice>> again{
        {"kf_0", {Slice{nv.data(), nv.size()}}}};
    EXPECT_FALSE(backend.BatchOffloadBackup(again, ok).has_value());
    // The next offload that can report retries first.
    master_up = true;
    std::unordered_map<std::string, std::vector<Slice>> unrelated{
        {"kz_0", {Slice{nv.data(), nv.size()}}}};
    EXPECT_TRUE(backend.BatchOffload(unrelated, ok, on_evicted).has_value());
    ASSERT_EQ(reports.size(), 2u);
    EXPECT_EQ(reports[1], std::vector<std::string>{"kf_0"});
    EXPECT_TRUE(backend.BatchOffloadBackup(again, ok).has_value());
}

static size_t CountExt(const fs::path& dir, const std::string& ext) {
    size_t n = 0;
    for (const auto& e : fs::directory_iterator(dir)) {
        if (e.path().extension() == ext) ++n;
    }
    return n;
}

// F2: a crash part-way through deleting a backup bucket (here: data gone,
// metadata and marker left) must not let the bucket come back as a primary.
TEST_F(StorageBackendBackupTest, PartiallyDeletedBackupNeverLoadsAsPrimary) {
    int64_t id = -1;
    {
        BucketStorageBackend backend(config_, bucket_config_);
        ASSERT_TRUE(backend.Init().has_value());
        Write(backend, "p_", 2, false);
        id = Write(backend, "bk_", 2, true);
    }
    // Simulate the crash window: only the data file was unlinked.
    for (const auto& e : fs::directory_iterator(dir_)) {
        if (e.path().stem() == std::to_string(id) &&
            e.path().extension() != ".meta" &&
            e.path().extension() != ".backup") {
            fs::remove(e.path());
        }
    }
    ASSERT_TRUE(fs::exists(dir_ / (std::to_string(id) + ".backup")));
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    EXPECT_FALSE(backend.IsExist("bk_0").value());
    EXPECT_EQ(ScanKeys(backend), 2u);  // only the primary bucket
    EXPECT_FALSE(fs::exists(dir_ / (std::to_string(id) + ".meta")));
    EXPECT_FALSE(fs::exists(dir_ / (std::to_string(id) + ".backup")));
}

// F10: RemoveAll and eviction leave no .backup markers behind.
TEST_F(StorageBackendBackupTest, RemoveAllAndEvictionDropMarkers) {
    bucket_config_.eviction_policy = BucketEvictionPolicy::LRU;
    bucket_config_.max_total_size = 3 * (2 * 4096 + 1024);
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    const int64_t a = Write(backend, "ba_", 2, true);
    ASSERT_TRUE(backend.DemoteBackupBucket(a, int64_t{0}));
    for (int r = 0; r < 4; ++r)
        Write(backend, "p" + std::to_string(r) + "_", 2, false);
    EXPECT_FALSE(backend.IsExist("ba_0").value());  // evicted
    EXPECT_FALSE(fs::exists(dir_ / (std::to_string(a) + ".backup")));
    Write(backend, "bb_", 2, true);
    std::ofstream(dir_ / "999999.backup").put('x');  // stray, no files
    backend.RemoveAll();
    EXPECT_EQ(CountExt(dir_, ".backup"), 0u);
    EXPECT_EQ(CountExt(dir_, ".meta"), 0u);
}

// R10-Q1: a foreign file that only looks like a marker ("<id>-x.backup",
// "0<id>.backup", an out-of-range number) never deletes the primary bucket
// <id>, and is left alone; a canonical marker still deletes its backup.
TEST_F(StorageBackendBackupTest, NonCanonicalMarkerNeverDeletesPrimary) {
    int64_t primary = -1, backup = -1;
    {
        BucketStorageBackend backend(config_, bucket_config_);
        ASSERT_TRUE(backend.Init().has_value());
        primary = Write(backend, "p_", 2, false);
        backup = Write(backend, "bk_", 2, true);
    }
    const std::string id = std::to_string(primary);
    const std::vector<std::string> foreign{
        id + "-not-a-bucket.backup", "0" + id + ".backup", "+" + id + ".backup",
        "99999999999999999999.backup"};
    for (const auto& f : foreign) std::ofstream(dir_ / f).put('x');
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    EXPECT_TRUE(fs::exists(dir_ / (id + ".meta")));
    EXPECT_TRUE(fs::exists(dir_ / (id + ".bucket")));
    EXPECT_EQ(ScanKeys(backend), 2u);
    std::unordered_map<std::string, Slice> read{
        {"p_0", Slice{std::malloc(4096), 4096}},
        {"p_1", Slice{std::malloc(4096), 4096}}};
    ASSERT_TRUE(backend.BatchLoad(read).has_value());
    EXPECT_EQ(std::string(static_cast<char*>(read["p_0"].ptr), 4096),
              std::string(4096, 'a'));
    EXPECT_EQ(std::string(static_cast<char*>(read["p_1"].ptr), 4096),
              std::string(4096, 'b'));
    for (auto& [k, s] : read) std::free(s.ptr);
    for (const auto& f : foreign) EXPECT_TRUE(fs::exists(dir_ / f)) << f;
    // The canonical marker of the real backup bucket did its job.
    EXPECT_FALSE(backend.IsExist("bk_0").value());
    EXPECT_FALSE(fs::exists(dir_ / (std::to_string(backup) + ".backup")));
    EXPECT_FALSE(fs::exists(dir_ / (std::to_string(backup) + ".meta")));
}

// R5-Q1: a backup whose cleanup at startup fails (here its data path is a
// non-empty directory, so it cannot be unlinked) keeps its marker, and its id
// stays reserved: the next new bucket gets a higher id and is never deleted
// on a later start as if it were that backup.
TEST_F(StorageBackendBackupTest, FailedMarkerCleanupReservesItsId) {
    int64_t high = -1;
    {
        BucketStorageBackend backend(config_, bucket_config_);
        ASSERT_TRUE(backend.Init().has_value());
        high = Write(backend, "p_", 2, false) + 1000;
    }
    const std::string h = std::to_string(high);
    fs::create_directories(dir_ / (h + ".bucket") / "x");
    std::ofstream(dir_ / (h + ".backup")).put('x');
    int64_t next = -1;
    {
        BucketStorageBackend backend(config_, bucket_config_);
        ASSERT_TRUE(backend.Init().has_value());
        EXPECT_TRUE(fs::exists(dir_ / (h + ".backup")));  // cleanup failed
        next = Write(backend, "n_", 2, false);
        EXPECT_GT(next, high);
    }
    fs::remove_all(dir_ / (h + ".bucket"));
    BucketStorageBackend backend(config_, bucket_config_);
    ASSERT_TRUE(backend.Init().has_value());
    EXPECT_TRUE(backend.IsExist("n_0").value());
    EXPECT_TRUE(fs::exists(dir_ / (std::to_string(next) + ".meta")));
    EXPECT_EQ(ScanKeys(backend), 4u);
    EXPECT_FALSE(fs::exists(dir_ / (h + ".backup")));  // now cleaned up
}

// Cost of the per-tick backup bookkeeping at production-like bucket counts.
// Off by default: MC_SSD_BACKUP_BENCH=1 [MC_SSD_BACKUP_BENCH_BUCKETS=N]
// [MC_SSD_BACKUP_BENCH_DIR=path].
TEST(StorageBackendBackupBench, BookkeepingCost) {
    if (std::getenv("MC_SSD_BACKUP_BENCH") == nullptr) {
        GTEST_SKIP() << "set MC_SSD_BACKUP_BENCH=1";
    }
    using Clock = std::chrono::steady_clock;
    const char* n_env = std::getenv("MC_SSD_BACKUP_BENCH_BUCKETS");
    const int total = n_env ? std::atoi(n_env) : 40000;
    const int pinned = 1200;  // ~300 GB of 256 MB backup buckets
    const char* dir_env = std::getenv("MC_SSD_BACKUP_BENCH_DIR");
    fs::path dir = dir_env ? fs::path(dir_env) : fs::temp_directory_path();
    dir /= "mc_backup_bench_" + std::to_string(::getpid());
    fs::remove_all(dir);
    fs::create_directories(dir);

    FileStorageConfig config;
    config.storage_filepath = dir.string();
    BucketBackendConfig bucket_config;
    bucket_config.eviction_policy = BucketEvictionPolicy::LRU;
    BucketStorageBackend backend(config, bucket_config);
    ASSERT_TRUE(backend.Init().has_value());

    std::string payload(4096, 'x');
    auto handler = [](const std::vector<std::string>&,
                      std::vector<StorageObjectMetadata>&) {
        return ErrorCode::OK;
    };
    std::vector<std::string> read_keys;
    const auto fill_start = Clock::now();
    for (int b = 0; b < total; ++b) {
        const bool backup = b % (total / pinned) == 0;
        const int keys = backup ? 64 : 1;
        std::unordered_map<std::string, std::vector<Slice>> batch;
        for (int k = 0; k < keys; ++k) {
            batch["b" + std::to_string(b) + "_" + std::to_string(k)] = {
                Slice{payload.data(), payload.size()}};
        }
        auto res = backup ? backend.BatchOffloadBackup(batch, handler)
                          : backend.BatchOffload(batch, handler);
        ASSERT_TRUE(res.has_value()) << b;
        if (!backup && b % 97 == 0)
            read_keys.push_back("b" + std::to_string(b) + "_0");
    }
    const double fill_s =
        std::chrono::duration<double>(Clock::now() - fill_start).count();

    auto time_us = [](auto&& fn, int reps) {
        std::vector<double> v;
        for (int r = 0; r < reps; ++r) {
            auto t0 = Clock::now();
            fn();
            v.push_back(
                std::chrono::duration<double, std::micro>(Clock::now() - t0)
                    .count());
        }
        std::sort(v.begin(), v.end());
        return std::make_pair(v[v.size() / 2], v.back());
    };
    auto usage = time_us([&] { (void)backend.GetBackupUsage(); }, 50);
    auto list32 =
        time_us([&] { (void)backend.ListPinnedBackupBuckets(32); }, 20);
    auto list0 = time_us([&] { (void)backend.ListPinnedBackupBuckets(0); }, 20);
    const auto u = backend.GetBackupUsage();

    // Write latency (exclusive lock on commit) with and without a poller
    // that does what the backup thread does each tick, but 100x as often.
    auto write_p99 = [&](bool poll) {
        std::atomic<bool> stop{false};
        std::thread poller([&] {
            while (poll && !stop.load()) {
                (void)backend.GetBackupUsage();
                (void)backend.GetBackupUsage();
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });
        std::vector<double> v;
        for (int r = 0; r < 300; ++r) {
            std::unordered_map<std::string, std::vector<Slice>> batch;
            batch["w" + std::to_string(poll) + "_" + std::to_string(r)] = {
                Slice{payload.data(), payload.size()}};
            auto t0 = Clock::now();
            EXPECT_TRUE(backend.BatchOffload(batch, handler).has_value());
            v.push_back(
                std::chrono::duration<double, std::micro>(Clock::now() - t0)
                    .count());
        }
        stop = true;
        poller.join();
        std::sort(v.begin(), v.end());
        return std::make_pair(v[v.size() / 2], v[v.size() * 99 / 100]);
    };
    auto w_off = write_p99(false);
    auto w_on = write_p99(true);

    printf("BENCH buckets=%d pinned_buckets=%zu fill_s=%.1f\n", total,
           u.pinned_buckets, fill_s);
    printf("BENCH GetBackupUsage p50_us=%.0f max_us=%.0f\n", usage.first,
           usage.second);
    printf("BENCH ListPinned(32) p50_us=%.0f max_us=%.0f\n", list32.first,
           list32.second);
    printf("BENCH ListPinned(0) p50_us=%.0f max_us=%.0f\n", list0.first,
           list0.second);
    printf("BENCH write no_poll p50_us=%.0f p99_us=%.0f\n", w_off.first,
           w_off.second);
    printf("BENCH write poll_100Hz p50_us=%.0f p99_us=%.0f\n", w_on.first,
           w_on.second);
    fs::remove_all(dir);
}

}  // namespace mooncake::test
