// Ordinary SSD read held after its restore (SSD backup off). A reader R
// queries a key whose only copy is A's SSD primary; the restore RPC to A
// returns (the restored buffer holds the value), and the read is held
// there (the test's ssd_restore observer) while the master lease runs out
// and the object is removed or replaced with a bigger value. Required:
// the released read returns the complete old value, and a fresh read then
// reports not found, or the new value.
//
// Uses only the public RealClient API plus two test seams: the
// "ssd_restore" transfer observer and the master-service accessor. The
// same file is built on the base tree of the SSD backup feature
// (b5d79e69 plus those seams) and on the feature tree, so the result
// shows whether this behaviour is inherited.

#include <gtest/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "master_service.h"
#include "master_service/master_service_test_peer.h"
#include "real_client.h"
#include "rpc_service.h"
#include "test_server_helpers.h"

namespace mooncake {
namespace {

namespace fs = std::filesystem;

constexpr int64_t kLeaseMs = 2000;
constexpr size_t kOld = 4096, kNew = 8192;

void Trace(const std::string& line) {
    const char* dir = std::getenv("MC_TEST_TRACE_DIR");
    if (dir == nullptr) return;
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    const std::string name =
        info ? std::string(info->test_suite_name()) + "." + info->name()
             : std::string("unknown");
    std::ofstream out(std::string(dir) + "/" + name + ".trace", std::ios::app);
    out << ::getpid() << " " << line << "\n";
}

int Fail(int code, const std::string& why) {
    std::fprintf(stderr, "check %d failed: %s\n", code, why.c_str());
    return code;
}

std::string Payload(int seed, int gen, size_t n) {
    std::string s(n, '\0');
    for (size_t i = 0; i < n; ++i) {
        s[i] = static_cast<char>(
            (seed * 131 + gen * 17 + i * 7 + (i >> 8) * 3) & 0xff);
    }
    return s;
}

void SleepPast(int64_t ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms + 400));
}

class HeldSsdRestoreTest : public ::testing::Test {
   protected:
    static std::vector<std::pair<const char*, const char*>> Env() {
        return {{"MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "1"},
                {"MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES", "16777216"},
                // The restored buffer outlives the held read by far.
                {"MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS", "8000"},
                {"MC_STORE_LOCAL_HOT_CACHE_SIZE", "0"}};
    }
    void SetUp() override {
        unsetenv("MC_SSD_BACKUP_ENABLE");
        for (const auto& [k, v] : Env()) setenv(k, v, 1);
        dir_ = fs::temp_directory_path() /
               ("mc_held_ssd_restore_test_" + std::to_string(::getpid()));
        fs::remove_all(dir_);
        fs::create_directories(dir_);
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder()
                                      .set_enable_offload(true)
                                      .set_default_kv_lease_ttl(kLeaseMs)
                                      .set_root_fs_dir("")
                                      .build()));
    }
    void TearDown() override {
        for (auto* c : {&r_, &a_}) {
            if (*c) (*c)->tearDownAll();
            c->reset();
        }
        master_.Stop();
        for (const auto& [k, v] : Env()) unsetenv(k);
        fs::remove_all(dir_);
    }
    std::shared_ptr<RealClient> Make(const std::string& host, bool offload,
                                     std::string& hostname) {
        setenv("MOONCAKE_HOST_ID", host.c_str(), 1);
        auto rc = RealClient::create();
        const auto path = (dir_ / host).string();
        if (offload) fs::create_directories(path);
        hostname = "127.0.0.1:" + std::to_string(getFreeTcpPort());
        const int r =
            rc->setup_real(hostname, master_.metadata_url(), 32 << 20, 16 << 20,
                           "tcp", "", master_.master_address(), nullptr, "",
                           offload, offload ? path : std::string());
        unsetenv("MOONCAKE_HOST_ID");
        return r == 0 ? rc : nullptr;
    }
    void Evict() {
        test::MasterServiceTestPeer(
            master_.service()->GetMasterServiceForTest())
            .RunBatchEvictForTesting(1.0, 1.0);
    }

    // Destination: `cap` bytes after a guard, as one slice (api 0,
    // batch_get_into) or two unequal ones (api 1, multi-buffer).
    static constexpr size_t kGuard = 256;
    static constexpr char kGuardByte = static_cast<char>(0xEE);
    struct Dest {
        std::vector<void*> ptrs;
        std::vector<size_t> lens;
    };
    Dest MakeDest(int api, size_t cap) {
        std::memset(buf_.data(), kGuardByte, buf_.size());
        Dest d;
        char* base = buf_.data() + kGuard;
        if (api == 0) {
            d.ptrs = {base};
            d.lens = {cap};
        } else {
            const size_t first = cap / 3;
            d.ptrs = {base, base + first + kGuard};
            d.lens = {first, cap - first};
        }
        return d;
    }
    // The bytes written into `d` (n of them) and whether everything else
    // in the buffer is still guard bytes.
    std::string Bytes(const Dest& d, size_t n) const {
        std::string out;
        for (size_t j = 0; j < d.ptrs.size() && out.size() < n; ++j) {
            const size_t take = std::min(d.lens[j], n - out.size());
            out.append(static_cast<const char*>(d.ptrs[j]), take);
        }
        return out;
    }
    bool OnlyInside(const Dest& d, size_t n) const {
        std::vector<bool> inside(buf_.size(), false);
        size_t left = n;
        for (size_t j = 0; j < d.ptrs.size(); ++j) {
            const size_t off =
                static_cast<const char*>(d.ptrs[j]) - buf_.data();
            const size_t take = std::min(d.lens[j], left);
            for (size_t k = 0; k < take; ++k) inside[off + k] = true;
            left -= take;
        }
        for (size_t k = 0; k < buf_.size(); ++k) {
            if (!inside[k] && buf_[k] != kGuardByte) return false;
        }
        return true;
    }
    int64_t Read(int api, const std::string& key, const Dest& d) {
        if (api == 0) {
            auto r = r_->batch_get_into({key}, {d.ptrs[0]}, {d.lens[0]});
            return r.size() == 1 ? r[0] : -9999;
        }
        auto r =
            r_->batch_get_into_multi_buffers({key}, {d.ptrs}, {d.lens}, false);
        return r.size() == 1 ? r[0] : -9999;
    }

    // Child body: 0, or the failing check.
    int HeldAfterRestoreChild(int api, bool upsert) {
        std::thread([] {
            std::this_thread::sleep_for(std::chrono::seconds(150));
            std::fprintf(stderr, "watchdog: held SSD read did not finish\n");
            std::_Exit(9);
        }).detach();
        std::string host_a, host_r;
        a_ = Make("hsr-owner", true, host_a);
        r_ = Make("hsr-reader", false, host_r);
        if (!a_ || !r_) return Fail(10, "client setup");
        buf_.assign(1 << 20, 0);
        if (r_->register_buffer(buf_.data(), buf_.size()) != 0) {
            return Fail(11, "register reader buffer");
        }
        const std::string key = "k", old = Payload(1, 1, kOld),
                          fresh = Payload(1, 2, kNew);
        ReplicateConfig rc;
        rc.replica_num = 1;
        rc.preferred_segment = host_a;
        if (a_->put(key, std::span<const char>(old.data(), old.size()), rc)) {
            return Fail(12, "put");
        }
        // Until a read is served by A's SSD copy alone: evict (the first
        // eviction may queue the offload instead), probe, repeat.
        std::mutex mu;
        std::vector<std::string> sites;
        r_->transfer_observer_for_test_ =
            [&](const RealClient::TransferObservation& o) {
                std::lock_guard<std::mutex> l(mu);
                sites.push_back(o.site + "|" + o.key);
            };
        bool ssd_only = false;
        for (int i = 0; i < 20 && !ssd_only; ++i) {
            SleepPast(kLeaseMs);
            Evict();
            {
                std::lock_guard<std::mutex> l(mu);
                sites.clear();
            }
            const Dest d = MakeDest(api, kOld);
            const int64_t r = Read(api, key, d);
            std::lock_guard<std::mutex> l(mu);
            ssd_only = r == static_cast<int64_t>(kOld) &&
                       Bytes(d, kOld) == old &&
                       std::count(sites.begin(), sites.end(),
                                  std::string("ssd_restore|k")) == 1;
        }
        r_->transfer_observer_for_test_ = nullptr;
        if (!ssd_only) return Fail(13, "no read served by the SSD copy alone");
        SleepPast(kLeaseMs);  // the probe's lease

        // The held read: parked at its restore response.
        std::mutex gm;
        std::condition_variable gcv;
        bool parked = false, released = false;
        int restores = 0;
        r_->transfer_observer_for_test_ =
            [&](const RealClient::TransferObservation& o) {
                if (o.site != "ssd_restore" || o.key != key) return;
                std::unique_lock<std::mutex> l(gm);
                if (++restores != 1) return;
                parked = true;
                gcv.notify_all();
                gcv.wait_for(l, std::chrono::seconds(30),
                             [&] { return released; });
            };
        const Dest held = MakeDest(api, kOld);
        int64_t held_result = 0;
        std::thread reader;
        const auto release_and_join = [&] {
            {
                std::lock_guard<std::mutex> l(gm);
                released = true;
            }
            gcv.notify_all();
            if (reader.joinable()) reader.join();
            r_->transfer_observer_for_test_ = nullptr;
        };
        struct Guard {
            std::function<void()> fn;
            ~Guard() {
                if (fn) fn();
            }
        } guard{release_and_join};
        reader = std::thread([&] { held_result = Read(api, key, held); });
        {
            std::unique_lock<std::mutex> l(gm);
            if (!gcv.wait_for(l, std::chrono::seconds(20),
                              [&] { return parked; })) {
                return Fail(14, "read never reached its restore response");
            }
        }
        SleepPast(kLeaseMs);  // the held read's master lease runs out
        const int m =
            upsert ? a_->upsert(
                         key, std::span<const char>(fresh.data(), fresh.size()),
                         rc)
                   : a_->remove(key);
        if (m != 0) {
            return Fail(15, std::string(upsert ? "upsert" : "remove") +
                                " after the lease gave " + std::to_string(m));
        }
        guard.fn();
        guard.fn = nullptr;
        if (held_result != static_cast<int64_t>(kOld) ||
            Bytes(held, kOld) != old || !OnlyInside(held, kOld)) {
            return Fail(16, "held read gave " + std::to_string(held_result) +
                                (Bytes(held, kOld) == old
                                     ? " with the old bytes"
                                     : " without the complete old bytes"));
        }
        // A fresh read sees the mutation.
        const Dest after = MakeDest(api, kNew);
        const int64_t r2 = Read(api, key, after);
        if (upsert ? (r2 != static_cast<int64_t>(kNew) ||
                      Bytes(after, kNew) != fresh || !OnlyInside(after, kNew))
                   : r2 != static_cast<int64_t>(ErrorCode::OBJECT_NOT_FOUND)) {
            return Fail(17, "fresh read after the " +
                                std::string(upsert ? "upsert" : "remove") +
                                " gave " + std::to_string(r2));
        }
        Trace("api=" + std::to_string(api) + " " +
              (upsert ? "upsert" : "remove") +
              " after the lease, read held after its restore: result " +
              std::to_string(held_result) +
              " with the complete old value; fresh read " + std::to_string(r2));
        r_->unregister_buffer(buf_.data());
        return 0;
    }

    // Same-size Upsert of an object whose only copy is A's SSD primary (no
    // read in progress, lease expired). The master takes the in-place path
    // (all complete replicas to PROCESSING, their descriptors returned);
    // there is no memory replica to write, so the client revokes and the
    // Upsert fails with NO_AVAILABLE_HANDLE, and the SSD replica stays
    // PROCESSING: reads give REPLICA_IS_NOT_READY. A second Upsert preempts
    // it and writes the new value. Required, on both trees: exactly that.
    int SameSizeUpsertChild(int api) {
        std::thread([] {
            std::this_thread::sleep_for(std::chrono::seconds(150));
            std::fprintf(stderr, "watchdog: same-size upsert did not finish\n");
            std::_Exit(9);
        }).detach();
        std::string host_a, host_r;
        a_ = Make("hsr-owner", true, host_a);
        r_ = Make("hsr-reader", false, host_r);
        if (!a_ || !r_) return Fail(10, "client setup");
        buf_.assign(1 << 20, 0);
        if (r_->register_buffer(buf_.data(), buf_.size()) != 0) {
            return Fail(11, "register reader buffer");
        }
        const std::string key = "k", old = Payload(1, 1, kOld),
                          fresh = Payload(1, 2, kOld);
        ReplicateConfig rc;
        rc.replica_num = 1;
        rc.preferred_segment = host_a;
        if (a_->put(key, std::span<const char>(old.data(), old.size()), rc)) {
            return Fail(12, "put");
        }
        std::mutex mu;
        std::vector<std::string> sites;
        r_->transfer_observer_for_test_ =
            [&](const RealClient::TransferObservation& o) {
                std::lock_guard<std::mutex> l(mu);
                sites.push_back(o.site + "|" + o.key);
            };
        bool ssd_only = false;
        for (int i = 0; i < 20 && !ssd_only; ++i) {
            SleepPast(kLeaseMs);
            Evict();
            {
                std::lock_guard<std::mutex> l(mu);
                sites.clear();
            }
            const Dest d = MakeDest(api, kOld);
            const int64_t r = Read(api, key, d);
            std::lock_guard<std::mutex> l(mu);
            ssd_only = r == static_cast<int64_t>(kOld) &&
                       Bytes(d, kOld) == old &&
                       std::count(sites.begin(), sites.end(),
                                  std::string("ssd_restore|k")) == 1;
        }
        r_->transfer_observer_for_test_ = nullptr;
        if (!ssd_only) return Fail(13, "no read served by the SSD copy alone");
        SleepPast(kLeaseMs);
        const int u = a_->upsert(
            key, std::span<const char>(fresh.data(), fresh.size()), rc);
        std::string seen;
        for (int i = 0; i < 3; ++i) {
            const Dest d = MakeDest(api, kOld);
            const int64_t r = Read(api, key, d);
            seen += " read" + std::to_string(i) + "=" + std::to_string(r) +
                    (r == static_cast<int64_t>(kOld)
                         ? (Bytes(d, kOld) == old     ? "(old)"
                            : Bytes(d, kOld) == fresh ? "(new)"
                                                      : "(other)")
                         : "");
            SleepPast(kLeaseMs);
        }
        const int u2 = a_->upsert(
            key, std::span<const char>(fresh.data(), fresh.size()), rc);
        const Dest d = MakeDest(api, kOld);
        const int64_t r3 = Read(api, key, d);
        const bool new_bytes =
            r3 == static_cast<int64_t>(kOld) && Bytes(d, kOld) == fresh;
        const std::string not_ready =
            std::to_string(static_cast<int>(ErrorCode::REPLICA_IS_NOT_READY));
        const std::string want_seen = " read0=" + not_ready +
                                      " read1=" + not_ready +
                                      " read2=" + not_ready;
        const std::string got = "upsert=" + std::to_string(u) + seen +
                                "; second upsert=" + std::to_string(u2) +
                                " read=" + std::to_string(r3) +
                                (new_bytes ? "(new)" : "(not new)");
        if (u != static_cast<int>(ErrorCode::NO_AVAILABLE_HANDLE) ||
            seen != want_seen || u2 != 0 || !new_bytes) {
            return Fail(18, got);
        }
        Trace("api=" + std::to_string(api) +
              " same-size upsert of an SSD-only object: " + got);
        r_->unregister_buffer(buf_.data());
        return 0;
    }

    fs::path dir_;
    mooncake::testing::InProcMaster master_;
    std::shared_ptr<RealClient> a_, r_;
    std::vector<char> buf_;
};

TEST_F(HeldSsdRestoreTest, BatchGetIntoRemove) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeldAfterRestoreChild(0, false)),
                ::testing::ExitedWithCode(0), "");
}

TEST_F(HeldSsdRestoreTest, BatchGetIntoUpsert) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeldAfterRestoreChild(0, true)),
                ::testing::ExitedWithCode(0), "");
}

TEST_F(HeldSsdRestoreTest, MultiBuffersRemove) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeldAfterRestoreChild(1, false)),
                ::testing::ExitedWithCode(0), "");
}

TEST_F(HeldSsdRestoreTest, MultiBuffersUpsert) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeldAfterRestoreChild(1, true)),
                ::testing::ExitedWithCode(0), "");
}

TEST_F(HeldSsdRestoreTest, SameSizeUpsertOfSsdOnlyObject) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(SameSizeUpsertChild(0)),
                ::testing::ExitedWithCode(0), "");
}

}  // namespace
}  // namespace mooncake
