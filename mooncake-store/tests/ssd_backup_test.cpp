// Tests for the SSD backup feature (master side): wire compatibility of the
// backup flag, coordinator placement, and the master's backup replica
// lifecycle (register, read fallback, residency, refresh, cleanup).

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <malloc.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <future>
#include <memory>
#include <set>
#include <map>
#include <thread>
#include <string>
#include <vector>

#include <filesystem>

#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "common/network.h"
#include "local_ssd/backup_coordinator.h"
#include "master_client.h"
#include "master_config.h"
#include "master_service.h"
#include "rpc_service.h"
#include "tenant/quota_policy_store.h"
#include "master_service/master_service_test_peer.h"
#include "read_fallback_policy.h"
#include "real_client.h"
#include "transfer_task.h"
#include "replica.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake::test {

// LocalDiskDescriptor as it was before the backup flag.
struct OldLocalDiskDescriptor {
    UUID client_id;
    uint64_t object_size = 0;
    std::string transport_endpoint;
    YLT_REFL(OldLocalDiskDescriptor, client_id, object_size,
             transport_endpoint);
};

TEST(SsdBackupWireTest, TypeCodeUnchanged) {
    EXPECT_EQ(struct_pack::get_type_code<LocalDiskDescriptor>(),
              struct_pack::get_type_code<OldLocalDiskDescriptor>());
}

TEST(SsdBackupWireTest, OldAndNewInterop) {
    const UUID id{7, 9};
    OldLocalDiskDescriptor old_desc{id, 123, "host:1"};
    auto old_bytes = struct_pack::serialize<std::string>(old_desc);
    auto as_new = struct_pack::deserialize<LocalDiskDescriptor>(old_bytes);
    ASSERT_TRUE(as_new.has_value());
    EXPECT_EQ(as_new->client_id, id);
    EXPECT_EQ(as_new->object_size, 123u);
    EXPECT_FALSE(as_new->backup.has_value());

    LocalDiskDescriptor new_desc;
    new_desc.client_id = id;
    new_desc.object_size = 456;
    new_desc.transport_endpoint = "host:2";
    new_desc.backup = true;
    auto new_bytes = struct_pack::serialize<std::string>(new_desc);
    auto as_old = struct_pack::deserialize<OldLocalDiskDescriptor>(new_bytes);
    ASSERT_TRUE(as_old.has_value());
    EXPECT_EQ(as_old->object_size, 456u);
    EXPECT_EQ(as_old->transport_endpoint, "host:2");
    auto round = struct_pack::deserialize<LocalDiskDescriptor>(new_bytes);
    ASSERT_TRUE(round.has_value());
    ASSERT_TRUE(round->backup.has_value());
    EXPECT_TRUE(round->backup.value());
}

// RPC-01: the complete messages as a pre-backup peer defines them: the
// replica descriptor variant with the old LocalDiskDescriptor, and the
// replica-list response that carries a vector of them (b5d79e69 layout).
struct OldDescriptor {
    ReplicaID id{0};
    std::variant<MemoryDescriptor, NoFDescriptor, DiskDescriptor,
                 OldLocalDiskDescriptor, DistributedFSDescriptor>
        descriptor_variant;
    ReplicaStatus status;
    YLT_REFL(OldDescriptor, id, descriptor_variant, status);
};
struct OldGetReplicaListResponse {
    std::vector<OldDescriptor> replicas;
    uint64_t lease_ttl_ms = 0;
    std::optional<uint64_t> object_checksum;
    struct_pack::compatible<std::string, 1> resolved_tenant_id;
    YLT_REFL(OldGetReplicaListResponse, replicas, lease_ttl_ms, object_checksum,
             resolved_tenant_id);
};

GetReplicaListResponse NewResponseWithBackup() {
    Replica::Descriptor mem;
    mem.id = 11;
    mem.descriptor_variant = MemoryDescriptor{};
    mem.status = ReplicaStatus::COMPLETE;
    LocalDiskDescriptor ld;
    ld.client_id = UUID{3, 4};
    ld.object_size = 4096;
    ld.transport_endpoint = "holder:7";
    ld.backup = true;
    Replica::Descriptor disk;
    disk.id = 12;
    disk.descriptor_variant = ld;
    disk.status = ReplicaStatus::COMPLETE;
    std::vector<Replica::Descriptor> list{mem, disk};
    GetReplicaListResponse r(std::move(list), 5000, uint64_t{77});
    r.resolved_tenant_id = std::string("tenant-a");
    return r;
}

TEST(SsdBackupWireTest, CompleteEnvelopeTypeCodesUnchanged) {
    EXPECT_EQ(struct_pack::get_type_code<Replica::Descriptor>(),
              struct_pack::get_type_code<OldDescriptor>());
    EXPECT_EQ(struct_pack::get_type_code<GetReplicaListResponse>(),
              struct_pack::get_type_code<OldGetReplicaListResponse>());
    EXPECT_EQ(
        struct_pack::get_type_code<std::vector<GetReplicaListResponse>>(),
        struct_pack::get_type_code<std::vector<OldGetReplicaListResponse>>());
}

// New master -> old reader, and old master -> new reader, for the complete
// replica-list response and for a vector of them (batch queries).
TEST(SsdBackupWireTest, CompleteEnvelopeOldAndNewInterop) {
    const auto fresh = NewResponseWithBackup();
    const auto bytes = struct_pack::serialize<std::string>(fresh);
    auto as_old = struct_pack::deserialize<OldGetReplicaListResponse>(bytes);
    ASSERT_TRUE(as_old.has_value());
    ASSERT_EQ(as_old->replicas.size(), 2u);
    EXPECT_EQ(as_old->replicas[0].id, 11u);
    EXPECT_TRUE(std::holds_alternative<MemoryDescriptor>(
        as_old->replicas[0].descriptor_variant));
    ASSERT_TRUE(std::holds_alternative<OldLocalDiskDescriptor>(
        as_old->replicas[1].descriptor_variant));
    const auto& old_ld = std::get<OldLocalDiskDescriptor>(
        as_old->replicas[1].descriptor_variant);
    EXPECT_EQ(old_ld.client_id, (UUID{3, 4}));
    EXPECT_EQ(old_ld.object_size, 4096u);
    EXPECT_EQ(old_ld.transport_endpoint, "holder:7");
    EXPECT_EQ(as_old->replicas[1].status, ReplicaStatus::COMPLETE);
    EXPECT_EQ(as_old->lease_ttl_ms, 5000u);
    ASSERT_TRUE(as_old->object_checksum.has_value());
    EXPECT_EQ(*as_old->object_checksum, 77u);
    ASSERT_TRUE(as_old->resolved_tenant_id.has_value());
    EXPECT_EQ(as_old->resolved_tenant_id.value(), "tenant-a");

    // Old -> new: the backup flag is absent, everything else survives.
    const auto old_bytes = struct_pack::serialize<std::string>(*as_old);
    auto as_new = struct_pack::deserialize<GetReplicaListResponse>(old_bytes);
    ASSERT_TRUE(as_new.has_value());
    ASSERT_EQ(as_new->replicas.size(), 2u);
    ASSERT_TRUE(as_new->replicas[1].is_local_disk_replica());
    const auto& ld = as_new->replicas[1].get_local_disk_descriptor();
    EXPECT_EQ(ld.object_size, 4096u);
    EXPECT_EQ(ld.transport_endpoint, "holder:7");
    EXPECT_FALSE(ld.backup.has_value());
    EXPECT_EQ(as_new->lease_ttl_ms, 5000u);

    // An old master never sets a backup flag and does set the tenant: a new
    // reader must keep the tenant and see no flag on the primary.
    OldGetReplicaListResponse from_old_master;
    OldDescriptor primary_disk;
    primary_disk.id = 21;
    primary_disk.descriptor_variant =
        OldLocalDiskDescriptor{UUID{5, 6}, 8192, "owner:9"};
    primary_disk.status = ReplicaStatus::COMPLETE;
    from_old_master.replicas.push_back(primary_disk);
    from_old_master.lease_ttl_ms = 5000;
    from_old_master.resolved_tenant_id = std::string("default");
    auto read_new = struct_pack::deserialize<GetReplicaListResponse>(
        struct_pack::serialize<std::string>(from_old_master));
    ASSERT_TRUE(read_new.has_value());
    ASSERT_TRUE(read_new->resolved_tenant_id.has_value());
    EXPECT_EQ(read_new->resolved_tenant_id.value(), "default");
    EXPECT_FALSE(
        read_new->replicas[0].get_local_disk_descriptor().backup.has_value());

    // New -> new keeps the flag and the tenant.
    auto round = struct_pack::deserialize<GetReplicaListResponse>(bytes);
    ASSERT_TRUE(round.has_value());
    ASSERT_TRUE(
        round->replicas[1].get_local_disk_descriptor().backup.has_value());
    EXPECT_TRUE(round->replicas[1].get_local_disk_descriptor().backup.value());
    ASSERT_TRUE(round->resolved_tenant_id.has_value());
    EXPECT_EQ(round->resolved_tenant_id.value(), "tenant-a");

    // Vector envelopes, both directions.
    std::vector<GetReplicaListResponse> many{fresh, NewResponseWithBackup()};
    const auto vbytes = struct_pack::serialize<std::string>(many);
    auto vold =
        struct_pack::deserialize<std::vector<OldGetReplicaListResponse>>(
            vbytes);
    ASSERT_TRUE(vold.has_value());
    ASSERT_EQ(vold->size(), 2u);
    EXPECT_EQ((*vold)[1].replicas.size(), 2u);
    const auto vold_bytes = struct_pack::serialize<std::string>(*vold);
    auto vnew = struct_pack::deserialize<std::vector<GetReplicaListResponse>>(
        vold_bytes);
    ASSERT_TRUE(vnew.has_value());
    ASSERT_EQ(vnew->size(), 2u);
    EXPECT_FALSE(
        (*vnew)[0].replicas[1].get_local_disk_descriptor().backup.has_value());
}

// RPC-01 (complete): the actual RPC return types, tl::expected of the
// replica-list response and a vector of them (BatchGetReplicaList,
// BatchGetBackupReplicaList), and the serialized cached query result, with
// elements that differ in every field: success and error, two tenants and
// none, IDs, endpoints, sizes, statuses, checksum present/absent, memory and
// local-disk replicas in both orders, MemoryDescriptor::locality present and
// absent, and the backup flag true, false and absent.
struct OldCachedQueryResultResponse {
    bool success = false;
    OldGetReplicaListResponse value;
    ErrorCode error = ErrorCode::INVALID_PARAMS;
    YLT_REFL(OldCachedQueryResultResponse, success, value, error);
};

namespace wire {
Replica::Descriptor Mem(ReplicaID id, uint64_t size, uintptr_t addr,
                        const std::string& proto, const std::string& ep,
                        std::optional<SegmentLocality> loc, ReplicaStatus st) {
    MemoryDescriptor m;
    m.buffer_descriptor.size_ = size;
    m.buffer_descriptor.buffer_address_ = addr;
    m.buffer_descriptor.protocol_ = proto;
    m.buffer_descriptor.transport_endpoint_ = ep;
    if (loc) m.locality = *loc;
    Replica::Descriptor d;
    d.id = id;
    d.descriptor_variant = m;
    d.status = st;
    return d;
}
Replica::Descriptor Disk(ReplicaID id, UUID client, uint64_t size,
                         const std::string& ep, std::optional<bool> backup,
                         ReplicaStatus st) {
    LocalDiskDescriptor l;
    l.client_id = client;
    l.object_size = size;
    l.transport_endpoint = ep;
    if (backup) l.backup = *backup;
    Replica::Descriptor d;
    d.id = id;
    d.descriptor_variant = l;
    d.status = st;
    return d;
}
std::vector<GetReplicaListResponse> Responses() {
    SegmentLocality loc{1, {"mlx5_0", "mlx5_1"}};
    std::vector<GetReplicaListResponse> out;
    {
        std::vector<Replica::Descriptor> r{
            Disk(21, UUID{1, 2}, 100, "h1:1", true, ReplicaStatus::COMPLETE),
            Mem(22, 100, 0x1000, "tcp", "m1:1", loc, ReplicaStatus::COMPLETE)};
        GetReplicaListResponse e(std::move(r), 1000, uint64_t{5});
        e.resolved_tenant_id = std::string("tenant-a");
        out.push_back(std::move(e));
    }
    {
        std::vector<Replica::Descriptor> r{
            Mem(31, 200, 0x2000, "rdma", "m2:2", std::nullopt,
                ReplicaStatus::PROCESSING),
            Disk(32, UUID{3, 4}, 200, "h2:2", std::nullopt,
                 ReplicaStatus::COMPLETE)};
        GetReplicaListResponse e(std::move(r), 2000, std::nullopt);
        e.resolved_tenant_id = std::string("default");
        out.push_back(std::move(e));
    }
    {
        std::vector<Replica::Descriptor> r{
            Disk(41, UUID{5, 6}, 300, "h3:3", false, ReplicaStatus::COMPLETE)};
        out.emplace_back(std::move(r), 0, uint64_t{0});  // no tenant
    }
    return out;
}
OldGetReplicaListResponse ToOld(const GetReplicaListResponse& n) {
    OldGetReplicaListResponse o;
    for (const auto& d : n.replicas) {
        OldDescriptor od;
        od.id = d.id;
        od.status = d.status;
        if (d.is_local_disk_replica()) {
            const auto& l = d.get_local_disk_descriptor();
            od.descriptor_variant = OldLocalDiskDescriptor{
                l.client_id, l.object_size, l.transport_endpoint};
        } else {
            od.descriptor_variant = d.get_memory_descriptor();
        }
        o.replicas.push_back(std::move(od));
    }
    o.lease_ttl_ms = n.lease_ttl_ms;
    o.object_checksum = n.object_checksum;
    if (n.resolved_tenant_id.has_value()) {
        o.resolved_tenant_id = n.resolved_tenant_id.value();
    }
    return o;
}
void ExpectMemEq(const MemoryDescriptor& a, const MemoryDescriptor& b) {
    EXPECT_EQ(a.buffer_descriptor.size_, b.buffer_descriptor.size_);
    EXPECT_EQ(a.buffer_descriptor.buffer_address_,
              b.buffer_descriptor.buffer_address_);
    EXPECT_EQ(a.buffer_descriptor.protocol_, b.buffer_descriptor.protocol_);
    EXPECT_EQ(a.buffer_descriptor.transport_endpoint_,
              b.buffer_descriptor.transport_endpoint_);
    ASSERT_EQ(a.locality.has_value(), b.locality.has_value());
    if (a.locality.has_value()) {
        EXPECT_TRUE(a.locality.value() == b.locality.value());
    }
}
// Old view of a response against the new one it came from (or went to).
void ExpectOldEq(const OldGetReplicaListResponse& o,
                 const GetReplicaListResponse& n) {
    ASSERT_EQ(o.replicas.size(), n.replicas.size());
    for (size_t i = 0; i < n.replicas.size(); ++i) {
        SCOPED_TRACE(i);
        EXPECT_EQ(o.replicas[i].id, n.replicas[i].id);
        EXPECT_EQ(o.replicas[i].status, n.replicas[i].status);
        if (n.replicas[i].is_local_disk_replica()) {
            ASSERT_TRUE(std::holds_alternative<OldLocalDiskDescriptor>(
                o.replicas[i].descriptor_variant));
            const auto& ol = std::get<OldLocalDiskDescriptor>(
                o.replicas[i].descriptor_variant);
            const auto& nl = n.replicas[i].get_local_disk_descriptor();
            EXPECT_EQ(ol.client_id, nl.client_id);
            EXPECT_EQ(ol.object_size, nl.object_size);
            EXPECT_EQ(ol.transport_endpoint, nl.transport_endpoint);
        } else {
            ASSERT_TRUE(std::holds_alternative<MemoryDescriptor>(
                o.replicas[i].descriptor_variant));
            ExpectMemEq(
                std::get<MemoryDescriptor>(o.replicas[i].descriptor_variant),
                n.replicas[i].get_memory_descriptor());
        }
    }
    EXPECT_EQ(o.lease_ttl_ms, n.lease_ttl_ms);
    EXPECT_EQ(o.object_checksum, n.object_checksum);
    ASSERT_EQ(o.resolved_tenant_id.has_value(),
              n.resolved_tenant_id.has_value());
    if (n.resolved_tenant_id.has_value()) {
        EXPECT_EQ(o.resolved_tenant_id.value(), n.resolved_tenant_id.value());
    }
}
// New against new. `from_old`: the bytes came from an old peer, so every
// backup flag must be absent instead of equal.
void ExpectNewEq(const GetReplicaListResponse& got,
                 const GetReplicaListResponse& want, bool from_old) {
    ExpectOldEq(ToOld(got), want);
    for (size_t i = 0; i < want.replicas.size(); ++i) {
        if (!want.replicas[i].is_local_disk_replica()) continue;
        const auto& g = got.replicas[i].get_local_disk_descriptor().backup;
        const auto& w = want.replicas[i].get_local_disk_descriptor().backup;
        if (from_old) {
            EXPECT_FALSE(g.has_value()) << "replica " << i;
            continue;
        }
        ASSERT_EQ(g.has_value(), w.has_value()) << "replica " << i;
        if (w.has_value()) EXPECT_EQ(g.value(), w.value()) << "replica " << i;
    }
}
}  // namespace wire

TEST(SsdBackupWireTest, RpcEnvelopesAllFieldsBothWays) {
    using NewE = tl::expected<GetReplicaListResponse, ErrorCode>;
    using OldE = tl::expected<OldGetReplicaListResponse, ErrorCode>;
    const auto resp = wire::Responses();
    // Batch RPC result: successes and an error, in one vector.
    std::vector<NewE> batch{resp[0],
                            tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND),
                            resp[1], resp[2]};
    std::vector<OldE> old_batch;
    for (const auto& e : batch) {
        old_batch.push_back(e ? OldE(wire::ToOld(*e))
                              : OldE(tl::make_unexpected(e.error())));
    }
    const auto check_batch = [&](const std::vector<NewE>& got, bool from_old) {
        ASSERT_EQ(got.size(), batch.size());
        for (size_t i = 0; i < batch.size(); ++i) {
            SCOPED_TRACE(i);
            ASSERT_EQ(got[i].has_value(), batch[i].has_value());
            if (!batch[i]) {
                EXPECT_EQ(got[i].error(), batch[i].error());
                continue;
            }
            wire::ExpectNewEq(*got[i], *batch[i], from_old);
        }
    };
    {  // new -> old
        auto got = struct_pack::deserialize<std::vector<OldE>>(
            struct_pack::serialize<std::string>(batch));
        ASSERT_TRUE(got.has_value());
        ASSERT_EQ(got->size(), batch.size());
        for (size_t i = 0; i < batch.size(); ++i) {
            SCOPED_TRACE(i);
            ASSERT_EQ((*got)[i].has_value(), batch[i].has_value());
            if (batch[i]) {
                wire::ExpectOldEq(*(*got)[i], *batch[i]);
            } else {
                EXPECT_EQ((*got)[i].error(), batch[i].error());
            }
        }
    }
    {  // old -> new
        auto got = struct_pack::deserialize<std::vector<NewE>>(
            struct_pack::serialize<std::string>(old_batch));
        ASSERT_TRUE(got.has_value());
        check_batch(*got, /*from_old=*/true);
    }
    {  // new -> new
        auto got = struct_pack::deserialize<std::vector<NewE>>(
            struct_pack::serialize<std::string>(batch));
        ASSERT_TRUE(got.has_value());
        check_batch(*got, /*from_old=*/false);
    }
    // Single RPC result, success and error, both ways.
    for (const NewE& one :
         {NewE(resp[0]),
          NewE(tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY))}) {
        SCOPED_TRACE(one.has_value() ? "success" : "error");
        auto as_old = struct_pack::deserialize<OldE>(
            struct_pack::serialize<std::string>(one));
        ASSERT_TRUE(as_old.has_value());
        ASSERT_EQ(as_old->has_value(), one.has_value());
        const OldE old_one = one ? OldE(wire::ToOld(*one))
                                 : OldE(tl::make_unexpected(one.error()));
        auto as_new = struct_pack::deserialize<NewE>(
            struct_pack::serialize<std::string>(old_one));
        ASSERT_TRUE(as_new.has_value());
        ASSERT_EQ(as_new->has_value(), one.has_value());
        auto as_same = struct_pack::deserialize<NewE>(
            struct_pack::serialize<std::string>(one));
        ASSERT_TRUE(as_same.has_value());
        ASSERT_EQ(as_same->has_value(), one.has_value());
        if (one) {
            wire::ExpectOldEq(**as_old, *one);
            wire::ExpectNewEq(**as_new, *one, /*from_old=*/true);
            wire::ExpectNewEq(**as_same, *one, /*from_old=*/false);
        } else {
            EXPECT_EQ(as_old->error(), one.error());
            EXPECT_EQ(as_new->error(), one.error());
            EXPECT_EQ(as_same->error(), one.error());
        }
    }
    // Cached query result (pyclient), success and error, both ways.
    for (int k = 0; k < 3; ++k) {
        // 0: success without a backup flag, 1: error, 2: success whose
        // local-disk replica carries backup = true.
        SCOPED_TRACE(k == 0 ? "cached success"
                            : (k == 1 ? "cached error" : "cached backup"));
        CachedQueryResultResponse cached =
            k == 0 ? CachedQueryResultResponse(GetReplicaListResponse(resp[1]))
            : k == 1
                ? CachedQueryResultResponse(ErrorCode::OBJECT_NOT_FOUND)
                : CachedQueryResultResponse(GetReplicaListResponse(resp[0]));
        auto as_old = struct_pack::deserialize<OldCachedQueryResultResponse>(
            struct_pack::serialize<std::string>(cached));
        ASSERT_TRUE(as_old.has_value());
        EXPECT_EQ(as_old->success, cached.success);
        EXPECT_EQ(as_old->error, cached.error);
        wire::ExpectOldEq(as_old->value, cached.value);
        OldCachedQueryResultResponse old_cached;
        old_cached.success = cached.success;
        old_cached.value = wire::ToOld(cached.value);
        old_cached.error = cached.error;
        auto as_new = struct_pack::deserialize<CachedQueryResultResponse>(
            struct_pack::serialize<std::string>(old_cached));
        ASSERT_TRUE(as_new.has_value());
        EXPECT_EQ(as_new->success, cached.success);
        EXPECT_EQ(as_new->error, cached.error);
        wire::ExpectNewEq(as_new->value, cached.value, /*from_old=*/true);
        auto round = struct_pack::deserialize<CachedQueryResultResponse>(
            struct_pack::serialize<std::string>(cached));
        ASSERT_TRUE(round.has_value());
        EXPECT_EQ(round->success, cached.success);
        EXPECT_EQ(round->error, cached.error);
        wire::ExpectNewEq(round->value, cached.value, /*from_old=*/false);
        if (k == 2) {
            const auto& disk = round->value.replicas.at(0);
            ASSERT_TRUE(disk.is_local_disk_replica());
            ASSERT_TRUE(disk.get_local_disk_descriptor().backup.has_value());
            EXPECT_TRUE(disk.get_local_disk_descriptor().backup.value());
        }
    }
    EXPECT_EQ(struct_pack::get_type_code<CachedQueryResultResponse>(),
              struct_pack::get_type_code<OldCachedQueryResultResponse>());
    EXPECT_EQ(struct_pack::get_type_code<std::vector<NewE>>(),
              struct_pack::get_type_code<std::vector<OldE>>());
}

struct OldLocalDiskReplicaData {
    UUID client_id;
    uint64_t object_size = 0;
    std::string transport_endpoint;
    std::shared_ptr<ClientLivenessRecord> client_liveness;
};

// Per-backup master memory is one extra Replica in the object's replica
// vector; the flag itself must not grow Replica.
TEST(SsdBackupWireTest, ReplicaSizeReport) {
    printf("SIZE Replica=%zu LocalDiskReplicaData=%zu old=%zu\n",
           sizeof(Replica), sizeof(LocalDiskReplicaData),
           sizeof(OldLocalDiskReplicaData));
    EXPECT_EQ(sizeof(LocalDiskReplicaData), sizeof(OldLocalDiskReplicaData));
}

// ---- Read fallback retry policy (F1, F4) ----

TEST(SsdBackupReadFallbackPolicy, NoRetryWhileOldTransferMayBeInFlight) {
    using E = ErrorCode;
    std::vector<std::optional<E>> err{E::TRANSFER_FAIL, E::RPC_TIMEOUT,
                                      E::RPC_FAIL, std::nullopt,
                                      E::OBJECT_NOT_FOUND};
    std::vector<bool> via(5, false), prim(5, false);
    // Fast terminal failure: retry from backup. RPC_TIMEOUT too: it comes
    // from the SSD restore request, before the reader starts any transfer.
    auto p = PlanReadFallback(err, via, prim, /*may_be_in_flight=*/false);
    EXPECT_EQ(p.to_backup, (std::vector<size_t>{0, 1, 2}));
    EXPECT_EQ(p.mark_suspect, (std::vector<size_t>{0, 1, 2}));
    // Transfer wait hit its timeout: the old DMA may still be running into
    // the same buffer, so no TRANSFER_FAIL retry (suspect marking still
    // happens); the RPC errors are unaffected.
    p = PlanReadFallback(err, via, prim, /*may_be_in_flight=*/true);
    EXPECT_EQ(p.to_backup, (std::vector<size_t>{1, 2}));
    EXPECT_EQ(p.mark_suspect, (std::vector<size_t>{0, 1, 2}));
    EXPECT_TRUE(p.requery.empty());
}

TEST(SsdBackupReadFallbackPolicy, FailedBackupFallsBackToPrimary) {
    using E = ErrorCode;
    // Keys 0..4 were sent to their backup because the primary was suspect.
    std::vector<std::optional<E>> err{
        E::RPC_FAIL,    E::RPC_FAIL, E::FILE_READ_FAIL, E::BUCKET_NOT_FOUND,
        E::RPC_TIMEOUT, E::RPC_FAIL, E::FILE_OPEN_FAIL, E::INVALID_KEY};
    std::vector<bool> via{true, true, true, true, true, false, true, true};
    std::vector<bool> prim{true, false, true, true, true, false, true, true};
    auto p = PlanReadFallback(err, via, prim, false);
    // R2: storage and RPC errors of the backup also go back to the primary
    // (1 has no primary).
    EXPECT_EQ(p.requery, (std::vector<size_t>{0, 2, 3, 4, 6, 7}));
    EXPECT_EQ(p.to_backup, (std::vector<size_t>{5}));
    EXPECT_EQ(p.mark_suspect, (std::vector<size_t>{5}));  // not the backups
}

// G1: a read that completed after its lease ran out is re-queried and read
// once more, whether it came from a primary or a backup.
TEST(SsdBackupReadFallbackPolicy, LeaseExpiredIsReRead) {
    using E = ErrorCode;
    // OBJECT_HAS_LEASE: an SSD (offload) read finished after the holder's
    // buffer lease ran out.
    std::vector<std::optional<E>> err{E::LEASE_EXPIRED, std::nullopt,
                                      E::LEASE_EXPIRED, E::OBJECT_HAS_LEASE,
                                      E::OBJECT_HAS_LEASE};
    std::vector<bool> via{false, false, true, true, false};
    std::vector<bool> prim{false, false, true, true, false};
    auto p = PlanReadFallback(err, via, prim, true);
    EXPECT_EQ(p.requery, (std::vector<size_t>{0, 2, 3, 4}));
    EXPECT_TRUE(p.to_backup.empty());
    EXPECT_TRUE(p.mark_suspect.empty());
}

// R1: the "may still be using the buffers" marker is per thread, so a read
// only sees failures of its own transfers.
TEST(SsdBackupReadFallbackPolicy, UnquiescedMarkerIsPerThread) {
    const uint64_t before = UnquiescedTransferFailureCount();
    std::thread other([] { NoteUnquiescedTransferFailure(); });
    other.join();
    EXPECT_EQ(UnquiescedTransferFailureCount(), before);
    NoteUnquiescedTransferFailure();
    EXPECT_EQ(UnquiescedTransferFailureCount(), before + 1);
}

// ---- Read fallback orchestration with fakes (T3, T4, T5) ----

namespace {

// A fake per-key query: which endpoint it points at and which address
// (generation of the answer) it carries.
struct FakeQ {
    std::string endpoint;
    std::string addr;
};

struct FakeWorld {
    using QV = std::vector<tl::expected<FakeQ, ErrorCode>>;
    std::vector<std::string> keys;
    std::set<std::string> suspect;
    std::vector<std::string> marked;
    // Per address: result of reading it (bytes on success).
    std::map<std::string, tl::expected<int64_t, ErrorCode>> read_result;
    std::map<std::string, std::string> backup_addr;  // key -> addr ("" none)
    std::map<std::string, std::string> fresh_addr;   // key -> addr
    std::vector<std::vector<std::string>> reads;     // addrs per read call
    std::vector<std::string> re_queried, backup_queried;
    uint64_t unquiesced = 0;
    bool bump_unquiesced_on_read = false;

    QV QueryFor(const std::vector<std::string>& ks,
                const std::map<std::string, std::string>& m,
                const std::string& ep) {
        QV out;
        for (const auto& k : ks) {
            auto it = m.find(k);
            if (it == m.end() || it->second.empty()) {
                out.emplace_back(
                    tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY));
            } else {
                out.emplace_back(FakeQ{ep, it->second});
            }
        }
        return out;
    }

    ReadFallbackHooks<FakeQ> Hooks() {
        ReadFallbackHooks<FakeQ> h;
        h.endpoint_of = [](const FakeQ& q) { return q.endpoint; };
        h.is_suspect = [this](const std::string& e) {
            return suspect.count(e) > 0;
        };
        h.mark_suspect = [this](const std::string& e) { marked.push_back(e); };
        h.query_backup = [this](const std::vector<std::string>& ks) {
            backup_queried.insert(backup_queried.end(), ks.begin(), ks.end());
            return QueryFor(ks, backup_addr, "B");
        };
        h.requery = [this](const std::vector<std::string>& ks) {
            re_queried.insert(re_queried.end(), ks.begin(), ks.end());
            return QueryFor(ks, fresh_addr, "A");
        };
        h.read = [this](const std::vector<size_t>& idx, const QV& qs) {
            std::vector<tl::expected<int64_t, ErrorCode>> out;
            std::vector<std::string> addrs;
            for (size_t j = 0; j < idx.size(); ++j) {
                const auto& q = qs[j];
                if (!q) {
                    out.emplace_back(tl::make_unexpected(q.error()));
                    addrs.push_back("-");
                    continue;
                }
                addrs.push_back(q->addr);
                auto it = read_result.find(q->addr);
                out.push_back(it == read_result.end()
                                  ? tl::expected<int64_t, ErrorCode>(1)
                                  : it->second);
            }
            if (bump_unquiesced_on_read && reads.empty()) ++unquiesced;
            reads.push_back(addrs);
            return out;
        };
        h.unquiesced_count = [this] { return unquiesced; };
        return h;
    }
};

tl::expected<int64_t, ErrorCode> Fail(ErrorCode e) {
    return tl::make_unexpected(e);
}

}  // namespace

// T3: A was marked suspect but has recovered; the backup on B cannot serve
// the key from its SSD. Every storage error re-queries and reads the healthy
// primary, as does an RPC timeout of B's restore request (nothing was
// written to the reader yet); no primary means no retry; never more than one
// extra read.
TEST(SsdBackupReadFallback, BackupStorageErrorsRetryFreshPrimary) {
    const std::vector<ErrorCode> storage{
        ErrorCode::INVALID_KEY,      ErrorCode::OBJECT_NOT_FOUND,
        ErrorCode::BUCKET_NOT_FOUND, ErrorCode::FILE_READ_FAIL,
        ErrorCode::FILE_OPEN_FAIL,   ErrorCode::RPC_FAIL,
        ErrorCode::RPC_TIMEOUT,      ErrorCode::OBJECT_HAS_LEASE};
    for (ErrorCode e : storage) {
        FakeWorld w;
        w.suspect = {"A"};
        w.backup_addr = {{"k", "b1"}};
        w.fresh_addr = {{"k", "a2"}};
        w.read_result = {
            {"b1", Fail(e)}, {"a1", Fail(ErrorCode::TRANSFER_FAIL)}, {"a2", 7}};
        std::vector<tl::expected<FakeQ, ErrorCode>> q;
        q.emplace_back(FakeQ{"A", "a1"});
        auto r =
            RunReadWithBackupFallback<FakeQ>({"k"}, std::move(q), w.Hooks());
        ASSERT_TRUE(r[0].has_value()) << static_cast<int>(e);
        EXPECT_EQ(r[0].value(), 7);
        ASSERT_EQ(w.reads.size(), 2u) << static_cast<int>(e);
        EXPECT_EQ(w.reads[0], std::vector<std::string>{"b1"});
        EXPECT_EQ(w.reads[1], std::vector<std::string>{"a2"});  // fresh, not a1
        EXPECT_EQ(w.re_queried, std::vector<std::string>{"k"});
        EXPECT_TRUE(w.marked.empty());  // a backup failure blames nobody
    }
    {  // Fresh query finds no primary: one failed retry, no loop.
        FakeWorld w;
        w.suspect = {"A"};
        w.backup_addr = {{"k", "b1"}};
        w.read_result = {{"b1", Fail(ErrorCode::FILE_OPEN_FAIL)}};
        std::vector<tl::expected<FakeQ, ErrorCode>> q;
        q.emplace_back(FakeQ{"A", "a1"});
        auto r =
            RunReadWithBackupFallback<FakeQ>({"k"}, std::move(q), w.Hooks());
        EXPECT_FALSE(r[0].has_value());
        EXPECT_EQ(w.reads.size(), 1u);
        EXPECT_EQ(w.re_queried, std::vector<std::string>{"k"});
    }
}

// T4: a transfer of this read failed without being shown to have stopped.
// Its TRANSFER_FAIL is returned as is: no backup read into the same buffer,
// no suspect marking. A normal terminal failure still falls back.
TEST(SsdBackupReadFallback, UnquiescedFailureIsNotRetried) {
    {
        FakeWorld w;
        w.bump_unquiesced_on_read = true;  // this read hit an unknown stop
        w.backup_addr = {{"k", "b1"}};
        w.read_result = {{"a1", Fail(ErrorCode::TRANSFER_FAIL)}};
        std::vector<tl::expected<FakeQ, ErrorCode>> q;
        q.emplace_back(FakeQ{"A", "a1"});
        auto r =
            RunReadWithBackupFallback<FakeQ>({"k"}, std::move(q), w.Hooks());
        EXPECT_FALSE(r[0].has_value());
        EXPECT_EQ(w.reads.size(), 1u);
        EXPECT_TRUE(w.backup_queried.empty());
        EXPECT_TRUE(w.marked.empty());
    }
    {  // Earlier unquiesced failures (counter already non-zero) do not matter.
        FakeWorld w;
        w.unquiesced = 5;
        w.backup_addr = {{"k", "b1"}};
        w.read_result = {{"a1", Fail(ErrorCode::TRANSFER_FAIL)}, {"b1", 3}};
        std::vector<tl::expected<FakeQ, ErrorCode>> q;
        q.emplace_back(FakeQ{"A", "a1"});
        auto r =
            RunReadWithBackupFallback<FakeQ>({"k"}, std::move(q), w.Hooks());
        ASSERT_TRUE(r[0].has_value());
        EXPECT_EQ(r[0].value(), 3);
        EXPECT_EQ(w.marked, std::vector<std::string>{"A"});
    }
    {  // Elapsed-time backstop.
        FakeWorld w;
        w.backup_addr = {{"k", "b1"}};
        w.read_result = {{"a1", Fail(ErrorCode::TRANSFER_FAIL)}, {"b1", 3}};
        std::vector<tl::expected<FakeQ, ErrorCode>> q;
        q.emplace_back(FakeQ{"A", "a1"});
        auto h = w.Hooks();
        h.unterminated_after = std::chrono::nanoseconds(0);
        auto r = RunReadWithBackupFallback<FakeQ>({"k"}, std::move(q), h);
        EXPECT_FALSE(r[0].has_value());
        EXPECT_EQ(w.reads.size(), 1u);
    }
}

// T5: mixed batch. k0 finished after its lease ran out and is re-read from a
// fresh address; k1 succeeded; k2 failed on A and is read from its backup.
// Results stay with their keys; one extra read covers both retries.
TEST(SsdBackupReadFallback, MixedBatchKeepsResultsPerKey) {
    FakeWorld w;
    w.backup_addr = {{"k2", "b2"}};
    w.fresh_addr = {{"k0", "a0new"}};
    w.read_result = {{"a0", Fail(ErrorCode::LEASE_EXPIRED)},
                     {"a1", 11},
                     {"a2", Fail(ErrorCode::RPC_FAIL)},
                     {"a0new", 10},
                     {"b2", 12}};
    std::vector<tl::expected<FakeQ, ErrorCode>> q;
    q.emplace_back(FakeQ{"A0", "a0"});
    q.emplace_back(FakeQ{"A1", "a1"});
    q.emplace_back(FakeQ{"A2", "a2"});
    auto r = RunReadWithBackupFallback<FakeQ>({"k0", "k1", "k2"}, std::move(q),
                                              w.Hooks());
    ASSERT_TRUE(r[0] && r[1] && r[2]);
    EXPECT_EQ(r[0].value(), 10);
    EXPECT_EQ(r[1].value(), 11);
    EXPECT_EQ(r[2].value(), 12);
    ASSERT_EQ(w.reads.size(), 2u);
    EXPECT_EQ(w.reads[1], (std::vector<std::string>{"b2", "a0new"}));
    EXPECT_EQ(w.marked,
              std::vector<std::string>{"A2"});  // not for LEASE_EXPIRED
}

// ---- Coordinator ----

SsdBackupCoordinator::Config TestConfig() {
    SsdBackupCoordinator::Config c;
    c.enabled = true;
    c.batch_keys = 2;
    c.batch_bytes = 1ULL << 40;
    // Busy batches are dropped at once unless a test turns waiting on.
    c.defer_max_age = std::chrono::milliseconds(0);
    return c;
}

BackupTaskItem Item(const std::string& key, int64_t size = 100) {
    return BackupTaskItem{
        .tenant_id = "default", .key = key, .size = size, .version = 1};
}

TEST(SsdBackupCoordinatorTest, ExcludesSourceAndSameHost) {
    SsdBackupCoordinator coord(TestConfig());
    const UUID a{1, 1}, same_host{2, 2}, other{3, 3};
    coord.OnHeartbeat(a, "h1", "ep_a", 0, 1 << 30);
    coord.OnHeartbeat(same_host, "h1", "ep_same_host", 0, 1 << 30);
    coord.OnHeartbeat(other, "h2", "ep_other", 0, 1 << 30);
    for (int i = 0; i < 20; ++i) {
        coord.OnPut(a, "", Item("k" + std::to_string(2 * i)));
        coord.OnPut(a, "", Item("k" + std::to_string(2 * i + 1)));
    }
    EXPECT_TRUE(coord.OnHeartbeat(a, "h1", "ep_a", 0, 1 << 30).batches.empty());
    EXPECT_TRUE(coord.OnHeartbeat(same_host, "h1", "ep_same_host", 0, 1 << 30)
                    .batches.empty());
    // max_batches_per_heartbeat = 4 per call.
    size_t got = 0;
    for (int i = 0; i < 10; ++i) {
        auto resp = coord.OnHeartbeat(other, "h2", "ep_other", 0, 1 << 30);
        for (const auto& b : resp.batches) {
            EXPECT_EQ(b.source_client_id, a);
            EXPECT_EQ(b.tasks.size(), 2u);
            coord.OnBatchDone(other, b.batch_id);
        }
        got += resp.batches.size();
    }
    EXPECT_EQ(got, 20u);
    EXPECT_EQ(coord.GetStats().batches_completed, 20u);
}

// P2-01 (review 2026-11-25): Remove + Put of a key while its task is still
// pending (default batch window). The later put replaces the pending task:
// one task per tenant/key with the new version and size, the batch's bytes
// counted once, and the same bare key in another tenant kept as its own task.
TEST(SsdBackupCoordinatorTest, LaterPutOfPendingKeyReplacesItsTask) {
    auto cfg = TestConfig();
    cfg.batch_keys = 3;
    SsdBackupCoordinator coord(cfg);
    const UUID a{1, 1}, other{3, 3};
    coord.OnHeartbeat(a, "h1", "ep_a", 0, 1 << 30);
    coord.OnHeartbeat(other, "h2", "ep_other", 0, 1 << 30);
    BackupTaskItem v1 = Item("k", 100);
    BackupTaskItem v2 = Item("k", 300);
    v2.version = 2;
    BackupTaskItem t = Item("k", 50);
    t.tenant_id = "tenant-a";
    coord.OnPut(a, "", v1);
    coord.OnPut(a, "", v2);             // replaces v1: still one pending item
    coord.OnPut(a, "", t);              // two items
    coord.OnPut(a, "", Item("z", 10));  // three: the batch is flushed
    auto resp = coord.OnHeartbeat(other, "h2", "ep_other", 0, 1 << 30);
    ASSERT_EQ(resp.batches.size(), 1u);
    const auto& tasks = resp.batches[0].tasks;
    ASSERT_EQ(tasks.size(), 3u);
    std::map<std::pair<std::string, std::string>, BackupTaskItem> by_key;
    for (const auto& task : tasks) by_key[{task.tenant_id, task.key}] = task;
    ASSERT_EQ(by_key.size(), 3u) << "a tenant/key appears twice";
    const auto& dk =
        by_key[std::make_pair(std::string("default"), std::string("k"))];
    const auto& tk =
        by_key[std::make_pair(std::string("tenant-a"), std::string("k"))];
    const auto& dz =
        by_key[std::make_pair(std::string("default"), std::string("z"))];
    EXPECT_EQ(dk.version, 2);
    EXPECT_EQ(dk.size, 300);
    EXPECT_EQ(tk.size, 50);
    EXPECT_EQ(dz.size, 10);
    // The reservation counts each task once: 300 + 50 + 10.
    auto snap = coord.SnapshotForTest(other);
    ASSERT_TRUE(snap.has_value());
    EXPECT_EQ(snap->inflight_bytes, 360u);
    // A key put again after its batch was flushed starts a new task.
    coord.OnPut(a, "", v1);
    coord.OnPut(a, "", Item("y", 1));
    coord.OnPut(a, "", Item("x", 1));
    resp = coord.OnHeartbeat(other, "h2", "ep_other", 0, 1 << 30);
    ASSERT_EQ(resp.batches.size(), 1u);
    EXPECT_EQ(resp.batches[0].tasks.size(), 3u);
}

TEST(SsdBackupCoordinatorTest, NoCandidateDropsBatch) {
    SsdBackupCoordinator coord(TestConfig());
    const UUID a{1, 1};
    coord.OnHeartbeat(a, "h1", "ep_a", 0, 1 << 30);
    coord.OnPut(a, "", Item("x"));
    coord.OnPut(a, "", Item("y"));
    EXPECT_EQ(coord.GetStats().batches_unplaced, 1u);
}

TEST(SsdBackupCoordinatorTest, FullClientNotChosen) {
    SsdBackupCoordinator coord(TestConfig());
    const UUID a{1, 1}, full{2, 2}, free_c{3, 3};
    coord.OnHeartbeat(a, "h1", "ep_a", 0, 1 << 30);
    coord.OnHeartbeat(full, "h2", "ep_full", 99, 100);  // 99% >= 98%
    coord.OnHeartbeat(free_c, "h3", "ep_free_c", 0, 1 << 30);
    for (int i = 0; i < 10; ++i) {
        coord.OnPut(a, "", Item("a" + std::to_string(i)));
    }
    EXPECT_TRUE(
        coord.OnHeartbeat(full, "h2", "ep_full", 99, 100).batches.empty());
    EXPECT_FALSE(coord.OnHeartbeat(free_c, "h3", "ep_free_c", 0, 1 << 30)
                     .batches.empty());
}

TEST(SsdBackupCoordinatorTest, RefreshDedupAndDrain) {
    SsdBackupCoordinator coord(TestConfig());
    const UUID b{2, 2};
    coord.OnHeartbeat(b, "h2", "ep_b", 0, 1 << 30);
    coord.EnqueueRefresh(b, {"default", "k"});
    coord.EnqueueRefresh(b, {"default", "k"});
    coord.EnqueueRefresh(b, {"default", "j"});
    auto resp = coord.OnHeartbeat(b, "h2", "ep_b", 0, 1 << 30);
    EXPECT_EQ(resp.refreshes.size(), 2u);
    EXPECT_TRUE(
        coord.OnHeartbeat(b, "h2", "ep_b", 0, 1 << 30).refreshes.empty());
    // Unknown holder: ignored.
    coord.EnqueueRefresh(UUID{9, 9}, {"default", "k"});
}

TEST(SsdBackupCoordinatorTest, RepeatedTimeoutsCoolDown) {
    auto cfg = TestConfig();
    cfg.task_timeout = std::chrono::milliseconds(1000);
    cfg.candidate_stale_after = std::chrono::hours(1);
    cfg.failure_cooldown = std::chrono::milliseconds(60000);
    SsdBackupCoordinator coord(cfg);
    using Clock = SsdBackupCoordinator::Clock;
    const auto t0 = Clock::now();
    const UUID a{1, 1}, slow{2, 2};
    coord.OnHeartbeat(a, "h1", "ep_a", 0, 1 << 30, t0);
    coord.OnHeartbeat(slow, "h2", "ep_s", 0, 1 << 30, t0);
    for (int i = 0; i < 6; ++i)
        coord.OnPut(a, "", Item("t" + std::to_string(i)), t0);
    auto got = coord.OnHeartbeat(slow, "h2", "ep_s", 0, 1 << 30, t0);
    ASSERT_EQ(got.batches.size(), 3u);  // never acknowledged
    coord.OnHeartbeat(slow, "h2", "ep_s", 0, 1 << 30,
                      t0 + std::chrono::seconds(2));
    EXPECT_EQ(coord.GetStats().batches_timed_out, 3u);
    EXPECT_EQ(coord.GetStats().candidates_cooling_down, 1u);
    // While cooling down, slow is not chosen even though it is the only one.
    const auto unplaced = coord.GetStats().batches_unplaced;
    coord.OnPut(a, "", Item("u0"), t0 + std::chrono::seconds(3));
    coord.OnPut(a, "", Item("u1"), t0 + std::chrono::seconds(3));
    EXPECT_EQ(coord.GetStats().batches_unplaced, unplaced + 1);
    // After the cooldown it is eligible again.
    const auto later = t0 + std::chrono::seconds(70);
    coord.OnHeartbeat(slow, "h2", "ep_s", 0, 1 << 30, later);
    coord.OnPut(a, "", Item("v0"), later);
    coord.OnPut(a, "", Item("v1"), later);
    EXPECT_EQ(
        coord.OnHeartbeat(slow, "h2", "ep_s", 0, 1 << 30, later).batches.size(),
        1u);
}

TEST(SsdBackupCoordinatorTest, UnknownSourceHostFailsClosed) {
    SsdBackupCoordinator coord(TestConfig());
    const UUID src{1, 1}, b{2, 2}, nohost{3, 3};
    coord.OnHeartbeat(b, "h2", "ep_b", 0, 1 << 30);
    coord.OnHeartbeat(nohost, "", "ep_n", 0, 1 << 30);  // host unknown
    // Source never reported a host: nothing is placed.
    coord.OnPut(src, "", Item("x0"));
    coord.OnPut(src, "", Item("x1"));
    EXPECT_EQ(coord.GetStats().batches_unknown_host, 1u);
    EXPECT_TRUE(coord.OnHeartbeat(b, "h2", "ep_b", 0, 1 << 30).batches.empty());
    // Once the master learns it (segment mount), placement works, and a
    // candidate without a host is never chosen.
    coord.SetClientHost(src, "h1");
    for (int i = 0; i < 20; ++i)
        coord.OnPut(src, "", Item("y" + std::to_string(i)));
    EXPECT_TRUE(
        coord.OnHeartbeat(nohost, "", "ep_n", 0, 1 << 30).batches.empty());
    EXPECT_FALSE(
        coord.OnHeartbeat(b, "h2", "ep_b", 0, 1 << 30).batches.empty());
}

// F3: a repair backup avoids the primary SSD owner's machine as well as the
// DRAM source's.
TEST(SsdBackupCoordinatorTest, AlsoAvoidExcludesPrimaryHost) {
    SsdBackupCoordinator coord(TestConfig());
    const UUID src{1, 1}, primary{2, 2}, same_as_primary{3, 3}, free_c{4, 4};
    coord.SetClientHost(src, "h1");
    coord.SetClientHost(primary, "h2");
    coord.OnHeartbeat(same_as_primary, "h2", "ep_s", 0, 1 << 30);
    coord.OnHeartbeat(free_c, "h3", "ep_f", 0, 1 << 30);
    for (int i = 0; i < 20; ++i) {
        coord.OnPut(src, "", Item("r" + std::to_string(i)),
                    SsdBackupCoordinator::Clock::now(), primary);
    }
    EXPECT_TRUE(coord.OnHeartbeat(same_as_primary, "h2", "ep_s", 0, 1 << 30)
                    .batches.empty());
    size_t got = 0;
    for (int i = 0; i < 5; ++i) {
        got +=
            coord.OnHeartbeat(free_c, "h3", "ep_f", 0, 1 << 30).batches.size();
    }
    EXPECT_EQ(got, 10u);
    // Unknown host of the client to avoid: fail closed.
    const UUID unknown{5, 5};
    const auto before = coord.GetStats().batches_unknown_host;
    coord.OnPut(src, "", Item("u0"), SsdBackupCoordinator::Clock::now(),
                unknown);
    coord.OnPut(src, "", Item("u1"), SsdBackupCoordinator::Clock::now(),
                unknown);
    EXPECT_EQ(coord.GetStats().batches_unknown_host, before + 1);
}

// F5: once removed, a client is never offered as a target again.
TEST(SsdBackupCoordinatorTest, RemovedClientNotChosen) {
    SsdBackupCoordinator coord(TestConfig());
    const UUID src{1, 1}, gone{2, 2}, live{3, 3};
    coord.SetClientHost(src, "h1");
    coord.OnHeartbeat(gone, "h2", "ep_g", 0, 1 << 30);
    coord.OnHeartbeat(live, "h3", "ep_l", 0, 1 << 30);
    coord.RemoveClient(gone);
    for (int i = 0; i < 20; ++i)
        coord.OnPut(src, "", Item("k" + std::to_string(i)));
    size_t got = 0;
    for (int i = 0; i < 5; ++i) {
        got += coord.OnHeartbeat(live, "h3", "ep_l", 0, 1 << 30).batches.size();
    }
    EXPECT_EQ(got, 10u);
}

// R8-Q2: a healthy holder with a backlog (13 batches queued at once, 4
// delivered per 10 s heartbeat, 30 s timeout) gets every batch: the timeout
// starts at delivery, so nothing queued expires and no cooldown starts. If
// the holder stops heartbeating, its queued batches are released after the
// timeout without counting as its timeouts.
TEST(SsdBackupCoordinatorTest, QueuedBacklogTimesOutOnlyAfterDelivery) {
    auto cfg = TestConfig();
    cfg.batch_keys = 1;
    cfg.task_timeout = std::chrono::seconds(30);
    cfg.candidate_stale_after = std::chrono::hours(1);
    using Clock = SsdBackupCoordinator::Clock;
    using std::chrono::seconds;
    const UUID src{1, 1}, holder{2, 2}, other{3, 3};
    {
        SsdBackupCoordinator coord(cfg);
        const auto t0 = Clock::now();
        coord.SetClientHost(src, "h1");
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
        for (int i = 0; i < 13; ++i) {
            coord.OnPut(src, "", Item("q" + std::to_string(i)), t0);
        }
        ASSERT_EQ(coord.GetStats().batches_dispatched, 13u);
        size_t delivered = 0;
        for (int beat = 1; beat <= 5; ++beat) {
            auto resp = coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30,
                                          t0 + seconds(10 * beat));
            for (const auto& b : resp.batches) {
                coord.OnBatchDone(holder, b.batch_id);
            }
            delivered += resp.batches.size();
        }
        EXPECT_EQ(delivered, 13u);
        EXPECT_EQ(coord.GetStats().batches_completed, 13u);
        EXPECT_EQ(coord.GetStats().batches_timed_out, 0u);
        EXPECT_EQ(coord.GetStats().candidates_cooling_down, 0u);
        auto snap = coord.SnapshotForTest(holder);
        ASSERT_TRUE(snap.has_value());
        EXPECT_EQ(snap->inflight_bytes, 0u);
        EXPECT_EQ(snap->queued_batches, 0u);
    }
    {
        SsdBackupCoordinator coord(cfg);
        const auto t0 = Clock::now();
        coord.SetClientHost(src, "h1");
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
        for (int i = 0; i < 13; ++i) {
            coord.OnPut(src, "", Item("s" + std::to_string(i)), t0);
        }
        // holder is silent; another client's heartbeats drive expiry.
        coord.OnHeartbeat(other, "h3", "ep3", 0, 1 << 30, t0 + seconds(29));
        EXPECT_EQ(coord.GetStats().batches_timed_out, 0u);
        coord.OnHeartbeat(other, "h3", "ep3", 0, 1 << 30, t0 + seconds(30));
        EXPECT_EQ(coord.GetStats().batches_timed_out, 13u);
        EXPECT_EQ(coord.GetStats().candidates_cooling_down, 0u);
        auto snap = coord.SnapshotForTest(holder);
        ASSERT_TRUE(snap.has_value());
        EXPECT_EQ(snap->inflight_bytes, 0u);
        EXPECT_EQ(snap->queued_batches, 0u);
    }
}

// R1-F07: a timed-out reservation is released before aged batches are
// flushed in the same heartbeat, so the freed capacity is used.
TEST(SsdBackupCoordinatorTest, ExpiredReservationFreedBeforeAgedFlush) {
    auto cfg = TestConfig();
    cfg.batch_keys = 100;
    cfg.batch_bytes = 1000;
    cfg.max_inflight_bytes_per_client = 1000;
    cfg.task_timeout = std::chrono::milliseconds(30);
    cfg.batch_max_age = std::chrono::milliseconds(10);
    cfg.candidate_stale_after = std::chrono::hours(1);
    cfg.max_consecutive_timeouts = 100;
    SsdBackupCoordinator coord(cfg);
    using Clock = SsdBackupCoordinator::Clock;
    using std::chrono::milliseconds;
    const auto t0 = Clock::now();
    const UUID src{1, 1}, holder{2, 2};
    coord.SetClientHost(src, "h1");
    coord.OnHeartbeat(holder, "h2", "ep_h", 0, 1 << 30, t0);
    // 10 x 100 bytes reach batch_bytes: one 1000-byte batch fills the limit.
    for (int i = 0; i < 10; ++i) {
        coord.OnPut(src, "", Item("old" + std::to_string(i)), t0);
    }
    ASSERT_EQ(
        coord.OnHeartbeat(holder, "h2", "ep_h", 0, 1 << 30, t0).batches.size(),
        1u);  // never acknowledged
    // A 500-byte partial batch that is due by age at the same heartbeat in
    // which the old reservation times out.
    coord.OnPut(src, "", Item("new", 500), t0 + milliseconds(20));
    const auto unplaced = coord.GetStats().batches_unplaced;
    auto resp = coord.OnHeartbeat(holder, "h2", "ep_h", 0, 1 << 30,
                                  t0 + milliseconds(31));
    EXPECT_EQ(coord.GetStats().batches_timed_out, 1u);
    EXPECT_EQ(coord.GetStats().batches_unplaced, unplaced);
    ASSERT_EQ(resp.batches.size(), 1u);
    ASSERT_EQ(resp.batches[0].tasks.size(), 1u);
    EXPECT_EQ(resp.batches[0].tasks[0].key, "new");
}

// CFG-07 (master side): signs, garbage, overflow and over-long durations
// fall back to the defaults instead of wrapping into huge or negative values.
TEST(SsdBackupConfigTest, CoordinatorNumericBoundaries) {
    const char* vars[] = {
        "MC_SSD_BACKUP_BATCH_BYTES", "MC_SSD_BACKUP_BATCH_KEYS",
        "MC_SSD_BACKUP_BATCH_MAX_AGE_MS", "MC_SSD_BACKUP_TASK_TIMEOUT_MS",
        "MC_SSD_BACKUP_MAX_INFLIGHT_BYTES"};
    const SsdBackupCoordinator::Config d;
    for (const char* bad : {"-1", "abc", "1x", "+5", "18446744073709551616"}) {
        SCOPED_TRACE(bad);
        for (const char* v : vars) setenv(v, bad, 1);
        const auto c = SsdBackupCoordinator::Config::FromEnvironment();
        EXPECT_EQ(c.batch_bytes, d.batch_bytes);
        EXPECT_EQ(c.batch_keys, d.batch_keys);
        EXPECT_EQ(c.batch_max_age, d.batch_max_age);
        EXPECT_EQ(c.task_timeout, d.task_timeout);
        EXPECT_EQ(c.max_inflight_bytes_per_client,
                  d.max_inflight_bytes_per_client);
    }
    for (const char* v : vars) unsetenv(v);
    setenv("MC_SSD_BACKUP_BATCH_KEYS", "4294967296", 1);  // > uint32
    EXPECT_EQ(SsdBackupCoordinator::Config::FromEnvironment().batch_keys,
              d.batch_keys);
    setenv("MC_SSD_BACKUP_BATCH_KEYS", "0", 1);
    EXPECT_EQ(SsdBackupCoordinator::Config::FromEnvironment().batch_keys, 1u);
    unsetenv("MC_SSD_BACKUP_BATCH_KEYS");
    setenv("MC_SSD_BACKUP_TASK_TIMEOUT_MS", "86400001", 1);  // > 24 h
    EXPECT_EQ(SsdBackupCoordinator::Config::FromEnvironment().task_timeout,
              d.task_timeout);
    setenv("MC_SSD_BACKUP_TASK_TIMEOUT_MS", "45000", 1);
    EXPECT_EQ(SsdBackupCoordinator::Config::FromEnvironment().task_timeout,
              std::chrono::milliseconds(45000));
    unsetenv("MC_SSD_BACKUP_TASK_TIMEOUT_MS");
}

// MST-05: a batch is cut exactly at batch_keys / batch_bytes, and a single
// object larger than batch_bytes is sent alone.
TEST(SsdBackupCoordinatorTest, BatchThresholdBoundaries) {
    auto cfg = TestConfig();
    cfg.batch_keys = 3;
    cfg.batch_bytes = 1000;
    cfg.max_batches_per_heartbeat = 100;
    SsdBackupCoordinator coord(cfg);
    const UUID src{1, 1}, holder{2, 2};
    coord.SetClientHost(src, "h1");
    coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30);
    auto drain = [&] {
        return coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30).batches;
    };
    // Keys: N-1 = 2 items stay pending, the Nth cuts a batch of 3.
    coord.OnPut(src, "", Item("k0", 10));
    coord.OnPut(src, "", Item("k1", 10));
    EXPECT_TRUE(drain().empty());
    coord.OnPut(src, "", Item("k2", 10));
    auto b = drain();
    ASSERT_EQ(b.size(), 1u);
    EXPECT_EQ(b[0].tasks.size(), 3u);
    coord.OnBatchDone(holder, b[0].batch_id);
    // Bytes: 999 pending, +1 reaches 1000 exactly.
    coord.OnPut(src, "", Item("b0", 999));
    EXPECT_TRUE(drain().empty());
    coord.OnPut(src, "", Item("b1", 1));
    b = drain();
    ASSERT_EQ(b.size(), 1u);
    EXPECT_EQ(b[0].tasks.size(), 2u);
    coord.OnBatchDone(holder, b[0].batch_id);
    // One object above batch_bytes goes out on its own.
    coord.OnPut(src, "", Item("big", 1500));
    b = drain();
    ASSERT_EQ(b.size(), 1u);
    ASSERT_EQ(b[0].tasks.size(), 1u);
    EXPECT_EQ(b[0].tasks[0].key, "big");
}

// MST-09: duplicate, unknown and late ACKs change nothing twice; the
// reservation never goes negative and the client stays usable.
TEST(SsdBackupCoordinatorTest, DuplicateUnknownAndLateAcks) {
    auto cfg = TestConfig();
    cfg.batch_keys = 1;
    cfg.max_inflight_bytes_per_client = 150;
    cfg.task_timeout = std::chrono::milliseconds(30);
    cfg.candidate_stale_after = std::chrono::hours(1);
    cfg.max_consecutive_timeouts = 100;
    SsdBackupCoordinator coord(cfg);
    using Clock = SsdBackupCoordinator::Clock;
    const auto t0 = Clock::now();
    const UUID src{1, 1}, holder{2, 2};
    coord.SetClientHost(src, "h1");
    coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
    coord.OnPut(src, "", Item("a", 100), t0);
    auto b = coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0).batches;
    ASSERT_EQ(b.size(), 1u);
    coord.OnBatchDone(holder, b[0].batch_id);
    coord.OnBatchDone(holder, b[0].batch_id);       // duplicate
    coord.OnBatchDone(holder, b[0].batch_id + 99);  // unknown batch
    coord.OnBatchDone(UUID{9, 9}, b[0].batch_id);   // unknown client
    EXPECT_EQ(coord.GetStats().batches_completed, 1u);
    // Full 100 bytes of 150 were released: another 100-byte batch fits.
    coord.OnPut(src, "", Item("b", 100), t0);
    b = coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0).batches;
    ASSERT_EQ(b.size(), 1u);
    // It times out; the late ACK afterwards is ignored.
    const auto later = t0 + std::chrono::milliseconds(50);
    coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, later);
    EXPECT_EQ(coord.GetStats().batches_timed_out, 1u);
    coord.OnBatchDone(holder, b[0].batch_id);
    EXPECT_EQ(coord.GetStats().batches_completed, 1u);
    // Nothing is left reserved: a new 100-byte batch is still placed.
    coord.OnPut(src, "", Item("c", 100), later);
    EXPECT_EQ(
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, later).batches.size(),
        1u);
}

// MST-05: bytes crossing the threshold (999 + 2 >= 1000) cut one batch with
// both tasks, in order, nothing lost or duplicated; a single object above
// the per-client inflight limit (batch_keys = 1) is counted +1 batch / +1
// object and a later small object is placed normally.
TEST(SsdBackupCoordinatorTest, ByteCrossingAndOversizedSingleton) {
    using Clock = SsdBackupCoordinator::Clock;
    const auto t0 = Clock::now();
    const UUID src{1, 1}, holder{2, 2};
    {
        auto cfg = TestConfig();
        cfg.batch_keys = 100;
        cfg.batch_bytes = 1000;
        SsdBackupCoordinator coord(cfg);
        coord.SetClientHost(src, "h1");
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
        coord.OnPut(src, "", Item("c0", 999), t0);
        EXPECT_TRUE(coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0)
                        .batches.empty());
        coord.OnPut(src, "", Item("c1", 2), t0);
        auto b = coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0).batches;
        ASSERT_EQ(b.size(), 1u);
        ASSERT_EQ(b[0].tasks.size(), 2u);
        EXPECT_EQ(b[0].tasks[0].key, "c0");
        EXPECT_EQ(b[0].tasks[0].size, 999);
        EXPECT_EQ(b[0].tasks[1].key, "c1");
        EXPECT_EQ(b[0].tasks[1].size, 2);
        EXPECT_TRUE(coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0)
                        .batches.empty());
    }
    {
        auto cfg = TestConfig();
        cfg.batch_keys = 1;
        cfg.max_inflight_bytes_per_client = 10000;
        SsdBackupCoordinator coord(cfg);
        coord.SetClientHost(src, "h1");
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
        coord.OnPut(src, "", Item("big", 10001), t0);
        EXPECT_EQ(coord.GetStats().batches_dispatched, 0u);
        EXPECT_EQ(coord.GetStats().batches_unplaced, 1u);
        EXPECT_EQ(coord.GetStats().objects_unplaced, 1u);
        EXPECT_TRUE(coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0)
                        .batches.empty());
        coord.OnPut(src, "", Item("small", 100), t0);
        auto b = coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0).batches;
        ASSERT_EQ(b.size(), 1u);
        ASSERT_EQ(b[0].tasks.size(), 1u);
        EXPECT_EQ(b[0].tasks[0].key, "small");
    }
}

// MST-05: N+1 keys give one full batch plus a pending one; each batch keeps
// its tasks' identity (key, size, version) and put order. Then a three-key
// batch whose total exceeds the inflight limit (one 20000-byte object plus
// two padding objects) is rejected as a whole: +1 batch, +3 objects.
TEST(SsdBackupCoordinatorTest, BatchIdentityOrderAndOversizedBatchCount) {
    auto cfg = TestConfig();
    cfg.batch_keys = 3;
    cfg.batch_bytes = 1ULL << 40;
    cfg.max_inflight_bytes_per_client = 10000;
    cfg.max_batches_per_heartbeat = 100;
    cfg.batch_max_age = std::chrono::milliseconds(100);
    SsdBackupCoordinator coord(cfg);
    using Clock = SsdBackupCoordinator::Clock;
    using std::chrono::milliseconds;
    const auto t0 = Clock::now();
    const UUID src{1, 1}, holder{2, 2};
    coord.SetClientHost(src, "h1");
    coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
    for (int i = 0; i < 4; ++i) {  // N + 1 = 4
        BackupTaskItem it = Item("k" + std::to_string(i), 10 + i);
        it.version = 100 + i;
        coord.OnPut(src, "", it, t0);
    }
    auto b = coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0).batches;
    ASSERT_EQ(b.size(), 1u);
    ASSERT_EQ(b[0].tasks.size(), 3u);
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(b[0].tasks[i].key, "k" + std::to_string(i));
        EXPECT_EQ(b[0].tasks[i].size, 10 + i);
        EXPECT_EQ(b[0].tasks[i].version, 100 + i);
    }
    EXPECT_EQ(b[0].source_client_id, src);
    // The (N+1)th waits for its age, then goes out alone, unchanged.
    b = coord
            .OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0 + milliseconds(100))
            .batches;
    ASSERT_EQ(b.size(), 1u);
    ASSERT_EQ(b[0].tasks.size(), 1u);
    EXPECT_EQ(b[0].tasks[0].key, "k3");
    EXPECT_EQ(b[0].tasks[0].version, 103);
    // Oversized singleton: no client can take it.
    const auto before = coord.GetStats();
    auto huge = Item("huge", 20000);
    coord.OnPut(src, "", huge, t0);
    coord.OnPut(src, "", Item("pad1"), t0);
    coord.OnPut(src, "", Item("pad2"), t0);
    EXPECT_EQ(coord.GetStats().batches_unplaced, before.batches_unplaced + 1);
    EXPECT_EQ(coord.GetStats().objects_unplaced, before.objects_unplaced + 3);
}

// MST-06: a partial batch is dispatched when its oldest item reaches
// batch_max_age (not before), and items added meanwhile join it.
TEST(SsdBackupCoordinatorTest, AgedPartialBatchBoundary) {
    auto cfg = TestConfig();
    cfg.batch_keys = 100;
    cfg.batch_max_age = std::chrono::milliseconds(100);
    cfg.candidate_stale_after = std::chrono::hours(1);
    SsdBackupCoordinator coord(cfg);
    using Clock = SsdBackupCoordinator::Clock;
    using std::chrono::milliseconds;
    const auto t0 = Clock::now();
    const UUID src{1, 1}, holder{2, 2};
    coord.SetClientHost(src, "h1");
    coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
    coord.OnPut(src, "", Item("a"), t0);
    coord.OnPut(src, "", Item("b"), t0 + milliseconds(50));
    EXPECT_TRUE(
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0 + milliseconds(99))
            .batches.empty());
    auto b =
        coord
            .OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0 + milliseconds(100))
            .batches;
    ASSERT_EQ(b.size(), 1u);
    ASSERT_EQ(b[0].tasks.size(), 2u);
    EXPECT_EQ(b[0].tasks[0].key, "a");
    EXPECT_EQ(b[0].tasks[1].key, "b");
    // Later than the age: still exactly one batch, nothing left behind.
    EXPECT_TRUE(
        coord
            .OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0 + milliseconds(500))
            .batches.empty());
}

// MST-07: stale candidates, a full queue and a full inflight budget each
// refuse work, counted as unplaced; memory stays bounded by the queue limit.
TEST(SsdBackupCoordinatorTest, CandidateLimitsCountUnplaced) {
    using Clock = SsdBackupCoordinator::Clock;
    using std::chrono::milliseconds;
    const UUID src{1, 1}, holder{2, 2};
    const auto t0 = Clock::now();
    {  // stale: last heartbeat older than candidate_stale_after
        auto cfg = TestConfig();
        cfg.batch_keys = 1;
        cfg.candidate_stale_after = milliseconds(1000);
        SsdBackupCoordinator coord(cfg);
        coord.SetClientHost(src, "h1");
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
        coord.OnPut(src, "", Item("s"), t0 + milliseconds(1001));
        EXPECT_EQ(coord.GetStats().batches_unplaced, 1u);
        EXPECT_EQ(coord.GetStats().objects_unplaced, 1u);
    }
    {  // queue full: holder never heartbeats again to take its batches
        auto cfg = TestConfig();
        cfg.batch_keys = 1;
        cfg.max_pending_batches_per_client = 2;
        cfg.candidate_stale_after = std::chrono::hours(1);
        SsdBackupCoordinator coord(cfg);
        coord.SetClientHost(src, "h1");
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
        for (int i = 0; i < 5; ++i) {
            coord.OnPut(src, "", Item("q" + std::to_string(i)), t0);
        }
        EXPECT_EQ(coord.GetStats().batches_dispatched, 2u);
        EXPECT_EQ(coord.GetStats().batches_unplaced, 3u);
        EXPECT_EQ(coord.GetStats().objects_unplaced, 3u);
    }
    {  // inflight budget: 2 x 100 bytes fill 200
        auto cfg = TestConfig();
        cfg.batch_keys = 1;
        cfg.max_inflight_bytes_per_client = 200;
        cfg.candidate_stale_after = std::chrono::hours(1);
        SsdBackupCoordinator coord(cfg);
        coord.SetClientHost(src, "h1");
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
        for (int i = 0; i < 3; ++i) {
            coord.OnPut(src, "", Item("f" + std::to_string(i), 100), t0);
        }
        EXPECT_EQ(coord.GetStats().batches_dispatched, 2u);
        EXPECT_EQ(coord.GetStats().batches_unplaced, 1u);
        // Completing one frees room again.
        auto b = coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0).batches;
        ASSERT_EQ(b.size(), 2u);
        coord.OnBatchDone(holder, b[0].batch_id);
        coord.OnPut(src, "", Item("f3", 100), t0);
        EXPECT_EQ(coord.GetStats().batches_dispatched, 3u);
    }
}

// A batch whose only eligible client is busy (inflight budget full) waits
// on the master and goes out once a completion frees room, instead of being
// dropped; later batches of the source wait behind it, in put order.
TEST(SsdBackupCoordinatorTest, BusyBatchWaitsAndIsPlacedInOrder) {
    using Clock = SsdBackupCoordinator::Clock;
    auto cfg = TestConfig();
    cfg.batch_keys = 1;
    cfg.max_inflight_bytes_per_client = 200;
    cfg.max_batches_per_heartbeat = 100;
    cfg.candidate_stale_after = std::chrono::hours(1);
    cfg.defer_max_age = std::chrono::milliseconds(60000);
    SsdBackupCoordinator coord(cfg);
    const UUID src{1, 1}, holder{2, 2};
    const auto t0 = Clock::now();
    coord.SetClientHost(src, "h1");
    coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
    for (int i = 0; i < 4; ++i) {
        coord.OnPut(src, "", Item("w" + std::to_string(i), 100), t0);
    }
    EXPECT_EQ(coord.GetStats().batches_dispatched, 2u);
    EXPECT_EQ(coord.GetStats().batches_deferred, 2u);
    EXPECT_EQ(coord.GetStats().batches_unplaced, 0u);
    EXPECT_EQ(coord.GetStats().objects_unplaced, 0u);
    auto b = coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0).batches;
    ASSERT_EQ(b.size(), 2u);
    EXPECT_EQ(b[0].tasks[0].key, "w0");
    EXPECT_EQ(b[1].tasks[0].key, "w1");
    // One completion frees room for exactly one waiting batch: the oldest.
    coord.OnBatchDone(holder, b[0].batch_id);
    b = coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0).batches;
    ASSERT_EQ(b.size(), 1u);
    EXPECT_EQ(b[0].tasks[0].key, "w2");
    // A new put waits behind the still-waiting w3 even once room frees.
    coord.OnPut(src, "", Item("w4", 100), t0);
    EXPECT_EQ(coord.GetStats().batches_deferred, 3u);
    coord.OnBatchDone(holder, b[0].batch_id);
    b = coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0).batches;
    ASSERT_EQ(b.size(), 1u);
    EXPECT_EQ(b[0].tasks[0].key, "w3");
    EXPECT_EQ(coord.GetStats().batches_unplaced, 0u);
}

// A client at the usage limit is busy, not ineligible: the batch waits and
// goes to it once it reports less use (it demotes pinned backups from a
// lower watermark), instead of being dropped while the client frees room.
TEST(SsdBackupCoordinatorTest, BatchWaitsWhileClientAtUsageLimit) {
    using Clock = SsdBackupCoordinator::Clock;
    using std::chrono::milliseconds;
    auto cfg = TestConfig();
    cfg.batch_keys = 1;
    cfg.candidate_stale_after = std::chrono::hours(1);
    cfg.defer_max_age = milliseconds(60000);
    SsdBackupCoordinator coord(cfg);
    const UUID src{1, 1}, holder{2, 2};
    const auto t0 = Clock::now();
    coord.SetClientHost(src, "h1");
    coord.OnHeartbeat(holder, "h2", "ep", 99, 100, t0);  // 99% >= 98%
    coord.OnPut(src, "", Item("u0", 1), t0);
    EXPECT_EQ(coord.GetStats().batches_deferred, 1u);
    EXPECT_EQ(coord.GetStats().batches_unplaced, 0u);
    // Still full: nothing goes out.
    EXPECT_TRUE(
        coord.OnHeartbeat(holder, "h2", "ep", 99, 100, t0 + milliseconds(10))
            .batches.empty());
    // It demoted down to 93%: the waiting batch is handed out.
    auto b =
        coord.OnHeartbeat(holder, "h2", "ep", 93, 100, t0 + milliseconds(20))
            .batches;
    ASSERT_EQ(b.size(), 1u);
    EXPECT_EQ(b[0].tasks[0].key, "u0");
    EXPECT_EQ(coord.GetStats().batches_unplaced, 0u);
}

// A known client back from a stale heartbeat (or with more capacity) counts
// as freed room: a batch waiting while it was stale and the other client
// was full goes to it on that heartbeat, not after the wait limit.
TEST(SsdBackupCoordinatorTest, RecoveredClientTakesWaitingBatch) {
    using Clock = SsdBackupCoordinator::Clock;
    using std::chrono::milliseconds;
    auto cfg = TestConfig();
    cfg.batch_keys = 1;
    cfg.candidate_stale_after = milliseconds(1000);
    cfg.defer_max_age = milliseconds(600000);
    const UUID src{1, 1}, a{2, 2}, b{3, 3};
    const auto t0 = Clock::now();
    {  // a stale, then back
        SsdBackupCoordinator coord(cfg);
        coord.SetClientHost(src, "h1");
        coord.OnHeartbeat(a, "h2", "ep_a", 0, 1 << 30, t0);
        coord.OnHeartbeat(b, "h3", "ep_b", 99, 100, t0 + milliseconds(1500));
        coord.OnPut(src, "", Item("r0", 1), t0 + milliseconds(1500));
        EXPECT_EQ(coord.GetStats().batches_deferred, 1u);
        // b is still full; its heartbeat retries (fallback) but cannot place.
        EXPECT_TRUE(
            coord.OnHeartbeat(b, "h3", "ep_b", 99, 100, t0 + milliseconds(2600))
                .batches.empty());
        auto got = coord
                       .OnHeartbeat(a, "h2", "ep_a", 0, 1 << 30,
                                    t0 + milliseconds(2610))
                       .batches;
        ASSERT_EQ(got.size(), 1u) << "recovered client not given the batch";
        EXPECT_EQ(got[0].tasks[0].key, "r0");
        EXPECT_EQ(coord.GetStats().batches_unplaced, 0u);
    }
    {  // more capacity at the same use
        SsdBackupCoordinator coord(cfg);
        coord.SetClientHost(src, "h1");
        coord.OnHeartbeat(b, "h3", "ep_b", 99, 100, t0);
        coord.OnPut(src, "", Item("c0", 1), t0);
        EXPECT_EQ(coord.GetStats().batches_deferred, 1u);
        auto got =
            coord.OnHeartbeat(b, "h3", "ep_b", 99, 200, t0 + milliseconds(10))
                .batches;
        ASSERT_EQ(got.size(), 1u);
        EXPECT_EQ(got[0].tasks[0].key, "c0");
    }
}

// Two sources waiting on one client that frees one slot at a time: the
// oldest waiting batch goes first across sources, so the source that just
// got a slot does not take the next one too (no starvation).
TEST(SsdBackupCoordinatorTest, WaitingSourcesShareFreedSlots) {
    using Clock = SsdBackupCoordinator::Clock;
    using std::chrono::milliseconds;
    auto cfg = TestConfig();
    cfg.batch_keys = 1;
    cfg.max_inflight_bytes_per_client = 100;
    cfg.max_batches_per_heartbeat = 100;
    cfg.candidate_stale_after = std::chrono::hours(1);
    cfg.defer_max_age = std::chrono::hours(1);
    SsdBackupCoordinator coord(cfg);
    const UUID s1{1, 1}, s2{4, 4}, holder{2, 2};
    auto t = Clock::now();
    coord.SetClientHost(s1, "h1");
    coord.SetClientHost(s2, "h4");
    coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t);
    int n1 = 0, n2 = 0;
    coord.OnPut(s1, "", Item("a0", 100), t);  // dispatched at once
    coord.OnPut(s2, "", Item("b0", 100), t);  // waits
    for (int round = 0; round < 40; ++round) {
        t += milliseconds(1);
        // Each source keeps one more batch waiting.
        coord.OnPut(s1, "", Item("a" + std::to_string(round + 1), 100), t);
        coord.OnPut(s2, "", Item("b" + std::to_string(round + 1), 100), t);
        auto got = coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t).batches;
        ASSERT_EQ(got.size(), 1u) << round;
        (got[0].source_client_id == s1 ? n1 : n2) += 1;
        coord.OnBatchDone(holder, got[0].batch_id);
    }
    EXPECT_GE(n1, 15);
    EXPECT_GE(n2, 15);
    EXPECT_EQ(coord.GetStats().batches_unplaced, 0u);
}

// A new batch does not take room freed for a waiting one: after a
// completion, a new put of the source that just finished (no batch of its
// own waiting) and an aged partial batch both come after the oldest waiting
// batch of another source.
TEST(SsdBackupCoordinatorTest, NewBatchDoesNotOvertakeWaitingOnes) {
    using Clock = SsdBackupCoordinator::Clock;
    using std::chrono::milliseconds;
    auto cfg = TestConfig();
    cfg.batch_keys = 1;
    cfg.max_inflight_bytes_per_client = 100;
    cfg.max_batches_per_heartbeat = 100;
    cfg.candidate_stale_after = std::chrono::hours(1);
    cfg.defer_max_age = std::chrono::hours(1);
    const UUID a{1, 1}, b{4, 4}, holder{2, 2};
    auto t = Clock::now();
    {  // completion -> new put of A -> heartbeat
        SsdBackupCoordinator coord(cfg);
        coord.SetClientHost(a, "h1");
        coord.SetClientHost(b, "h4");
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t);
        coord.OnPut(a, "", Item("a0", 100), t);  // takes the only slot
        coord.OnPut(b, "", Item("b0", 100), t);  // waits
        int a_got = 0;
        bool b0_got = false;
        for (int round = 0; round < 10 && !b0_got; ++round) {
            t += milliseconds(1);
            auto got =
                coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t).batches;
            for (const auto& batch : got) {
                if (batch.tasks[0].key == "b0") b0_got = true;
                if (batch.source_client_id == a) ++a_got;
                coord.OnBatchDone(holder, batch.batch_id);
            }
            coord.OnPut(a, "", Item("a" + std::to_string(round + 1), 100), t);
        }
        EXPECT_TRUE(b0_got) << "A's new batches took every freed slot";
        EXPECT_LE(a_got, 1);
    }
    {  // aged partial batch of A vs waiting batch of B
        auto c = cfg;
        c.batch_keys = 100;
        c.batch_bytes = 100;
        c.batch_max_age = milliseconds(100);
        SsdBackupCoordinator coord(c);
        const auto t0 = Clock::now();
        coord.SetClientHost(a, "h1");
        coord.SetClientHost(b, "h4");
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
        coord.OnPut(b, "", Item("b0", 100), t0);  // full batch: placed
        coord.OnPut(b, "", Item("b1", 100), t0);  // waits
        coord.OnPut(a, "", Item("a0", 50), t0);   // partial, not yet due
        auto got = coord
                       .OnHeartbeat(holder, "h2", "ep", 0, 1 << 30,
                                    t0 + milliseconds(50))
                       .batches;
        ASSERT_EQ(got.size(), 1u);
        EXPECT_EQ(got[0].tasks[0].key, "b0");
        coord.OnBatchDone(holder, got[0].batch_id);
        // Both b1 (waiting) and a0 (now aged) could take the freed slot.
        got = coord
                  .OnHeartbeat(holder, "h2", "ep", 0, 1 << 30,
                               t0 + milliseconds(200))
                  .batches;
        ASSERT_EQ(got.size(), 1u);
        EXPECT_EQ(got[0].tasks[0].key, "b1");
    }
}

// Put-to-backup latency: from the first put of a batch to the holder's ACK
// that registered it, kept across a wait; an empty ACK is not counted.
TEST(SsdBackupCoordinatorTest, PutToBackupLatencyIsMeasured) {
    using Clock = SsdBackupCoordinator::Clock;
    using std::chrono::milliseconds;
    auto cfg = TestConfig();
    cfg.batch_keys = 1;
    cfg.max_inflight_bytes_per_client = 100;
    cfg.max_batches_per_heartbeat = 100;
    cfg.candidate_stale_after = std::chrono::hours(1);
    cfg.defer_max_age = std::chrono::hours(1);
    cfg.task_timeout = std::chrono::milliseconds(30000);
    SsdBackupCoordinator coord(cfg);
    const UUID src{1, 1}, holder{2, 2};
    const auto t0 = Clock::now();
    coord.SetClientHost(src, "h1");
    coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
    coord.OnPut(src, "", Item("l0", 100), t0);                      // placed
    coord.OnPut(src, "", Item("l1", 100), t0 + milliseconds(500));  // waits
    auto b = coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0).batches;
    ASSERT_EQ(b.size(), 1u);
    const uint64_t first = b[0].batch_id;
    coord.OnBatchDone(holder, first);
    // Completion alone is not a backup.
    EXPECT_EQ(coord.GetStats().backed_up_batches, 0u);
    // Two ACKs of one batch: whichever registered claims the timing once.
    auto s = coord.RecordBackedUp(first, t0 + milliseconds(2000));
    ASSERT_TRUE(s.has_value());
    EXPECT_NEAR(*s, 2.0, 1e-6);
    EXPECT_FALSE(coord.RecordBackedUp(first, t0 + milliseconds(2100)));
    auto st = coord.GetStats();
    EXPECT_EQ(st.backed_up_batches, 2u);
    EXPECT_EQ(st.backed_up_untimed, 1u);
    // The mean is over timed registrations only (one 2 s sample), not over
    // every registration call.
    EXPECT_EQ(st.put_to_backup_timed, 1u);
    EXPECT_NEAR(st.put_to_backup_seconds_sum, 2.0, 1e-6);
    // l1: first put at 0.5 s, kept across its wait; its reservation times
    // out, yet the late successful ACK still finds the timing.
    b = coord
            .OnHeartbeat(holder, "h2", "ep", 0, 1 << 30,
                         t0 + milliseconds(2000))
            .batches;
    ASSERT_EQ(b.size(), 1u);
    const uint64_t second = b[0].batch_id;
    coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30,
                      t0 + milliseconds(40000));  // > task_timeout
    ASSERT_TRUE(coord.SnapshotForTest(holder)->inflight_batches.empty());
    s = coord.RecordBackedUp(second, t0 + milliseconds(40500));
    ASSERT_TRUE(s.has_value());
    EXPECT_NEAR(*s, 40.0, 1e-6);
    EXPECT_NEAR(coord.GetStats().put_to_backup_seconds_max, 40.0, 1e-6);
    // Timing never claimed (no registration) is dropped after 30 minutes.
    coord.OnPut(src, "", Item("l2", 100), t0 + milliseconds(40500));
    b = coord
            .OnHeartbeat(holder, "h2", "ep", 0, 1 << 30,
                         t0 + milliseconds(40500))
            .batches;
    ASSERT_EQ(b.size(), 1u);
    coord.OnBatchDone(holder, b[0].batch_id);  // empty ACK: no registration
    coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30,
                      t0 + std::chrono::minutes(31) + milliseconds(40500));
    EXPECT_EQ(coord.GetStats().batch_timing_dropped, 1u);
    EXPECT_FALSE(coord.RecordBackedUp(b[0].batch_id));
}

// The waiting limits are per source client, over all its batches whatever
// other client they must avoid, and count tasks as well as bytes; a batch
// over a limit is dropped and counted as overflow.
TEST(SsdBackupCoordinatorTest, WaitingLimitsArePerSource) {
    using Clock = SsdBackupCoordinator::Clock;
    using std::chrono::milliseconds;
    auto cfg = TestConfig();
    cfg.batch_keys = 1;
    cfg.max_inflight_bytes_per_client = 100;
    cfg.candidate_stale_after = std::chrono::hours(1);
    cfg.defer_max_age = std::chrono::hours(1);
    const UUID src{1, 1}, holder{2, 2}, avoid1{7, 7}, avoid2{8, 8};
    const auto t0 = Clock::now();
    {  // bytes: 200 per source, across two avoid groups
        auto c = cfg;
        c.max_deferred_bytes_per_source = 200;
        SsdBackupCoordinator coord(c);
        coord.SetClientHost(src, "h1");
        coord.SetClientHost(avoid1, "h7");
        coord.SetClientHost(avoid2, "h8");
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
        coord.OnPut(src, "", Item("x", 100), t0);  // fills the budget
        coord.OnPut(src, "", Item("g1a", 100), t0, avoid1);
        coord.OnPut(src, "", Item("g2a", 100), t0, avoid2);
        coord.OnPut(src, "", Item("g1b", 100), t0, avoid1);  // over 200
        const auto st = coord.GetStats();
        EXPECT_EQ(st.batches_deferred, 2u);
        EXPECT_EQ(st.deferred_overflow, 1u);
        EXPECT_EQ(st.objects_unplaced, 1u);
        EXPECT_EQ(st.waiting_batches, 2u);
        EXPECT_EQ(st.waiting_bytes, 200u);
        EXPECT_EQ(st.waiting_tasks, 2u);
    }
    {  // tasks: 2 per source
        auto c = cfg;
        c.max_deferred_tasks_per_source = 2;
        SsdBackupCoordinator coord(c);
        coord.SetClientHost(src, "h1");
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
        for (int i = 0; i < 4; ++i) {
            coord.OnPut(src, "", Item("t" + std::to_string(i), 100), t0);
        }
        EXPECT_EQ(coord.GetStats().batches_deferred, 2u);
        EXPECT_EQ(coord.GetStats().deferred_overflow, 1u);
    }
    {  // tasks in total, over sources
        auto c = cfg;
        c.max_deferred_tasks_total = 1;
        SsdBackupCoordinator coord(c);
        const UUID src2{5, 5};
        coord.SetClientHost(src, "h1");
        coord.SetClientHost(src2, "h5");
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
        coord.OnPut(src, "", Item("p0", 100), t0);
        coord.OnPut(src, "", Item("p1", 100), t0);   // waits
        coord.OnPut(src2, "", Item("q0", 100), t0);  // over the total
        EXPECT_EQ(coord.GetStats().batches_deferred, 1u);
        EXPECT_EQ(coord.GetStats().deferred_overflow, 1u);
    }
}

// Removing a source drops its waiting batches, counted (removed, unplaced),
// and the waiting gauges go back to zero.
TEST(SsdBackupCoordinatorTest, RemovedSourceWaitingBatchesAreCounted) {
    using Clock = SsdBackupCoordinator::Clock;
    auto cfg = TestConfig();
    cfg.batch_keys = 1;
    cfg.max_inflight_bytes_per_client = 100;
    cfg.candidate_stale_after = std::chrono::hours(1);
    cfg.defer_max_age = std::chrono::hours(1);
    SsdBackupCoordinator coord(cfg);
    const UUID src{1, 1}, holder{2, 2};
    const auto t0 = Clock::now();
    coord.SetClientHost(src, "h1");
    coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
    coord.OnPut(src, "", Item("m0", 100), t0);
    coord.OnPut(src, "", Item("m1", 100), t0);
    coord.OnPut(src, "", Item("m2", 100), t0);
    ASSERT_EQ(coord.GetStats().waiting_batches, 2u);
    EXPECT_GT(coord.GetStats().waiting_oldest_age_seconds, -1.0);
    coord.RemoveClient(src);
    const auto st = coord.GetStats();
    EXPECT_EQ(st.deferred_removed, 2u);
    EXPECT_EQ(st.objects_unplaced, 2u);
    EXPECT_EQ(st.waiting_batches, 0u);
    EXPECT_EQ(st.waiting_bytes, 0u);
    EXPECT_EQ(st.waiting_tasks, 0u);
    EXPECT_EQ(st.waiting_oldest_age_seconds, 0.0);
}

// A waiting batch goes to a client that joins later (a new candidate counts
// as freed room); one waiting only on a client that is then removed is not
// lost while another client can take it.
TEST(SsdBackupCoordinatorTest, WaitingBatchGoesToNewClient) {
    using Clock = SsdBackupCoordinator::Clock;
    using std::chrono::milliseconds;
    auto cfg = TestConfig();
    cfg.batch_keys = 1;
    cfg.max_inflight_bytes_per_client = 100;
    cfg.candidate_stale_after = std::chrono::hours(1);
    cfg.defer_max_age = milliseconds(60000);
    SsdBackupCoordinator coord(cfg);
    const UUID src{1, 1}, busy{2, 2}, fresh{3, 3};
    const auto t0 = Clock::now();
    coord.SetClientHost(src, "h1");
    coord.OnHeartbeat(busy, "h2", "ep2", 0, 1 << 30, t0);
    coord.OnPut(src, "", Item("n0", 100), t0);
    coord.OnPut(src, "", Item("n1", 100), t0);
    EXPECT_EQ(coord.GetStats().batches_deferred, 1u);
    auto b = coord.OnHeartbeat(busy, "h2", "ep2", 0, 1 << 30, t0).batches;
    ASSERT_EQ(b.size(), 1u);
    EXPECT_EQ(b[0].tasks[0].key, "n0");
    // Nothing completes; a later heartbeat changes nothing.
    EXPECT_TRUE(
        coord.OnHeartbeat(busy, "h2", "ep2", 0, 1 << 30, t0 + milliseconds(10))
            .batches.empty());
    coord.RemoveClient(busy);
    b = coord.OnHeartbeat(fresh, "h3", "ep3", 0, 1 << 30, t0 + milliseconds(20))
            .batches;
    ASSERT_EQ(b.size(), 1u);
    EXPECT_EQ(b[0].tasks[0].key, "n1");
    EXPECT_EQ(coord.GetStats().batches_unplaced, 0u);
}

// A waiting batch older than defer_max_age is dropped (counted unplaced);
// at most max_deferred_bytes_per_source wait; a batch with no eligible
// client at all, or larger than the whole inflight budget, is not kept.
TEST(SsdBackupCoordinatorTest, WaitingBatchesAreBounded) {
    using Clock = SsdBackupCoordinator::Clock;
    using std::chrono::milliseconds;
    const UUID src{1, 1}, holder{2, 2};
    const auto t0 = Clock::now();
    auto cfg = TestConfig();
    cfg.batch_keys = 1;
    cfg.max_inflight_bytes_per_client = 100;
    cfg.candidate_stale_after = std::chrono::hours(1);
    cfg.defer_max_age = milliseconds(1000);
    cfg.max_deferred_bytes_per_source = 250;
    {  // age and per-source bytes
        SsdBackupCoordinator coord(cfg);
        coord.SetClientHost(src, "h1");
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
        for (int i = 0; i < 4; ++i) {  // 1 dispatched, 2 wait, 1 over 250
            coord.OnPut(src, "", Item("a" + std::to_string(i), 100), t0);
        }
        EXPECT_EQ(coord.GetStats().batches_dispatched, 1u);
        EXPECT_EQ(coord.GetStats().batches_deferred, 2u);
        EXPECT_EQ(coord.GetStats().objects_unplaced, 1u);
        auto b = coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0).batches;
        ASSERT_EQ(b.size(), 1u);
        // Never acknowledged: the waiting ones age out.
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30,
                          t0 + milliseconds(999));
        EXPECT_EQ(coord.GetStats().deferred_expired, 0u);
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30,
                          t0 + milliseconds(1000));
        EXPECT_EQ(coord.GetStats().deferred_expired, 2u);
        EXPECT_EQ(coord.GetStats().objects_unplaced, 3u);
        // Room again after the completion: nothing old is resurrected.
        coord.OnBatchDone(holder, b[0].batch_id);
        EXPECT_TRUE(coord
                        .OnHeartbeat(holder, "h2", "ep", 0, 1 << 30,
                                     t0 + milliseconds(1001))
                        .batches.empty());
    }
    {  // no eligible client (only the source's own host): dropped at once
        SsdBackupCoordinator coord(cfg);
        coord.SetClientHost(src, "h1");
        coord.OnHeartbeat(holder, "h1", "ep", 0, 1 << 30, t0);
        coord.OnPut(src, "", Item("same-host", 10), t0);
        EXPECT_EQ(coord.GetStats().batches_deferred, 0u);
        EXPECT_EQ(coord.GetStats().batches_unplaced, 1u);
    }
    {  // larger than the inflight budget: never fits, dropped at once
        SsdBackupCoordinator coord(cfg);
        coord.SetClientHost(src, "h1");
        coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t0);
        coord.OnPut(src, "", Item("huge", 101), t0);
        EXPECT_EQ(coord.GetStats().batches_deferred, 0u);
        EXPECT_EQ(coord.GetStats().batches_unplaced, 1u);
    }
}

// MST-15: during the cooldown after repeated timeouts a late ACK of a
// timed-out batch changes nothing; after the cooldown the client is chosen
// again, and a completed batch resets the consecutive-timeout count.
TEST(SsdBackupCoordinatorTest, CooldownLateAckAndReset) {
    auto cfg = TestConfig();
    cfg.batch_keys = 1;
    cfg.task_timeout = std::chrono::milliseconds(100);
    cfg.max_consecutive_timeouts = 2;
    cfg.failure_cooldown = std::chrono::milliseconds(1000);
    cfg.candidate_stale_after = std::chrono::hours(1);
    SsdBackupCoordinator coord(cfg);
    using Clock = SsdBackupCoordinator::Clock;
    using std::chrono::milliseconds;
    const auto t0 = Clock::now();
    const UUID src{1, 1}, holder{2, 2};
    coord.SetClientHost(src, "h1");
    auto hb = [&](Clock::time_point t) {
        return coord.OnHeartbeat(holder, "h2", "ep", 0, 1 << 30, t).batches;
    };
    hb(t0);
    coord.OnPut(src, "", Item("t0"), t0);
    coord.OnPut(src, "", Item("t1"), t0);
    auto timed_out = hb(t0);
    ASSERT_EQ(timed_out.size(), 2u);
    hb(t0 + milliseconds(100));  // both time out: cooldown starts
    EXPECT_EQ(coord.GetStats().batches_timed_out, 2u);
    EXPECT_EQ(coord.GetStats().candidates_cooling_down, 1u);
    coord.OnBatchDone(holder, timed_out[0].batch_id);  // late ACK: ignored
    EXPECT_EQ(coord.GetStats().batches_completed, 0u);
    coord.OnPut(src, "", Item("during"), t0 + milliseconds(500));
    EXPECT_EQ(coord.GetStats().batches_unplaced, 1u);  // still cooling down
    const auto after = t0 + milliseconds(1200);
    hb(after);
    coord.OnPut(src, "", Item("after1"), after);
    auto b = hb(after);
    ASSERT_EQ(b.size(), 1u);
    hb(after + milliseconds(100));  // one timeout
    coord.OnPut(src, "", Item("after2"), after + milliseconds(100));
    b = hb(after + milliseconds(100));
    ASSERT_EQ(b.size(), 1u);
    coord.OnBatchDone(holder, b[0].batch_id);  // success resets the count
    coord.OnPut(src, "", Item("after3"), after + milliseconds(100));
    b = hb(after + milliseconds(100));
    ASSERT_EQ(b.size(), 1u);
    hb(after + milliseconds(200));  // one timeout again: no new cooldown
    EXPECT_EQ(coord.GetStats().candidates_cooling_down, 1u);
    coord.OnPut(src, "", Item("after4"), after + milliseconds(200));
    EXPECT_EQ(hb(after + milliseconds(200)).size(), 1u);
}

// R1-F08: master and worker parse MC_SSD_BACKUP_ENABLE the same way.
TEST(SsdBackupConfigTest, EnableParsedLikeWorker) {
    for (const char* v : {"1", "true", "TRUE", "True", "on", "ON", "yes"}) {
        setenv("MC_SSD_BACKUP_ENABLE", v, 1);
        EXPECT_TRUE(SsdBackupCoordinator::Config::FromEnvironment().enabled)
            << v;
    }
    for (const char* v : {"0", "false", "off", "", "bogus"}) {
        setenv("MC_SSD_BACKUP_ENABLE", v, 1);
        EXPECT_FALSE(SsdBackupCoordinator::Config::FromEnvironment().enabled)
            << v;
    }
    unsetenv("MC_SSD_BACKUP_ENABLE");
}

// R1-F10: the backup holder's restore pool was full (BUFFER_OVERFLOW, reported
// before any transfer to the reader): fall back to a fresh primary query.
TEST(SsdBackupReadFallbackPolicy, BackupBufferOverflowFallsBackToPrimary) {
    using E = ErrorCode;
    std::vector<std::optional<E>> err{E::BUFFER_OVERFLOW, E::BUFFER_OVERFLOW};
    std::vector<bool> via{true, true};
    std::vector<bool> prim{true, false};
    auto p = PlanReadFallback(err, via, prim, /*may_be_in_flight=*/false);
    EXPECT_EQ(p.requery, (std::vector<size_t>{0}));  // 1 has no primary
    EXPECT_TRUE(p.to_backup.empty());
    EXPECT_TRUE(p.mark_suspect.empty());
}

// ---- Reader suspect TTL (R1-M1, CFG-07) ----

// The TTL is read once per process (function-static), so each environment
// value runs in its own child process. Exit code 0: the endpoint is still
// suspect right after being marked; 1: it is not.
static int SuspectRightAfterMark(const char* ttl_env) {
    if (ttl_env) {
        setenv("MC_SSD_BACKUP_SUSPECT_TTL_MS", ttl_env, 1);
    } else {
        unsetenv("MC_SSD_BACKUP_SUSPECT_TTL_MS");
    }
    RealClient client;
    client.MarkSuspectEndpoint("ep");
    const bool suspect = client.IsSuspectEndpoint("ep");
    std::_Exit(suspect ? 0 : 1);
}

TEST(SsdBackupSuspectTtlDeathTest, HugeOrOverflowingTtlStillBypasses) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    // Before the fix, now + ms(INT64_MAX) wrapped into the past, so a failed
    // endpoint was never bypassed.
    EXPECT_EXIT(SuspectRightAfterMark("9223372036854775807"),
                ::testing::ExitedWithCode(0), "");
    EXPECT_EXIT(SuspectRightAfterMark("99999999999999999999"),
                ::testing::ExitedWithCode(0), "");
    EXPECT_EXIT(SuspectRightAfterMark("86400001"),  // > 24 h: default
                ::testing::ExitedWithCode(0), "");
}

TEST(SsdBackupSuspectTtlDeathTest, DefaultZeroAndInvalid) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(SuspectRightAfterMark(nullptr), ::testing::ExitedWithCode(0),
                "");
    EXPECT_EXIT(SuspectRightAfterMark(""), ::testing::ExitedWithCode(0), "");
    EXPECT_EXIT(SuspectRightAfterMark("5000"), ::testing::ExitedWithCode(0),
                "");
    EXPECT_EXIT(SuspectRightAfterMark("-1"), ::testing::ExitedWithCode(0), "");
    EXPECT_EXIT(SuspectRightAfterMark("abc"), ::testing::ExitedWithCode(0), "");
    // 0 disables the bypass: not suspect even right after a failure.
    EXPECT_EXIT(SuspectRightAfterMark("0"), ::testing::ExitedWithCode(1), "");
}

// R2-PARSER-BEGIN (uses helpers added in round 2; stripped from red trees)
TEST(SsdBackupSuspectTtl, ParserBoundaries) {
    using std::chrono::milliseconds;
    bool bad = true;
    EXPECT_EQ(ParseSuspectTtlMs(nullptr, &bad), kDefaultSuspectTtl);
    EXPECT_FALSE(bad);
    EXPECT_EQ(ParseSuspectTtlMs("", &bad), kDefaultSuspectTtl);
    EXPECT_FALSE(bad);
    EXPECT_EQ(ParseSuspectTtlMs("0", &bad), milliseconds(0));
    EXPECT_FALSE(bad);
    EXPECT_EQ(ParseSuspectTtlMs("1500", &bad), milliseconds(1500));
    EXPECT_FALSE(bad);
    EXPECT_EQ(ParseSuspectTtlMs("86400000", &bad), kMaxSuspectTtl);
    EXPECT_FALSE(bad);
    for (const char* v : {"-1", "+5", "abc", "12x", " 7", "86400001",
                          "9223372036854775807", "99999999999999999999"}) {
        SCOPED_TRACE(v);
        EXPECT_EQ(ParseSuspectTtlMs(v, &bad), kDefaultSuspectTtl);
        EXPECT_TRUE(bad);
    }
}

TEST(SsdBackupSuspectTtl, DeadlineCreationAndExpiry) {
    using std::chrono::milliseconds;
    const auto t0 = std::chrono::steady_clock::now();
    const auto d = SuspectDeadline(t0, milliseconds(30000));
    EXPECT_EQ(d - t0, milliseconds(30000));
    EXPECT_LT(t0 + milliseconds(29999), d);  // still suspect
    EXPECT_GE(t0 + milliseconds(30000), d);  // expired
    // A fresh failure later moves the deadline forward from that time.
    const auto t1 = t0 + milliseconds(10000);
    EXPECT_EQ(SuspectDeadline(t1, milliseconds(30000)) - t1,
              milliseconds(30000));
    // Zero: expired at once; out-of-range inputs are bounded, never wrap.
    EXPECT_EQ(SuspectDeadline(t0, milliseconds(0)), t0);
    EXPECT_EQ(SuspectDeadline(t0, milliseconds(-5)), t0);
    EXPECT_EQ(SuspectDeadline(t0, milliseconds::max()) - t0, kMaxSuspectTtl);
}
// R2-PARSER-END

// R3-NEW-BEGIN (uses the explicit-time suspect overloads added in round 3)
// CFG-07: the real suspect map with a controlled clock (default 30 s TTL):
// suspect at 29,999 ms, gone at 30,000 ms (entry removed), and a later
// failure renews the same endpoint from that time.
TEST(SsdBackupSuspectTtl, SuspectMapExpiryAndRenewal) {
    using std::chrono::milliseconds;
    RealClient client;
    const auto t0 = std::chrono::steady_clock::now();
    client.MarkSuspectEndpoint("ep", t0);
    EXPECT_EQ(client.SuspectEndpointCountForTest(), 1u);
    EXPECT_TRUE(client.IsSuspectEndpoint("ep", t0 + milliseconds(29999)));
    EXPECT_FALSE(client.IsSuspectEndpoint("ep", t0 + milliseconds(30000)));
    EXPECT_EQ(client.SuspectEndpointCountForTest(), 0u);  // removed
    EXPECT_FALSE(client.IsSuspectEndpoint("ep", t0 + milliseconds(29999)));
    // Renewal: marked again at 20 s, still suspect at 49.999 s.
    client.MarkSuspectEndpoint("ep", t0);
    client.MarkSuspectEndpoint("ep", t0 + milliseconds(20000));
    EXPECT_EQ(client.SuspectEndpointCountForTest(), 1u);
    EXPECT_TRUE(client.IsSuspectEndpoint("ep", t0 + milliseconds(49999)));
    EXPECT_FALSE(client.IsSuspectEndpoint("ep", t0 + milliseconds(50000)));
    // Endpoints are independent.
    client.MarkSuspectEndpoint("a", t0);
    EXPECT_FALSE(client.IsSuspectEndpoint("b", t0));
    EXPECT_TRUE(client.IsSuspectEndpoint("a", t0));
}
// R3-NEW-END

// CFG-07: with MC_SSD_BACKUP_READ_FALLBACK unset (this process) the real
// wrapper reads once, as before: no suspect check, no backup query, no
// retry, and a failed key stays failed.
TEST(SsdBackupReadFallback, DisabledWrapperReadsOnce) {
    ASSERT_EQ(std::getenv("MC_SSD_BACKUP_READ_FALLBACK"), nullptr);
    RealClient client;  // no client_: any backup query would crash
    std::vector<tl::expected<QueryResult, ErrorCode>> queries;
    queries.emplace_back(tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
    queries.emplace_back(tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
    int reads = 0;
    auto results = client.ReadWithBackupFallback(
        {"k0", "k1"}, std::move(queries),
        [&](const std::vector<size_t>& idx, const auto&) {
            ++reads;
            std::vector<tl::expected<int64_t, ErrorCode>> out;
            for (size_t i : idx) {
                if (i == 0) {
                    out.emplace_back(int64_t{7});
                } else {
                    out.emplace_back(
                        tl::make_unexpected(ErrorCode::TRANSFER_FAIL));
                }
            }
            return out;
        });
    EXPECT_EQ(reads, 1);
    ASSERT_EQ(results.size(), 2u);
    ASSERT_TRUE(results[0].has_value());
    EXPECT_EQ(results[0].value(), 7);
    ASSERT_FALSE(results[1].has_value());
    EXPECT_EQ(results[1].error(), ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(client.IsSuspectEndpoint("anything"), false);
}

// ---- MasterService ----

class SsdBackupMasterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        setenv("MC_SSD_BACKUP_ENABLE", "1", 1);
        setenv("MC_SSD_BACKUP_BATCH_KEYS", "1", 1);
        MasterServiceConfig config;
        config.enable_offload = true;
        config.default_kv_lease_ttl = lease_ttl_ms_;
        config.promotion_on_hit = promotion_on_hit_;
        config.promotion_admission_threshold = 1;
        service_ = std::make_unique<MasterService>(config);
        Mount(a_, "seg_a", 0x100000000);
        Mount(b_, "seg_b", 0x200000000);
        Heartbeat(a_);
        Heartbeat(b_);
    }
    void TearDown() override {
        service_.reset();
        unsetenv("MC_SSD_BACKUP_ENABLE");
        unsetenv("MC_SSD_BACKUP_BATCH_KEYS");
    }

    void Mount(const UUID& client, const std::string& name, size_t base,
               const std::string& host = "") {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = name;
        segment.base = base;
        segment.size = 64 * 1024 * 1024;
        segment.te_endpoint = name;
        segment.host_id = host.empty() ? "host_" + name : host;
        if (client == a_ || client == b_ || client == c_) {
            segments_[client == a_ ? 0 : (client == b_ ? 1 : 2)] = segment.id;
        }
        ASSERT_TRUE(service_->MountSegment(segment, client).has_value());
        ASSERT_TRUE(service_->MountLocalDiskSegment(client, true).has_value());
        ASSERT_TRUE(service_->ReportSsdCapacity(client, 1 << 30).has_value());
    }

    // Deliver and acknowledge the one pending backup batch, wherever the
    // coordinator placed it; returns the holder.
    UUID BackupDoneAnyHolder(const UUID& src) {
        for (const UUID& c : {a_, b_, c_}) {
            if (c == src) continue;
            auto r = service_->BackupObjectHeartbeat(
                c, c == a_ ? "seg_a" : (c == b_ ? "seg_b" : "seg_c"), 0,
                1 << 30);
            if (!r.has_value() || r->batches.empty()) continue;
            std::vector<StorageObjectMetadata> metas(
                r->batches[0].tasks.size());
            for (auto& m : metas) {
                m.data_size = 1024;
                m.transport_endpoint = "x";
            }
            EXPECT_TRUE(service_
                            ->NotifyBackupDone(c, r->batches[0].batch_id,
                                               r->batches[0].tasks, metas)
                            .has_value());
            holder_for_residency_ = c;
            return c;
        }
        ADD_FAILURE() << "no backup batch was dispatched";
        return UUID{};
    }

    UUID SegOf(const UUID& client) const {
        return segments_[client == a_ ? 0 : (client == b_ ? 1 : 2)];
    }

    BackupHeartbeatResponse Heartbeat(const UUID& client) {
        auto r = service_->BackupObjectHeartbeat(
            client, client == a_ ? "seg_a" : (client == b_ ? "seg_b" : "seg_c"),
            0, 1 << 30);
        EXPECT_TRUE(r.has_value());
        return r ? std::move(*r) : BackupHeartbeatResponse{};
    }

    // Put `key`; return the DRAM owner (the backup source).
    UUID Put(const std::string& key, int64_t size = 1024) {
        ReplicateConfig config;
        config.replica_num = 1;
        EXPECT_TRUE(
            service_->PutStart(a_, key, TenantId::Default(), size, config)
                .has_value());
        EXPECT_TRUE(
            service_->PutEnd(a_, key, TenantId::Default(), ReplicaType::MEMORY)
                .has_value());
        auto list = service_->GetReplicaList(key, TenantId::Default());
        EXPECT_TRUE(list.has_value());
        const auto& mem = list->replicas.at(0).get_memory_descriptor();
        const auto& ep = mem.buffer_descriptor.transport_endpoint_;
        return ep == "seg_a" ? a_ : (ep == "seg_b" ? b_ : c_);
    }

    void PrimaryOffload(const UUID& owner, const std::string& key,
                        int64_t size = 1024) {
        StorageObjectMetadata meta;
        meta.data_size = size;
        meta.transport_endpoint =
            owner == a_ ? "seg_a" : (owner == b_ ? "seg_b" : "seg_c");
        OffloadTaskItem task{
            .tenant_id = TenantId::Default().value(), .key = key, .size = size};
        ASSERT_TRUE(
            service_->NotifyOffloadSuccess(owner, {task}, {meta}).has_value());
    }

    // Deliver the backup batch for `key` and acknowledge it as written.
    void BackupDone(const UUID& target, const std::string& key,
                    int64_t size = 1024) {
        holder_for_residency_ = target;
        auto resp = Heartbeat(target);
        ASSERT_EQ(resp.batches.size(), 1u);
        const auto& batch = resp.batches[0];
        ASSERT_EQ(batch.tasks.size(), 1u);
        EXPECT_EQ(batch.tasks[0].key, key);
        StorageObjectMetadata meta;
        meta.data_size = size;
        meta.transport_endpoint = target == a_ ? "seg_a" : "seg_b";
        ASSERT_TRUE(
            service_
                ->NotifyBackupDone(target, batch.batch_id, batch.tasks, {meta})
                .has_value());
    }

    BackupResidency Residency(const std::string& key) {
        return ResidencyFor(holder_for_residency_, key);
    }
    BackupResidency ResidencyFor(const UUID& holder, const std::string& key) {
        auto r = service_->QueryBackupResidency(
            holder, {BackupRefreshItem{TenantId::Default().value(), key}});
        EXPECT_TRUE(r.has_value());
        return static_cast<BackupResidency>(r->at(0));
    }

    static size_t CountBackups(const GetReplicaListResponse& list) {
        size_t n = 0;
        for (const auto& d : list.replicas) {
            if (d.is_local_disk_replica() &&
                d.get_local_disk_descriptor().backup.has_value() &&
                d.get_local_disk_descriptor().backup.value()) {
                ++n;
            }
        }
        return n;
    }

    bool promotion_on_hit_ = false;
    uint64_t lease_ttl_ms_ = 0;
    UUID holder_for_residency_{};
    UUID c_{0xc, 0xc};
    UUID a_{0xa, 0xa};
    UUID b_{0xb, 0xb};
    UUID segments_[3];
    std::unique_ptr<MasterService> service_;
};

TEST_F(SsdBackupMasterTest, BackupHiddenWhilePrimaryReadable) {
    const UUID src = Put("k1");
    const UUID dst = src == a_ ? b_ : a_;
    PrimaryOffload(src, "k1");
    BackupDone(dst, "k1");
    EXPECT_EQ(service_->GetSsdBackupStatsForTest().batches_completed, 1u);

    auto list = service_->GetReplicaList("k1", TenantId::Default());
    ASSERT_TRUE(list.has_value());
    EXPECT_EQ(CountBackups(*list), 0u);
    EXPECT_EQ(Residency("k1"), BackupResidency::kInDram);
    EXPECT_EQ(Residency("missing"), BackupResidency::kMissing);
}

TEST_F(SsdBackupMasterTest, ReadablePrimaryOrderIgnoresBackupPosition) {
    const std::string key = "primary-order";
    const UUID src = Put(key);
    const UUID dst = src == a_ ? b_ : a_;
    PrimaryOffload(src, key);
    auto primary = service_->GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(primary.has_value());
    ASSERT_EQ(primary->replicas.size(), 2u);
    const auto expected =
        struct_pack::serialize<std::string>(primary->replicas);
    BackupDone(dst, key);

    // Exercise backup after, before and between the readable DRAM/SSD
    // primaries. All primary descriptors, including their order, must survive.
    for (int position : {2, 0, 1}) {
        SCOPED_TRACE(position);
        if (position != 2) {
            std::shared_lock lock(
                MasterServiceTestPeer::MasterStateMutex(*service_));
            auto object = MasterServiceTestPeer::WriteObject(
                *service_, {TenantId::Default(), key});
            ASSERT_TRUE(object.has_value());
            auto replicas = object->metadata().MutablePart(0).replicas();
            ASSERT_EQ(replicas.size(), 3u);
            if (position == 0) {
                ASSERT_TRUE(replicas.back().is_local_disk_backup_replica());
                std::rotate(replicas.begin(), replicas.end() - 1,
                            replicas.end());
            } else {
                ASSERT_TRUE(replicas.front().is_local_disk_backup_replica());
                std::rotate(replicas.begin(), replicas.begin() + 1,
                            replicas.begin() + 2);
            }
        }
        auto actual = service_->GetReplicaList(key, TenantId::Default());
        ASSERT_TRUE(actual.has_value());
        EXPECT_EQ(struct_pack::serialize<std::string>(actual->replicas),
                  expected);
    }
}

TEST_F(SsdBackupMasterTest, BackupServesReadAfterOwnerUnmount) {
    const UUID src = Put("k2");
    const UUID dst = src == a_ ? b_ : a_;
    PrimaryOffload(src, "k2");
    BackupDone(dst, "k2");

    // Owner loses its DRAM segment and its SSD.
    ASSERT_TRUE(service_->UnmountSegment(SegOf(src), src).has_value());
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(src).has_value());

    auto list = service_->GetReplicaList("k2", TenantId::Default());
    ASSERT_TRUE(list.has_value()) << list.error();
    ASSERT_EQ(list->replicas.size(), 1u);
    EXPECT_EQ(CountBackups(*list), 1u);
    EXPECT_EQ(list->replicas[0].get_local_disk_descriptor().client_id, dst);
    // Not stored on the replica; filled from the holder's backup heartbeat.
    EXPECT_EQ(list->replicas[0].get_local_disk_descriptor().transport_endpoint,
              dst == a_ ? "seg_a" : "seg_b");
    EXPECT_EQ(Residency("k2"), BackupResidency::kOwnerGone);
}

TEST_F(SsdBackupMasterTest, PrimarySsdReadRefreshesBackup) {
    const UUID src = Put("k3");
    const UUID dst = src == a_ ? b_ : a_;
    PrimaryOffload(src, "k3");
    BackupDone(dst, "k3");
    // Drop DRAM only: primary SSD serves the read.
    ASSERT_TRUE(service_->UnmountSegment(SegOf(src), src).has_value());
    EXPECT_EQ(Residency("k3"), BackupResidency::kNoDram);

    auto list = service_->GetReplicaList("k3", TenantId::Default());
    ASSERT_TRUE(list.has_value());
    EXPECT_EQ(CountBackups(*list), 0u);
    auto resp = Heartbeat(dst);
    ASSERT_EQ(resp.refreshes.size(), 1u);
    EXPECT_EQ(resp.refreshes[0].key, "k3");
}

// Review F2: a same-size Upsert of an object held only by its SSD backup
// must reallocate, not "update in place" by dropping the only copy.
TEST_F(SsdBackupMasterTest, UpsertOfBackupOnlyObjectReallocates) {
    const UUID src = Put("ku");
    const UUID dst = src == a_ ? b_ : a_;
    PrimaryOffload(src, "ku");
    BackupDone(dst, "ku");
    ASSERT_TRUE(service_->UnmountSegment(SegOf(src), src).has_value());
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(src).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    auto started =
        service_->UpsertStart(dst, "ku", TenantId::Default(), 1024, config);
    ASSERT_TRUE(started.has_value()) << started.error();
    ASSERT_EQ(started->size(), 1u);
    EXPECT_TRUE((*started)[0].is_memory_replica());
    ASSERT_TRUE(
        service_->UpsertEnd(dst, "ku", TenantId::Default(), ReplicaType::MEMORY)
            .has_value());
    auto list = service_->GetReplicaList("ku", TenantId::Default());
    ASSERT_TRUE(list.has_value());
    EXPECT_EQ(CountBackups(*list), 0u);  // the old value's backup is gone
}

// ... and when no new buffer can be allocated, the old value stays readable.
TEST_F(SsdBackupMasterTest, FailedUpsertOfBackupOnlyObjectKeepsBackup) {
    const UUID src = Put("kv");
    const UUID dst = src == a_ ? b_ : a_;
    PrimaryOffload(src, "kv");
    BackupDone(dst, "kv");
    ASSERT_TRUE(service_->UnmountSegment(SegOf(src), src).has_value());
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(src).has_value());
    ASSERT_TRUE(service_->UnmountSegment(SegOf(dst), dst).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    EXPECT_FALSE(
        service_->UpsertStart(dst, "kv", TenantId::Default(), 1024, config)
            .has_value());
    auto list = service_->GetReplicaList("kv", TenantId::Default());
    ASSERT_TRUE(list.has_value()) << list.error();
    EXPECT_EQ(CountBackups(*list), 1u);
}

TEST_F(SsdBackupMasterTest, BackupHolderUnmountDropsBackup) {
    const UUID src = Put("k4");
    const UUID dst = src == a_ ? b_ : a_;
    PrimaryOffload(src, "k4");
    BackupDone(dst, "k4");
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(dst).has_value());
    // Owner then goes away too: no backup left to serve.
    ASSERT_TRUE(service_->UnmountSegment(SegOf(src), src).has_value());
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(src).has_value());
    auto list = service_->GetReplicaList("k4", TenantId::Default());
    EXPECT_FALSE(list.has_value());
}

TEST_F(SsdBackupMasterTest, StaleVersionRejected) {
    const UUID src = Put("k5");
    const UUID dst = src == a_ ? b_ : a_;
    auto resp = Heartbeat(dst);
    ASSERT_EQ(resp.batches.size(), 1u);
    auto stale = resp.batches[0].tasks;
    stale[0].version += 1;  // as if the key was re-put after batching
    StorageObjectMetadata meta;
    meta.data_size = 1024;
    meta.transport_endpoint = "x";
    PrimaryOffload(src, "k5");
    const auto before = service_->GetSsdBackupStatsForTest();
    auto done = service_->NotifyBackupDone(dst, resp.batches[0].batch_id, stale,
                                           {meta});
    ASSERT_TRUE(done.has_value());
    // The refused entry is reported back so the holder drops its copy.
    EXPECT_EQ(done.value(), std::vector<uint32_t>{0});
    // A non-empty ACK that registered nothing is not a backup for the
    // put-to-backup latency.
    EXPECT_EQ(service_->GetSsdBackupStatsForTest().backed_up_batches,
              before.backed_up_batches);
    ASSERT_TRUE(service_->UnmountSegment(SegOf(src), src).has_value());
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(src).has_value());
    EXPECT_FALSE(
        service_->GetReplicaList("k5", TenantId::Default()).has_value());
}

TEST_F(SsdBackupMasterTest, DuplicateAckAddsOneBackup) {
    const UUID src = Put("k6");
    const UUID dst = src == a_ ? b_ : a_;
    auto resp = Heartbeat(dst);
    ASSERT_EQ(resp.batches.size(), 1u);
    const auto tasks = resp.batches[0].tasks;
    StorageObjectMetadata meta;
    meta.data_size = 1024;
    meta.transport_endpoint = "x";
    PrimaryOffload(src, "k6");
    const auto before = service_->GetSsdBackupStatsForTest();
    ASSERT_TRUE(service_->NotifyBackupDone(dst, 1, tasks, {meta}).has_value());
    ASSERT_TRUE(service_->NotifyBackupDone(dst, 1, tasks, {meta}).has_value());
    // Registered once: one latency sample (the repeated ACK finds the batch
    // no longer in flight).
    EXPECT_EQ(service_->GetSsdBackupStatsForTest().backed_up_batches,
              before.backed_up_batches + 1);
    ASSERT_TRUE(service_->UnmountSegment(SegOf(src), src).has_value());
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(src).has_value());
    auto list = service_->GetReplicaList("k6", TenantId::Default());
    ASSERT_TRUE(list.has_value());
    EXPECT_EQ(list->replicas.size(), 1u);
    EXPECT_EQ(CountBackups(*list), 1u);
}

// MST-09: an empty ACK (nothing written) releases the batch but is not
// protection: no backup replica, and the object is lost with its owner.
TEST_F(SsdBackupMasterTest, EmptyAckIsNotProtection) {
    const UUID src = Put("ke");
    const UUID dst = src == a_ ? b_ : a_;
    auto resp = Heartbeat(dst);
    ASSERT_EQ(resp.batches.size(), 1u);
    const auto before = service_->GetSsdBackupStatsForTest();
    auto done =
        service_->NotifyBackupDone(dst, resp.batches[0].batch_id, {}, {});
    ASSERT_TRUE(done.has_value());
    EXPECT_TRUE(done->empty());
    EXPECT_EQ(service_->GetSsdBackupStatsForTest().batches_completed,
              before.batches_completed + 1);
    auto backups =
        service_->BatchGetBackupReplicaList({"ke"}, TenantId::Default());
    ASSERT_EQ(backups.size(), 1u);
    EXPECT_FALSE(backups[0].has_value());
    ASSERT_TRUE(service_->UnmountSegment(SegOf(src), src).has_value());
    EXPECT_FALSE(
        service_->GetReplicaList("ke", TenantId::Default()).has_value());
}

// MST-09: ACKs arriving out of order across generations. v2's backup is
// registered first; the late ACK of v1's batch is refused as stale and does
// not replace or remove v2's backup.
TEST_F(SsdBackupMasterTest, ReorderedGenerationAcks) {
    const UUID src1 = Put("kr");
    const UUID dst1 = src1 == a_ ? b_ : a_;
    auto r1 = Heartbeat(dst1);
    ASSERT_EQ(r1.batches.size(), 1u);
    const BackupBatch v1 = r1.batches[0];
    ASSERT_TRUE(service_->Remove("kr", TenantId::Default(), true).has_value());
    const UUID src2 = Put("kr");
    const UUID dst2 = src2 == a_ ? b_ : a_;
    auto r2 = Heartbeat(dst2);
    ASSERT_EQ(r2.batches.size(), 1u);
    const BackupBatch v2 = r2.batches[0];
    ASSERT_NE(v1.tasks[0].version, v2.tasks[0].version);
    StorageObjectMetadata meta;
    meta.data_size = 1024;
    meta.transport_endpoint = "x";
    auto ack2 = service_->NotifyBackupDone(dst2, v2.batch_id, v2.tasks, {meta});
    ASSERT_TRUE(ack2.has_value());
    EXPECT_TRUE(ack2->empty());
    auto ack1 = service_->NotifyBackupDone(dst1, v1.batch_id, v1.tasks, {meta});
    ASSERT_TRUE(ack1.has_value());
    EXPECT_EQ(ack1.value(), std::vector<uint32_t>{0});  // stale
    auto backups =
        service_->BatchGetBackupReplicaList({"kr"}, TenantId::Default());
    ASSERT_EQ(backups.size(), 1u);
    ASSERT_TRUE(backups[0].has_value());
    ASSERT_EQ(backups[0]->replicas.size(), 1u);
    EXPECT_EQ(backups[0]->replicas[0].get_local_disk_descriptor().client_id,
              dst2);
}

// RPC-03: malformed or out-of-place backup requests and their defined
// results (BackupObjectHeartbeat, NotifyBackupDone, QueryBackupResidency,
// BatchGetBackupReplicaList).
TEST_F(SsdBackupMasterTest, MalformedBackupRequests) {
    const UUID unknown{0x99, 0x99};
    // Heartbeat: negative usage or capacity; unknown client.
    auto hb = service_->BackupObjectHeartbeat(a_, "seg_a", -1, 1 << 30);
    ASSERT_FALSE(hb.has_value());
    EXPECT_EQ(hb.error(), ErrorCode::INVALID_PARAMS);
    hb = service_->BackupObjectHeartbeat(a_, "seg_a", 0, -1);
    ASSERT_FALSE(hb.has_value());
    EXPECT_EQ(hb.error(), ErrorCode::INVALID_PARAMS);
    hb = service_->BackupObjectHeartbeat(unknown, "x", 0, 1 << 30);
    ASSERT_FALSE(hb.has_value());
    EXPECT_EQ(hb.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);

    const UUID src = Put("km");
    const UUID dst = src == a_ ? b_ : a_;
    auto resp = Heartbeat(dst);
    ASSERT_EQ(resp.batches.size(), 1u);
    const auto batch = resp.batches[0];
    StorageObjectMetadata meta;
    meta.data_size = 1024;
    meta.transport_endpoint = "x";
    // Tasks and metadatas of different length.
    auto done =
        service_->NotifyBackupDone(dst, batch.batch_id, batch.tasks, {});
    ASSERT_FALSE(done.has_value());
    EXPECT_EQ(done.error(), ErrorCode::INVALID_PARAMS);
    // Zero and negative sizes: skipped, not stale, no backup added.
    for (int64_t size : {int64_t{0}, int64_t{-1}}) {
        StorageObjectMetadata bad = meta;
        bad.data_size = size;
        done =
            service_->NotifyBackupDone(dst, batch.batch_id, batch.tasks, {bad});
        ASSERT_TRUE(done.has_value());
        EXPECT_TRUE(done->empty());
    }
    // Unknown holder.
    done = service_->NotifyBackupDone(unknown, batch.batch_id, batch.tasks,
                                      {meta});
    ASSERT_FALSE(done.has_value());
    EXPECT_EQ(done.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    // Unknown key in the task: reported stale.
    BackupTaskItem ghost = batch.tasks[0];
    ghost.key = "no-such-key";
    done = service_->NotifyBackupDone(dst, batch.batch_id, {ghost}, {meta});
    ASSERT_TRUE(done.has_value());
    EXPECT_EQ(done.value(), std::vector<uint32_t>{0});
    // Nothing above registered a backup for "km".
    auto none =
        service_->BatchGetBackupReplicaList({"km"}, TenantId::Default());
    ASSERT_EQ(none.size(), 1u);
    EXPECT_FALSE(none[0].has_value());
    // Residency: more than 1000 keys is refused; an empty query is fine;
    // an unknown key answers kMissing.
    std::vector<BackupRefreshItem> too_many(
        1001, BackupRefreshItem{TenantId::Default().value(), "km"});
    auto res = service_->QueryBackupResidency(dst, too_many);
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::INVALID_PARAMS);
    res = service_->QueryBackupResidency(dst, {});
    ASSERT_TRUE(res.has_value());
    EXPECT_TRUE(res->empty());
    EXPECT_EQ(ResidencyFor(dst, "no-such-key"), BackupResidency::kMissing);
}

// RPC-03: a backup registered by a holder that never sent a backup heartbeat
// has no endpoint, so it is not handed to readers (REPLICA_IS_NOT_READY).
TEST_F(SsdBackupMasterTest, BackupWithoutHolderEndpointIsNotServed) {
    // c_ has only a local disk (no DRAM segment, so Put never lands on it)
    // and never sends a backup heartbeat.
    ASSERT_TRUE(service_->MountLocalDiskSegment(c_, true).has_value());
    const UUID src = Put("kn");
    const UUID dst = src == a_ ? b_ : a_;
    auto resp = Heartbeat(dst);  // the batch went to dst; c_ reports it
    ASSERT_EQ(resp.batches.size(), 1u);
    StorageObjectMetadata meta;
    meta.data_size = 1024;
    ASSERT_TRUE(service_
                    ->NotifyBackupDone(c_, resp.batches[0].batch_id,
                                       resp.batches[0].tasks, {meta})
                    .has_value());
    auto list =
        service_->BatchGetBackupReplicaList({"kn"}, TenantId::Default());
    ASSERT_EQ(list.size(), 1u);
    ASSERT_FALSE(list[0].has_value());
    EXPECT_EQ(list[0].error(), ErrorCode::REPLICA_IS_NOT_READY);

    // The normal read path must also reject the backup when it is the only
    // remaining copy, until the holder publishes its endpoint.
    ASSERT_TRUE(service_->UnmountSegment(SegOf(src), src).has_value());
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(src).has_value());
    auto only_backup = service_->GetReplicaList("kn", TenantId::Default());
    ASSERT_FALSE(only_backup.has_value());
    EXPECT_EQ(only_backup.error(), ErrorCode::REPLICA_IS_NOT_READY);

    Heartbeat(c_);
    only_backup = service_->GetReplicaList("kn", TenantId::Default());
    ASSERT_TRUE(only_backup.has_value());
    ASSERT_EQ(only_backup->replicas.size(), 1u);
    EXPECT_EQ(CountBackups(*only_backup), 1u);
    const auto& descriptor =
        only_backup->replicas[0].get_local_disk_descriptor();
    EXPECT_EQ(descriptor.client_id, c_);
    EXPECT_EQ(descriptor.transport_endpoint, "seg_c");
}

// MST-12: with a non-zero lease, a primary read and a backup read each hold
// a read lease: Remove is refused (OBJECT_HAS_LEASE) until it expires, and
// works after. The backup stays intact while the lease is held.
class SsdBackupLeaseTest : public SsdBackupMasterTest {
   protected:
    void SetUp() override {
        lease_ttl_ms_ = 300;
        SsdBackupMasterTest::SetUp();
    }
};

TEST_F(SsdBackupLeaseTest, ReadLeasesBlockRemoveUntilExpiry) {
    using std::chrono::milliseconds;
    // Primary lease.
    Put("kl1");
    ASSERT_TRUE(
        service_->GetReplicaList("kl1", TenantId::Default()).has_value());
    auto removed = service_->Remove("kl1", TenantId::Default());
    ASSERT_FALSE(removed.has_value());
    EXPECT_EQ(removed.error(), ErrorCode::OBJECT_HAS_LEASE);
    std::this_thread::sleep_for(milliseconds(400));
    EXPECT_TRUE(service_->Remove("kl1", TenantId::Default()).has_value());
    // Release kl1's dispatched backup batch (empty ACK), so the next
    // heartbeat carries only kl2's.
    for (const UUID& c : {a_, b_}) {
        for (const auto& b : Heartbeat(c).batches) {
            (void)service_->NotifyBackupDone(c, b.batch_id, {}, {});
        }
    }

    // Backup lease: only the backup is left after the owner goes away.
    const UUID src = Put("kl2");
    const UUID dst = src == a_ ? b_ : a_;
    PrimaryOffload(src, "kl2");
    BackupDone(dst, "kl2");
    std::this_thread::sleep_for(milliseconds(400));  // Put's own lease
    ASSERT_TRUE(service_->UnmountSegment(SegOf(src), src).has_value());
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(src).has_value());
    auto backups =
        service_->BatchGetBackupReplicaList({"kl2"}, TenantId::Default());
    ASSERT_EQ(backups.size(), 1u);
    ASSERT_TRUE(backups[0].has_value());
    EXPECT_EQ(backups[0]->lease_ttl_ms, 300u);
    removed = service_->Remove("kl2", TenantId::Default());
    ASSERT_FALSE(removed.has_value());
    EXPECT_EQ(removed.error(), ErrorCode::OBJECT_HAS_LEASE);
    auto still = service_->GetReplicaList("kl2", TenantId::Default());
    ASSERT_TRUE(still.has_value());
    EXPECT_EQ(CountBackups(*still), 1u);
    std::this_thread::sleep_for(milliseconds(400));
    EXPECT_TRUE(service_->Remove("kl2", TenantId::Default()).has_value());
    EXPECT_FALSE(
        service_->GetReplicaList("kl2", TenantId::Default()).has_value());
}

// MST-12 (mutations): while a read lease is held, eviction keeps the leased
// DRAM copy and Upsert (same or different size) of an object with an SSD
// primary and a backup is refused with OBJECT_HAS_LEASE, keeping both. After
// the lease expires, eviction drops the DRAM copy and Upsert goes through.
TEST_F(SsdBackupLeaseTest, LeasesHoldEvictionAndUpsertUntilExpiry) {
    using std::chrono::milliseconds;
    const auto memory_replicas = [](const GetReplicaListResponse& l) {
        size_t n = 0;
        for (const auto& d : l.replicas) {
            if (d.is_memory_replica() && d.status == ReplicaStatus::COMPLETE)
                ++n;
        }
        return n;
    };
    const auto backups_of = [&](const std::string& key) {
        auto r =
            service_->BatchGetBackupReplicaList({key}, TenantId::Default());
        return r.size() == 1 && r[0].has_value() ? CountBackups(*r[0]) : 0u;
    };
    // Eviction against a held lease. The object's SSD primary and backup
    // have landed, so nothing but the lease keeps eviction from freeing the
    // DRAM copy (a memory-only object would be queued for offload instead).
    const UUID esrc = Put("ke");
    PrimaryOffload(esrc, "ke");
    BackupDone(esrc == a_ ? b_ : a_, "ke");
    std::this_thread::sleep_for(milliseconds(400));  // earlier leases
    ASSERT_TRUE(
        service_->GetReplicaList("ke", TenantId::Default()).has_value());
    MasterServiceTestPeer(*service_).RunBatchEvictForTesting(1.0, 1.0);
    auto kept = service_->GetReplicaList("ke", TenantId::Default());
    ASSERT_TRUE(kept.has_value()) << "leased object evicted";
    EXPECT_EQ(memory_replicas(*kept), 1u) << "leased DRAM copy evicted";
    std::this_thread::sleep_for(milliseconds(400));  // that read's lease too
    MasterServiceTestPeer(*service_).RunBatchEvictForTesting(1.0, 1.0);
    auto gone = service_->GetReplicaList("ke", TenantId::Default());
    ASSERT_TRUE(gone.has_value()) << "SSD primary must stay";
    EXPECT_EQ(memory_replicas(*gone), 0u)
        << "expired lease still protected the DRAM copy";
    EXPECT_EQ(backups_of("ke"), 1u);

    // Upsert against a held lease (SSD primary + backup).
    const UUID src = Put("ku");
    const UUID dst = src == a_ ? b_ : a_;
    PrimaryOffload(src, "ku");
    BackupDone(dst, "ku");
    std::this_thread::sleep_for(milliseconds(400));
    ASSERT_TRUE(
        service_->GetReplicaList("ku", TenantId::Default()).has_value());
    ReplicateConfig config;
    config.replica_num = 1;
    for (int64_t size : {1024, 2048}) {
        SCOPED_TRACE(size);
        auto up =
            service_->UpsertStart(src, "ku", TenantId::Default(), size, config);
        ASSERT_FALSE(up.has_value());
        EXPECT_EQ(up.error(), ErrorCode::OBJECT_HAS_LEASE);
    }
    auto still = service_->GetReplicaList("ku", TenantId::Default());
    ASSERT_TRUE(still.has_value());
    EXPECT_EQ(memory_replicas(*still), 1u);
    EXPECT_EQ(backups_of("ku"), 1u);  // the backup query shows it
    std::this_thread::sleep_for(milliseconds(400));
    auto up =
        service_->UpsertStart(src, "ku", TenantId::Default(), 2048, config);
    ASSERT_TRUE(up.has_value()) << toString(up.error());
    ASSERT_TRUE(
        service_->UpsertEnd(src, "ku", TenantId::Default(), ReplicaType::MEMORY)
            .has_value());
    auto after = service_->GetReplicaList("ku", TenantId::Default());
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(memory_replicas(*after), 1u);
    for (const auto& d : after->replicas) {
        if (d.is_memory_replica()) {
            EXPECT_EQ(d.get_memory_descriptor().buffer_descriptor.size_, 2048u);
        }
    }
}

// MST-04: placement by host, not by client or segment. Host H1 runs client
// a (two DRAM segments) and client d (another process); H2 runs b, H3 runs
// c. A backup never lands on the source's host, also when the source is the
// second segment of a, the co-located client d, or a segment that was
// unmounted and mounted again; when a holder goes away the requeued backup
// again avoids the source's host.
class SsdBackupHostTest : public SsdBackupMasterTest {
   protected:
    void SetUp() override {
        SsdBackupMasterTest::SetUp();
        a2_.id = generate_uuid();
        a2_.name = "seg_a2";
        a2_.base = 0x500000000;
        a2_.size = 64 * 1024 * 1024;
        a2_.te_endpoint = "seg_a2";
        a2_.host_id = "host_seg_a";
        ASSERT_TRUE(service_->MountSegment(a2_, a_).has_value());
        Mount(c_, "seg_c", 0x300000000);
        Mount(d_, "seg_d", 0x600000000, "host_seg_a");
        for (const auto& [c, ep] : Clients()) {
            ASSERT_TRUE(service_->BackupObjectHeartbeat(c, ep, 0, 1 << 30));
        }
    }
    std::vector<std::pair<UUID, std::string>> Clients() const {
        return {{a_, "seg_a"}, {b_, "seg_b"}, {c_, "seg_c"}, {d_, "seg_d"}};
    }
    std::string HostOf(const UUID& c) const {
        return (c == a_ || c == d_) ? "H1" : (c == b_ ? "H2" : "H3");
    }
    UUID OwnerOfEndpoint(const std::string& ep) const {
        if (ep == "seg_a" || ep == "seg_a2") return a_;
        if (ep == "seg_b") return b_;
        if (ep == "seg_c") return c_;
        return d_;
    }
    // Put `key` with its DRAM copy on `segment`; returns the owner.
    UUID PutOn(const std::string& key, const std::string& segment) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = segment;
        EXPECT_TRUE(
            service_->PutStart(a_, key, TenantId::Default(), 1024, config)
                .has_value());
        EXPECT_TRUE(
            service_->PutEnd(a_, key, TenantId::Default(), ReplicaType::MEMORY)
                .has_value());
        auto list = service_->GetReplicaList(key, TenantId::Default());
        EXPECT_TRUE(list.has_value());
        return OwnerOfEndpoint(list->replicas.at(0)
                                   .get_memory_descriptor()
                                   .buffer_descriptor.transport_endpoint_);
    }
    // Delivers and acknowledges every pending batch; key -> holder.
    std::map<std::string, UUID> DeliverAll(const std::set<UUID>& skip = {}) {
        std::map<std::string, UUID> placed;
        for (int round = 0; round < 10; ++round) {
            bool any = false;
            for (const auto& [c, ep] : Clients()) {
                if (skip.count(c)) continue;
                auto r = service_->BackupObjectHeartbeat(c, ep, 0, 1 << 30);
                if (!r) continue;
                for (const auto& b : r->batches) {
                    any = true;
                    std::vector<StorageObjectMetadata> metas(b.tasks.size());
                    for (auto& m : metas) {
                        m.data_size = 1024;
                        m.transport_endpoint = ep;
                    }
                    EXPECT_TRUE(
                        service_
                            ->NotifyBackupDone(c, b.batch_id, b.tasks, metas)
                            .has_value());
                    for (const auto& t : b.tasks) placed[t.key] = c;
                }
            }
            if (!any) break;
        }
        return placed;
    }
    Segment a2_;
    UUID d_{0xd, 0xd};
};

TEST_F(SsdBackupHostTest, PlacementAndReprotectionAvoidSourceHost) {
    // a's second segment is unmounted and mounted again (new segment id)
    // before any key is placed on it; unmounting later would drop the DRAM
    // copies placed there.
    ASSERT_TRUE(service_->UnmountSegment(a2_.id, a_).has_value());
    a2_.id = generate_uuid();
    ASSERT_TRUE(service_->MountSegment(a2_, a_).has_value());
    std::map<std::string, UUID> src;
    for (int i = 0; i < 4; ++i) {
        const auto n = std::to_string(i);
        src["ha" + n] = PutOn("ha" + n, "seg_a2");
        src["hd" + n] = PutOn("hd" + n, "seg_d");
        src["hc" + n] = PutOn("hc" + n, "seg_c");
        ASSERT_EQ(src["ha" + n], a_);
        ASSERT_EQ(src["hd" + n], d_);
        ASSERT_EQ(src["hc" + n], c_);
    }

    auto placed = DeliverAll();
    std::set<UUID> holders;
    for (const auto& [key, s] : src) {
        SCOPED_TRACE(key);
        ASSERT_TRUE(placed.count(key)) << "no backup placed";
        EXPECT_NE(HostOf(placed[key]), HostOf(s));
        holders.insert(placed[key]);
    }
    // Re-protection: the holder of ha0 loses its disk; its backups are
    // requeued and must again avoid each key's source host.
    const UUID gone = placed["ha0"];
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(gone).has_value());
    auto again = DeliverAll({gone});
    size_t requeued = 0;
    for (const auto& [key, holder] : placed) {
        if (holder != gone) continue;
        SCOPED_TRACE(key);
        ASSERT_TRUE(again.count(key)) << "backup not re-placed";
        EXPECT_NE(again[key], gone);
        EXPECT_NE(HostOf(again[key]), HostOf(src[key]));
        ++requeued;
    }
    EXPECT_GT(requeued, 0u);
}

class SsdBackupPromotionTest : public SsdBackupMasterTest {
   protected:
    void SetUp() override {
        promotion_on_hit_ = true;
        SsdBackupMasterTest::SetUp();
    }
};

// Owner gone: a read served by the backup queues promotion on the backup's
// holder, which is then allowed to allocate the DRAM copy.
TEST_F(SsdBackupPromotionTest, PromotesFromBackupHolder) {
    const UUID src = Put("kp");
    const UUID dst = src == a_ ? b_ : a_;
    PrimaryOffload(src, "kp");
    BackupDone(dst, "kp");
    ASSERT_TRUE(service_->UnmountSegment(SegOf(src), src).has_value());
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(src).has_value());

    auto list = service_->GetReplicaList("kp", TenantId::Default());
    ASSERT_TRUE(list.has_value());
    EXPECT_EQ(CountBackups(*list), 1u);

    auto none = service_->PromotionObjectHeartbeat(src);
    EXPECT_TRUE(!none.has_value() || none->empty());
    auto tasks = service_->PromotionObjectHeartbeat(dst);
    ASSERT_TRUE(tasks.has_value()) << tasks.error();
    ASSERT_EQ(tasks->size(), 1u);
    EXPECT_EQ(tasks->at(0).key, "kp");
    auto alloc = service_->PromotionAllocStart(dst, "kp", TenantId::Default(),
                                               tasks->at(0).size, {});
    EXPECT_TRUE(alloc.has_value()) << alloc.error();
}

TEST_F(SsdBackupMasterTest, PrefetchFallsBackToBackupHolder) {
    const UUID src = Put("kf");
    const UUID dst = src == a_ ? b_ : a_;
    PrimaryOffload(src, "kf");
    BackupDone(dst, "kf");
    ASSERT_TRUE(service_->UnmountSegment(SegOf(src), src).has_value());
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(src).has_value());
    // Only the backup's holder may prefetch it.
    EXPECT_FALSE(service_->RegisterPrefetchTask(src, "kf", TenantId::Default())
                     .has_value());
    auto r = service_->RegisterPrefetchTask(dst, "kf", TenantId::Default());
    EXPECT_TRUE(r.has_value()) << r.error();
}

// A promotion served from the backup re-protects the object: a primary SSD
// copy on the new DRAM owner and a new backup on a third client, which
// replaces the old one (whose holder then sees its copy as orphaned).
TEST_F(SsdBackupPromotionTest, PromotionFromBackupReprotects) {
    Mount(c_, "seg_c", 0x300000000);
    Heartbeat(c_);
    const UUID src = Put("kr");
    PrimaryOffload(src, "kr");
    const UUID holder = BackupDoneAnyHolder(src);
    UUID other{};
    for (const UUID& c : {a_, b_, c_}) {
        if (c != src && c != holder) other = c;
    }
    // The holder keeps only its SSD, so promotion lands DRAM on `other`:
    // the offload path (not the F7 conversion) must run.
    ASSERT_TRUE(service_->UnmountSegment(SegOf(holder), holder).has_value());
    ASSERT_TRUE(service_->UnmountSegment(SegOf(src), src).has_value());
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(src).has_value());
    ASSERT_TRUE(
        service_->GetReplicaList("kr", TenantId::Default()).has_value());
    auto tasks = service_->PromotionObjectHeartbeat(holder);
    ASSERT_TRUE(tasks.has_value() && tasks->size() == 1);
    ASSERT_TRUE(service_
                    ->PromotionAllocStart(holder, "kr", TenantId::Default(),
                                          tasks->at(0).size, {})
                    .has_value());
    ASSERT_TRUE(
        service_->NotifyPromotionSuccess(holder, "kr", TenantId::Default())
            .has_value());
    auto off = service_->OffloadObjectHeartbeat(other, true);
    ASSERT_TRUE(off.has_value());
    ASSERT_EQ(off->size(), 1u);  // primary SSD copy on the new DRAM owner
    EXPECT_EQ(off->at(0).key, "kr");
    // The old backup on `holder` stays valid meanwhile.
    EXPECT_EQ(ResidencyFor(holder, "kr"), BackupResidency::kInDram);
}

// F7/R4: promotion lands DRAM on the backup's own holder. The holder gets a
// normal primary offload task (its FileStorage drops the local backup entry
// first and writes a real primary bucket), and a new backup is queued for
// another host.
TEST_F(SsdBackupPromotionTest, PromotionOntoHolderOffloadsToHolder) {
    Mount(c_, "seg_c", 0x300000000);
    Heartbeat(c_);
    const UUID src = Put("kh");
    PrimaryOffload(src, "kh");
    const UUID holder = BackupDoneAnyHolder(src);
    UUID other{};
    for (const UUID& c : {a_, b_, c_}) {
        if (c != src && c != holder) other = c;
    }
    ASSERT_TRUE(service_->UnmountSegment(SegOf(other), other).has_value());
    ASSERT_TRUE(service_->UnmountSegment(SegOf(src), src).has_value());
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(src).has_value());
    ASSERT_TRUE(
        service_->GetReplicaList("kh", TenantId::Default()).has_value());
    auto bq = service_->BatchGetBackupReplicaList({"kh"}, TenantId::Default());
    ASSERT_TRUE(bq[0].has_value());
    const ReplicaID backup_id = bq[0]->replicas.at(0).id;
    auto tasks = service_->PromotionObjectHeartbeat(holder);
    ASSERT_TRUE(tasks.has_value() && tasks->size() == 1);
    ASSERT_TRUE(service_
                    ->PromotionAllocStart(holder, "kh", TenantId::Default(),
                                          tasks->at(0).size, {})
                    .has_value());
    ASSERT_TRUE(
        service_->NotifyPromotionSuccess(holder, "kh", TenantId::Default())
            .has_value());
    auto off = service_->OffloadObjectHeartbeat(holder, true);
    ASSERT_TRUE(off.has_value());
    ASSERT_EQ(off->size(), 1u);
    EXPECT_EQ(off->at(0).key, "kh");
    // The holder registers its new primary copy.
    StorageObjectMetadata pm;
    pm.data_size = 1024;
    pm.transport_endpoint =
        holder == a_ ? "seg_a" : (holder == b_ ? "seg_b" : "seg_c");
    ASSERT_TRUE(service_->NotifyOffloadSuccess(holder, *off, {pm}).has_value());
    auto d = service_->DescribeSsdBackupsForAdmin({"kh"}, TenantId::Default());
    EXPECT_NE(d[0].find("primary=" + UuidToString(holder)), std::string::npos)
        << d[0];
    // Review P2: a late conditional eviction of the old backup (same owner as
    // the new primary) removes only that replica.
    auto ev = service_->EvictDiskReplicaIfCurrent(
        holder, "kh", TenantId::Default(), ReplicaType::LOCAL_DISK, backup_id);
    ASSERT_TRUE(ev.has_value());
    EXPECT_TRUE(ev.value());
    d = service_->DescribeSsdBackupsForAdmin({"kh"}, TenantId::Default());
    EXPECT_NE(d[0].find("primary=" + UuidToString(holder)), std::string::npos)
        << d[0];
    EXPECT_NE(d[0].find("backup=-"), std::string::npos) << d[0];
    // The new backup goes to the other host.
    auto resp = Heartbeat(other);
    ASSERT_EQ(resp.batches.size(), 1u);
    StorageObjectMetadata meta;
    meta.data_size = 1024;
    meta.transport_endpoint = "x";
    auto done = service_->NotifyBackupDone(other, resp.batches[0].batch_id,
                                           resp.batches[0].tasks, {meta});
    ASSERT_TRUE(done.has_value());
    EXPECT_TRUE(done->empty());
    d = service_->DescribeSsdBackupsForAdmin({"kh"}, TenantId::Default());
    EXPECT_NE(d[0].find("backup=" + UuidToString(other)), std::string::npos)
        << d[0];
    EXPECT_NE(d[0].find("primary=" + UuidToString(holder)), std::string::npos)
        << d[0];
}

// A leaving backup holder: objects whose primary is still in DRAM get a new
// backup queued elsewhere.
TEST_F(SsdBackupMasterTest, HolderUnmountRequeuesBackups) {
    Mount(c_, "seg_c", 0x300000000);
    Heartbeat(c_);
    const UUID src = Put("kq");
    UUID holder{};
    for (const UUID& c : {a_, b_, c_}) {
        if (c == src) continue;
        auto resp = Heartbeat(c);
        if (!resp.batches.empty()) {
            holder = c;
            StorageObjectMetadata meta;
            meta.data_size = 1024;
            meta.transport_endpoint = "x";
            ASSERT_TRUE(service_
                            ->NotifyBackupDone(c, resp.batches[0].batch_id,
                                               resp.batches[0].tasks, {meta})
                            .has_value());
        }
    }
    ASSERT_NE(holder, UUID{});
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(holder).has_value());
    const UUID third = [&] {
        for (const UUID& c : {a_, b_, c_}) {
            if (c != src && c != holder) return c;
        }
        return UUID{};
    }();
    auto resp = Heartbeat(third);
    ASSERT_EQ(resp.batches.size(), 1u);
    EXPECT_EQ(resp.batches[0].tasks.at(0).key, "kq");
}

// Read fallback query: the backup descriptor is returned even while the
// primary still looks readable, with the holder's endpoint filled in.
TEST_F(SsdBackupMasterTest, BackupQueryReturnsBackupWhilePrimaryListed) {
    const UUID src = Put("kb");
    const UUID dst = src == a_ ? b_ : a_;
    PrimaryOffload(src, "kb");
    BackupDone(dst, "kb");
    auto r = service_->BatchGetBackupReplicaList({"kb", "nope"},
                                                 TenantId::Default());
    ASSERT_EQ(r.size(), 2u);
    ASSERT_TRUE(r[0].has_value());
    ASSERT_EQ(r[0]->replicas.size(), 1u);
    const auto& d = r[0]->replicas[0].get_local_disk_descriptor();
    EXPECT_EQ(d.client_id, dst);
    EXPECT_EQ(d.transport_endpoint, dst == a_ ? "seg_a" : "seg_b");
    EXPECT_FALSE(r[1].has_value());
}

// T2 control: when the expected replica is already gone, a conditional
// eviction is a precondition miss and leaves the current copy alone.
TEST_F(SsdBackupMasterTest, ConditionalEvictionMissLeavesPrimary) {
    const UUID src = Put("ke");
    PrimaryOffload(src, "ke");
    auto ev = service_->EvictDiskReplicaIfCurrent(
        src, "ke", TenantId::Default(), ReplicaType::LOCAL_DISK,
        /*expected_replica_id=*/987654321);
    ASSERT_TRUE(ev.has_value());
    EXPECT_FALSE(ev.value());
    auto d = service_->DescribeSsdBackupsForAdmin({"ke"}, TenantId::Default());
    EXPECT_NE(d[0].find("primary=" + UuidToString(src)), std::string::npos)
        << d[0];
}

// T6 (R5): primary SSD on A (host H1) whose owner is SUSPECTED but still
// holds its resources; DRAM on X (H2); backup on B (H3) leaves. The
// re-queued backups must avoid H1 too, so C (also on H1) never gets them.
TEST_F(SsdBackupMasterTest, RequeueAvoidsSuspectedPrimaryHost) {
    const UUID c{0xc, 0xc}, x{0x11, 0x11}, d{0xd, 0xd};
    // Re-mount a and b with explicit hosts is not possible (already
    // mounted); use the fixture's a (host_seg_a) as A and b as B.
    Mount(c, "seg_c1", 0x300000000, "host_seg_a");  // same host as A
    Mount(x, "seg_x", 0x400000000, "host_x");
    Mount(d, "seg_d", 0x500000000, "host_d");
    for (const auto& [cl, ep] :
         std::vector<std::pair<UUID, std::string>>{{a_, "seg_a"},
                                                   {b_, "seg_b"},
                                                   {c, "seg_c1"},
                                                   {x, "seg_x"},
                                                   {d, "seg_d"}}) {
        ASSERT_TRUE(
            service_->BackupObjectHeartbeat(cl, ep, 0, 1 << 30).has_value());
    }
    constexpr int kKeys = 20;
    for (int i = 0; i < kKeys; ++i) {
        const std::string key = "ks" + std::to_string(i);
        ReplicateConfig rc;
        rc.replica_num = 1;
        rc.preferred_segment = "seg_x";
        ASSERT_TRUE(service_->PutStart(a_, key, TenantId::Default(), 1024, rc)
                        .has_value());
        ASSERT_TRUE(
            service_->PutEnd(a_, key, TenantId::Default(), ReplicaType::MEMORY)
                .has_value());
        PrimaryOffload(a_, key);  // primary SSD on A
    }
    // All backup batches (source X) are delivered to B only.
    for (int round = 0; round < 10; ++round) {
        for (const UUID& cl : {a_, c, d}) {
            auto r = service_->BackupObjectHeartbeat(
                cl, "e", 0, 1 << 30);  // drain others' queues: refuse them
            ASSERT_TRUE(r.has_value());
            for (const auto& batch : r->batches) {
                (void)service_->NotifyBackupDone(cl, batch.batch_id, {}, {});
            }
        }
        auto r = service_->BackupObjectHeartbeat(b_, "seg_b", 0, 1 << 30);
        ASSERT_TRUE(r.has_value());
        for (const auto& batch : r->batches) {
            std::vector<StorageObjectMetadata> metas(batch.tasks.size());
            for (auto& m : metas) {
                m.data_size = 1024;
                m.transport_endpoint = "seg_b";
            }
            ASSERT_TRUE(
                service_
                    ->NotifyBackupDone(b_, batch.batch_id, batch.tasks, metas)
                    .has_value());
        }
    }
    std::vector<std::string> keys;
    for (int i = 0; i < kKeys; ++i) keys.push_back("ks" + std::to_string(i));
    size_t backed_on_b = 0;
    for (const auto& line :
         service_->DescribeSsdBackupsForAdmin(keys, TenantId::Default())) {
        if (line.find("backup=" + UuidToString(b_)) != std::string::npos)
            ++backed_on_b;
    }
    ASSERT_GT(backed_on_b, 0u);
    // A becomes SUSPECTED (resources kept); B's SSD leaves.
    auto record = MasterServiceTestPeer(*service_).FindClientRecord(a_);
    ASSERT_NE(record, nullptr);
    (void)record->Evaluate(
        ClientLivenessRecord::Clock::now() + std::chrono::hours(1),
        std::chrono::seconds(0), std::chrono::hours(24));
    ASSERT_EQ(record->state(), ClientLivenessState::SUSPECTED);
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(b_).has_value());
    size_t to_c = 0, to_d = 0;
    for (int round = 0; round < 10; ++round) {
        auto rc_ = service_->BackupObjectHeartbeat(c, "seg_c1", 0, 1 << 30);
        auto rd = service_->BackupObjectHeartbeat(d, "seg_d", 0, 1 << 30);
        ASSERT_TRUE(rc_.has_value() && rd.has_value());
        for (const auto& b : rc_->batches) to_c += b.tasks.size();
        for (const auto& b : rd->batches) to_d += b.tasks.size();
    }
    EXPECT_EQ(to_c, 0u);  // never on the SUSPECTED primary's host
    EXPECT_EQ(to_d, backed_on_b);
}

// Best effort, made visible: the only other host's holder leaves, the repair
// backup has nowhere to go, the object is left on one domain and counted.
TEST_F(SsdBackupMasterTest, DegradedCoverageIsReported) {
    const UUID src = Put("kc");
    const UUID dst = src == a_ ? b_ : a_;
    PrimaryOffload(src, "kc");
    BackupDone(dst, "kc");
    auto c0 = service_->ScanSsdBackupCoverageForAdmin();
    EXPECT_EQ(c0.objects, 1u);
    EXPECT_EQ(c0.with_backup, 1u);
    EXPECT_EQ(c0.single_domain, 0u);
    ASSERT_TRUE(service_->UnmountLocalDiskSegment(dst).has_value());
    auto c1 = service_->ScanSsdBackupCoverageForAdmin();
    EXPECT_EQ(c1.with_backup, 0u);
    EXPECT_EQ(c1.single_domain, 1u);
    EXPECT_EQ(c1.unplaced_objects_total, 1u);
}

// Review P1: the coverage scan must not take client_mutex_ under the state
// lock (ReMountSegment takes client_mutex_, then the state lock). Force the
// interleaving: while the scan holds the state lock, start a remount and let
// it take client_mutex_ and block on the state lock, then let the scan go on.
// The old scan then waits for client_mutex_ and never finishes.
TEST_F(SsdBackupMasterTest, CoverageScanDoesNotDeadlockWithRemount) {
    for (int i = 0; i < 50; ++i) Put("kd" + std::to_string(i));
    Segment seg;
    seg.id = SegOf(a_);
    seg.name = "seg_a";
    seg.base = 0x100000000;
    seg.size = 64 * 1024 * 1024;
    seg.te_endpoint = "seg_a";
    seg.host_id = "host_seg_a";
    std::future<tl::expected<void, ErrorCode>> remount;
    service_->SetCoverageScanHookForTest([&] {
        remount = std::async(std::launch::async, [&] {
            return service_->ReMountSegment({seg}, a_);
        });
        // Give the remount time to take client_mutex_ and block on the
        // state lock this scan holds.
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    });
    auto scan = std::async(std::launch::async, [&] {
        return service_->ScanSsdBackupCoverageForAdmin();
    });
    if (scan.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
        // Deadlocked: fail loudly instead of hanging the whole suite.
        ADD_FAILURE() << "coverage scan and ReMountSegment deadlocked";
        std::_Exit(1);
    }
    service_->SetCoverageScanHookForTest(nullptr);
    EXPECT_EQ(scan.get().objects, 50u);
    ASSERT_TRUE(remount.valid());
    ASSERT_EQ(remount.wait_for(std::chrono::seconds(10)),
              std::future_status::ready);
    EXPECT_TRUE(remount.get().has_value());
}

// Review F7: backup registrations are not in the HA OpLog, so SSD backup is
// forced off when the OpLog is enabled.
TEST(SsdBackupConfigTest, DisabledWithHaOpLog) {
    setenv("MC_SSD_BACKUP_ENABLE", "1", 1);
    EXPECT_TRUE(MasterService::SsdBackupConfigFor(false).enabled);
    EXPECT_FALSE(MasterService::SsdBackupConfigFor(true).enabled);
    unsetenv("MC_SSD_BACKUP_ENABLE");
}

// CFG-06: backup enabled on a master with enable_offload=false: ordinary
// writes work and no backup task is ever queued or dispatched.
TEST(SsdBackupConfigTest, NoBackupTasksWithOffloadDisabled) {
    setenv("MC_SSD_BACKUP_ENABLE", "1", 1);
    setenv("MC_SSD_BACKUP_BATCH_KEYS", "1", 1);
    MasterServiceConfig config;
    config.enable_offload = false;
    config.default_kv_lease_ttl = 0;
    MasterService service(config);
    const UUID a{0xa, 0xa}, b{0xb, 0xb};
    for (const auto& [client, name, base] :
         std::vector<std::tuple<UUID, std::string, size_t>>{
             {a, "seg_a", 0x100000000}, {b, "seg_b", 0x200000000}}) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = name;
        segment.base = base;
        segment.size = 64 * 1024 * 1024;
        segment.te_endpoint = name;
        segment.host_id = "host_" + name;
        ASSERT_TRUE(service.MountSegment(segment, client).has_value());
    }
    (void)service.BackupObjectHeartbeat(b, "seg_b", 0, 1 << 30);
    ReplicateConfig rc;
    rc.replica_num = 1;
    for (int i = 0; i < 4; ++i) {
        const std::string key = "off" + std::to_string(i);
        ASSERT_TRUE(service.PutStart(a, key, TenantId::Default(), 1024, rc)
                        .has_value());
        ASSERT_TRUE(
            service.PutEnd(a, key, TenantId::Default(), ReplicaType::MEMORY)
                .has_value());
        ASSERT_TRUE(
            service.GetReplicaList(key, TenantId::Default()).has_value());
    }
    for (const UUID& c : {a, b}) {
        auto r = service.BackupObjectHeartbeat(c, c == a ? "seg_a" : "seg_b", 0,
                                               1 << 30);
        if (r.has_value()) EXPECT_TRUE(r->batches.empty());
    }
    const auto stats = service.GetSsdBackupStatsForTest();
    EXPECT_EQ(stats.batches_dispatched, 0u);
    EXPECT_EQ(stats.objects_unplaced, 0u);
    unsetenv("MC_SSD_BACKUP_ENABLE");
    unsetenv("MC_SSD_BACKUP_BATCH_KEYS");
}

// R3-B1 (round-3 review, P1): a backup heartbeat of client a that holds a's
// liveness guard must not take client_mutex_ (the host lookup did), because
// MountSegment / MountLocalDiskSegment of the same client hold client_mutex_
// exclusively while they wait for a's liveness transition lock. The real
// MasterService is driven with two gates: the heartbeat stops right after it
// got its guard until the mount holds client_mutex_; the mount goes on into
// its liveness step. Previously the two waited for each other forever (the
// watchdog ends the child with 9 after dumping all thread stacks); now the
// heartbeat finishes first, then the mount. Child body: 0 on success.
class SsdBackupLockOrderTest : public SsdBackupMasterTest {
   protected:
    static void DumpStacksAndExit(const char* what) {
        std::fprintf(stderr, "watchdog: %s\n", what);
        const char* dir = std::getenv("MC_TEST_TRACE_DIR");
        const std::string out = std::string(dir ? dir : "/tmp") +
                                "/lock-order-" + std::to_string(::getpid()) +
                                ".stacks";
        const std::string cmd =
            "timeout 30 gdb -p " + std::to_string(::getpid()) +
            " -batch -ex 'thread apply all bt' > " + out + " 2>&1";
        const int rc = std::system(cmd.c_str());
        std::fprintf(stderr, "thread stacks (gdb rc=%d): %s\n", rc,
                     out.c_str());
        std::_Exit(9);
    }
    // The causal part of the trace is fixed by the two gates: the heartbeat
    // holds its guard, the mount then holds client_mutex_, the heartbeat
    // resumes. Its two completions are recorded by the callers after the
    // calls returned (the heartbeat has already released its guard), so
    // either may come first; each must appear exactly once.
    static bool CompletionOrderOk(const std::string& trace) {
        const std::string prefix = "hb_guard mount_client_locked hb_resume ";
        if (trace.rfind(prefix, 0) != 0) return false;
        const std::string rest = trace.substr(prefix.size());
        return rest == "hb_done mount_done " ||
               rest == "mount_done hb_done ";  // T19-ORACLE
    }
    // mount_first: after BackupObjectHeartbeat returned, the heartbeat
    // thread waits for the mount to finish before it records hb_done (a
    // legal schedule the oracle must accept).
    int HeartbeatAgainstMountChild(bool local_disk, bool mount_first = false) {
        std::thread([] {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            DumpStacksAndExit("backup heartbeat / mount deadlock");
        }).detach();
        std::atomic<bool> hb_guarded{false}, mount_locked{false};
        std::atomic<bool> hb_seen{false}, mount_seen{false};
        std::atomic<bool> mount_finished{false};
        std::vector<std::string> events;
        std::mutex mu;
        const auto event = [&](const std::string& e) {
            std::lock_guard<std::mutex> l(mu);
            events.push_back(e);
        };
        service_->SetLockOrderHookForTest([&](const char* stage) {
            const std::string s(stage);
            if (s == "backup_heartbeat_guard" && !hb_seen.exchange(true)) {
                event("hb_guard");
                hb_guarded = true;
                const auto until =
                    std::chrono::steady_clock::now() + std::chrono::seconds(5);
                while (!mount_locked &&
                       std::chrono::steady_clock::now() < until) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                event(mount_locked ? "hb_resume" : "hb_resume_no_mount");
            } else if ((s == "mount_client_locked" ||
                        s == "mount_local_disk_client_locked") &&
                       hb_guarded && !mount_seen.exchange(true)) {
                event("mount_client_locked");
                mount_locked = true;
            }
        });
        bool hb_ok = false, mount_ok = false;
        std::thread hb([&] {
            hb_ok = service_->BackupObjectHeartbeat(a_, "seg_a", 0, 1 << 30)
                        .has_value();
            if (mount_first) {
                const auto until =
                    std::chrono::steady_clock::now() + std::chrono::seconds(5);
                while (!mount_finished &&
                       std::chrono::steady_clock::now() < until) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            }
            event("hb_done");
        });
        const auto until =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!hb_guarded && std::chrono::steady_clock::now() < until) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (!hb_guarded) {
            hb.join();
            service_->SetLockOrderHookForTest(nullptr);
            std::fprintf(stderr,
                         "check 2 failed: heartbeat never got its guard\n");
            return 2;
        }
        std::thread mount([&] {
            if (local_disk) {
                auto r = service_->MountLocalDiskSegment(a_, true);
                mount_ok = r.has_value() ||
                           r.error() == ErrorCode::SEGMENT_ALREADY_EXISTS;
            } else {
                Segment segment;
                segment.id = generate_uuid();
                segment.name = "seg_a2";
                segment.base = 0x500000000;
                segment.size = 64 * 1024 * 1024;
                segment.te_endpoint = "seg_a2";
                segment.host_id = "host_seg_a";
                mount_ok = service_->MountSegment(segment, a_).has_value();
            }
            event("mount_done");
            mount_finished = true;
        });
        hb.join();
        mount.join();
        service_->SetLockOrderHookForTest(nullptr);
        std::string trace;
        for (const auto& e : events) trace += e + " ";
        std::fprintf(stderr, "event order: %s\n", trace.c_str());
        if (!hb_ok || !mount_ok) {
            std::fprintf(stderr,
                         "check 3 failed: heartbeat ok=%d mount ok=%d\n", hb_ok,
                         mount_ok);
            return 3;
        }
        if (!CompletionOrderOk(trace)) {
            std::fprintf(stderr, "check 4 failed: order %s\n", trace.c_str());
            return 4;
        }
        if (mount_first &&
            trace !=
                "hb_guard mount_client_locked hb_resume mount_done hb_done ") {
            std::fprintf(stderr,
                         "check 5 failed: control did not put the "
                         "mount first: %s\n",
                         trace.c_str());
            return 5;
        }
        if (const char* dir = std::getenv("MC_TEST_TRACE_DIR")) {
            const std::string name =
                mount_first  ? "HeartbeatRecordedAfterMountCompletes"
                : local_disk ? "HeartbeatDuringLocalDiskMount"
                             : "HeartbeatDuringSegmentMount";
            std::ofstream(
                std::string(dir) + "/SsdBackupLockOrderTest." + name + ".trace",
                std::ios::app)
                << ::getpid() << " event order: " << trace << "\n";
        }
        return 0;
    }
};

// T19 (R4-M1): the mount completes and is recorded before the heartbeat
// records its own completion. Both succeeded with the established lock
// order, so the oracle must accept the trace.
TEST_F(SsdBackupLockOrderTest, HeartbeatRecordedAfterMountCompletes) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeartbeatAgainstMountChild(false, true)),
                ::testing::ExitedWithCode(0), "");
}

TEST_F(SsdBackupLockOrderTest, HeartbeatDuringSegmentMount) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeartbeatAgainstMountChild(false)),
                ::testing::ExitedWithCode(0), "");
}

TEST_F(SsdBackupLockOrderTest, HeartbeatDuringLocalDiskMount) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(std::_Exit(HeartbeatAgainstMountChild(true)),
                ::testing::ExitedWithCode(0), "");
}

TEST_F(SsdBackupMasterTest, DisabledByDefault) {
    unsetenv("MC_SSD_BACKUP_ENABLE");
    MasterServiceConfig config;
    config.enable_offload = true;
    MasterService off(config);
    auto r = off.BackupObjectHeartbeat(a_, "seg_a", 0, 1 << 30);
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->batches.empty());
}

// Heap bytes in use (glibc). RSS is too coarse: freed buffers are reused.
static int64_t RssBytes() {
    struct mallinfo2 mi = mallinfo2();
    return static_cast<int64_t>(mi.uordblks + mi.hblkhd);
}

// Master memory per backup replica. Off by default:
// MC_SSD_BACKUP_MEM_BENCH=N (objects).
// MST-18 / RPC-04 / RPC-03 through the real RPC layer: a WrappedMasterService
// with multi-tenant quotas (tenants "default" and "tenant-a" registered),
// served by coro_rpc, and MasterClients on the other end. Same bare key in
// both tenants with different sizes; every backup RPC (heartbeat, ACK,
// residency, backup list) goes over the wire and through the wrapper's
// tenant resolution.
class SsdBackupTenantRpcTest : public ::testing::Test {
   protected:
    static constexpr char kTenant[] = "tenant-a";
    void SetUp() override {
        setenv("MC_SSD_BACKUP_ENABLE", "1", 1);
        setenv("MC_SSD_BACKUP_BATCH_KEYS", "1", 1);
        if (max_inflight_) {
            setenv("MC_SSD_BACKUP_MAX_INFLIGHT_BYTES", max_inflight_, 1);
        }
        if (defer_max_age_ms_) {
            setenv("MC_SSD_BACKUP_DEFER_MAX_AGE_MS", defer_max_age_ms_, 1);
        }
        const UUID suffix = generate_uuid();
        policy_ =
            std::filesystem::temp_directory_path() /
            ("ssd_backup_tenants_" + std::to_string(suffix.first) + ".yaml");
        TenantQuotaPolicySnapshot policy;
        policy.tenant_quotas = {
            {std::string(TenantId::kDefaultValue), 64 * 1024 * 1024},
            {kTenant, 64 * 1024 * 1024},
        };
        {
            std::ofstream out(policy_);
            ASSERT_TRUE(out.is_open());
            out << FormatTenantQuotaPolicyYaml(policy);
        }
        WrappedMasterServiceConfig cfg;
        cfg.default_kv_lease_ttl = 0;
        cfg.enable_metric_reporting = false;
        cfg.enable_offload = true;
        cfg.enable_multi_tenants = true;
        cfg.tenant_quota_connector_type = "file";
        cfg.tenant_quota_connector_uri = policy_.string();
        service_ = std::make_shared<WrappedMasterService>(cfg);
        const auto ports = getFreeTcpPorts(1);
        ASSERT_EQ(ports.size(), 1u);
        addr_ = "127.0.0.1:" + std::to_string(ports.front());
        server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            2, ports.front(), "127.0.0.1", std::chrono::seconds(0), true);
        RegisterRpcService(*server_, *service_);
        ASSERT_FALSE(server_->async_start().hasResult());
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        MountOn(src_, "seg_src", 0x100000000, "host_src");
        MountOn(holder_, "seg_h", 0x200000000, "host_h");
        holder_rpc_ = std::make_unique<MasterClient>(holder_);
        ASSERT_EQ(holder_rpc_->Connect(addr_), ErrorCode::OK);
        ASSERT_TRUE(
            holder_rpc_->BackupObjectHeartbeat(holder_, "seg_h", 0, 1 << 30));
    }
    void TearDown() override {
        holder_rpc_.reset();
        server_.reset();
        service_.reset();
        std::error_code ignored;
        std::filesystem::remove(policy_, ignored);
        unsetenv("MC_SSD_BACKUP_ENABLE");
        unsetenv("MC_SSD_BACKUP_BATCH_KEYS");
        unsetenv("MC_SSD_BACKUP_MAX_INFLIGHT_BYTES");
        unsetenv("MC_SSD_BACKUP_DEFER_MAX_AGE_MS");
    }
    void MountOn(const UUID& client, const std::string& name, size_t base,
                 const std::string& host) {
        Segment s;
        s.id = generate_uuid();
        s.name = name;
        s.base = base;
        s.size = 64 * 1024 * 1024;
        s.te_endpoint = name;
        s.host_id = host;
        ASSERT_TRUE(service_->MountSegment(s, client).has_value());
        ASSERT_TRUE(service_->MountLocalDiskSegment(client, true).has_value());
        ASSERT_TRUE(service_->ReportSsdCapacity(client, 1 << 30).has_value());
    }
    void PutIn(const std::string& tenant, const std::string& key,
               uint64_t size) {
        ReplicateConfig rc;
        rc.replica_num = 1;
        rc.preferred_segment = "seg_src";
        auto started = service_->PutStart(src_, key, size, rc, tenant);
        ASSERT_TRUE(started.has_value()) << toString(started.error());
        ASSERT_TRUE(service_
                        ->PutEnd(src_, ObjectMeta{key, std::nullopt},
                                 ReplicaType::MEMORY, tenant)
                        .has_value());
    }
    // Heartbeats over RPC until `n` tasks were dispatched; ACKs each batch
    // over RPC as written. Returns tenant -> task.
    std::map<std::string, BackupTaskItem> DeliverOverRpc(size_t n) {
        std::map<std::string, BackupTaskItem> got;
        for (int i = 0; i < 20 && got.size() < n; ++i) {
            auto r = holder_rpc_->BackupObjectHeartbeat(holder_, "seg_h", 0,
                                                        1 << 30);
            EXPECT_TRUE(r.has_value());
            if (!r) break;
            for (const auto& b : r->batches) {
                std::vector<StorageObjectMetadata> metas(b.tasks.size());
                for (size_t j = 0; j < b.tasks.size(); ++j) {
                    metas[j].data_size = b.tasks[j].size;
                    metas[j].transport_endpoint = "seg_h";
                    got[b.tasks[j].tenant_id] = b.tasks[j];
                }
                auto ack = holder_rpc_->NotifyBackupDone(holder_, b.batch_id,
                                                         b.tasks, metas);
                EXPECT_TRUE(ack.has_value()) << toString(ack.error());
                if (ack) EXPECT_TRUE(ack->empty());  // nothing stale
            }
        }
        return got;
    }
    // Backup descriptor size for `key` in `tenant`, read over RPC by a
    // separate reader; -1 if none; also checks the resolved tenant.
    int64_t BackupSize(const std::string& key, const std::string& tenant,
                       ErrorCode* error = nullptr) {
        MasterClient reader(reader_, nullptr, tenant);
        EXPECT_EQ(reader.Connect(addr_), ErrorCode::OK);
        auto r = reader.BatchGetBackupReplicaList({key}, tenant);
        EXPECT_EQ(r.size(), 1u);
        if (r.empty()) return -1;
        if (!r[0]) {
            if (error) *error = r[0].error();
            return -1;
        }
        EXPECT_TRUE(r[0]->resolved_tenant_id.has_value());
        if (r[0]->resolved_tenant_id.has_value()) {
            EXPECT_EQ(r[0]->resolved_tenant_id.value(), tenant);
        }
        for (const auto& d : r[0]->replicas) {
            if (d.is_local_disk_replica()) {
                const auto& l = d.get_local_disk_descriptor();
                EXPECT_TRUE(l.backup.has_value() && l.backup.value());
                EXPECT_EQ(l.client_id, holder_);
                return static_cast<int64_t>(l.object_size);
            }
        }
        return -1;
    }

    std::optional<SsdBackupCoordinator::ClientSnapshot> Reservation() {
        return service_->GetMasterServiceForTest()
            .GetSsdBackupClientSnapshotForTest(holder_);
    }

    const char* max_inflight_ = nullptr;
    const char* defer_max_age_ms_ = nullptr;
    std::filesystem::path policy_;
    std::string addr_;
    std::shared_ptr<WrappedMasterService> service_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::unique_ptr<MasterClient> holder_rpc_;
    UUID src_{0x51, 0x51};
    UUID holder_{0x52, 0x52};
    UUID reader_{0x53, 0x53};
};

TEST_F(SsdBackupTenantRpcTest, EqualKeysStayInTheirTenants) {
    PutIn(kTenant, "shared", 1024);
    PutIn(std::string(TenantId::kDefaultValue), "shared", 2048);
    auto tasks = DeliverOverRpc(2);
    ASSERT_EQ(tasks.size(), 2u);
    ASSERT_TRUE(tasks.count(kTenant));
    ASSERT_TRUE(tasks.count("default"));
    EXPECT_EQ(tasks[kTenant].key, "shared");
    EXPECT_EQ(tasks[kTenant].size, 1024);
    EXPECT_EQ(tasks["default"].size, 2048);

    // Each tenant reads its own backup (size and resolved tenant).
    EXPECT_EQ(BackupSize("shared", kTenant), 1024);
    EXPECT_EQ(BackupSize("shared", "default"), 2048);
    // A valid but unregistered tenant sees nothing of either.
    ErrorCode other = ErrorCode::OK;
    EXPECT_EQ(BackupSize("shared", "tenant-z", &other), -1);
    EXPECT_NE(other, ErrorCode::OK);
    // An invalid tenant name is refused by the wrapper.
    ErrorCode invalid = ErrorCode::OK;
    EXPECT_EQ(BackupSize("shared",
                         std::string("\x01"
                                     "bad"),
                         &invalid),
              -1);
    EXPECT_EQ(invalid, ErrorCode::INVALID_PARAMS);

    // Residency per tenant over RPC.
    auto res = holder_rpc_->QueryBackupResidency(
        holder_, {BackupRefreshItem{kTenant, "shared"},
                  BackupRefreshItem{"default", "shared"},
                  BackupRefreshItem{"tenant-z", "shared"}});
    ASSERT_TRUE(res.has_value()) << toString(res.error());
    ASSERT_EQ(res->size(), 3u);
    EXPECT_EQ(static_cast<BackupResidency>((*res)[0]),
              BackupResidency::kInDram);
    EXPECT_EQ(static_cast<BackupResidency>((*res)[1]),
              BackupResidency::kInDram);
    EXPECT_EQ(static_cast<BackupResidency>((*res)[2]),
              BackupResidency::kMissing);
    auto bad_res = holder_rpc_->QueryBackupResidency(
        holder_, {BackupRefreshItem{std::string("\x01"
                                                "bad"),
                                    "shared"}});
    ASSERT_FALSE(bad_res.has_value());
    EXPECT_EQ(bad_res.error(), ErrorCode::INVALID_PARAMS);

    // An ACK whose task carries an invalid tenant is refused as a whole.
    BackupTaskItem forged = tasks["default"];
    forged.tenant_id = std::string(
        "\x01"
        "bad");
    StorageObjectMetadata meta;
    meta.data_size = forged.size;
    meta.transport_endpoint = "seg_h";
    auto forged_ack =
        holder_rpc_->NotifyBackupDone(holder_, 1, {forged}, {meta});
    ASSERT_FALSE(forged_ack.has_value());
    EXPECT_EQ(forged_ack.error(), ErrorCode::INVALID_PARAMS);

    // Deleting the key in one tenant leaves the other tenant's object and
    // backup.
    ASSERT_TRUE(service_->Remove("shared", false, kTenant).has_value());
    ErrorCode removed = ErrorCode::OK;
    EXPECT_EQ(BackupSize("shared", kTenant, &removed), -1);
    EXPECT_NE(removed, ErrorCode::OK);
    EXPECT_EQ(BackupSize("shared", "default"), 2048);
    auto after = holder_rpc_->QueryBackupResidency(
        holder_, {BackupRefreshItem{kTenant, "shared"},
                  BackupRefreshItem{"default", "shared"}});
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(static_cast<BackupResidency>((*after)[0]),
              BackupResidency::kMissing);
    EXPECT_EQ(static_cast<BackupResidency>((*after)[1]),
              BackupResidency::kInDram);
}

// RPC-03 over the wire: malformed backup requests get defined errors and
// change nothing; the service keeps serving the real backups afterwards and
// a new backup is still dispatched and acknowledged.
TEST_F(SsdBackupTenantRpcTest, MalformedRequestsOverRpcChangeNothing) {
    PutIn("default", "real", 1024);
    ASSERT_EQ(DeliverOverRpc(1).size(), 1u);
    ASSERT_EQ(BackupSize("real", "default"), 1024);

    // Negative usage / capacity.
    auto hb = holder_rpc_->BackupObjectHeartbeat(holder_, "seg_h", -1, 1 << 30);
    ASSERT_FALSE(hb.has_value());
    EXPECT_EQ(hb.error(), ErrorCode::INVALID_PARAMS);
    hb = holder_rpc_->BackupObjectHeartbeat(holder_, "seg_h", 0, -1);
    ASSERT_FALSE(hb.has_value());
    EXPECT_EQ(hb.error(), ErrorCode::INVALID_PARAMS);
    // Task / metadata count mismatch.
    BackupTaskItem t{"default", "real", 1024, 0};
    auto mismatch = holder_rpc_->NotifyBackupDone(holder_, 1, {t, t}, {{}});
    ASSERT_FALSE(mismatch.has_value());
    EXPECT_EQ(mismatch.error(), ErrorCode::INVALID_PARAMS);
    // Too many residency keys in one call.
    std::vector<BackupRefreshItem> many(1001,
                                        BackupRefreshItem{"default", "x"});
    auto too_many = holder_rpc_->QueryBackupResidency(holder_, many);
    ASSERT_FALSE(too_many.has_value());
    EXPECT_EQ(too_many.error(), ErrorCode::INVALID_PARAMS);
    // Over-long and unusual keys: not found, no crash, nothing created.
    const std::string long_key(64 * 1024, 'k');
    const std::string odd_key("a\0b/\n\xff", 6);
    ErrorCode e1 = ErrorCode::OK, e2 = ErrorCode::OK;
    EXPECT_EQ(BackupSize(long_key, "default", &e1), -1);
    EXPECT_EQ(BackupSize(odd_key, "default", &e2), -1);
    EXPECT_NE(e1, ErrorCode::OK);
    EXPECT_NE(e2, ErrorCode::OK);
    auto odd = holder_rpc_->QueryBackupResidency(
        holder_, {BackupRefreshItem{"default", long_key},
                  BackupRefreshItem{"default", odd_key}});
    ASSERT_TRUE(odd.has_value()) << toString(odd.error());
    EXPECT_EQ(static_cast<BackupResidency>((*odd)[0]),
              BackupResidency::kMissing);
    EXPECT_EQ(static_cast<BackupResidency>((*odd)[1]),
              BackupResidency::kMissing);
    // A real batch acknowledged with an unusual key that does not exist:
    // reported stale per key, registers nothing, and releases the batch.
    PutIn("default", "g1", 1024);
    auto hb_g =
        holder_rpc_->BackupObjectHeartbeat(holder_, "seg_h", 0, 1 << 30);
    ASSERT_TRUE(hb_g.has_value());
    ASSERT_EQ(hb_g->batches.size(), 1u);
    BackupTaskItem ghost = hb_g->batches[0].tasks.at(0);
    ghost.key = odd_key;
    StorageObjectMetadata meta;
    meta.data_size = 1024;
    meta.transport_endpoint = "seg_h";
    auto ghost_ack = holder_rpc_->NotifyBackupDone(
        holder_, hb_g->batches[0].batch_id, {ghost}, {meta});
    ASSERT_TRUE(ghost_ack.has_value()) << toString(ghost_ack.error());
    EXPECT_EQ(*ghost_ack, std::vector<uint32_t>{0});
    EXPECT_EQ(BackupSize(odd_key, "default"), -1);
    EXPECT_EQ(BackupSize("g1", "default"), -1);  // its own key not registered

    // The real backup is untouched and new work still flows.
    EXPECT_EQ(BackupSize("real", "default"), 1024);
    PutIn("default", "real2", 4096);
    auto again = DeliverOverRpc(1);
    ASSERT_EQ(again.size(), 1u);
    EXPECT_EQ(again["default"].key, "real2");
    EXPECT_EQ(BackupSize("real2", "default"), 4096);
}

// RPC-03 (live accounting): a dispatched batch is held while malformed
// requests arrive over RPC. The inflight limit (1500 bytes) holds one
// 1024-byte batch, so a second batch can only be admitted once the first is
// released. Every rejected request must leave the holder's reservation
// snapshot (inflight batch ids, inflight bytes, queued batches) unchanged;
// the valid ACK releases it exactly once (a duplicate releases nothing
// more), and the next batch is then admitted. A zero-size ACK (skipped by
// design) releases its batch and registers nothing.
class SsdBackupTenantRpcTightTest : public SsdBackupTenantRpcTest {
   protected:
    SsdBackupTenantRpcTightTest() { max_inflight_ = "1500"; }
};

TEST_F(SsdBackupTenantRpcTightTest,
       LiveBatchReservationSurvivesRejectedRequests) {
    PutIn("default", "live", 1024);
    auto hb = holder_rpc_->BackupObjectHeartbeat(holder_, "seg_h", 0, 1 << 30);
    ASSERT_TRUE(hb.has_value());
    ASSERT_EQ(hb->batches.size(), 1u);
    const BackupBatch batch = hb->batches[0];
    const auto snap0 = Reservation();
    ASSERT_TRUE(snap0.has_value());
    EXPECT_EQ(snap0->inflight_batches, std::vector<uint64_t>{batch.batch_id});
    EXPECT_EQ(snap0->inflight_bytes, 1024u);
    const auto unchanged = [&](const char* what) {
        EXPECT_TRUE(Reservation() == snap0)
            << what << " changed the reservation";
    };
    StorageObjectMetadata meta;
    meta.data_size = 1024;
    meta.transport_endpoint = "seg_h";
    const BackupTaskItem task = batch.tasks.at(0);

    auto mismatch = holder_rpc_->NotifyBackupDone(holder_, batch.batch_id,
                                                  {task, task}, {meta});
    ASSERT_FALSE(mismatch.has_value());
    EXPECT_EQ(mismatch.error(), ErrorCode::INVALID_PARAMS);
    unchanged("count mismatch");

    BackupTaskItem bad_tenant = task;
    bad_tenant.tenant_id = std::string(
        "\x01"
        "bad");
    auto bad = holder_rpc_->NotifyBackupDone(holder_, batch.batch_id,
                                             {bad_tenant}, {meta});
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error(), ErrorCode::INVALID_PARAMS);
    unchanged("invalid tenant");

    for (auto [used, cap] : {std::pair<int64_t, int64_t>{-1, 1 << 30},
                             std::pair<int64_t, int64_t>{0, -1}}) {
        auto neg =
            holder_rpc_->BackupObjectHeartbeat(holder_, "seg_h", used, cap);
        ASSERT_FALSE(neg.has_value());
        EXPECT_EQ(neg.error(), ErrorCode::INVALID_PARAMS);
        unchanged("negative usage/capacity");
    }

    // The same batch id acknowledged by a client that is not its holder.
    const UUID stranger{0x99, 0x99};
    MasterClient stranger_rpc(stranger);
    ASSERT_EQ(stranger_rpc.Connect(addr_), ErrorCode::OK);
    auto foreign =
        stranger_rpc.NotifyBackupDone(stranger, batch.batch_id, {task}, {meta});
    EXPECT_FALSE(foreign.has_value());
    unchanged("ACK from an unknown client");
    EXPECT_EQ(BackupSize("live", "default"), -1);  // nothing registered

    // The valid ACK: released exactly once.
    auto ok =
        holder_rpc_->NotifyBackupDone(holder_, batch.batch_id, {task}, {meta});
    ASSERT_TRUE(ok.has_value()) << toString(ok.error());
    EXPECT_TRUE(ok->empty());
    auto after = Reservation();
    ASSERT_TRUE(after.has_value());
    EXPECT_TRUE(after->inflight_batches.empty());
    EXPECT_EQ(after->inflight_bytes, 0u);
    EXPECT_EQ(BackupSize("live", "default"), 1024);
    auto dup =
        holder_rpc_->NotifyBackupDone(holder_, batch.batch_id, {task}, {meta});
    ASSERT_TRUE(dup.has_value());
    after = Reservation();
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(after->inflight_bytes, 0u) << "duplicate ACK released again";

    // The next batch is admitted at the tight limit; a zero-size ACK is a
    // skip: it releases the batch and registers nothing.
    PutIn("default", "next", 1024);
    auto hb2 = holder_rpc_->BackupObjectHeartbeat(holder_, "seg_h", 0, 1 << 30);
    ASSERT_TRUE(hb2.has_value());
    ASSERT_EQ(hb2->batches.size(), 1u);
    EXPECT_EQ(hb2->batches[0].tasks.at(0).key, "next");
    StorageObjectMetadata zero = meta;
    zero.data_size = 0;
    auto skipped = holder_rpc_->NotifyBackupDone(
        holder_, hb2->batches[0].batch_id, hb2->batches[0].tasks, {zero});
    ASSERT_TRUE(skipped.has_value()) << toString(skipped.error());
    after = Reservation();
    ASSERT_TRUE(after.has_value());
    EXPECT_TRUE(after->inflight_batches.empty());
    EXPECT_EQ(BackupSize("next", "default"), -1);
}

// R12-GREEN-ONLY-BEGIN (uses the new lease-neutral master record accessor)
// R8-M3: accounting around live batches, observed after every request
// through read-only snapshots: the coordinator (holder reservation and
// dispatch/complete/unplaced counters) and the master's per-key SSD records
// (version and every SSD replica; there is no separate holder index). The
// record reads take no lease.
//  - Rejected requests change nothing; the unknown holder gets exactly
//    UNAVAILABLE_IN_CURRENT_STATUS.
//  - A competing batch while the quota is held is dropped as unplaced
//    (MST-07: dropped, not queued) and the live reservation stays.
//  - After the release a fresh batch is admitted; replaying the old ACK
//    while it is reserved leaves it and every record untouched.
//  - Zero size, negative size and a real delete/re-put stale generation
//    each release their own live batch and register nothing.
//  - A freshly mounted holder that never advertised an endpoint gets the
//    backup registered but not served (REPLICA_IS_NOT_READY) until it
//    advertises one, without a second registration.
struct AccountingSnapshot {
    std::optional<SsdBackupCoordinator::ClientSnapshot> holder;
    uint64_t dispatched = 0, completed = 0, unplaced = 0, objects_unplaced = 0;
    std::vector<std::string> records;
    bool operator==(const AccountingSnapshot&) const = default;
};

class SsdBackupTenantRpcAccountingTest : public SsdBackupTenantRpcTightTest {
   protected:
    // A batch refused for the held reservation is dropped at once here, so
    // each request's accounting is checked in isolation (waiting batches are
    // covered by the coordinator tests).
    SsdBackupTenantRpcAccountingTest() { defer_max_age_ms_ = "0"; }
    const std::vector<std::string> keys_{"live", "competitor", "fresh", "z0",
                                         "zn",   "st",         "ep"};
    AccountingSnapshot Snap() {
        AccountingSnapshot s;
        auto& ms = service_->GetMasterServiceForTest();
        s.holder = ms.GetSsdBackupClientSnapshotForTest(holder_);
        const auto st = ms.GetSsdBackupStatsForTest();
        s.dispatched = st.batches_dispatched;
        s.completed = st.batches_completed;
        s.unplaced = st.batches_unplaced;
        s.objects_unplaced = st.objects_unplaced;
        s.records = ms.DescribeSsdRecordsForTest(keys_, TenantId::Default());
        return s;
    }
    std::string Record(const std::string& key) {
        auto& ms = service_->GetMasterServiceForTest();
        return ms.DescribeSsdRecordsForTest({key}, TenantId::Default()).at(0);
    }
    BackupBatch DispatchOne(const std::string& key) {
        PutIn("default", key, 1024);
        auto hb =
            holder_rpc_->BackupObjectHeartbeat(holder_, "seg_h", 0, 1 << 30);
        EXPECT_TRUE(hb.has_value());
        EXPECT_EQ(hb ? hb->batches.size() : 0u, 1u);
        return hb && !hb->batches.empty() ? hb->batches[0] : BackupBatch{};
    }
    static StorageObjectMetadata Meta(int64_t size) {
        StorageObjectMetadata m;
        m.data_size = size;
        m.transport_endpoint = "seg_h";
        return m;
    }
    std::string HolderTag(const UUID& id) {
        return std::to_string(id.first) + "-" + std::to_string(id.second);
    }
};

TEST_F(SsdBackupTenantRpcAccountingTest, LiveBatchAccountingAcrossRequests) {
    const BackupBatch live = DispatchOne("live");
    const BackupTaskItem task = live.tasks.at(0);
    const auto s0 = Snap();
    ASSERT_TRUE(s0.holder.has_value());
    EXPECT_EQ(s0.holder->inflight_batches,
              std::vector<uint64_t>{live.batch_id});
    EXPECT_EQ(s0.holder->inflight_bytes, 1024u);
    const auto same = [&](const char* what) {
        EXPECT_TRUE(Snap() == s0) << what << " changed the accounting";
    };

    // Rejected requests, one at a time.
    auto r1 = holder_rpc_->NotifyBackupDone(holder_, live.batch_id,
                                            {task, task}, {Meta(1024)});
    ASSERT_FALSE(r1.has_value());
    EXPECT_EQ(r1.error(), ErrorCode::INVALID_PARAMS);
    same("count mismatch");
    BackupTaskItem bad = task;
    bad.tenant_id = std::string(
        "\x01"
        "bad");
    auto r2 = holder_rpc_->NotifyBackupDone(holder_, live.batch_id, {bad},
                                            {Meta(1024)});
    ASSERT_FALSE(r2.has_value());
    EXPECT_EQ(r2.error(), ErrorCode::INVALID_PARAMS);
    same("invalid tenant");
    auto r3 = holder_rpc_->BackupObjectHeartbeat(holder_, "seg_h", -1, 1 << 30);
    ASSERT_FALSE(r3.has_value());
    EXPECT_EQ(r3.error(), ErrorCode::INVALID_PARAMS);
    same("negative usage");
    auto r4 = holder_rpc_->BackupObjectHeartbeat(holder_, "seg_h", 0, -1);
    ASSERT_FALSE(r4.has_value());
    EXPECT_EQ(r4.error(), ErrorCode::INVALID_PARAMS);
    same("negative capacity");
    const UUID stranger{0x99, 0x99};
    MasterClient stranger_rpc(stranger);
    ASSERT_EQ(stranger_rpc.Connect(addr_), ErrorCode::OK);
    auto r5 = stranger_rpc.NotifyBackupDone(stranger, live.batch_id, {task},
                                            {Meta(1024)});
    ASSERT_FALSE(r5.has_value());
    EXPECT_EQ(r5.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    same("unknown-holder ACK");

    // A competing batch while the reservation is held: dropped (unplaced),
    // never dispatched, the live reservation intact.
    PutIn("default", "competitor", 1024);
    auto hb = holder_rpc_->BackupObjectHeartbeat(holder_, "seg_h", 0, 1 << 30);
    ASSERT_TRUE(hb.has_value());
    EXPECT_TRUE(hb->batches.empty());
    const auto s1 = Snap();
    EXPECT_TRUE(s1.holder == s0.holder);
    EXPECT_EQ(s1.dispatched, s0.dispatched);
    EXPECT_EQ(s1.unplaced, s0.unplaced + 1);
    EXPECT_EQ(s1.objects_unplaced, s0.objects_unplaced + 1);

    // Valid ACK: released once, registered on the holder.
    auto ok = holder_rpc_->NotifyBackupDone(holder_, live.batch_id, {task},
                                            {Meta(1024)});
    ASSERT_TRUE(ok.has_value()) << toString(ok.error());
    EXPECT_TRUE(ok->empty());
    const auto s2 = Snap();
    ASSERT_TRUE(s2.holder.has_value());
    EXPECT_TRUE(s2.holder->inflight_batches.empty());
    EXPECT_EQ(s2.holder->inflight_bytes, 0u);
    EXPECT_EQ(s2.completed, s1.completed + 1);
    const std::string live_backup = "B:" + HolderTag(holder_) + ":1024:";
    EXPECT_NE(Record("live").find(live_backup), std::string::npos)
        << Record("live");
    EXPECT_EQ(Record("competitor").find("B:"), std::string::npos);

    // A fresh batch is admitted; replaying the old ACK while it is
    // reserved leaves its reservation and every record untouched.
    const BackupBatch fresh = DispatchOne("fresh");
    const auto s3 = Snap();
    ASSERT_TRUE(s3.holder.has_value());
    EXPECT_EQ(s3.holder->inflight_batches,
              std::vector<uint64_t>{fresh.batch_id});
    EXPECT_EQ(s3.holder->inflight_bytes, 1024u);
    auto replay = holder_rpc_->NotifyBackupDone(holder_, live.batch_id, {task},
                                                {Meta(1024)});
    ASSERT_TRUE(replay.has_value());
    const auto s4 = Snap();
    EXPECT_TRUE(s4.holder == s3.holder) << "old ACK touched the fresh batch";
    EXPECT_EQ(s4.records, s3.records);
    EXPECT_EQ(s4.dispatched, s3.dispatched);
    auto fresh_ok = holder_rpc_->NotifyBackupDone(holder_, fresh.batch_id,
                                                  fresh.tasks, {Meta(1024)});
    ASSERT_TRUE(fresh_ok.has_value());
    const auto s5 = Snap();
    EXPECT_TRUE(s5.holder->inflight_batches.empty());
    EXPECT_EQ(s5.holder->inflight_bytes, 0u);
    EXPECT_NE(Record("fresh").find(live_backup), std::string::npos);

    // Zero size, negative size and a real stale generation, each on its own
    // live batch: released, nothing registered.
    struct Case {
        const char* key;
        int64_t size;
        bool stale;
    };
    for (const Case& c : {Case{"z0", 0, false}, Case{"zn", -1, false},
                          Case{"st", 1024, true}}) {
        SCOPED_TRACE(c.key);
        const BackupBatch b = DispatchOne(c.key);
        const auto before = Snap();
        ASSERT_TRUE(before.holder.has_value());
        EXPECT_EQ(before.holder->inflight_batches,
                  std::vector<uint64_t>{b.batch_id});
        if (c.stale) {
            // Delete and put again: a new generation (new version). Its own
            // batch cannot be placed while b holds the quota.
            ASSERT_TRUE(service_->Remove(c.key, false, "default").has_value());
            PutIn("default", c.key, 1024);
        }
        const std::string record_before = Record(c.key);
        auto r = holder_rpc_->NotifyBackupDone(holder_, b.batch_id, b.tasks,
                                               {Meta(c.size)});
        ASSERT_TRUE(r.has_value()) << toString(r.error());
        EXPECT_EQ(*r,
                  c.stale ? std::vector<uint32_t>{0} : std::vector<uint32_t>{});
        const auto after = Snap();
        EXPECT_TRUE(after.holder->inflight_batches.empty());
        EXPECT_EQ(after.holder->inflight_bytes, 0u);
        EXPECT_EQ(Record(c.key), record_before) << "registered";
        EXPECT_EQ(Record(c.key).find("B:"), std::string::npos);
    }

    // A second holder, freshly mounted, that never advertised an endpoint,
    // registers a backup of a task dispatched to holder_: recorded but not
    // served until it advertises one, then served without a second
    // registration.
    const UUID h2{0x54, 0x54};
    MountOn(h2, "seg_h2", 0x300000000, "host_h2");
    MasterClient h2_rpc(h2);
    ASSERT_EQ(h2_rpc.Connect(addr_), ErrorCode::OK);
    const BackupBatch e = DispatchOne("ep");
    auto reg = h2_rpc.NotifyBackupDone(h2, e.batch_id, e.tasks, {Meta(1024)});
    ASSERT_TRUE(reg.has_value()) << toString(reg.error());
    EXPECT_TRUE(reg->empty());
    // holder_ releases its own reservation for that batch with an empty ACK.
    ASSERT_TRUE(holder_rpc_->NotifyBackupDone(holder_, e.batch_id, {}, {}));
    EXPECT_TRUE(Snap().holder->inflight_batches.empty());
    const std::string h2_backup = "B:" + HolderTag(h2) + ":1024:";
    const std::string recorded = Record("ep");
    EXPECT_NE(recorded.find(h2_backup), std::string::npos) << recorded;
    auto& ms = service_->GetMasterServiceForTest();
    auto not_ready = ms.BatchGetBackupReplicaList({"ep"}, TenantId::Default());
    ASSERT_EQ(not_ready.size(), 1u);
    ASSERT_FALSE(not_ready[0].has_value());
    EXPECT_EQ(not_ready[0].error(), ErrorCode::REPLICA_IS_NOT_READY);
    ASSERT_TRUE(h2_rpc.BackupObjectHeartbeat(h2, "seg_h2", 0, 1 << 30));
    auto served = ms.BatchGetBackupReplicaList({"ep"}, TenantId::Default());
    ASSERT_EQ(served.size(), 1u);
    ASSERT_TRUE(served[0].has_value()) << toString(served[0].error());
    size_t h2_backups = 0;
    for (const auto& d : served[0]->replicas) {
        if (d.is_local_disk_replica() &&
            d.get_local_disk_descriptor().client_id == h2) {
            ++h2_backups;
            EXPECT_EQ(d.get_local_disk_descriptor().transport_endpoint,
                      "seg_h2");
        }
    }
    EXPECT_EQ(h2_backups, 1u);
    EXPECT_EQ(Record("ep"), recorded) << "registered a second time";
}
// R12-GREEN-ONLY-END

TEST(SsdBackupMemBench, MasterBytesPerBackup) {
    const char* n_env = std::getenv("MC_SSD_BACKUP_MEM_BENCH");
    if (n_env == nullptr) GTEST_SKIP() << "set MC_SSD_BACKUP_MEM_BENCH=N";
    const int n = std::atoi(n_env);
    setenv("MC_SSD_BACKUP_ENABLE", "1", 1);
    setenv("MC_SSD_BACKUP_BATCH_KEYS", "500", 1);
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 0;
    MasterService service(config);
    const UUID a{0xa, 0xa}, b{0xb, 0xb};
    const std::string ep_a = "172.18.0.101:50052", ep_b = "172.18.0.102:50052";
    for (const auto& [c, name, base] :
         {std::tuple{a, ep_a, size_t{0x100000000}},
          std::tuple{b, ep_b, size_t{0x10000000000}}}) {
        Segment seg;
        seg.id = generate_uuid();
        seg.name = name;
        seg.base = base;
        seg.size = 64ULL << 30;
        seg.te_endpoint = name;
        seg.host_id = "host_" + name;
        ASSERT_TRUE(service.MountSegment(seg, c).has_value());
        ASSERT_TRUE(service.MountLocalDiskSegment(c, true).has_value());
        ASSERT_TRUE(service.ReportSsdCapacity(c, 1LL << 40).has_value());
        ASSERT_TRUE(
            service.BackupObjectHeartbeat(c, c == a ? ep_a : ep_b, 0, 1LL << 40)
                .has_value());
    }
    ReplicateConfig rc;
    rc.replica_num = 1;
    for (int i = 0; i < n; ++i) {
        const std::string key = "mem_bench_key_0123456789_" + std::to_string(i);
        ASSERT_TRUE(service.PutStart(a, key, TenantId::Default(), 4096, rc)
                        .has_value());
        ASSERT_TRUE(
            service.PutEnd(a, key, TenantId::Default(), ReplicaType::MEMORY)
                .has_value());
    }
    // Primary SSD copies, as the owner of each DRAM copy would register.
    // Batches of tasks are drained from the backup queues later.
    auto drain_offloads = [&](const UUID& c, const std::string& ep) {
        auto tasks = service.OffloadObjectHeartbeat(c, true);
        ASSERT_TRUE(tasks.has_value());
        std::vector<StorageObjectMetadata> metas(tasks->size());
        for (auto& m : metas) {
            m.data_size = 4096;
            m.transport_endpoint = ep;
        }
        if (!tasks->empty()) {
            ASSERT_TRUE(
                service.NotifyOffloadSuccess(c, *tasks, metas).has_value());
        }
    };
    drain_offloads(a, ep_a);
    drain_offloads(b, ep_b);
    // Drain every queued batch first, so only replica registration falls
    // between the two heap readings.
    std::vector<std::pair<UUID, BackupBatch>> batches;
    for (int round = 0; round < 100000; ++round) {
        bool any = false;
        for (const auto& [c, ep] : {std::pair{a, ep_a}, std::pair{b, ep_b}}) {
            auto resp = service.BackupObjectHeartbeat(c, ep, 0, 1LL << 40);
            ASSERT_TRUE(resp.has_value());
            for (auto& batch : resp->batches) {
                batches.emplace_back(c, std::move(batch));
                any = true;
            }
        }
        if (!any) break;
    }
    StorageObjectMetadata meta;
    meta.data_size = 4096;
    meta.transport_endpoint = ep_a;
    const int64_t rss0 = RssBytes();
    size_t added = 0;
    for (const auto& [c, batch] : batches) {
        std::vector<StorageObjectMetadata> metas(batch.tasks.size(), meta);
        ASSERT_TRUE(
            service.NotifyBackupDone(c, batch.batch_id, batch.tasks, metas)
                .has_value());
        added += batch.tasks.size();
    }
    const int64_t rss1 = RssBytes();
    printf(
        "MEMBENCH objects=%d backups=%zu heap_before_mb=%.1f "
        "heap_after_mb=%.1f bytes_per_backup=%.1f\n",
        n, added, rss0 / 1048576.0, rss1 / 1048576.0,
        added ? static_cast<double>(rss1 - rss0) / added : 0.0);
    unsetenv("MC_SSD_BACKUP_ENABLE");
    unsetenv("MC_SSD_BACKUP_BATCH_KEYS");
}

}  // namespace mooncake::test
