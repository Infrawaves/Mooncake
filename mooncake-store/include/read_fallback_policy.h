#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

// Which keys of a failed read may be retried, and from where (SSD backup read
// fallback, see RealClient::ReadWithBackupFallback). Pure function so the
// safety rules can be unit-tested without a transfer engine.
struct ReadFallbackPlan {
    std::vector<size_t> mark_suspect;  // primary endpoint failed
    std::vector<size_t> to_backup;     // retry from the SSD backup
    // Re-query the master and read again (fresh address and lease):
    // a backup read (suspect bypass) failed, or the read finished after the
    // lease expired (G1: a slow replica in the same batch).
    std::vector<size_t> requery;
};

// How long a failed primary endpoint is bypassed
// (MC_SSD_BACKUP_SUSPECT_TTL_MS). 0 disables the bypass. Bounded so `now + ttl`
// cannot overflow the clock.
inline constexpr std::chrono::milliseconds kDefaultSuspectTtl{30000};
inline constexpr std::chrono::milliseconds kMaxSuspectTtl{24LL * 3600 * 1000};

// Unset or empty: the default. A sign, garbage, out-of-range or > 24 h value
// is invalid (*invalid set, if given) and also gives the default.
inline std::chrono::milliseconds ParseSuspectTtlMs(const char* v,
                                                   bool* invalid = nullptr) {
    if (invalid) *invalid = false;
    if (v == nullptr || *v == '\0') return kDefaultSuspectTtl;
    unsigned long long ms = 0;
    const char* p = v;
    for (; *p >= '0' && *p <= '9'; ++p) {
        ms = ms * 10 + static_cast<unsigned>(*p - '0');
        if (ms > static_cast<unsigned long long>(kMaxSuspectTtl.count())) break;
    }
    if (p == v || *p != '\0') {
        if (invalid) *invalid = true;
        return kDefaultSuspectTtl;
    }
    return std::chrono::milliseconds(static_cast<int64_t>(ms));
}

// Deadline for a suspect endpoint marked at `now`; never overflows.
inline std::chrono::steady_clock::time_point SuspectDeadline(
    std::chrono::steady_clock::time_point now, std::chrono::milliseconds ttl) {
    if (ttl < std::chrono::milliseconds::zero()) ttl = {};
    if (ttl > kMaxSuspectTtl) ttl = kMaxSuspectTtl;
    return now + ttl;
}

// Errors that point at the replica's endpoint rather than at the request.
inline bool IsEndpointReadError(ErrorCode e) {
    return e == ErrorCode::TRANSFER_FAIL || e == ErrorCode::RPC_FAIL ||
           e == ErrorCode::RPC_TIMEOUT || e == ErrorCode::SEGMENT_NOT_FOUND;
}

// The backup holder could not serve the key from its SSD. It reports these
// before writing anything to the reader, so a retry is safe. BUFFER_OVERFLOW:
// the holder's restore buffer pool was full (AllocateBatch fails before any
// source pointer is returned or any transfer to the reader is started).
inline bool IsBackupStorageError(ErrorCode e) {
    return e == ErrorCode::INVALID_KEY || e == ErrorCode::FILE_OPEN_FAIL ||
           e == ErrorCode::FILE_READ_FAIL || e == ErrorCode::BUCKET_NOT_FOUND ||
           e == ErrorCode::OBJECT_NOT_FOUND || e == ErrorCode::BUFFER_OVERFLOW;
}

// Retrying into the same destination buffer is only safe once the failed
// transfer has ended. A transfer-engine wait that hit its timeout can leave
// the old write in flight, so it is never retried. An RPC timeout is safe:
// the only RPC on a read is the SSD restore request, which carries keys and
// sizes and returns the holder's source addresses; the reader starts its own
// transfer only after the reply, so nothing was written to its buffer.
inline bool IsFallbackRetrySafe(ErrorCode e, bool may_be_in_flight) {
    if (e == ErrorCode::TRANSFER_FAIL && may_be_in_flight) return false;
    return IsEndpointReadError(e);
}

// error[i]: nullopt when key i succeeded (or has no query result).
// had_primary[i]: key i was sent to its backup but has a primary to fall
// back to.
inline ReadFallbackPlan PlanReadFallback(
    const std::vector<std::optional<ErrorCode>>& error,
    const std::vector<bool>& via_backup, const std::vector<bool>& had_primary,
    bool may_be_in_flight) {
    ReadFallbackPlan plan;
    for (size_t i = 0; i < error.size(); ++i) {
        if (!error[i]) continue;
        const ErrorCode e = *error[i];
        // The transfer completed, only too late (master lease, or the SSD
        // holder's buffer lease for an offload read): reading again is safe.
        if (e == ErrorCode::LEASE_EXPIRED || e == ErrorCode::OBJECT_HAS_LEASE) {
            plan.requery.push_back(i);
            continue;
        }
        if (via_backup[i]) {
            const bool safe = IsBackupStorageError(e) ||
                              IsFallbackRetrySafe(e, may_be_in_flight);
            if (safe && had_primary[i]) plan.requery.push_back(i);
            continue;
        }
        if (!IsEndpointReadError(e)) continue;
        plan.mark_suspect.push_back(i);
        if (IsFallbackRetrySafe(e, may_be_in_flight))
            plan.to_backup.push_back(i);
    }
    return plan;
}

// The read-fallback orchestration, independent of RealClient so it can be
// tested with fakes. Q is the per-key query result type (QueryResult).
template <typename Q>
struct ReadFallbackHooks {
    using QueryVec = std::vector<tl::expected<Q, ErrorCode>>;
    // Endpoint the reader would use for this query result ("" if none).
    std::function<std::string(const Q&)> endpoint_of;
    std::function<bool(const std::string&)> is_suspect;
    std::function<void(const std::string&)> mark_suspect;
    // Backup-only query (master BatchGetBackupReplicaList).
    std::function<QueryVec(const std::vector<std::string>&)> query_backup;
    // Fresh normal query (new address and lease).
    std::function<QueryVec(const std::vector<std::string>&)> requery;
    // Test observation, null by default: called just before each
    // query_backup / requery with the input indices of its keys, in order.
    std::function<void(const std::vector<size_t>&)> on_subset_query;
    // Read the keys at `indices` (into their own destination buffers).
    std::function<std::vector<tl::expected<int64_t, ErrorCode>>(
        const std::vector<size_t>& indices, const QueryVec& queries)>
        read;
    // UnquiescedTransferFailureCount() of the reading thread.
    std::function<uint64_t()> unquiesced_count;
    // Elapsed time beyond which a failed transfer is treated as possibly
    // still in flight (backstop to the unquiesced counter).
    std::chrono::steady_clock::duration unterminated_after =
        std::chrono::seconds(50);
};

struct ReadFallbackStats {
    size_t to_backup = 0;
    size_t requery = 0;
    size_t retried = 0;
    size_t retried_ok = 0;
    bool unquiesced = false;
};

template <typename Q>
std::vector<tl::expected<int64_t, ErrorCode>> RunReadWithBackupFallback(
    const std::vector<std::string>& keys,
    std::vector<tl::expected<Q, ErrorCode>> query_results,
    const ReadFallbackHooks<Q>& h, ReadFallbackStats* stats = nullptr) {
    using QueryVec = std::vector<tl::expected<Q, ErrorCode>>;
    const size_t n = keys.size();
    std::vector<size_t> all(n);
    for (size_t i = 0; i < n; ++i) all[i] = i;
    if (n == 0 || query_results.size() != n) return h.read(all, query_results);

    // Endpoint each key would be read from, and keys headed to a suspect one.
    std::vector<std::string> endpoint(n);
    std::vector<size_t> to_backup;
    for (size_t i = 0; i < n; ++i) {
        if (!query_results[i]) continue;
        endpoint[i] = h.endpoint_of(*query_results[i]);
        if (!endpoint[i].empty() && h.is_suspect(endpoint[i])) {
            to_backup.push_back(i);
        }
    }
    // Keys sent to their backup keep a marker that they have a primary, so a
    // failed backup read can still try it (via a fresh query, R3).
    std::vector<bool> via_backup(n, false), has_primary(n, false);
    if (!to_backup.empty()) {
        std::vector<std::string> sub;
        for (size_t i : to_backup) sub.push_back(keys[i]);
        if (h.on_subset_query) h.on_subset_query(to_backup);
        auto backups = h.query_backup(sub);
        // Q may not be assignable: rebuild the vector.
        std::vector<std::optional<size_t>> backup_of(n);
        for (size_t j = 0; j < to_backup.size() && j < backups.size(); ++j) {
            if (backups[j]) backup_of[to_backup[j]] = j;
        }
        QueryVec merged;
        merged.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            if (backup_of[i]) {
                has_primary[i] = query_results[i].has_value();
                merged.push_back(std::move(backups[*backup_of[i]]));
                via_backup[i] = true;
            } else {
                merged.push_back(std::move(query_results[i]));
            }
        }
        query_results = std::move(merged);
    }
    const auto read_start = std::chrono::steady_clock::now();
    const uint64_t unquiesced_before = h.unquiesced_count();
    auto results = h.read(all, query_results);
    const auto read_elapsed = std::chrono::steady_clock::now() - read_start;
    // R1: some transfer of this read failed without being shown to have
    // stopped: it may still write the destination buffers. Do not reuse them
    // for a retry, and do not blame the remote endpoint (often local).
    const bool unquiesced = h.unquiesced_count() != unquiesced_before;
    const bool may_be_in_flight =
        unquiesced || read_elapsed >= h.unterminated_after;
    std::vector<std::optional<ErrorCode>> error(n);
    for (size_t i = 0; i < n && i < results.size(); ++i) {
        if (!results[i] && query_results[i]) error[i] = results[i].error();
    }
    const auto plan =
        PlanReadFallback(error, via_backup, has_primary, may_be_in_flight);
    if (!unquiesced) {
        for (size_t i : plan.mark_suspect) {
            if (!endpoint[i].empty()) h.mark_suspect(endpoint[i]);
        }
    }
    std::vector<size_t> retry;
    QueryVec retry_q;
    if (!plan.to_backup.empty()) {
        std::vector<std::string> sub;
        for (size_t i : plan.to_backup) sub.push_back(keys[i]);
        if (h.on_subset_query) h.on_subset_query(plan.to_backup);
        auto backups = h.query_backup(sub);
        for (size_t j = 0; j < plan.to_backup.size() && j < backups.size();
             ++j) {
            if (!backups[j]) continue;
            retry.push_back(plan.to_backup[j]);
            retry_q.push_back(std::move(backups[j]));
        }
    }
    // R3: never reuse a saved query (its lease may have run out); ask again.
    if (!plan.requery.empty()) {
        std::vector<std::string> sub;
        for (size_t i : plan.requery) sub.push_back(keys[i]);
        if (h.on_subset_query) h.on_subset_query(plan.requery);
        auto fresh = h.requery(sub);
        for (size_t j = 0; j < plan.requery.size() && j < fresh.size(); ++j) {
            if (!fresh[j]) continue;
            retry.push_back(plan.requery[j]);
            retry_q.push_back(std::move(fresh[j]));
        }
    }
    size_t ok = 0;
    if (!retry.empty()) {
        auto retried = h.read(retry, retry_q);  // at most one extra read
        for (size_t j = 0; j < retry.size() && j < retried.size(); ++j) {
            if (retried[j]) ++ok;
            results[retry[j]] = std::move(retried[j]);
        }
    }
    if (stats) {
        stats->to_backup = plan.to_backup.size();
        stats->requery = plan.requery.size();
        stats->retried = retry.size();
        stats->retried_ok = ok;
        stats->unquiesced = unquiesced;
    }
    return results;
}

}  // namespace mooncake
