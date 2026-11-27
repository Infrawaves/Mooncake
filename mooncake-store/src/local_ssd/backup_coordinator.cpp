#include "local_ssd/backup_coordinator.h"

#include "bool_parser.h"
#include "random.h"

#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <queue>
#include <string>
#include <string_view>
#include <utility>

namespace mooncake {

namespace {

// Same parser as FileStorage::BackupConfig so master and worker agree on
// MC_SSD_BACKUP_ENABLE (1/true/yes/on/enable, any case).
bool EnvBool(const char* name, bool fallback) {
    const char* v = std::getenv(name);
    if (v == nullptr) return fallback;
    return TryParseBool(v).value_or(false);
}

// Non-negative decimal no larger than `max`; anything else (sign, garbage,
// overflow) falls back with a warning. strtoull alone would accept "-1" as
// 2^64-1, which then wraps into a negative duration.
uint64_t EnvU64(const char* name, uint64_t fallback,
                uint64_t max = std::numeric_limits<int64_t>::max()) {
    const char* v = std::getenv(name);
    if (v == nullptr || *v == '\0') return fallback;
    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(v, &end, 10);
    if (*v < '0' || *v > '9' || end == v || *end != '\0' || errno == ERANGE ||
        parsed > max) {
        LOG(WARNING) << "Ignoring invalid " << name << "=" << v;
        return fallback;
    }
    return parsed;
}

// Durations are capped so later time arithmetic cannot overflow.
constexpr uint64_t kMaxDurationMs = 24ULL * 3600 * 1000;

std::string ScopedKey(const std::string& tenant, const std::string& key) {
    std::string out;
    out.reserve(tenant.size() + key.size() + 1);
    out.append(tenant).push_back('\0');
    out.append(key);
    return out;
}

}  // namespace

SsdBackupCoordinator::Config SsdBackupCoordinator::Config::FromEnvironment() {
    Config c;
    c.enabled = EnvBool("MC_SSD_BACKUP_ENABLE", false);
    c.batch_bytes = EnvU64("MC_SSD_BACKUP_BATCH_BYTES", c.batch_bytes);
    c.batch_keys =
        static_cast<uint32_t>(EnvU64("MC_SSD_BACKUP_BATCH_KEYS", c.batch_keys,
                                     std::numeric_limits<uint32_t>::max()));
    if (c.batch_keys == 0) c.batch_keys = 1;
    c.batch_max_age = std::chrono::milliseconds(
        EnvU64("MC_SSD_BACKUP_BATCH_MAX_AGE_MS", c.batch_max_age.count(),
               kMaxDurationMs));
    c.task_timeout = std::chrono::milliseconds(
        EnvU64("MC_SSD_BACKUP_TASK_TIMEOUT_MS", c.task_timeout.count(),
               kMaxDurationMs));
    c.max_inflight_bytes_per_client = EnvU64("MC_SSD_BACKUP_MAX_INFLIGHT_BYTES",
                                             c.max_inflight_bytes_per_client);
    c.defer_max_age = std::chrono::milliseconds(
        EnvU64("MC_SSD_BACKUP_DEFER_MAX_AGE_MS", c.defer_max_age.count(),
               kMaxDurationMs));
    c.max_deferred_bytes_per_source =
        EnvU64("MC_SSD_BACKUP_MAX_DEFERRED_BYTES_PER_SOURCE",
               c.max_deferred_bytes_per_source);
    c.max_deferred_tasks_per_source =
        EnvU64("MC_SSD_BACKUP_MAX_DEFERRED_TASKS_PER_SOURCE",
               c.max_deferred_tasks_per_source);
    c.max_deferred_tasks_total =
        EnvU64("MC_SSD_BACKUP_MAX_DEFERRED_TASKS", c.max_deferred_tasks_total);
    return c;
}

SsdBackupCoordinator::SsdBackupCoordinator(Config config)
    : config_(std::move(config)) {
    if (config_.enabled) {
        LOG(INFO) << "SSD backup enabled: batch_bytes=" << config_.batch_bytes
                  << ", batch_keys=" << config_.batch_keys
                  << ", batch_max_age_ms=" << config_.batch_max_age.count()
                  << ", task_timeout_ms=" << config_.task_timeout.count()
                  << ", max_inflight_bytes_per_client="
                  << config_.max_inflight_bytes_per_client
                  << ", defer_max_age_ms=" << config_.defer_max_age.count()
                  << ", max_deferred_bytes_per_source="
                  << config_.max_deferred_bytes_per_source
                  << ", max_deferred_tasks_per_source="
                  << config_.max_deferred_tasks_per_source
                  << ", max_deferred_tasks_total="
                  << config_.max_deferred_tasks_total;
    }
}

void SsdBackupCoordinator::OnPut(const UUID& source,
                                 const std::string& source_host,
                                 BackupTaskItem item, Clock::time_point now,
                                 std::optional<UUID> also_avoid) {
    if (!config_.enabled || item.size <= 0) return;
    if (also_avoid && *also_avoid == source) also_avoid.reset();
    MutexLocker lock(&mutex_);
    auto& pending = sources_[{source, also_avoid.value_or(UUID{})}];
    pending.source = source;
    pending.also_avoid = also_avoid;
    if (pending.items.empty()) pending.first_item_time = now;
    if (!source_host.empty()) pending.host = source_host;
    // A later put of a key still pending here (Remove + Put within the batch
    // window) replaces the older task: puts of one key reach OnPut in order,
    // so the last one is the version the source holds now. Two tasks of one
    // key in a batch would have the holder read the new bytes once and ACK
    // them under one of the two versions.
    auto [slot, fresh] = pending.index.try_emplace(
        ScopedKey(item.tenant_id, item.key), pending.items.size());
    if (!fresh) {
        auto& old = pending.items[slot->second];
        pending.bytes -= static_cast<uint64_t>(old.size);
        pending.bytes += static_cast<uint64_t>(item.size);
        old = std::move(item);
    } else {
        pending.bytes += static_cast<uint64_t>(item.size);
        pending.items.push_back(std::move(item));
    }
    if (pending.bytes >= config_.batch_bytes ||
        pending.items.size() >= config_.batch_keys) {
        FlushSource(pending, now);
    }
}

void SsdBackupCoordinator::FlushSource(PendingSource& pending,
                                       Clock::time_point now) {
    if (pending.items.empty()) return;
    const UUID& source = pending.source;
    const uint64_t bytes = pending.bytes;
    const Clock::time_point oldest_put = pending.first_item_time;
    // The source's host comes from its own backup heartbeat when the caller
    // did not know it (PutEnd runs under a key lock and does not look it up).
    if (pending.host.empty()) {
        auto hit = hosts_.find(source);
        if (hit != hosts_.end()) pending.host = hit->second;
    }
    if (pending.host.empty()) {
        auto sit = clients_.find(source);
        if (sit != clients_.end()) pending.host = sit->second.host;
    }
    std::vector<BackupTaskItem> items;
    items.swap(pending.items);
    pending.index.clear();
    pending.bytes = 0;
    if (pending.host.empty()) {
        // Fail closed: without the source's host we cannot keep the backup
        // off the source's machine.
        ++stats_.batches_unknown_host;
        stats_.objects_unplaced += items.size();
        LOG_EVERY_N(WARNING, 100)
            << "ssd_backup_unknown_source_host source=" << source
            << " dropped_batch_keys=" << items.size();
        return;
    }
    if (pending.also_avoid && pending.avoid_host.empty()) {
        if (auto hit = hosts_.find(*pending.also_avoid); hit != hosts_.end()) {
            pending.avoid_host = hit->second;
        } else if (auto cit = clients_.find(*pending.also_avoid);
                   cit != clients_.end()) {
            pending.avoid_host = cit->second.host;
        }
    }
    if (pending.also_avoid && pending.avoid_host.empty()) {
        // Same fail-closed rule as for the source host.
        ++stats_.batches_unknown_host;
        stats_.objects_unplaced += items.size();
        return;
    }
    // Room freed since the last pass goes to waiting batches first (oldest
    // first, any source): a new batch must not take it from them. What is
    // still free afterwards (e.g. a client the waiting batches cannot use)
    // the new batch may take.
    if (deferred_batches_ > 0 &&
        (deferred_room_changed_ ||
         now - last_deferred_retry_ >= kDeferredRetryFallback)) {
        RetryAllDeferred(now);
    }
    // Later batches wait behind earlier waiting ones (dispatch in put order).
    PlaceResult placed =
        pending.deferred.empty()
            ? PlaceBatch(pending, items, bytes, now, oldest_put)
            : PlaceResult::kBusy;
    if (placed == PlaceResult::kPlaced) return;
    if (placed == PlaceResult::kBusy && config_.defer_max_age.count() > 0) {
        if (DeferFits(source, bytes, items.size())) {
            AccountDeferred(source, bytes, items.size(), /*add=*/true);
            pending.deferred.push_back(
                {std::move(items), bytes, now, oldest_put});
            next_deferred_expiry_ =
                std::min(next_deferred_expiry_, now + config_.defer_max_age);
            ++stats_.batches_deferred;
            return;
        }
        ++stats_.deferred_overflow;  // busy, and no room left to wait
    }
    // Best effort: no eligible backup client right now (or no room to wait).
    ++stats_.batches_unplaced;
    stats_.objects_unplaced += items.size();
}

bool SsdBackupCoordinator::DeferFits(const UUID& source, uint64_t bytes,
                                     uint64_t tasks) const {
    if (deferred_tasks_total_ + tasks > config_.max_deferred_tasks_total) {
        return false;
    }
    DeferredUsage u;
    if (auto it = deferred_usage_.find(source); it != deferred_usage_.end()) {
        u = it->second;
    }
    return u.bytes + bytes <= config_.max_deferred_bytes_per_source &&
           u.tasks + tasks <= config_.max_deferred_tasks_per_source;
}

void SsdBackupCoordinator::AccountDeferred(const UUID& source, uint64_t bytes,
                                           uint64_t tasks, bool add) {
    if (add) {
        auto& u = deferred_usage_[source];
        u.bytes += bytes;
        u.tasks += tasks;
        deferred_tasks_total_ += tasks;
        ++deferred_batches_;
        return;
    }
    auto it = deferred_usage_.find(source);
    if (it != deferred_usage_.end()) {
        it->second.bytes -= std::min(it->second.bytes, bytes);
        it->second.tasks -= std::min(it->second.tasks, tasks);
        if (it->second.tasks == 0) deferred_usage_.erase(it);
    }
    deferred_tasks_total_ -= std::min(deferred_tasks_total_, tasks);
    --deferred_batches_;
}

void SsdBackupCoordinator::DropDeferredFront(PendingSource& pending,
                                             uint64_t* cause) {
    auto& d = pending.deferred.front();
    AccountDeferred(pending.source, d.bytes, d.items.size(), /*add=*/false);
    ++stats_.batches_unplaced;
    ++*cause;
    stats_.objects_unplaced += d.items.size();
    pending.deferred.pop_front();
}

SsdBackupCoordinator::PlaceResult SsdBackupCoordinator::PlaceBatch(
    PendingSource& pending, std::vector<BackupTaskItem>& items, uint64_t bytes,
    Clock::time_point now, Clock::time_point oldest_put) {
    const UUID& source = pending.source;
    bool busy = false;
    auto target = PickBackupClient(pending, bytes, now, &busy);
    if (!target) {
        return busy ? PlaceResult::kBusy : PlaceResult::kNoCandidate;
    }
    auto& c = clients_[*target];
    BackupBatch batch;
    batch.batch_id = next_batch_id_++;
    batch.source_client_id = source;
    batch.tasks = std::move(items);
    // Reserve the bytes now so concurrent selections see this load before
    // the client's next heartbeat reports it.
    c.inflight_bytes += bytes;
    c.inflight[batch.batch_id] = Inflight{bytes, now};
    if (oldest_put != Clock::time_point{}) {
        batch_timing_[batch.batch_id] = BatchTiming{oldest_put, now};
    }
    c.queued.push_back(std::move(batch));
    ++stats_.batches_dispatched;
    VLOG(1) << "ssd_backup_dispatch batch=" << c.queued.back().batch_id
            << " source=" << source << " target=" << *target
            << " bytes=" << bytes;
    return PlaceResult::kPlaced;
}

void SsdBackupCoordinator::RetryAllDeferred(Clock::time_point now) {
    deferred_room_changed_ = false;
    last_deferred_retry_ = now;
    next_deferred_expiry_ = Clock::time_point::max();
    // Oldest waiting batch first across sources (a fixed source order would
    // let the first sources take every freed slot while later ones starve),
    // in order within a source: each source enters with its oldest batch and
    // re-enters with the next one once that is placed or dropped.
    using Entry = std::pair<Clock::time_point, PendingSource*>;
    auto later = [](const Entry& a, const Entry& b) {
        return a.first > b.first;
    };
    std::priority_queue<Entry, std::vector<Entry>, decltype(later)> heap(later);
    for (auto& [key, pending] : sources_) {
        if (!pending.deferred.empty()) {
            heap.emplace(pending.deferred.front().since, &pending);
        }
    }
    while (!heap.empty()) {
        PendingSource& pending = *heap.top().second;
        heap.pop();
        auto& d = pending.deferred.front();
        if (now - d.since >= config_.defer_max_age) {
            DropDeferredFront(pending, &stats_.deferred_expired);
        } else {
            const uint64_t bytes = d.bytes;
            const uint64_t tasks = d.items.size();
            const auto r =
                PlaceBatch(pending, d.items, bytes, now, d.oldest_put);
            if (r == PlaceResult::kBusy) {
                // Keep the order: this source waits for the next pass.
                next_deferred_expiry_ = std::min(
                    next_deferred_expiry_, d.since + config_.defer_max_age);
                continue;
            }
            if (r == PlaceResult::kNoCandidate) {
                // e.g. the only candidate left or went stale
                DropDeferredFront(pending, &stats_.deferred_no_candidate);
            } else {
                AccountDeferred(pending.source, bytes, tasks, /*add=*/false);
                pending.deferred.pop_front();
            }
        }
        if (!pending.deferred.empty()) {
            heap.emplace(pending.deferred.front().since, &pending);
        }
    }
}

double SsdBackupCoordinator::Load(const Candidate& c) const {
    if (c.capacity_bytes <= 0) return 1.0;
    return (static_cast<double>(c.used_bytes) +
            static_cast<double>(c.inflight_bytes)) /
           static_cast<double>(c.capacity_bytes);
}

bool SsdBackupCoordinator::Eligible(const UUID& id, const Candidate& c,
                                    const PendingSource& pending,
                                    uint64_t batch_bytes, Clock::time_point now,
                                    bool* busy) const {
    if (id == pending.source) return false;
    if (c.host.empty() || c.host == pending.host) return false;
    if (pending.also_avoid) {
        // Unknown host of the client to avoid: fail closed on that client.
        if (id == *pending.also_avoid) return false;
        if (!pending.avoid_host.empty() && c.host == pending.avoid_host)
            return false;
    }
    if (c.capacity_bytes <= 0) return false;
    if (now - c.last_heartbeat > config_.candidate_stale_after) return false;
    if (now < c.cooldown_until) return false;
    // A batch larger than the whole inflight budget never fits.
    if (batch_bytes > config_.max_inflight_bytes_per_client) return false;
    // Busy: room frees up as dispatched batches complete, and a client at
    // the usage limit demotes pinned backups below it within a heartbeat
    // (its pin high watermark is under max_usage_ratio) and reports less.
    const uint64_t budget = config_.max_inflight_bytes_per_client;
    if (Load(c) >= config_.max_usage_ratio ||
        c.inflight_bytes + batch_bytes > budget ||
        c.queued.size() >= config_.max_pending_batches_per_client) {
        if (busy) *busy = true;
        return false;
    }
    return true;
}

std::optional<UUID> SsdBackupCoordinator::PickBackupClient(
    const PendingSource& pending, uint64_t batch_bytes, Clock::time_point now,
    bool* busy) {
    const UUID& source = pending.source;
    // Power of two choices among eligible clients: sample two at random and
    // keep the less loaded one. Load counts bytes already dispatched but not
    // yet acknowledged, which the master tracks without heartbeat delay.
    std::vector<const ClientMap::value_type*> eligible;
    eligible.reserve(clients_.size());
    for (const auto& entry : clients_) {
        if (Eligible(entry.first, entry.second, pending, batch_bytes, now,
                     busy)) {
            eligible.push_back(&entry);
        }
    }
    if (eligible.empty()) return std::nullopt;
    const auto* a = eligible[randomIndex(eligible.size())];
    const auto* b = eligible[randomIndex(eligible.size())];
    const auto* pick = Load(a->second) <= Load(b->second) ? a : b;
    VLOG(1) << "ssd_backup_pick source=" << source << " cand1=" << a->first
            << " load1=" << Load(a->second) << " cand2=" << b->first
            << " load2=" << Load(b->second) << " picked=" << pick->first
            << " eligible=" << eligible.size();
    return pick->first;
}

void SsdBackupCoordinator::FlushAgedSources(Clock::time_point now) {
    for (auto& [key, pending] : sources_) {
        if (!pending.items.empty() &&
            now - pending.first_item_time >= config_.batch_max_age) {
            FlushSource(pending, now);
        }
    }
}

void SsdBackupCoordinator::ExpireInflight(const UUID& id, Candidate& c,
                                          Clock::time_point now) {
    // A queued batch has not reached the holder yet: it only expires when the
    // holder itself stopped heartbeating (it would never be delivered), and
    // that does not count toward the holder's cooldown. A healthy holder with
    // a backlog gets every batch; its timeout starts at delivery (R8-Q2).
    const bool holder_silent = now - c.last_heartbeat >= config_.task_timeout;
    for (auto it = c.inflight.begin(); it != c.inflight.end();) {
        const bool delivered = it->second.delivered;
        if (delivered ? now - it->second.dispatched_at >= config_.task_timeout
                      : holder_silent) {
            c.inflight_bytes -= std::min(c.inflight_bytes, it->second.bytes);
            ++stats_.batches_timed_out;
            if (delivered &&
                ++c.consecutive_timeouts >= config_.max_consecutive_timeouts) {
                c.cooldown_until = now + config_.failure_cooldown;
                c.consecutive_timeouts = 0;
                ++stats_.candidates_cooling_down;
                LOG(WARNING) << "ssd_backup_cooldown client=" << id
                             << " after repeated batch timeouts";
            }
            const uint64_t batch_id = it->first;
            it = c.inflight.erase(it);
            deferred_room_changed_ = true;
            c.queued.erase(std::remove_if(c.queued.begin(), c.queued.end(),
                                          [batch_id](const BackupBatch& b) {
                                              return b.batch_id == batch_id;
                                          }),
                           c.queued.end());
        } else {
            ++it;
        }
    }
}

BackupHeartbeatResponse SsdBackupCoordinator::OnHeartbeat(
    const UUID& client, const std::string& client_host,
    const std::string& endpoint, int64_t backup_used_bytes,
    int64_t backup_capacity_bytes, Clock::time_point now) {
    BackupHeartbeatResponse response;
    if (!config_.enabled) return response;
    MutexLocker lock(&mutex_);
    auto& c = clients_[client];
    const bool host_was_empty = c.host.empty();
    if (!client_host.empty()) {
        c.host = client_host;
    } else if (auto hit = hosts_.find(client); hit != hosts_.end()) {
        c.host = hit->second;
    }
    if (!endpoint.empty()) c.endpoint = endpoint;
    // A client that becomes able to take waiting batches: new, back from a
    // stale heartbeat or a cooldown, with more capacity, or with less backup
    // in use. (Anything missed here is retried by the fallback below.)
    if (c.last_heartbeat == Clock::time_point{} ||
        now - c.last_heartbeat > config_.candidate_stale_after ||
        (c.cooldown_until != Clock::time_point{} && now >= c.cooldown_until) ||
        backup_capacity_bytes > c.capacity_bytes ||
        backup_used_bytes < c.used_bytes ||
        (host_was_empty && !c.host.empty())) {
        deferred_room_changed_ = true;
    }
    if (c.cooldown_until != Clock::time_point{} && now >= c.cooldown_until) {
        c.cooldown_until = Clock::time_point{};  // over: noticed once
    }
    c.used_bytes = backup_used_bytes;
    c.capacity_bytes = backup_capacity_bytes;
    c.last_heartbeat = now;
    // Release timed-out reservations first, so aged batches flushed in this
    // same heartbeat see the freed capacity instead of being dropped.
    for (auto& [id, cand] : clients_) {
        ExpireInflight(id, cand, now);
    }
    PruneBatchTimingLocked(now);
    // Waiting batches before aged partial ones (FlushAgedSources), which
    // would otherwise take room freed for older work.
    if (deferred_batches_ > 0 &&
        (deferred_room_changed_ || now >= next_deferred_expiry_ ||
         now - last_deferred_retry_ >= kDeferredRetryFallback)) {
        RetryAllDeferred(now);
    }
    FlushAgedSources(now);
    MaybeLogSummary(now);
    while (!c.queued.empty() &&
           response.batches.size() < config_.max_batches_per_heartbeat) {
        // The timeout clock starts when the client actually receives it.
        auto it = c.inflight.find(c.queued.front().batch_id);
        if (it != c.inflight.end()) {
            it->second.dispatched_at = now;
            it->second.delivered = true;
        }
        response.batches.push_back(std::move(c.queued.front()));
        c.queued.pop_front();
        deferred_room_changed_ = true;  // the queue has room again
    }
    response.refreshes.swap(c.refreshes);
    c.refresh_keys.clear();
    return response;
}

void SsdBackupCoordinator::OnBatchDone(const UUID& client, uint64_t batch_id) {
    if (!config_.enabled) return;
    MutexLocker lock(&mutex_);
    auto cit = clients_.find(client);
    if (cit == clients_.end()) return;
    auto& c = cit->second;
    auto it = c.inflight.find(batch_id);
    if (it == c.inflight.end()) return;
    c.inflight_bytes -= std::min(c.inflight_bytes, it->second.bytes);
    c.inflight.erase(it);
    c.consecutive_timeouts = 0;
    deferred_room_changed_ = true;
    ++stats_.batches_completed;
}

std::optional<double> SsdBackupCoordinator::RecordBackedUp(
    uint64_t batch_id, Clock::time_point now) {
    if (!config_.enabled) return std::nullopt;
    MutexLocker lock(&mutex_);
    ++stats_.backed_up_batches;
    auto it = batch_timing_.find(batch_id);
    if (it == batch_timing_.end()) {
        ++stats_.backed_up_untimed;
        return std::nullopt;
    }
    const double s =
        std::chrono::duration<double>(now - it->second.oldest_put).count();
    batch_timing_.erase(it);
    ++stats_.put_to_backup_timed;
    stats_.put_to_backup_seconds_sum += s;
    stats_.put_to_backup_seconds_max =
        std::max(stats_.put_to_backup_seconds_max, s);
    stats_.put_to_backup_seconds_window_max =
        std::max(stats_.put_to_backup_seconds_window_max, s);
    return s;
}

void SsdBackupCoordinator::PruneBatchTimingLocked(Clock::time_point now) {
    while (!batch_timing_.empty()) {
        const auto& front = batch_timing_.begin()->second;
        if (batch_timing_.size() <= kMaxBatchTimings &&
            now - front.dispatched_at < kBatchTimingHorizon) {
            break;
        }
        batch_timing_.erase(batch_timing_.begin());
        ++stats_.batch_timing_dropped;
    }
}

void SsdBackupCoordinator::MaybeLogSummary(Clock::time_point now) {
    if (now - last_summary_ < config_.summary_interval) return;
    last_summary_ = now;
    const auto& s = stats_;
    const auto& p = last_summary_stats_;
    size_t queued = 0;
    uint64_t inflight = 0;
    for (const auto& [id, c] : clients_) {
        queued += c.queued.size();
        inflight += c.inflight_bytes;
    }
    LOG(INFO)
        << "ssd_backup_summary clients=" << clients_.size()
        << " dispatched=" << s.batches_dispatched - p.batches_dispatched
        << " completed=" << s.batches_completed - p.batches_completed
        << " timed_out=" << s.batches_timed_out - p.batches_timed_out
        << " unplaced=" << s.batches_unplaced - p.batches_unplaced
        << " unknown_host=" << s.batches_unknown_host - p.batches_unknown_host
        << " objects_unplaced=" << s.objects_unplaced - p.objects_unplaced
        << " refreshes_dropped=" << s.refreshes_dropped - p.refreshes_dropped
        << " deferred=" << s.batches_deferred - p.batches_deferred
        << " deferred_overflow=" << s.deferred_overflow - p.deferred_overflow
        << " deferred_expired=" << s.deferred_expired - p.deferred_expired
        << " deferred_no_candidate="
        << s.deferred_no_candidate - p.deferred_no_candidate
        << " deferred_removed=" << s.deferred_removed - p.deferred_removed
        << " queued_batches=" << queued << " inflight_bytes=" << inflight
        << " waiting_batches=" << deferred_batches_
        << " waiting_tasks=" << deferred_tasks_total_
        << " waiting_bytes=" << WaitingBytesLocked()
        << " backed_up=" << s.backed_up_batches - p.backed_up_batches
        << " backed_up_untimed=" << s.backed_up_untimed - p.backed_up_untimed
        << " batch_timing_dropped="
        << s.batch_timing_dropped - p.batch_timing_dropped
        << " put_to_backup_avg_s="
        << (s.put_to_backup_timed > p.put_to_backup_timed
                ? (s.put_to_backup_seconds_sum - p.put_to_backup_seconds_sum) /
                      static_cast<double>(s.put_to_backup_timed -
                                          p.put_to_backup_timed)
                : 0.0)
        << " put_to_backup_max_s=" << s.put_to_backup_seconds_window_max;
    stats_.put_to_backup_seconds_window_max = 0;
    last_summary_stats_ = stats_;
}

void SsdBackupCoordinator::EnqueueRefresh(const UUID& holder,
                                          BackupRefreshItem item) {
    if (!config_.enabled) return;
    MutexLocker lock(&mutex_);
    auto cit = clients_.find(holder);
    if (cit == clients_.end()) return;
    auto& c = cit->second;
    if (c.refreshes.size() >= config_.max_refreshes_per_client) {
        ++stats_.refreshes_dropped;
        return;
    }
    if (c.refresh_keys.insert(ScopedKey(item.tenant_id, item.key)).second) {
        c.refreshes.push_back(std::move(item));
    }
}

void SsdBackupCoordinator::SetClientHost(const UUID& client,
                                         const std::string& host) {
    if (!config_.enabled || host.empty()) return;
    MutexLocker lock(&mutex_);
    hosts_[client] = host;
    auto it = clients_.find(client);
    if (it != clients_.end()) it->second.host = host;
}

std::string SsdBackupCoordinator::GetEndpoint(const UUID& client) const {
    MutexLocker lock(&mutex_);
    auto it = clients_.find(client);
    return it == clients_.end() ? std::string() : it->second.endpoint;
}

void SsdBackupCoordinator::RemoveClient(const UUID& client) {
    if (!config_.enabled) return;
    MutexLocker lock(&mutex_);
    clients_.erase(client);
    for (auto it = sources_.begin(); it != sources_.end();) {
        if (it->first.first == client) {
            // Its DRAM copies are gone with it: the waiting batches can no
            // longer be read. Counted as dropped (removed).
            while (!it->second.deferred.empty()) {
                DropDeferredFront(it->second, &stats_.deferred_removed);
            }
            it = sources_.erase(it);
        } else {
            ++it;
        }
    }
    // A waiting batch may have been held back by this client's busy queue
    // or now have no candidate left: let the next heartbeat look again.
    deferred_room_changed_ = true;
    hosts_.erase(client);
}

uint64_t SsdBackupCoordinator::WaitingBytesLocked() const {
    uint64_t bytes = 0;
    for (const auto& [id, u] : deferred_usage_) bytes += u.bytes;
    return bytes;
}

SsdBackupCoordinator::Stats SsdBackupCoordinator::GetStats() const {
    MutexLocker lock(&mutex_);
    Stats s = stats_;
    s.waiting_batches = deferred_batches_;
    s.waiting_tasks = deferred_tasks_total_;
    s.waiting_bytes = WaitingBytesLocked();
    if (deferred_batches_ > 0) {
        auto oldest = Clock::time_point::max();
        for (const auto& [key, pending] : sources_) {
            if (!pending.deferred.empty()) {
                oldest = std::min(oldest, pending.deferred.front().since);
            }
        }
        s.waiting_oldest_age_seconds =
            std::chrono::duration<double>(Clock::now() - oldest).count();
    }
    return s;
}

std::optional<SsdBackupCoordinator::ClientSnapshot>
SsdBackupCoordinator::SnapshotForTest(const UUID& client) const {
    MutexLocker lock(&mutex_);
    auto it = clients_.find(client);
    if (it == clients_.end()) return std::nullopt;
    ClientSnapshot s;
    s.inflight_bytes = it->second.inflight_bytes;
    for (const auto& [id, inflight] : it->second.inflight) {
        s.inflight_batches.push_back(id);
    }
    std::sort(s.inflight_batches.begin(), s.inflight_batches.end());
    s.queued_batches = it->second.queued.size();
    return s;
}

}  // namespace mooncake
