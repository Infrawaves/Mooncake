#include "file_storage.h"

#include <algorithm>
#include <cerrno>
#include <future>
#include <cstdlib>
#include <string_view>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "aligned_client_buffer.h"
#include "bool_parser.h"
#include "random.h"
#include "storage_backend.h"
#include "transfer_task.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "client_metric.h"
#include "common/client_buffer_allocation.h"
#include "device/accelerator_registry.h"
#ifdef USE_URING
#include "file_interface.h"
#endif

namespace mooncake {

namespace {

std::vector<OffloadTaskItem> BuildOffloadTasksFromStorageKeys(
    const std::vector<std::string>& storage_keys,
    const std::vector<StorageObjectMetadata>& metadatas) {
    std::vector<OffloadTaskItem> tasks;
    tasks.reserve(storage_keys.size());
    for (size_t i = 0; i < storage_keys.size(); ++i) {
        auto [tenant_id, key] = TenantId::ParseScopedKey(storage_keys[i]);
        const int64_t size =
            i < metadatas.size() ? metadatas[i].data_size : int64_t{0};
        tasks.push_back(OffloadTaskItem{.tenant_id = tenant_id.value(),
                                        .key = std::move(key),
                                        .size = size});
    }
    return tasks;
}

}  // namespace

FileStorage::FileStorage(const FileStorageConfig& config,
                         std::shared_ptr<Client> client,
                         const std::string& local_rpc_addr,
                         SsdMetric* ssd_metric)
    : config_(config),
      client_(client),
      ssd_metric_(ssd_metric),
      local_rpc_addr_(local_rpc_addr),
      pinned_buffer_pool_(std::make_unique<PinnedBufferPool>()),
      client_buffer_allocator_(AlignedClientBufferAllocator::create(
          config.local_buffer_size, client ? client->GetProtocol() : "")) {
    if (!config_.Validate()) {
        throw std::invalid_argument("Invalid FileStorage configuration");
    }

    if (config.pinned_restore_arena_size > 0) {
        if (config.use_uring) {
            LOG(WARNING) << "Pinned SSD restore is disabled with io_uring";
        } else if (!client ||
                   !client->CanUseLocalMemcpy(client->GetSegmentEndpoint())) {
            LOG(WARNING)
                << "Pinned SSD restore is disabled: local memcpy unavailable";
        } else {
            auto buffer = PinnedBufferPool::AllocatePinned(
                static_cast<size_t>(config.pinned_restore_arena_size));
            if (buffer.pinned_host.addr) {
                pinned_restore_arena_ = std::move(buffer);
                pinned_restore_arena_allocator_ = ClientBufferAllocator::create(
                    pinned_restore_arena_.data, pinned_restore_arena_.capacity,
                    client->GetProtocol());
                LOG(INFO) << "Initialized pinned SSD restore arena, size="
                          << pinned_restore_arena_.capacity;
            } else {
                LOG(WARNING) << "Failed to allocate pinned SSD restore arena";
            }
        }
    }

    auto create_storage_backend_result = CreateStorageBackend(config_);
    if (!create_storage_backend_result) {
        LOG(ERROR) << "Failed to create storage backend";
        throw std::runtime_error("Failed to create storage backend");
    }

    storage_backend_ = create_storage_backend_result.value();
    if (auto distributed_backend =
            std::dynamic_pointer_cast<DistributedStorageBackend>(
                storage_backend_)) {
        config_.enable_dfs = !distributed_backend->UsesObjectStorage() ||
                             distributed_backend->UsesKvcs();
        if (config_.enable_dfs && client_) {
            client_->SetDfsStorageBackend(distributed_backend);
        }
    }

    // Register the client buffer with the process-wide io_uring fixed-buffer
    // mechanism. This must happen before any I/O threads start so that they
    // can lazily pick up the registration on their first I/O call.
#ifdef USE_URING
    if (config.use_uring) {
        auto aligned_allocator =
            std::static_pointer_cast<AlignedClientBufferAllocator>(
                client_buffer_allocator_);
        if (aligned_allocator) {
            void* base_ptr = aligned_allocator->get_base_pointer();
            size_t size = aligned_allocator->get_total_size();
            if (UringFile::register_global_buffer(base_ptr, size)) {
                LOG(INFO) << "Successfully registered buffer with UringFile: "
                          << "base=" << base_ptr << ", size=" << size;
            } else {
                LOG(WARNING) << "Failed to register buffer with UringFile";
            }
        }
    }
#endif
}

FileStorage::~FileStorage() {
    LOG(INFO) << "Shutdown FileStorage...";
    backup_running_ = false;
    backup_stop_ = true;
    WakeBackupWorker();  // a pacing or tick wait returns at once
    if (backup_thread_.joinable()) {
        backup_thread_.join();
    }
    heartbeat_running_ = false;
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }
    client_buffer_gc_running_ = false;
    if (client_buffer_gc_thread_.joinable()) {
        client_buffer_gc_thread_.join();
    }
    // A quarantined staging may still be the target of a transfer that never
    // reported its end: leak it (and, through its handles, the staging
    // allocator) instead of freeing memory a late DMA could still write.
    ReleaseBackupStaging();
}

void FileStorage::ReleaseBackupStaging() {
    ReleaseExpiredBackupQuarantine();  // their time is up: freed normally
    std::lock_guard<std::mutex> lock(backup_quarantine_mutex_);
    if (!backup_staging_quarantine_.empty()) {
        // The leaked handles keep the whole staging allocator alive; keep its
        // registration as well, so a late DMA lands in registered memory.
        LOG(WARNING) << "Leaking " << backup_staging_quarantine_.size()
                     << " quarantined SSD backup staging batch(es), "
                     << backup_quarantined_bytes_
                     << " bytes, and keeping the staging registered";
        for (const auto& q : backup_staging_quarantine_) {
            StagingEvent("leak", *q.staging);
        }
        (void)new std::vector<QuarantinedStaging>(
            std::move(backup_staging_quarantine_));  // intentional leak
        backup_staging_quarantine_.clear();
        if (backup_staging_allocator_) {
            StagingEvent("staging_leaked", backup_staging_allocator_->getBase(),
                         backup_staging_allocator_->size());
        }
        return;
    }
    if (!backup_staging_allocator_) return;
    void* const staging_base = backup_staging_allocator_->getBase();
    const size_t staging_bytes = backup_staging_allocator_->size();
    if (backup_staging_registered_) {
        // Unregister before the memory is freed: a region left in the
        // transfer engine would refuse a later registration at this address
        // and keep its transport resources until the engine goes away.
        void* base = staging_base;
        StagingEvent("unregister", base, staging_bytes);
        tl::expected<void, ErrorCode> unreg;
        if (backup_staging_unregister_for_test_) {
            unreg = backup_staging_unregister_for_test_(base);
        } else if (client_) {
            unreg = client_->unregisterLocalMemory(base, true);
        }
        backup_staging_registered_ = false;
        if (!unreg && unreg.error() != ErrorCode::NOT_REGISTERED) {
            // Still registered: never free memory under a live region.
            LOG(ERROR) << "ssd_backup_staging_unregister_failed error="
                       << unreg.error()
                       << " bytes=" << backup_config_.staging_bytes
                       << " (staging kept until exit)";
            StagingEvent("unregister_failed", base, staging_bytes);
            (void)new std::shared_ptr<ClientBufferAllocator>(
                std::move(backup_staging_allocator_));  // intentional leak
            StagingEvent("staging_leaked", base, staging_bytes);
            return;
        }
    }
    backup_staging_allocator_.reset();
    StagingEvent("staging_freed", staging_base, staging_bytes);
}

std::pair<const void*, size_t> FileStorage::StagingSpan(
    const AllocatedBatch& batch) {
    const char* lo = nullptr;
    const char* hi = nullptr;
    for (const auto& [key, slice] : batch.slices) {
        const auto* p = static_cast<const char*>(slice.ptr);
        if (!lo || p < lo) lo = p;
        if (!hi || p + slice.size > hi) hi = p + slice.size;
    }
    return {lo, lo ? static_cast<size_t>(hi - lo) : 0};
}

tl::expected<void, ErrorCode> FileStorage::Init() {
    auto register_memory_result = RegisterLocalMemory();
    if (!register_memory_result) {
        LOG(ERROR) << "Failed to register local memory: "
                   << register_memory_result.error();
        return register_memory_result;
    }
    auto init_storage_backend_result = storage_backend_->Init();
    if (!init_storage_backend_result) {
        LOG(ERROR) << "Failed to init storage backend: "
                   << init_storage_backend_result.error();
        return init_storage_backend_result;
    }
    if (config_.enable_dfs) {
        client_buffer_gc_running_.store(true);
        client_buffer_gc_thread_ =
            std::thread(&FileStorage::ClientBufferGCThreadFunc, this);
        return {};
    }
    auto enable_offloading_result = IsEnableOffloading();
    if (enable_offloading_result.has_value()) {
        LOG(INFO) << "IsEnableOffloading result: "
                  << (enable_offloading_result.value() ? "true" : "false");
    } else {
        LOG(INFO) << "IsEnableOffloading result: error: "
                  << enable_offloading_result.error();
    }
    if (!enable_offloading_result) {
        LOG(ERROR) << "Failed to get enable persist result, error : "
                   << enable_offloading_result.error();
        return tl::make_unexpected(enable_offloading_result.error());
    }
    {
        MutexLocker locker(&offloading_mutex_);
        enable_offloading_ = enable_offloading_result.value();
        auto mount_file_storage_result =
            client_->MountLocalDiskSegment(enable_offloading_);
        if (!mount_file_storage_result) {
            LOG(ERROR) << "Failed to mount file storage: "
                       << mount_file_storage_result.error();
            return mount_file_storage_result;
        }
    }
    // Report configured SSD capacity to Master so it can populate
    // file_total_capacity_ (the denominator in "SSD Storage: X / Y").
    // Called once at init; old Masters that lack this RPC will log an error
    // but FileStorage continues normally.
    if (config_.total_size_limit > 0) {
        auto cap_result = client_->ReportSsdCapacity(config_.total_size_limit);
        if (!cap_result) {
            LOG(WARNING) << "ReportSsdCapacity failed (old Master?): "
                         << cap_result.error();
        }
    }

    auto scan_meta_result = storage_backend_->ScanMeta(
        [this](const std::vector<std::string>& keys,
               std::vector<StorageObjectMetadata>& metadatas) {
            for (auto& metadata : metadatas) {
                metadata.transport_endpoint = local_rpc_addr_;
            }
            auto tasks = BuildOffloadTasksFromStorageKeys(keys, metadatas);
            auto add_object_result =
                client_->NotifyOffloadSuccess(tasks, metadatas);
            if (!add_object_result) {
                LOG(ERROR) << "Failed to add object to master: "
                           << add_object_result.error();
                return add_object_result.error();
            }
            return ErrorCode::OK;
        });

    if (!scan_meta_result) {
        if (config_.enable_dfs &&
            scan_meta_result.error() == ErrorCode::NOT_SUPPORTED) {
            LOG(INFO) << "Currently, DFS backend does not support ScanMeta; "
                         "skip re-registering offloaded objects";
        } else {
            LOG(ERROR) << "Failed to scan meta and send to master: "
                       << scan_meta_result.error();
            return scan_meta_result;
        }
    }

    heartbeat_running_.store(true);
    heartbeat_thread_ = std::thread([this]() {
        LOG(INFO) << "Starting periodic task with interval: "
                  << config_.heartbeat_interval_seconds
                  << "s, running is: " << heartbeat_running_.load();
        while (heartbeat_running_.load()) {
            Heartbeat();
            std::this_thread::sleep_for(
                std::chrono::seconds(config_.heartbeat_interval_seconds));
        }
    });
    client_buffer_gc_running_.store(true);
    client_buffer_gc_thread_ =
        std::thread(&FileStorage::ClientBufferGCThreadFunc, this);
    // SSD backup is best effort: a failure to start it is logged, not fatal.
    if (auto backup_res = StartBackupWorker(); !backup_res) {
        // Backup is best effort, so the client still serves; the gauge lets
        // a deployment that requires backups catch this.
        LOG(ERROR) << "SSD backup enabled but its worker failed to start: "
                   << backup_res.error();
        if (ssd_metric_) ssd_metric_->ssd_backup_worker_up.update(0);
    } else if (backup_running_.load() && ssd_metric_) {
        ssd_metric_->ssd_backup_worker_up.update(1);
    }
    return {};
}

tl::expected<std::shared_ptr<FileStorage::AllocatedBatch>, ErrorCode>
FileStorage::LoadBatch(const std::vector<std::string>& keys,
                       const std::vector<int64_t>& sizes, bool prefer_pinned) {
    const bool use_pinned = prefer_pinned && pinned_restore_arena_allocator_;
    auto& allocator = use_pinned ? *pinned_restore_arena_allocator_
                                 : *client_buffer_allocator_;
    auto allocate_res = AllocateBatch(keys, sizes, allocator);
    if (!allocate_res && use_pinned &&
        allocate_res.error() == ErrorCode::BUFFER_OVERFLOW) {
        VLOG(1) << "Pinned SSD restore arena exhausted; using default arena";
        allocate_res = AllocateBatch(keys, sizes, *client_buffer_allocator_);
    }
    if (!allocate_res) {
        LOG(ERROR) << "Failed to allocate batch objects";
        return tl::make_unexpected(allocate_res.error());
    }
    auto allocated_batch = std::move(allocate_res.value());
    auto result = BatchLoad(allocated_batch->slices);
    if (!result) {
        LOG(ERROR) << "Batch load object failed,err_code = " << result.error();
        return tl::make_unexpected(result.error());
    }

    // After BatchLoad, slice.ptr may have been adjusted by offset_in_buffer
    // (for O_DIRECT aligned reads). Update pointers to reflect actual data
    // positions.
    for (size_t i = 0; i < keys.size(); ++i) {
        auto it = allocated_batch->slices.find(keys[i]);
        if (it != allocated_batch->slices.end()) {
            allocated_batch->pointers[i] =
                reinterpret_cast<uintptr_t>(it->second.ptr);
        }
    }

    return allocated_batch;
}

tl::expected<std::shared_ptr<FileStorage::AllocatedBatch>, ErrorCode>
FileStorage::LoadRanges(const std::vector<std::string>& keys,
                        const std::vector<int64_t>& offsets,
                        const std::vector<int64_t>& lengths,
                        const std::vector<int64_t>& object_sizes,
                        bool prefer_pinned) {
    if (keys.size() != offsets.size() || keys.size() != lengths.size() ||
        keys.size() != object_sizes.size()) {
        LOG(ERROR) << "Mismatched range load arguments";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        if (offsets[i] < 0 || lengths[i] < 0 || object_sizes[i] < 0 ||
            offsets[i] > object_sizes[i] ||
            lengths[i] > object_sizes[i] - offsets[i]) {
            LOG(ERROR) << "Value span out of range for key " << keys[i];
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    const bool use_pinned = prefer_pinned && pinned_restore_arena_allocator_;
    auto& allocator = use_pinned ? *pinned_restore_arena_allocator_
                                 : *client_buffer_allocator_;
    auto allocate_res = AllocateBatch(keys, lengths, allocator);
    if (!allocate_res && use_pinned &&
        allocate_res.error() == ErrorCode::BUFFER_OVERFLOW) {
        VLOG(1) << "Pinned SSD restore arena exhausted; using default arena";
        allocate_res = AllocateBatch(keys, lengths, *client_buffer_allocator_);
    }
    if (!allocate_res) {
        LOG(ERROR) << "Failed to allocate ranged batch objects";
        return tl::make_unexpected(allocate_res.error());
    }
    auto allocated_batch = std::move(allocate_res.value());

    std::unordered_map<std::string, StorageValueSpan> spans;
    spans.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto it = allocated_batch->slices.find(keys[i]);
        if (it == allocated_batch->slices.end()) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        spans.emplace(keys[i],
                      StorageValueSpan{static_cast<uint64_t>(offsets[i]),
                                       static_cast<uint64_t>(object_sizes[i]),
                                       it->second});
    }
    auto result = BatchLoadSpans(spans);
    if (!result) {
        LOG(ERROR) << "Batch load span failed, err_code = " << result.error();
        return tl::make_unexpected(result.error());
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& span = spans.at(keys[i]);
        allocated_batch->slices[keys[i]].ptr = span.slice.ptr;
        allocated_batch->pointers[i] =
            reinterpret_cast<uintptr_t>(span.slice.ptr);
    }
    return allocated_batch;
}

tl::expected<FileStorage::BatchGetResult, ErrorCode> FileStorage::BatchGet(
    const std::vector<std::string>& keys, const std::vector<int64_t>& sizes) {
    auto start_time = std::chrono::steady_clock::now();
    auto load_result = LoadBatch(keys, sizes, false);
    if (!load_result) return tl::make_unexpected(load_result.error());

    auto allocated_batch = std::move(load_result.value());
    uint64_t batch_id = allocated_batch->batch_id;
    BatchGetResult batch_result{batch_id, allocated_batch->pointers};

    MutexLocker locker(&client_buffer_mutex_);
    client_buffer_allocated_batches_.emplace(batch_id,
                                             std::move(allocated_batch));
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(
                            end_time - start_time)
                            .count();
    VLOG(1) << "Time taken for FileStorage::BatchGet: " << elapsed_time
            << "us, key size: " << keys.size() << ", batch_id: " << batch_id;
    return batch_result;
}

void FileStorage::PartitionUnbucketedTasks(
    const std::unordered_map<std::string, OffloadTaskItem>& task_by_storage_key,
    const std::unordered_set<std::string>& all_bucket_keys,
    const std::vector<std::string>& deferred_keys,
    const std::unordered_map<std::string, OffloadTaskItem>& previously_carried,
    const std::unordered_set<std::string>& redrained_carried_keys,
    std::vector<OffloadTaskItem>& failed_tasks,
    std::unordered_map<std::string, OffloadTaskItem>& carried_tasks) {
    for (const auto& storage_key : deferred_keys) {
        auto it = task_by_storage_key.find(storage_key);
        if (it != task_by_storage_key.end()) {
            carried_tasks.emplace(storage_key, it->second);
        } else {
            // A deferred key with no known task can be neither carried nor
            // NACKed; the pool will re-emit it and it will be dropped — the
            // pre-fix behavior. Unreachable while the invariant holds
            // (deferred keys are a subset of this cycle's effective input);
            // logged at WARNING so a caller change that breaks the contract
            // is observable rather than a silent regression.
            LOG(WARNING) << "[OFFLOAD] deferred key has no known task and "
                            "cannot be carried; it will be dropped on "
                            "re-emission (broken caller invariant?): "
                         << storage_key;
        }
    }
    // Retain previously carried keys the backend did not emit this cycle:
    // their pooled copies are still waiting (the pool only moves on a call
    // with non-empty input), so their tasks must stay alive. Keys re-drained
    // this cycle are excluded: the caller dropped their pooled copies on
    // collision, so nothing is waiting for them and the fresh task — already
    // in task_by_storage_key — is the only live one. Retaining a stale task
    // here would both strand it forever and suppress the fresh task's NACK
    // in the sweep below when the backend skipped the key outright.
    for (const auto& [storage_key, task] : previously_carried) {
        if (redrained_carried_keys.find(storage_key) !=
            redrained_carried_keys.end()) {
            continue;
        }
        if (all_bucket_keys.find(storage_key) == all_bucket_keys.end()) {
            carried_tasks.emplace(storage_key, task);
        }
    }
    for (const auto& [storage_key, task] : task_by_storage_key) {
        if (all_bucket_keys.find(storage_key) == all_bucket_keys.end() &&
            carried_tasks.find(storage_key) == carried_tasks.end()) {
            failed_tasks.push_back(task);
        }
    }
}

tl::expected<FileStorage::LocalBatchResult, ErrorCode>
FileStorage::BatchGetLocal(const std::vector<std::string>& keys,
                           const std::vector<int64_t>& sizes) {
    auto load_result = LoadBatch(keys, sizes, true);
    if (!load_result) return tl::make_unexpected(load_result.error());

    auto batch = std::move(load_result.value());
    LocalBatchResult result;
    result.pointers = std::move(batch->pointers);
    result.owner = std::move(batch);
    return result;
}

tl::expected<FileStorage::BatchGetResult, ErrorCode>
FileStorage::BatchGetRanges(const std::vector<std::string>& keys,
                            const std::vector<int64_t>& offsets,
                            const std::vector<int64_t>& lengths,
                            const std::vector<int64_t>& object_sizes) {
    auto load_result = LoadRanges(keys, offsets, lengths, object_sizes, false);
    if (!load_result) return tl::make_unexpected(load_result.error());

    auto allocated_batch = std::move(load_result.value());
    uint64_t batch_id = allocated_batch->batch_id;
    BatchGetResult batch_result{batch_id, allocated_batch->pointers};
    MutexLocker locker(&client_buffer_mutex_);
    client_buffer_allocated_batches_.emplace(batch_id,
                                             std::move(allocated_batch));
    return batch_result;
}

tl::expected<FileStorage::LocalBatchResult, ErrorCode>
FileStorage::BatchGetLocalRanges(const std::vector<std::string>& keys,
                                 const std::vector<int64_t>& offsets,
                                 const std::vector<int64_t>& lengths,
                                 const std::vector<int64_t>& object_sizes) {
    auto load_result = LoadRanges(keys, offsets, lengths, object_sizes, true);
    if (!load_result) return tl::make_unexpected(load_result.error());

    auto batch = std::move(load_result.value());
    LocalBatchResult result;
    result.pointers = std::move(batch->pointers);
    result.owner = std::move(batch);
    return result;
}

bool FileStorage::IsPerBucketSoftOffloadError(ErrorCode error) {
    return error == ErrorCode::INVALID_READ ||
           error == ErrorCode::OBJECT_ALREADY_EXISTS;
}

tl::expected<void, ErrorCode> FileStorage::OffloadObjects(
    const std::vector<OffloadTaskItem>& offloading_objects) {
    if (offloading_objects.empty() && deferred_task_by_storage_key_.empty()) {
        return {};
    }
    std::unordered_map<std::string, int64_t> storage_object_sizes;
    std::unordered_map<std::string, OffloadTaskItem> task_by_storage_key;
    storage_object_sizes.reserve(offloading_objects.size());
    task_by_storage_key.reserve(offloading_objects.size());
    for (const auto& task : offloading_objects) {
        const auto storage_key =
            TenantId(task.tenant_id).MakeScopedKey(task.key);
        storage_object_sizes.emplace(storage_key, task.size);
        task_by_storage_key.emplace(storage_key, task);
    }

    auto bucket_backend =
        std::dynamic_pointer_cast<BucketStorageBackend>(storage_backend_);

    // Merge in tasks carried across a bucket deferral (see
    // deferred_task_by_storage_key_): the backend's ungrouped pool will
    // re-emit those keys in a bucket this call, and they need their original
    // tasks to be queried, stored and success-notified. Collision (a carried
    // key re-drained before re-emission, i.e. re-PUT while deferred): the
    // fresh drain wins entirely -- drop the pooled copy so the key flows
    // through grouping as new input with its current size, under its fresh
    // task. Without this the fresh task would win the task map while the
    // pooled stale size won the packing accounting. Collided keys are recorded
    // so the partition below does not also retain their stale carried tasks —
    // nothing is waiting for a key whose pooled copy we just dropped.
    std::unordered_set<std::string> redrained_carried_keys;
    for (const auto& [storage_key, task] : deferred_task_by_storage_key_) {
        if (storage_object_sizes.find(storage_key) !=
            storage_object_sizes.end()) {
            if (bucket_backend) {
                bucket_backend->RemoveUngroupedOffloadingObject(storage_key);
            }
            redrained_carried_keys.insert(storage_key);
            VLOG(1) << "Deferred offload key re-drained before re-emission; "
                       "fresh task and size supersede the pooled copy: "
                    << storage_key;
            continue;
        }
        storage_object_sizes.emplace(storage_key, task.size);
        task_by_storage_key.emplace(storage_key, task);
    }

    std::vector<std::vector<std::string>> buckets_keys;
    std::vector<std::string> deferred_keys;
    if (bucket_backend) {
        auto allocate_res = bucket_backend->AllocateOffloadingBuckets(
            storage_object_sizes, buckets_keys, &deferred_keys);
        if (!allocate_res) {
            LOG(ERROR) << "AllocateOffloadingBuckets failed with error: "
                       << allocate_res.error();
            return allocate_res;
        }
    } else {
        std::vector<std::string> keys;
        keys.reserve(storage_object_sizes.size());
        for (const auto& it : storage_object_sizes) {
            keys.emplace_back(it.first);
        }
        buckets_keys.emplace_back(std::move(keys));
    }

    auto complete_handler =
        [this, &task_by_storage_key](
            const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas) -> ErrorCode {
        VLOG(1) << "Success to store objects, keys count: " << keys.size();
        for (auto& metadata : metadatas) {
            metadata.transport_endpoint = local_rpc_addr_;
        }
        std::vector<OffloadTaskItem> tasks;
        tasks.reserve(keys.size());
        for (const auto& key : keys) {
            auto it = task_by_storage_key.find(key);
            if (it == task_by_storage_key.end()) {
                LOG(ERROR) << "Offload task not found for storage key";
                return ErrorCode::INVALID_KEY;
            }
            tasks.push_back(it->second);
        }
        auto result = client_->NotifyOffloadSuccess(tasks, metadatas);
        if (!result) {
            LOG(ERROR) << "[OFFLOAD] NotifyOffloadSuccess failed with error: "
                       << result.error() << " keys count: " << keys.size();
            return result.error();
        }
        return ErrorCode::OK;
    };

    // Collect keys drained from master queue but not actually offloaded.
    // Report them back with data_size=-1 sentinel so the master can clean up
    // orphaned offloading tasks and release source replica refcounts.
    std::vector<OffloadTaskItem> failed_tasks;
    std::unordered_set<std::string> all_bucket_keys;
    // Set when a whole-cycle error aborts the bucket loop early. We still fall
    // through to the NACK flush below before returning it, so no drained key is
    // left waiting on the TTL reaper.
    std::optional<ErrorCode> abort_error;

    for (const auto& keys : buckets_keys) {
        for (const auto& k : keys) all_bucket_keys.insert(k);
        std::unordered_map<std::string, std::vector<Slice>> batch_object;
        std::unordered_map<std::string, std::vector<std::string>>
            storage_keys_by_tenant;
        for (const auto& storage_key : keys) {
            const auto it = task_by_storage_key.find(storage_key);
            if (it != task_by_storage_key.end()) {
                storage_keys_by_tenant[it->second.tenant_id].push_back(
                    storage_key);
            }
        }
        for (const auto& [tenant_id, storage_keys] : storage_keys_by_tenant) {
            std::vector<std::string> user_keys;
            user_keys.reserve(storage_keys.size());
            for (const auto& storage_key : storage_keys) {
                user_keys.push_back(task_by_storage_key.at(storage_key).key);
            }
            std::unordered_map<std::string, std::vector<Slice>>
                user_batch_object;
            [[maybe_unused]] auto query_result = BatchQuerySegmentSlices(
                user_keys, tenant_id, user_batch_object);
            // BatchQuerySegmentSlices is now best-effort: it always returns
            // OK. Keys present in user_batch_object go to batch_object; the
            // rest are reported as failed.
            for (const auto& storage_key : storage_keys) {
                const auto& task = task_by_storage_key.at(storage_key);
                auto it = user_batch_object.find(task.key);
                if (it != user_batch_object.end()) {
                    batch_object.emplace(storage_key, std::move(it->second));
                } else {
                    failed_tasks.push_back(task);
                }
            }
        }
        if (batch_object.empty()) {
            continue;
        }

        // D2H staging: replace device slices with host memory slices
        // so that storage_backend (ConcatSlicesToString / BuildBucket /
        // WriteBucket) always receives host pointers.
        std::unordered_map<std::string, std::vector<Slice>> host_batch_object;
        std::vector<PinnedBufferPool::Buffer> staging_bufs;
        auto runtime_accelerator =
            device::GetAcceleratorRegistry().RuntimeAccelerators();

        for (auto& [obj_key, slices] : batch_object) {
            std::vector<Slice> host_slices;
            bool obj_success = true;
            for (const auto& slice : slices) {
                device::PointerInfo info{};
                auto* device =
                    runtime_accelerator.FindDeviceForPointer(slice.ptr, &info);
                if (device) {
                    device->SetContext(info.device_id);
                    auto buf = pinned_buffer_pool_->Acquire(slice.size);
                    if (!device->Copy(buf.data, slice.ptr, slice.size,
                                      device::CopyDirection::kDeviceToHost)) {
                        LOG(ERROR) << "D2H staging failed for key: " << obj_key;
                        pinned_buffer_pool_->Release(std::move(buf));
                        obj_success = false;
                        failed_tasks.push_back(task_by_storage_key.at(obj_key));
                        break;
                    }
                    host_slices.emplace_back(Slice{buf.data, slice.size});
                    staging_bufs.push_back(std::move(buf));
                } else {
                    host_slices.push_back(slice);
                }
            }
            if (obj_success) {
                host_batch_object[obj_key] = std::move(host_slices);
            }
        }

        // If every object in this bucket failed D2H staging, host_batch_object
        // is empty (those keys are already in failed_tasks). Skip BatchOffload,
        // which rejects an empty map as INVALID_KEY and would otherwise trip
        // the whole-cycle abort below for a bucket that has nothing left to
        // persist. staging_bufs can still be non-empty here (an object whose
        // first slices copied fine but a later one failed), so hand those
        // buffers back before continuing: the release loop after BatchOffload
        // is unreachable on this path.
        if (host_batch_object.empty()) {
            for (auto& buf : staging_bufs) {
                pinned_buffer_pool_->Release(std::move(buf));
            }
            continue;
        }

        auto offload_start = std::chrono::steady_clock::now();
        std::unordered_set<std::string> reported_keys;
        reported_keys.reserve(host_batch_object.size());
        auto bucket_complete_handler =
            [this, offload_start, complete_handler, &reported_keys](
                const std::vector<std::string>& keys,
                std::vector<StorageObjectMetadata>& metadatas) -> ErrorCode {
            auto res = complete_handler(keys, metadatas);
            if (res == ErrorCode::OK) {
                reported_keys.insert(keys.begin(), keys.end());
            }
            if (res == ErrorCode::OK && ssd_metric_) {
                auto elapsed_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - offload_start)
                        .count();
                int64_t total_bytes = 0;
                for (const auto& metadata : metadatas) {
                    total_bytes += metadata.data_size;
                }
                ssd_metric_->ssd_write_ops.inc(keys.size());
                ssd_metric_->ssd_write_bytes.inc(total_bytes);
                ssd_metric_->ssd_write_latency_us.observe(elapsed_us);
                ssd_metric_->ssd_write_latency_summary.observe(elapsed_us);
                ssd_metric_->ssd_total_ops.inc(keys.size());
                ssd_metric_->ssd_total_bytes.inc(total_bytes);
                ssd_metric_->ssd_total_latency_us.observe(elapsed_us);
                ssd_metric_->ssd_total_latency_summary.observe(elapsed_us);
            }
            return res;
        };
        auto offload_res = storage_backend_->BatchOffload(
            host_batch_object, bucket_complete_handler,
            [this](const std::vector<std::string>& evicted_keys) {
                return NotifyEvictedDiskReplicas(evicted_keys);
            });

        // Release staging buffers back to pool.
        for (auto& buf : staging_bufs) {
            pinned_buffer_pool_->Release(std::move(buf));
        }
        if (offload_res) {
            // A successful batch may skip duplicates or individual writes.
            // Only the completion callback's keys were reported to Master;
            // NACK the rest to release their tasks and source replica refs.
            for (const auto& [key, _] : host_batch_object) {
                if (reported_keys.find(key) == reported_keys.end()) {
                    failed_tasks.push_back(task_by_storage_key.at(key));
                }
            }
        }
        if (!offload_res) {
            LOG(ERROR) << "Failed to store objects with error: "
                       << offload_res.error();
            // This bucket did not persist, so report its keys back to the
            // master as failed regardless of whether we continue or abort.
            // Doing it here (rather than only on the soft path) keeps their
            // offloading tasks and source-replica refcounts from leaking until
            // the put_start_release_timeout_sec_ TTL reaper fires.
            for (const auto& [key, _] : host_batch_object) {
                failed_tasks.push_back(task_by_storage_key.at(key));
            }
            if (offload_res.error() == ErrorCode::KEYS_ULTRA_LIMIT) {
                // Disk is over the key-count limit: stop offloading entirely.
                MutexLocker locker(&offloading_mutex_);
                enable_offloading_ = false;
            }
            if (!IsPerBucketSoftOffloadError(offload_res.error())) {
                // Whole-cycle error (KEYS_ULTRA_LIMIT or any hard failure):
                // stop processing further buckets, but fall through to the NACK
                // flush below so every drained key is released. Unvisited
                // buckets are not yet in all_bucket_keys, so the sweep NACKs
                // them too; this bucket's keys were just pushed above.
                abort_error = offload_res.error();
                break;
            }
            // Soft per-bucket error: keep processing the remaining buckets.
        }
    }

    // Keys absent from every bucket are either (a) deliberately skipped by
    // GroupOffloadingKeysByBucket (size over bucket limit / already on disk)
    // — NACK those so the master releases task + refcount — or (b) DEFERRED
    // to the backend's ungrouped pool for the next cycle. Deferred keys must
    // keep their tasks alive across the deferral instead of being NACKed:
    // the pool carries only key+size, and a re-emitted key without a task
    // was silently dropped above — the deferred-tail leak (upstream #3006).
    std::unordered_map<std::string, OffloadTaskItem> carried_tasks;
    PartitionUnbucketedTasks(task_by_storage_key, all_bucket_keys,
                             deferred_keys, deferred_task_by_storage_key_,
                             redrained_carried_keys, failed_tasks,
                             carried_tasks);
    if (abort_error == ErrorCode::KEYS_ULTRA_LIMIT) {
        // Offloading is now disabled, so no later heartbeat can consume the
        // deferred tail. Release its tasks along with the failed buckets and
        // discard the corresponding pooled keys to keep both states in sync.
        for (const auto& [_, task] : carried_tasks) {
            failed_tasks.push_back(task);
        }
        carried_tasks.clear();
        if (bucket_backend) {
            bucket_backend->ClearUngroupedOffloadingObjects();
        }
    }
    if (!carried_tasks.empty()) {
        VLOG(1) << "Carrying " << carried_tasks.size()
                << " deferred offload task(s) to the next cycle";
    }
    deferred_task_by_storage_key_ = std::move(carried_tasks);

    if (!failed_tasks.empty()) {
        std::vector<StorageObjectMetadata> failed_metadatas;
        failed_metadatas.reserve(failed_tasks.size());
        for (size_t i = 0; i < failed_tasks.size(); ++i) {
            failed_metadatas.push_back(StorageObjectMetadata{-1, 0, 0, -1, ""});
        }
        auto result =
            client_->NotifyOffloadSuccess(failed_tasks, failed_metadatas);
        if (!result) {
            LOG(WARNING) << "[OFFLOAD] NotifyOffloadSuccess for failed tasks "
                            "returned error: "
                         << result.error() << " count: " << failed_tasks.size();
        }
    }

    if (abort_error) {
        return tl::make_unexpected(*abort_error);
    }
    return {};
}

tl::expected<void, ErrorCode> FileStorage::NotifyEvictedDiskReplicas(
    const std::vector<std::string>& evicted_keys) {
    if (evicted_keys.empty()) return {};

    std::optional<ErrorCode> first_error;
    std::unordered_map<TenantId, std::vector<std::string>, TenantIdHash>
        keys_by_tenant;
    for (const auto& storage_key : evicted_keys) {
        auto [tenant_id, key] = TenantId::ParseScopedKey(storage_key);
        keys_by_tenant[tenant_id].push_back(key);
    }

    for (const auto& [tenant_id, keys] : keys_by_tenant) {
        auto results = client_->BatchEvictDiskReplica(keys, tenant_id.value(),
                                                      ReplicaType::LOCAL_DISK);
        if (results.size() != keys.size()) {
            LOG(ERROR) << "BatchEvictDiskReplica returned " << results.size()
                       << " result(s) for " << keys.size()
                       << " key(s), tenant_id=" << tenant_id;
            if (!first_error.has_value()) {
                first_error = ErrorCode::INTERNAL_ERROR;
            }
            continue;
        }

        for (size_t i = 0; i < results.size(); ++i) {
            if (!results[i]) {
                if (results[i].error() == ErrorCode::OBJECT_NOT_FOUND) {
                    VLOG(1)
                        << "Master no longer tracks evicted local disk key: "
                        << keys[i] << ", tenant_id=" << tenant_id;
                    continue;
                }
                if (!first_error.has_value()) {
                    first_error = results[i].error();
                }
                LOG(WARNING)
                    << "Failed to notify master about evicted local disk key: "
                    << keys[i] << ", tenant_id=" << tenant_id
                    << ", error: " << results[i].error();
            }
        }
    }
    if (first_error.has_value()) {
        return tl::make_unexpected(first_error.value());
    }
    return {};
}

tl::expected<void, ErrorCode> FileStorage::RunDiskWatermarkEviction() {
    if (!config_.enable_disk_watermark_eviction) {
        return {};
    }

    auto eviction_result = storage_backend_->EvictAboveDiskWatermark(
        config_.disk_eviction_high_watermark_ratio,
        config_.disk_eviction_low_watermark_ratio,
        [this](const std::vector<std::string>& evicted_keys) {
            return NotifyEvictedDiskReplicas(evicted_keys);
        });
    if (!eviction_result) {
        return tl::make_unexpected(eviction_result.error());
    }
    if (!eviction_result.value().empty()) {
        LOG(INFO) << "Disk watermark eviction removed "
                  << eviction_result.value().size() << " LOCAL_DISK key(s)";
    }
    return {};
}

tl::expected<bool, ErrorCode> FileStorage::IsEnableOffloading() {
    auto is_enable_offloading_result = storage_backend_->IsEnableOffloading();
    if (!is_enable_offloading_result) {
        LOG(ERROR) << "Failed to get enabling offload: "
                   << is_enable_offloading_result.error();
        return tl::make_unexpected(is_enable_offloading_result.error());
    }

    auto enable_offloading = is_enable_offloading_result.value();

    return enable_offloading;
}

tl::expected<void, ErrorCode> FileStorage::Heartbeat() {
    if (client_ == nullptr) {
        LOG(ERROR) << "client is nullptr";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // A drain deregistered the disk tier on purpose. Skipping the whole tick
    // matters for the SEGMENT_NOT_FOUND branch below, which would re-mount the
    // segment and re-register every object; there is also no point taking new
    // offload work for a store that is leaving. The client TTL is renewed by
    // Ping on the storage heartbeat thread, not here, so the client stays
    // alive for its memory segments.
    if (draining_.load()) {
        return {};
    }

    // Join previous rescan if completed
    if (rescan_future_.valid() && rescan_future_.wait_for(std::chrono::seconds(
                                      0)) == std::future_status::ready) {
        rescan_future_ = std::future<void>();
    }

    // Retry metadata resync if previous attempt failed.
    if (metadata_resync_pending_.load() && !rescan_future_.valid()) {
        LOG(INFO) << "Retrying background metadata rescan";
        rescan_future_ = std::async(std::launch::async, [this]() {
            auto result = ReRegisterOffloadedObjects();
            if (!result) {
                LOG(ERROR) << "Background metadata rescan retry "
                           << "failed: " << result.error();
            } else {
                metadata_resync_pending_.store(false);
            }
        });
    }

    std::vector<OffloadTaskItem>
        offloading_objects;  // Objects selected for offloading

    // === STEP 1: Send heartbeat and get offloading decisions ===
    {
        MutexLocker locker(&offloading_mutex_);
        // Re-checked under the lock: the entry check above is only a fast
        // path. A drain (which latches and deregisters under this same
        // mutex) may have completed while this tick was parked on it; going
        // on would hit SEGMENT_NOT_FOUND below and re-mount the segment the
        // drain just deregistered.
        if (draining_.load()) {
            return {};
        }
        auto fetch_offload_tasks = [&]() -> tl::expected<void, ErrorCode> {
            return client_->OffloadObjectHeartbeat(enable_offloading_,
                                                   offloading_objects);
        };
        auto heartbeat_result = fetch_offload_tasks();
        if (!heartbeat_result) {
            ErrorCode err = heartbeat_result.error();
            if (err == ErrorCode::SEGMENT_NOT_FOUND) {
                // Master lost our LOCAL_DISK segment (likely restarted).
                // Re-register the segment, retry the heartbeat, and
                // trigger async ScanMeta to re-register object metadata.
                LOG(WARNING) << "OffloadObjectHeartbeat returned "
                             << "SEGMENT_NOT_FOUND, attempting to "
                             << "re-register local disk segment and "
                             << "re-register object metadata";
                auto remount_result =
                    client_->MountLocalDiskSegment(enable_offloading_);
                if (remount_result) {
                    // Report configured SSD capacity so the Master can
                    // restore file_total_capacity_ (the denominator in
                    // "SSD Storage: X / Y").  This was lost on restart;
                    // re-reporting it here avoids the 0 B display.
                    if (config_.total_size_limit > 0) {
                        auto cap_result = client_->ReportSsdCapacity(
                            config_.total_size_limit);
                        if (!cap_result) {
                            LOG(WARNING)
                                << "ReportSsdCapacity failed during "
                                << "heartbeat recovery: " << cap_result.error();
                        }
                    }
                    heartbeat_result = fetch_offload_tasks();
                    if (!heartbeat_result) {
                        LOG(ERROR) << "Heartbeat failed after re-registration: "
                                   << heartbeat_result.error();
                        return heartbeat_result;
                    }
                    // Master lost all object metadata on restart.
                    // Trigger async ScanMeta to re-register them,
                    // same as what Init() does on startup.
                    if (!rescan_future_.valid()) {
                        LOG(INFO) << "Triggering background metadata rescan "
                                  << "after LOCAL_DISK segment re-registration";
                        metadata_resync_pending_.store(true);
                        rescan_future_ =
                            std::async(std::launch::async, [this]() {
                                auto result = ReRegisterOffloadedObjects();
                                if (!result) {
                                    LOG(ERROR) << "Background metadata rescan "
                                               << "failed: " << result.error();
                                } else {
                                    metadata_resync_pending_.store(false);
                                }
                            });
                    }
                } else {
                    LOG(ERROR) << "Failed to re-register local disk segment: "
                               << remount_result.error();
                    return tl::make_unexpected(remount_result.error());
                }
            } else {
                LOG(ERROR) << "Failed to send heartbeat with error: " << err;
                return heartbeat_result;
            }
        }
    }

    // Generic removal reclaims files through metadata-checked eviction, not a
    // backend-wide wipe that could remove managed objects or another tenant.
    // === STEP 2: Persist offloaded objects (trigger actual data migration) ===
    if (!offloading_objects.empty() || !deferred_task_by_storage_key_.empty()) {
        auto offload_result = OffloadObjects(offloading_objects);
        if (!offload_result) {
            LOG(ERROR) << "Failed to persist objects with error: "
                       << offload_result.error();
            return offload_result;
        }
    }

    VLOG(1) << "Completed heartbeat with offloaded objects count: "
            << offloading_objects.size();

    // Drive any pending L2->L1 promotion work for this client. Failures
    // inside ProcessPromotionTasks are logged per-key and do not propagate;
    // promotion is best-effort and must never break offload.
    (void)ProcessPromotionTasks();

    // Proactive disk watermarks keep LOCAL_DISK usage below the configured
    // low watermark even when no new write arrives to trigger reactive
    // eviction.
    auto disk_eviction_result = RunDiskWatermarkEviction();
    if (!disk_eviction_result) {
        LOG(WARNING) << "Disk watermark eviction failed: "
                     << disk_eviction_result.error();
    }
    return {};
}

void FileStorage::RemoveAll() {
    // Explicit physical reset: this bypasses per-object metadata protection
    // and clears every tenant sharing the backend. Generic Client::RemoveAll
    // must not request it; callers must ensure no live object depends on the
    // files being removed.
    if (storage_backend_) {
        storage_backend_->RemoveAll();
    }
    LOG(INFO) << "FileStorage::RemoveAll: cleared storage backend";
}

tl::expected<void, ErrorCode> FileStorage::DrainLocalDiskSegment(
    uint64_t grace_period_ms) {
    if (client_ == nullptr) {
        LOG(ERROR) << "client is nullptr";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Latch and deregister under offloading_mutex_, the lock Heartbeat holds
    // across its master RPCs and the SEGMENT_NOT_FOUND re-mount. The latch
    // alone is not enough: a heartbeat that read draining_ == false before
    // this store() and was then scheduled out would, on resume, see
    // SEGMENT_NOT_FOUND from the deregistration below and re-mount the
    // segment. Under the mutex, such a tick is either still parked on the
    // lock (it re-checks draining_ once it gets it and skips) or it already
    // finished its RPCs before we latched (anything it mounted or registered
    // is deregistered again right here, and the master refuses registrations
    // that arrive after this call returns -- see
    // MasterService::NotifyOffloadSuccess).
    {
        MutexLocker locker(&offloading_mutex_);
        draining_.store(true);
        WakeBackupWorker();  // stop a backup write paced by the rate limit

        auto result = client_->UnmountLocalDiskSegment();
        if (!result) {
            LOG(ERROR) << "action=drain_local_disk_segment, error="
                       << result.error();
            draining_.store(false);
            return result;
        }
    }

    LOG(INFO) << "action=drain_local_disk_segment, grace_period_ms="
              << grace_period_ms;
    if (grace_period_ms > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(grace_period_ms));
    }
    return {};
}

namespace {

// RAII guard for master-side promotion state: unless dismissed, the
// destructor notifies the master that promotion failed so the staged
// PROCESSING replica and the in-flight slot are released eagerly instead of
// waiting for the reaper TTL. NotifyPromotionFailure is idempotent and
// best-effort - the reaper is the long-stop.
class PromotionStateGuard {
   public:
    PromotionStateGuard(Client* client, std::string key, std::string tenant_id)
        : client_(client),
          key_(std::move(key)),
          tenant_id_(std::move(tenant_id)) {}

    ~PromotionStateGuard() {
        if (dismissed_ || client_ == nullptr) {
            return;
        }
        auto release = client_->NotifyPromotionFailure(key_, tenant_id_);
        if (!release) {
            VLOG(1) << "Promotion: NotifyPromotionFailure failed for key="
                    << key_ << ", error=" << release.error()
                    << "; master reaper will reclaim on TTL expiry";
        }
    }

    void Dismiss() { dismissed_ = true; }

    PromotionStateGuard(const PromotionStateGuard&) = delete;
    PromotionStateGuard& operator=(const PromotionStateGuard&) = delete;

   private:
    Client* client_;
    std::string key_;
    std::string tenant_id_;
    bool dismissed_{false};
};

}  // namespace

tl::expected<void, ErrorCode> FileStorage::PromoteOneKeyFromLocalDisk(
    const std::string& key, const std::string& tenant_id, int64_t size,
    bool* dram_pressure) {
    const auto storage_key = TenantId(tenant_id).MakeScopedKey(key);
    if (size <= 0) {
        LOG(WARNING) << "Promotion: skipping key=" << key
                     << " with non-positive size=" << size;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // No segment preference from the client: let master pick from any
    // DRAM segment.
    const std::vector<std::string> preferred_segments;

    auto alloc_result = client_->PromotionAllocStart(
        key, tenant_id, static_cast<uint64_t>(size), preferred_segments);
    if (!alloc_result) {
        // AllocStart failed (typically NO_AVAILABLE_HANDLE under
        // DRAM pressure). No staged buffer to release, but the
        // task entry already claimed a promotion_in_flight_ slot
        // at admission. Notify the master to release it
        // immediately; otherwise the slot stays pinned for the
        // reaper TTL (~10 min default), turning transient DRAM
        // pressure into a sustained outage of promotion_queue_limit_.
        // Notify is idempotent and handles alloc_id == 0 correctly.
        VLOG(1) << "PromotionAllocStart failed for key=" << key
                << ", error=" << alloc_result.error()
                << " (likely no free DRAM); releasing master slot";
        if (dram_pressure != nullptr &&
            alloc_result.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
            *dram_pressure = true;
        }
        auto release = client_->NotifyPromotionFailure(key, tenant_id);
        if (!release) {
            VLOG(1) << "Promotion: NotifyPromotionFailure failed for key="
                    << key << ", error=" << release.error()
                    << "; master reaper will reclaim on TTL expiry";
        }
        return tl::make_unexpected(alloc_result.error());
    }

    // Every failure path past this point has a master-side staged
    // PROCESSING MEMORY buffer and an incremented in-flight slot.
    // The guard eagerly notifies the master on failure so the buffer is
    // reclaimed and the slot is freed; otherwise transient SSD
    // throttling or RDMA flakes saturate promotion_queue_limit_
    // for the full reaper TTL.
    PromotionStateGuard guard(client_.get(), key, tenant_id);

    // (a) Allocate an O_DIRECT-aligned staging buffer and read the bytes
    // from the local SSD backend into it. AllocateBatch returns a
    // shared_ptr<AllocatedBatch> whose BufferHandles RAII-release the
    // staging space when the local goes out of scope.
    std::vector<std::string> single_key{storage_key};
    std::vector<int64_t> single_size{size};
    auto allocate_res =
        AllocateBatch(single_key, single_size, *client_buffer_allocator_);
    if (!allocate_res) {
        LOG(WARNING) << "Promotion: AllocateBatch failed for key=" << key
                     << ", error=" << allocate_res.error();
        return tl::make_unexpected(allocate_res.error());
    }
    auto staging = allocate_res.value();
    auto load_res = BatchLoad(staging->slices);
    if (!load_res) {
        LOG(WARNING) << "Promotion: BatchLoad failed for key=" << key
                     << ", error=" << load_res.error();
        return tl::make_unexpected(load_res.error());
    }

    // (b) TE-write from the staging slice into the freshly-allocated
    // MEMORY replica. Slice ptr may have been bumped by O_DIRECT offset
    // correction in BatchLoad, so re-read it from the slice map.
    auto slice_it = staging->slices.find(storage_key);
    if (slice_it == staging->slices.end()) {
        LOG(WARNING) << "Promotion: staging slice missing for key=" << key;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    std::vector<Slice> tx_slices{slice_it->second};
    ErrorCode write_err = client_->PromotionWrite(
        alloc_result.value().memory_descriptor, tx_slices);
    if (write_err != ErrorCode::OK) {
        LOG(WARNING) << "Promotion: TransferWrite failed for key=" << key
                     << ", error=" << write_err;
        return tl::make_unexpected(write_err);
    }

    // (c) Commit. Master flips the PROCESSING replica to COMPLETE and it
    // becomes visible to readers.
    auto notify_res = client_->NotifyPromotionSuccess(key, tenant_id);
    if (!notify_res) {
        // The write landed but the commit failed. We can't retry the
        // commit (the success path is one-shot via alloc_id), and we
        // don't know whether the failure was transient or structural.
        // Release the master-side state so the slot is reusable; the
        // bytes we wrote become stranded under a soon-to-be-erased
        // PROCESSING replica, which is harmless.
        LOG(WARNING) << "Promotion: NotifyPromotionSuccess failed for key="
                     << key << ", error=" << notify_res.error();
        return tl::make_unexpected(notify_res.error());
    }

    guard.Dismiss();
    VLOG(1) << "Promotion completed for key=" << key << ", size=" << size;
    return {};
}

tl::expected<void, ErrorCode> FileStorage::PrefetchKeys(
    const std::vector<std::string>& keys, const std::vector<int64_t>& sizes,
    bool* dram_pressure, PrefetchKeyCallback on_key_done,
    const std::string& tenant_id) {
    if (client_ == nullptr) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (keys.size() != sizes.size()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Default (empty) means the holder's own tenant; a delegated prefetch
    // carries the requester's tenant explicitly.
    const std::string effective_tenant =
        tenant_id.empty() ? client_->tenant_id() : tenant_id;
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];
        const int64_t size = sizes[i];
        bool key_dram_pressure = false;
        auto result = PromoteOneKeyFromLocalDisk(key, effective_tenant, size,
                                                 &key_dram_pressure);
        if (key_dram_pressure && dram_pressure != nullptr) {
            *dram_pressure = true;
        }
        if (ssd_metric_ != nullptr) {
            if (result) {
                ssd_metric_->prefetch_complete_total.fetch_add(
                    1, std::memory_order_relaxed);
            } else {
                ssd_metric_->prefetch_fail_total.fetch_add(
                    1, std::memory_order_relaxed);
            }
        }
        if (on_key_done) {
            on_key_done(key, result.has_value());
        }
        if (key_dram_pressure) {
            // DRAM saturated: stop the batch here. Each further key would
            // burn the same doomed alloc round-trip while eviction fights
            // for the same memory; the caller enters cooldown instead.
            VLOG(1) << "PrefetchKeys: DRAM pressure at key=" << key
                    << ", skipping " << (keys.size() - i - 1)
                    << " remaining keys";
            break;
        }
    }
    return {};
}

std::optional<int64_t> FileStorage::LookupLocalObjectSize(
    const std::string& key, const std::string& tenant_id) const {
    if (!storage_backend_) {
        return std::nullopt;
    }
    if (auto size = storage_backend_->GetObjectDataSize(key);
        size && *size > 0) {
        return size;
    }
    // Tenant-scoped fallback: bucket keys are stored scoped by tenant.
    const std::string& lookup_tenant = tenant_id.empty() && client_ != nullptr
                                           ? client_->tenant_id()
                                           : tenant_id;
    if (!lookup_tenant.empty()) {
        const auto scoped = TenantId(lookup_tenant).MakeScopedKey(key);
        if (scoped != key) {
            if (auto size = storage_backend_->GetObjectDataSize(scoped);
                size && *size > 0) {
                return size;
            }
        }
    }
    return std::nullopt;
}

tl::expected<void, ErrorCode> FileStorage::ProcessPromotionTasks() {
    if (client_ == nullptr) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<PromotionTaskItem> promotion_objects;
    auto heartbeat_result =
        client_->PromotionObjectHeartbeat(promotion_objects);
    if (!heartbeat_result) {
        // SEGMENT_NOT_FOUND happens between MountLocalDiskSegment and the
        // first heartbeat tick if the master forgets us (e.g. across a master
        // restart): benign no-op until next ReMount.
        if (heartbeat_result.error() == ErrorCode::SEGMENT_NOT_FOUND) {
            return {};
        }
        LOG(WARNING) << "PromotionObjectHeartbeat failed: "
                     << heartbeat_result.error();
        return tl::make_unexpected(heartbeat_result.error());
    }
    if (promotion_objects.empty()) {
        return {};
    }

    VLOG(1) << "ProcessPromotionTasks pulled " << promotion_objects.size()
            << " promotion candidate(s) from master";

    // The master caps per-heartbeat work via PromotionObjectHeartbeat,
    // returning at most one task per call so the heartbeat thread stays
    // within the client-liveness window even for large objects. Leftover
    // work stays queued in the master's promotion_objects map and is
    // returned on subsequent heartbeats; we process whatever we received
    // here without a second client-side cap.
    for (const auto& task : promotion_objects) {
        PromoteOneKeyFromLocalDisk(task.key, task.tenant_id, task.size,
                                   nullptr);
    }

    return {};
}

tl::expected<void, ErrorCode> FileStorage::BatchLoad(
    std::unordered_map<std::string, Slice>& batch_object) {
    auto start_time = std::chrono::steady_clock::now();
    auto result = storage_backend_->BatchLoad(batch_object);
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(
                            end_time - start_time)
                            .count();
    VLOG(1) << "Time taken for BatchStore: " << elapsed_time
            << "us,with keys count: " << batch_object.size();
    if (!result) {
        LOG(ERROR) << "Batch load object failed,err_code = " << result.error();
    } else if (ssd_metric_) {
        int64_t total_bytes = 0;
        for (const auto& [key, slice] : batch_object) {
            total_bytes += slice.size;
        }
        ssd_metric_->ssd_read_ops.inc(batch_object.size());
        ssd_metric_->ssd_read_bytes.inc(total_bytes);
        ssd_metric_->ssd_read_latency_us.observe(elapsed_time);
        ssd_metric_->ssd_read_latency_summary.observe(elapsed_time);
        ssd_metric_->ssd_total_ops.inc(batch_object.size());
        ssd_metric_->ssd_total_bytes.inc(total_bytes);
        ssd_metric_->ssd_total_latency_us.observe(elapsed_time);
        ssd_metric_->ssd_total_latency_summary.observe(elapsed_time);
    }
    return result;
}

tl::expected<void, ErrorCode> FileStorage::BatchLoadSpans(
    std::unordered_map<std::string, StorageValueSpan>& spans) {
    auto start_time = std::chrono::steady_clock::now();
    auto result = storage_backend_->BatchLoadSpans(spans);
    auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - start_time)
                            .count();
    if (!result) {
        LOG(ERROR) << "Batch load span failed, err_code = " << result.error();
        return result;
    }
    if (ssd_metric_) {
        int64_t total_bytes = 0;
        for (const auto& [key, span] : spans) {
            (void)key;
            total_bytes += static_cast<int64_t>(span.slice.size);
        }
        ssd_metric_->ssd_read_ops.inc(spans.size());
        ssd_metric_->ssd_read_bytes.inc(total_bytes);
        ssd_metric_->ssd_read_latency_us.observe(elapsed_time);
        ssd_metric_->ssd_read_latency_summary.observe(elapsed_time);
        ssd_metric_->ssd_total_ops.inc(spans.size());
        ssd_metric_->ssd_total_bytes.inc(total_bytes);
        ssd_metric_->ssd_total_latency_us.observe(elapsed_time);
        ssd_metric_->ssd_total_latency_summary.observe(elapsed_time);
    }
    return result;
}

tl::expected<void, ErrorCode> FileStorage::BatchQuerySegmentSlices(
    const std::vector<std::string>& keys, const std::string& tenant_id,
    std::unordered_map<std::string, std::vector<Slice>>& batched_slices) {
    auto batched_query_results = client_->BatchQuery(keys, tenant_id);
    if (batched_query_results.empty()) {
        return {};
    }
    for (size_t i = 0; i < batched_query_results.size(); ++i) {
        if (batched_query_results[i]) {
            for (const auto& descriptor :
                 batched_query_results[i].value().replicas) {
                if (client_->IsReplicaOnLocalMemory(descriptor)) {
                    const auto& memory_descriptor =
                        descriptor.get_memory_descriptor();
                    std::vector<Slice> slices;
                    void* slice_ptr = reinterpret_cast<void*>(
                        memory_descriptor.buffer_descriptor.buffer_address_);
                    slices.emplace_back(Slice{
                        slice_ptr, memory_descriptor.buffer_descriptor.size_});
                    batched_slices.insert({keys[i], std::move(slices)});
                    break;
                }
            }
        }
    }
    return {};
}

tl::expected<void, ErrorCode> FileStorage::RegisterLocalMemory() {
    // The buffer pool backs SSD-offload read results that are fetched by
    // remote peers via RDMA READ.  It must therefore be registered with
    // remote_accessible=true so its BufferDesc publishes an rkey; otherwise
    // every remote read of an offloaded object fails with "No rkey for MR
    // access" (the pool is only reachable through the address-range lookup,
    // and with remote_accessible=false the rkey array is left empty).
    auto error_code = client_->RegisterLocalMemory(
        client_buffer_allocator_->getBase(), config_.local_buffer_size,
        kWildcardLocation, true, true);
    if (!error_code) {
        LOG(ERROR) << "Failed to register local memory: " << error_code.error();
        return error_code;
    }
    return {};
}

tl::expected<std::shared_ptr<FileStorage::AllocatedBatch>, ErrorCode>
FileStorage::AllocateBatch(const std::vector<std::string>& keys,
                           const std::vector<int64_t>& sizes,
                           ClientBufferAllocator& allocator) {
    if (keys.size() != sizes.size()) {
        LOG(ERROR) << "Mismatched keys and sizes count: keys=" << keys.size()
                   << ", sizes=" << sizes.size();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto result = std::make_shared<AllocatedBatch>();
    result->batch_id = next_batch_id_.fetch_add(1, std::memory_order_relaxed);
    std::chrono::steady_clock::time_point now =
        std::chrono::steady_clock::now();
    auto lease_timeout =
        now + std::chrono::milliseconds(config_.client_buffer_gc_ttl_ms);
    static constexpr size_t kDirectIOAlignment = 4096;

    u_int64_t total_size = 0;
    bool gc_triggered = false;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (sizes[i] < 0 || sizes[i] > config_.local_buffer_size) {
            LOG(ERROR) << "Invalid size for key " << keys[i] << ": " << sizes[i]
                       << " (limit: " << config_.local_buffer_size << ")";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        size_t data_size = static_cast<size_t>(sizes[i]);
        size_t alloc_size = std::max<size_t>(data_size, 1);
        if (config_.use_uring) {
            // O_DIRECT reads need room to align the pointer and to round an
            // unaligned file range out to full pages.
            alloc_size = align_up(data_size, kDirectIOAlignment) +
                         2 * kDirectIOAlignment;
        }

        auto alloc_result = allocator.allocate(alloc_size);
        if (!alloc_result && !gc_triggered &&
            &allocator == client_buffer_allocator_.get()) {
            gc_triggered = true;
            {
                MutexLocker locker(&client_buffer_mutex_);
                auto gc_now = std::chrono::steady_clock::now();
                for (auto it = client_buffer_allocated_batches_.begin();
                     it != client_buffer_allocated_batches_.end();) {
                    if (gc_now >= it->second->lease_timeout) {
                        it = client_buffer_allocated_batches_.erase(it);
                    } else {
                        ++it;
                    }
                }
            }
            alloc_result = allocator.allocate(alloc_size);
        }
        if (!alloc_result) {
            return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
        }

        void* data_ptr = alloc_result->ptr();
        if (config_.use_uring) {
            data_ptr =
                reinterpret_cast<void*>((reinterpret_cast<uintptr_t>(data_ptr) +
                                         kDirectIOAlignment - 1) &
                                        ~(kDirectIOAlignment - 1));
        }

        total_size += data_size;
        result->slices.emplace(keys[i], Slice{data_ptr, data_size});
        // pointers will be adjusted after BatchLoad (offset_in_buffer
        // correction)
        result->pointers.emplace_back(reinterpret_cast<uintptr_t>(data_ptr));
        result->handles.emplace_back(std::move(alloc_result.value()));
        result->lease_timeout = lease_timeout;
    }
    result->total_size = total_size;
    return result;
}

void FileStorage::ClientBufferGCThreadFunc() {
    LOG(INFO) << "action=client_buffer_gc_thread_started";
    while (client_buffer_gc_running_) {
        {
            MutexLocker locker(&client_buffer_mutex_);
            if (!client_buffer_allocated_batches_.empty()) {
                auto now = std::chrono::steady_clock::now();
                for (auto it = client_buffer_allocated_batches_.begin();
                     it != client_buffer_allocated_batches_.end();) {
                    if (now >= it->second->lease_timeout) {
                        VLOG(1) << "GC releasing batch_id: " << it->first
                                << " (lease expired)";
                        it = client_buffer_allocated_batches_.erase(it);
                    } else {
                        ++it;
                    }
                }
            }
        }
        std::this_thread::sleep_for(
            std::chrono::seconds(config_.client_buffer_gc_interval_seconds));
    }
    LOG(INFO) << "action=client_buffer_gc_thread_stopped";
}

tl::expected<bool, ErrorCode> FileStorage::Exists(const std::string& key) {
    if (!storage_backend_) {
        LOG(ERROR) << "Storage backend is not initialized. Call Init() first.";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return storage_backend_->IsExist(key);
}

bool FileStorage::ReleaseBuffer(uint64_t batch_id) {
    MutexLocker locker(&client_buffer_mutex_);
    auto it = client_buffer_allocated_batches_.find(batch_id);
    if (it != client_buffer_allocated_batches_.end()) {
        VLOG(1) << "Releasing buffer for batch_id: " << batch_id
                << " (transfer completed)";
        client_buffer_allocated_batches_.erase(it);
        return true;
    }
    VLOG(1) << "batch_id " << batch_id
            << " not found (may have been GC'd already)";
    return false;
}

tl::expected<void, ErrorCode> FileStorage::ReRegisterOffloadedObjects() {
    LOG(INFO) << "ReRegisterOffloadedObjects: starting ScanMeta to re-register "
              << "offloaded objects with master";
    int total_keys = 0;
    int total_batches = 0;
    int total_failures = 0;
    // Reset the scan iterator so ScanMeta starts from the beginning.
    // BucketStorageBackend uses cursor-based iteration (next_bucket_);
    // after Init() completes the cursor is 0 and HasNext() returns false,
    // which would make ScanMeta skip all buckets.
    storage_backend_->ResetScanIterator();
    LOG(INFO) << "ReRegisterOffloadedObjects: about to call "
                 "storage_backend_->ScanMeta()";
    auto scan_meta_result =
        storage_backend_->ScanMeta(
            [this, &total_keys, &total_batches, &total_failures](
                const std::vector<std::string>& keys,
                std::vector<StorageObjectMetadata>& metadatas) {
                // A rescan can be mid-flight when a drain deregisters the
                // disk tier; every batch it would register from here on
                // re-advertises the departing store. The master refuses such
                // registrations anyway (the segment entry is gone), so this
                // stops the scan on the first batch instead of on a refused
                // RPC.
                if (draining_.load()) {
                    LOG(INFO) << "ReRegisterOffloadedObjects: aborting scan, "
                              << "the disk tier is draining";
                    return ErrorCode::SEGMENT_NOT_FOUND;
                }
                total_batches++;
                total_keys += keys.size();
                for (auto& metadata : metadatas) {
                    metadata.transport_endpoint = local_rpc_addr_;
                }
                auto tasks = BuildOffloadTasksFromStorageKeys(keys, metadatas);
                auto add_object_result =
                    client_->NotifyOffloadSuccess(tasks, metadatas);
                if (!add_object_result) {
                    total_failures++;
                    LOG(ERROR)
                        << "ReRegisterOffloadedObjects: NotifyOffloadSuccess "
                        << "failed for batch " << total_batches << " with "
                        << keys.size()
                        << " keys, error: " << add_object_result.error();
                    return add_object_result.error();
                }
                LOG(INFO) << "ReRegisterOffloadedObjects: NotifyOffloadSuccess "
                          << "succeeded for batch " << total_batches << " with "
                          << keys.size() << " keys";
                return ErrorCode::OK;
            });

    LOG(INFO) << "ReRegisterOffloadedObjects: ScanMeta returned. success="
              << scan_meta_result.has_value();
    if (!scan_meta_result) {
        if (config_.enable_dfs &&
            scan_meta_result.error() == ErrorCode::NOT_SUPPORTED) {
            LOG(INFO) << "ReRegisterOffloadedObjects: Currently, DFS ScanMeta "
                         "is not supported; skip";
            return {};
        }
        LOG(ERROR) << "ReRegisterOffloadedObjects: ScanMeta failed: "
                   << scan_meta_result.error();
        return scan_meta_result;
    }
    LOG(INFO) << "ReRegisterOffloadedObjects: completed. "
              << "total_keys=" << total_keys
              << " total_batches=" << total_batches
              << " total_failures=" << total_failures;
    return {};
}

// ---- SSD backup ----

namespace {

int64_t BackupEnvI64(const char* name, int64_t fallback) {
    const char* v = std::getenv(name);
    if (v == nullptr || *v == '\0') return fallback;
    char* end = nullptr;
    errno = 0;
    const long long parsed = std::strtoll(v, &end, 10);
    if (end == v || *end != '\0' || parsed < 0 || errno == ERANGE) {
        LOG(WARNING) << "Ignoring invalid " << name << "=" << v;
        return fallback;
    }
    return parsed;
}

// Test-only pause points for fault-injection audits, compiled in only with
// -DMOONCAKE_TEST_FAILPOINTS=ON. Then off unless MC_SSD_BACKUP_TEST_FAILPOINT
// names a stage (dispatched | read_done | written); the first batch reaching
// it logs a marker and sleeps MC_SSD_BACKUP_TEST_FAILPOINT_MS (default 30 s)
// so a controller can kill a process inside that window.
#ifndef MOONCAKE_TEST_FAILPOINTS
void BackupFailpoint(const char*, const BackupBatch&) {}
#else
void BackupFailpoint(const char* stage, const BackupBatch& batch) {
    static const char* want = std::getenv("MC_SSD_BACKUP_TEST_FAILPOINT");
    if (want == nullptr || std::string_view(want) != stage) return;
    static std::atomic<bool> fired{false};
    if (fired.exchange(true)) return;
    const int64_t ms = BackupEnvI64("MC_SSD_BACKUP_TEST_FAILPOINT_MS", 30000);
    std::string keys;
    for (size_t i = 0; i < batch.tasks.size() && i < 3; ++i) {
        keys += (i ? "," : "") + batch.tasks[i].key;
    }
    LOG(WARNING) << "ssd_backup_failpoint stage=" << stage
                 << " batch=" << batch.batch_id
                 << " source=" << batch.source_client_id
                 << " tasks=" << batch.tasks.size() << " first_keys=" << keys
                 << " pausing_ms=" << ms;
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}
#endif  // MOONCAKE_TEST_FAILPOINTS

}  // namespace

FileStorage::BackupConfig FileStorage::BackupConfig::FromEnvironment(
    int64_t ssd_total_size_limit, int64_t dram_segment_bytes) {
    BackupConfig c;
    const char* enable = std::getenv("MC_SSD_BACKUP_ENABLE");
    // Same parser as SsdBackupCoordinator::Config (master side).
    c.enabled = enable != nullptr && TryParseBool(enable).value_or(false);
    const int64_t default_cap = dram_segment_bytes > 0
                                    ? 2 * dram_segment_bytes
                                    : ssd_total_size_limit / 8;
    c.capacity_bytes =
        BackupEnvI64("MC_SSD_BACKUP_CAPACITY_BYTES", default_cap);
    // The pin region must leave room for this client's own primaries.
    if (ssd_total_size_limit > 0 &&
        c.capacity_bytes > ssd_total_size_limit / 2) {
        LOG(WARNING) << "MC_SSD_BACKUP_CAPACITY_BYTES=" << c.capacity_bytes
                     << " exceeds half the SSD cap; clamped to "
                     << ssd_total_size_limit / 2;
        c.capacity_bytes = ssd_total_size_limit / 2;
    }
    c.staging_bytes =
        BackupEnvI64("MC_SSD_BACKUP_STAGING_BYTES", c.staging_bytes);
    c.interval = std::chrono::milliseconds(
        BackupEnvI64("MC_SSD_BACKUP_INTERVAL_MS", c.interval.count()));
    c.scan_interval = std::chrono::milliseconds(BackupEnvI64(
        "MC_SSD_BACKUP_SCAN_INTERVAL_MS", c.scan_interval.count()));
    c.sample_keys = static_cast<size_t>(
        BackupEnvI64("MC_SSD_BACKUP_SAMPLE_KEYS", c.sample_keys));
    c.min_buckets_per_scan = static_cast<size_t>(BackupEnvI64(
        "MC_SSD_BACKUP_MIN_BUCKETS_PER_SCAN", c.min_buckets_per_scan));
    c.max_buckets_per_scan = static_cast<size_t>(BackupEnvI64(
        "MC_SSD_BACKUP_MAX_BUCKETS_PER_SCAN", c.max_buckets_per_scan));
    c.max_write_bytes_per_sec =
        BackupEnvI64("MC_SSD_BACKUP_MAX_WRITE_BPS", c.max_write_bytes_per_sec);
    c.read_chunk_bytes =
        BackupEnvI64("MC_SSD_BACKUP_READ_CHUNK_BYTES", c.read_chunk_bytes);
    c.read_query_bytes =
        BackupEnvI64("MC_SSD_BACKUP_READ_QUERY_BYTES", c.read_query_bytes);
    {
        // Both or neither: a fallback of one alone could invert them.
        const auto ratio = [](const char* name, double fallback) {
            const char* v = std::getenv(name);
            if (v == nullptr || *v == '\0') return fallback;
            char* end = nullptr;
            const double r = std::strtod(v, &end);
            return end != v && *end == '\0' ? r : -1.0;  // -1: invalid
        };
        const double high =
            ratio("MC_SSD_BACKUP_PIN_HIGH_RATIO", c.pin_high_ratio);
        const double low =
            ratio("MC_SSD_BACKUP_PIN_LOW_RATIO", c.pin_low_ratio);
        // Below the master's usage limit (SsdBackupCoordinator 0.98).
        constexpr double kMasterUsageLimit = 0.98;
        if (low > 0.0 && low < high && high < kMasterUsageLimit) {
            c.pin_high_ratio = high;
            c.pin_low_ratio = low;
        } else {
            LOG(WARNING) << "Ignoring MC_SSD_BACKUP_PIN_HIGH_RATIO=" << high
                         << " / MC_SSD_BACKUP_PIN_LOW_RATIO=" << low
                         << " (want 0 < low < high < " << kMasterUsageLimit
                         << "); using " << c.pin_high_ratio << " / "
                         << c.pin_low_ratio;
        }
    }
    // Both bounded to [0, 24 h] so deadline arithmetic cannot overflow.
    const auto bounded_ms = [](const char* name,
                               std::chrono::milliseconds fallback) {
        const int64_t v = BackupEnvI64(name, fallback.count());
        constexpr int64_t kMax = 24LL * 3600 * 1000;
        if (v < 0 || v > kMax) {
            const int64_t used = v < 0 ? fallback.count() : kMax;
            LOG(WARNING) << name << "=" << v << " out of [0, " << kMax
                         << "]; using " << used;
            return std::chrono::milliseconds(used);
        }
        return std::chrono::milliseconds(v);
    };
    c.staging_quarantine =
        bounded_ms("MC_SSD_BACKUP_STAGING_QUARANTINE_MS", c.staging_quarantine);
    c.staging_wait =
        bounded_ms("MC_SSD_BACKUP_STAGING_WAIT_MS", c.staging_wait);
    // The worker sleeps `interval` between ticks: 0 would spin, a huge value
    // would stall shutdown (the destructor joins the worker).
    const auto interval = std::clamp(c.interval, std::chrono::milliseconds(10),
                                     std::chrono::milliseconds(60000));
    if (interval != c.interval) {
        LOG(WARNING) << "MC_SSD_BACKUP_INTERVAL_MS=" << c.interval.count()
                     << " out of [10, 60000]; using " << interval.count();
        c.interval = interval;
    }
    // Bounded so scan_interval * backoff rounds cannot overflow.
    c.scan_interval = std::min(c.scan_interval,
                               std::chrono::milliseconds(24LL * 3600 * 1000));
    // std::clamp(x, lo, hi) needs lo <= hi.
    if (c.min_buckets_per_scan > c.max_buckets_per_scan) {
        LOG(WARNING) << "MC_SSD_BACKUP_MIN_BUCKETS_PER_SCAN="
                     << c.min_buckets_per_scan
                     << " exceeds MC_SSD_BACKUP_MAX_BUCKETS_PER_SCAN="
                     << c.max_buckets_per_scan << "; using the maximum";
        c.min_buckets_per_scan = c.max_buckets_per_scan;
    }
    if (c.sample_keys == 0) c.sample_keys = 1;
    // One bucket's sample must fit a single residency query; a larger one
    // would be refused by the master on every scan (and block later buckets).
    if (c.sample_keys > kBackupScanMaxKeysPerCall) {
        LOG(WARNING) << "MC_SSD_BACKUP_SAMPLE_KEYS=" << c.sample_keys
                     << " exceeds the per-call limit; clamped to "
                     << kBackupScanMaxKeysPerCall;
        c.sample_keys = kBackupScanMaxKeysPerCall;
    }
    return c;
}

tl::expected<void, ErrorCode> FileStorage::StartBackupWorker() {
    backup_config_ = BackupConfig::FromEnvironment(config_.total_size_limit,
                                                   config_.dram_segment_bytes);
    if (!backup_config_.enabled) {
        return {};
    }
    if (!std::dynamic_pointer_cast<BucketStorageBackend>(storage_backend_)) {
        LOG(WARNING) << "SSD backup needs the bucket storage backend";
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (backup_config_.capacity_bytes <= 0 ||
        backup_config_.staging_bytes <= 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto staging_size = static_cast<size_t>(backup_config_.staging_bytes);
    backup_staging_allocator_ =
        backup_staging_alloc_for_test_
            ? backup_staging_alloc_for_test_(staging_size)
            : AlignedClientBufferAllocator::create(staging_size,
                                                   client_->GetProtocol());
    if (!backup_staging_allocator_) {
        return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
    }
    // Local RDMA destination only: no remote access needed.
    auto reg = backup_staging_register_for_test_
                   ? backup_staging_register_for_test_(
                         backup_staging_allocator_->getBase(), staging_size)
                   : client_->RegisterLocalMemory(
                         backup_staging_allocator_->getBase(), staging_size,
                         kWildcardLocation, false, true);
    if (!reg) {
        backup_staging_allocator_.reset();
        return tl::make_unexpected(reg.error());
    }
    backup_staging_registered_ = true;
    StagingEvent("register", backup_staging_allocator_->getBase(),
                 staging_size);
    backup_running_.store(true);
    backup_thread_ = std::thread([this]() {
        LOG(INFO) << "SSD backup worker started: capacity_bytes="
                  << backup_config_.capacity_bytes
                  << ", staging_bytes=" << backup_config_.staging_bytes
                  << ", interval_ms=" << backup_config_.interval.count()
                  << ", read_chunk_bytes=" << backup_config_.read_chunk_bytes
                  << ", read_query_bytes=" << backup_config_.read_query_bytes;
        while (backup_running_.load()) {
            if (!draining_.load()) {
                BackupTick();
                BackupStage("tick_end", 0);
            }
            // Tick wait: only stop ends it early (a drain just skips ticks).
            WaitBackupUntil(
                std::chrono::steady_clock::now() + backup_config_.interval,
                /*wake_on_drain=*/false);
        }
    });
    return {};
}

bool FileStorage::BackupStopRequested() const {
    return backup_stop_.load() || draining_.load();
}

bool FileStorage::WaitBackupUntil(std::chrono::steady_clock::time_point t,
                                  bool wake_on_drain) {
    const auto stop = [this, wake_on_drain] {
        return wake_on_drain ? BackupStopRequested() : backup_stop_.load();
    };
    std::unique_lock<std::mutex> lock(backup_wait_mutex_);
    // Waits in bounded slices, so a far (possibly saturated) deadline never
    // goes through the clock conversions of one wait_until call.
    constexpr auto kSlice = std::chrono::seconds(1);
    while (!stop()) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= t) return true;
        backup_wait_cv_.wait_until(lock, t - now > kSlice ? now + kSlice : t,
                                   stop);
    }
    return false;
}

void FileStorage::WakeBackupWorker() {
    // Taking the mutex orders the flag change before the waiter's predicate
    // check, so the notification cannot be lost.
    {
        std::lock_guard<std::mutex> lock(backup_wait_mutex_);
    }
    backup_wait_cv_.notify_all();
}

void FileStorage::BackupTick() {
    auto backend =
        std::dynamic_pointer_cast<BucketStorageBackend>(storage_backend_);
    EnforceBackupPinCap();
    // Only pinned bytes count against the backup capacity: demoted backups
    // are ordinary evictable cache.
    const auto usage = backend->GetBackupUsage();
    auto resp = client_->BackupObjectHeartbeat(
        local_rpc_addr_, usage.pinned_bytes, backup_config_.capacity_bytes);
    if (!resp) {
        LOG_EVERY_N(WARNING, 60)
            << "BackupObjectHeartbeat failed: " << resp.error();
        return;
    }
    BackupStage("heartbeat", resp->batches.size());
    // Stop or drain may have been requested while the heartbeat RPC was in
    // flight: admit no new work, just release what the master handed us.
    if (BackupStopRequested()) {
        ReleaseUnreadBackupBatches(resp->batches, 0);
        return;
    }
    ApplyBackupRefreshes(resp->refreshes);
    RunBackupBatches(resp->batches);
    if (!BackupStopRequested() &&
        std::chrono::steady_clock::now() >= next_backup_scan_) {
        ScanPinnedBackups();
    }
}

void FileStorage::ReleaseUnreadBackupBatches(
    const std::vector<BackupBatch>& batches, size_t from) {
    // An empty ACK releases the batch's reservation on the master.
    for (size_t j = from; j < batches.size(); ++j) {
        auto r = client_->NotifyBackupDone(batches[j].batch_id, {}, {});
        BackupAcked(batches[j].batch_id, 0, r.has_value());
    }
}

void FileStorage::ApplyBackupRefreshes(
    const std::vector<BackupRefreshItem>& items) {
    if (items.empty()) return;
    auto backend =
        std::dynamic_pointer_cast<BucketStorageBackend>(storage_backend_);
    std::vector<std::string> keys;
    keys.reserve(items.size());
    for (const auto& item : items) {
        keys.push_back(TenantId(item.tenant_id).MakeScopedKey(item.key));
    }
    backup_scan_stats_.refreshed += backend->RefreshBackupKeys(keys);
}

void FileStorage::EnforceBackupPinCap() {
    auto backend =
        std::dynamic_pointer_cast<BucketStorageBackend>(storage_backend_);
    const int64_t cap = backup_config_.capacity_bytes;
    const auto mark = [cap](double ratio) {
        return static_cast<int64_t>(static_cast<double>(cap) * ratio);
    };
    const int64_t high = mark(backup_config_.pin_high_ratio);
    const int64_t low = mark(backup_config_.pin_low_ratio);
    auto usage = backend->GetBackupUsage();
    if (usage.pinned_bytes < high) return;
    // At the high mark: demote the buckets the last scans found coldest
    // (never scanned ones count as hot), oldest first among equals, until
    // below the low mark. Demoted buckets stay on disk as ordinary cache.
    auto pinned = backend->ListPinnedBackupBuckets(0);
    // Buckets that protect nothing (no live key) go first.
    std::sort(pinned.begin(), pinned.end(),
              [this](const auto& a, const auto& b) {
                  if ((a.live_keys == 0) != (b.live_keys == 0)) {
                      return a.live_keys == 0;
                  }
                  auto ra = backup_scan_state_.count(a.bucket_id)
                                ? backup_scan_state_[a.bucket_id].last_ratio
                                : 1.0;
                  auto rb = backup_scan_state_.count(b.bucket_id)
                                ? backup_scan_state_[b.bucket_id].last_ratio
                                : 1.0;
                  if (ra != rb) return ra < rb;
                  return a.last_access_ns < b.last_access_ns;
              });
    int64_t pinned_bytes = usage.pinned_bytes;
    size_t demoted = 0;
    for (const auto& b : pinned) {
        if (pinned_bytes < low) break;
        if (backend->DemoteBackupBucket(b.bucket_id)) {
            pinned_bytes -= b.bytes;
            cap_demoted_bytes_total_ += static_cast<uint64_t>(b.bytes);
            backup_scan_state_.erase(b.bucket_id);
            ++demoted;
        }
    }
    backup_scan_stats_.demoted_cap += demoted;
    const auto now = std::chrono::steady_clock::now();
    // Our ratios were stale enough to reach the mark: rescan soon (not on
    // every demotion, which now comes every few seconds under steady writes).
    if (now - last_cap_rescan_ >= kBackupCapRescanInterval) {
        last_cap_rescan_ = now;
        next_backup_scan_ = now;
    }
    if (now - last_cap_log_ >= kBackupCapLogInterval) {
        last_cap_log_ = now;
        LOG(INFO) << "ssd_backup_pin_cap demoted=" << demoted
                  << " pinned_bytes=" << pinned_bytes << " cap=" << cap
                  << " high=" << high << " low=" << low
                  << " total_demoted_cap=" << backup_scan_stats_.demoted_cap
                  << " total_demoted_cap_bytes=" << cap_demoted_bytes_total_;
    }
}

void FileStorage::ScanPinnedBackups() {
    using Clock = std::chrono::steady_clock;
    auto backend =
        std::dynamic_pointer_cast<BucketStorageBackend>(storage_backend_);
    const auto now = Clock::now();
    const auto& cfg = backup_config_;
    // Jitter +-10% so clients do not scan in lockstep.
    const auto jitter = std::chrono::milliseconds(
        cfg.scan_interval.count() > 10
            ? randomUniform<int64_t>(0, cfg.scan_interval.count() / 5) -
                  cfg.scan_interval.count() / 10
            : 0);
    next_backup_scan_ = now + cfg.scan_interval + jitter;
    ++backup_scan_stats_.scans;

    auto pinned = backend->ListPinnedBackupBuckets(
        cfg.sample_keys, randomUniform<uint64_t>(0, UINT32_MAX));
    const auto usage = backend->GetBackupUsage();
    const double keep =
        usage.pinned_bytes >=
                static_cast<int64_t>(cfg.tight_fraction *
                                     static_cast<double>(cfg.capacity_bytes))
            ? cfg.keep_ratio_tight
            : cfg.keep_ratio;

    // Forget state of buckets no longer pinned. A bucket's first check is
    // due one interval after it was written or last refreshed (a fresh
    // backup is in DRAM by construction), not one interval after the scan
    // that first sees it: that would add up to another interval, during
    // which a bucket whose primaries are long gone keeps its pin.
    std::unordered_set<int64_t> live;
    std::vector<const BucketStorageBackend::PinnedBackupBucket*> due;
    size_t new_buckets = 0;
    for (const auto& b : pinned) {
        if (b.live_keys == 0) {
            // No key maps here any more (all replaced by newer backups or
            // undone as stale): the bucket protects nothing. Demote it with
            // the oldest LRU time so eviction takes it first; no RPC needed.
            if (backend->DemoteBackupBucket(b.bucket_id, 0)) {
                ++backup_scan_stats_.demoted_unmapped;
            }
            continue;  // its scan state, if any, is forgotten below
        }
        live.insert(b.bucket_id);
        auto [it, inserted] = backup_scan_state_.try_emplace(b.bucket_id);
        if (inserted) {
            // last_access_ns is steady_clock time (see the backend).
            const auto written =
                std::min(now, Clock::time_point(Clock::duration(
                                  std::max<int64_t>(b.last_access_ns, 0))));
            it->second.next_check = written + cfg.scan_interval;
            ++new_buckets;
        }
        if (it->second.next_check <= now) due.push_back(&b);
    }
    for (auto it = backup_scan_state_.begin();
         it != backup_scan_state_.end();) {
        it = live.count(it->first) ? std::next(it)
                                   : backup_scan_state_.erase(it);
    }
    backup_new_buckets_since_scan_ += new_buckets;
    size_t budget =
        std::clamp<size_t>(backup_new_buckets_since_scan_ * 3 / 2,
                           cfg.min_buckets_per_scan, cfg.max_buckets_per_scan);
    // Near the cap: check twice as many, so demotions keep up.
    if (keep == cfg.keep_ratio_tight) budget *= 2;
    backup_new_buckets_since_scan_ = 0;
    // Oldest first: the earliest next_check has waited longest.
    std::sort(due.begin(), due.end(), [this](const auto* a, const auto* b) {
        return backup_scan_state_[a->bucket_id].next_check <
               backup_scan_state_[b->bucket_id].next_check;
    });
    if (due.size() > budget) due.resize(budget);

    // Query in chunks that fit the master's per-call limit (a bucket's
    // sample is at most sample_keys <= kBackupScanMaxKeysPerCall keys).
    constexpr size_t kMaxKeysPerCall = kBackupScanMaxKeysPerCall;
    size_t i = 0;
    while (i < due.size()) {
        // Stop or drain: issue no further residency query (unchecked
        // buckets keep their state and are checked by a later scan).
        if (BackupStopRequested()) return;
        std::vector<BackupRefreshItem> query;
        std::vector<std::pair<size_t, size_t>> ranges;  // [begin, end) in query
        size_t j = i;
        for (; j < due.size(); ++j) {
            const auto& keys = due[j]->sample_keys;
            if (!query.empty() && query.size() + keys.size() > kMaxKeysPerCall)
                break;
            const size_t begin = query.size();
            for (const auto& storage_key : keys) {
                auto [tenant, key] = TenantId::ParseScopedKey(storage_key);
                query.push_back({tenant.value(), std::move(key)});
            }
            ranges.emplace_back(begin, query.size());
        }
        auto res = client_->QueryBackupResidency(query);
        if (!res || res->size() != query.size()) {
            LOG_EVERY_N(WARNING, 10) << "QueryBackupResidency failed";
            // Retry these buckets next round.
            return;
        }
        for (size_t k = 0; k < ranges.size(); ++k) {
            const auto* b = due[i + k];
            auto& st = backup_scan_state_[b->bucket_id];
            size_t in_dram = 0, owner_gone = 0;
            const size_t n = ranges[k].second - ranges[k].first;
            for (size_t q = ranges[k].first; q < ranges[k].second; ++q) {
                const auto r = static_cast<BackupResidency>((*res)[q]);
                if (r == BackupResidency::kInDram) ++in_dram;
                if (r == BackupResidency::kOwnerGone) ++owner_gone;
            }
            ++backup_scan_stats_.buckets_checked;
            const double ratio =
                n == 0 ? 0.0 : static_cast<double>(in_dram) / n;
            st.last_ratio = ratio;
            if (ratio >= keep) {
                // Still hot: check again later, backing off while it stays hot.
                st.keeps = std::min(st.keeps + 1, cfg.max_backoff_rounds);
                st.next_check = now + cfg.scan_interval * st.keeps;
                continue;
            }
            // Cold. If the owner is gone for most keys this backup is now the
            // only copy: give it a fresh LRU life instead of its old time.
            const bool gone = n > 0 && owner_gone * 2 > n;
            std::optional<int64_t> ts;
            if (gone) {
                ts = Clock::now().time_since_epoch().count();
            }
            if (backend->DemoteBackupBucket(b->bucket_id, ts)) {
                ++(gone ? backup_scan_stats_.demoted_owner_gone
                        : backup_scan_stats_.demoted_cold);
            }
            backup_scan_state_.erase(b->bucket_id);
        }
        i = j;
    }
    const auto stats = GetBackupScanStats();
    LOG(INFO) << "ssd_backup_scan pinned=" << pinned.size()
              << " pinned_bytes=" << usage.pinned_bytes
              << " checked=" << due.size() << " budget=" << budget
              << " keep_ratio=" << keep
              << " total_demoted_cold=" << stats.demoted_cold
              << " total_demoted_owner_gone=" << stats.demoted_owner_gone
              << " total_demoted_cap=" << stats.demoted_cap
              << " total_demoted_unmapped=" << stats.demoted_unmapped
              << " total_refreshed=" << stats.refreshed;
}

void FileStorage::QuarantineBackupStaging(
    std::shared_ptr<AllocatedBatch> staging) {
    if (!staging) return;
    std::lock_guard<std::mutex> lock(backup_quarantine_mutex_);
    backup_quarantined_bytes_ += static_cast<int64_t>(staging->total_size);
    ++backup_quarantined_total_;
    StagingEvent("quarantine", *staging);
    backup_staging_quarantine_.push_back(
        {std::move(staging), BackupNow() + backup_config_.staging_quarantine});
    UpdateBackupQuarantineMetricsLocked();
    if (ssd_metric_) ssd_metric_->ssd_backup_staging_quarantined_total.inc();
    LOG(ERROR) << "ssd_backup_staging_quarantined batches="
               << backup_staging_quarantine_.size()
               << " bytes=" << backup_quarantined_bytes_
               << " total=" << backup_quarantined_total_
               << " staging_bytes=" << backup_config_.staging_bytes
               << " release_after_ms="
               << backup_config_.staging_quarantine.count();
}

void FileStorage::ReleaseExpiredBackupQuarantine() {
    std::vector<QuarantinedStaging> expired;
    {
        std::lock_guard<std::mutex> lock(backup_quarantine_mutex_);
        if (backup_staging_quarantine_.empty()) return;
        const auto now = BackupNow();
        auto& q = backup_staging_quarantine_;
        auto keep = std::stable_partition(
            q.begin(), q.end(),
            [now](const QuarantinedStaging& s) { return s.release_at > now; });
        if (keep == q.end()) return;
        for (auto it = keep; it != q.end(); ++it) {
            backup_quarantined_bytes_ -=
                static_cast<int64_t>(it->staging->total_size);
            ++backup_quarantine_released_total_;
            StagingEvent("quarantine_expired", *it->staging);
        }
        expired.assign(std::make_move_iterator(keep),
                       std::make_move_iterator(q.end()));
        q.erase(keep, q.end());
        UpdateBackupQuarantineMetricsLocked();
        LOG(WARNING) << "ssd_backup_staging_quarantine_released batches="
                     << expired.size() << " still_quarantined=" << q.size()
                     << " bytes=" << backup_quarantined_bytes_;
    }
    expired.clear();  // back to the allocator, outside the lock
    {
        std::lock_guard<std::mutex> lock(backup_wait_mutex_);
        ++backup_staging_releases_;
    }
    backup_wait_cv_.notify_all();
}

void FileStorage::UpdateBackupQuarantineMetricsLocked() {
    if (ssd_metric_) {
        ssd_metric_->ssd_backup_staging_quarantined_bytes.update(
            backup_quarantined_bytes_);
    }
}

FileStorage::BackupStagingStats FileStorage::GetBackupStagingStats() {
    BackupStagingStats s;
    {
        std::lock_guard<std::mutex> lock(backup_quarantine_mutex_);
        s.quarantined_batches = backup_staging_quarantine_.size();
        s.quarantined_bytes = backup_quarantined_bytes_;
        s.quarantined_total = backup_quarantined_total_;
        s.quarantine_released = backup_quarantine_released_total_;
    }
    s.alloc_waits = backup_staging_alloc_waits_.load();
    s.alloc_failed = backup_staging_alloc_failed_.load();
    return s;
}

void FileStorage::HoldBackupStaging(PreparedBackup& out,
                                    std::shared_ptr<AllocatedBatch> staging) {
    {
        std::lock_guard<std::mutex> lock(backup_wait_mutex_);
        ++backup_staging_held_;
    }
    // Counted as held until the last handle goes, however the batch ends:
    // the memory goes back to the allocator first, then a waiting
    // allocation is woken.
    AllocatedBatch* raw = staging.get();
    StagingEvent("hold", *raw);
    out.staging.push_back(std::shared_ptr<AllocatedBatch>(
        raw, [this, owner = std::move(staging)](AllocatedBatch*) mutable {
            owner.reset();
            {
                std::lock_guard<std::mutex> lock(backup_wait_mutex_);
                --backup_staging_held_;
                ++backup_staging_releases_;
            }
            backup_wait_cv_.notify_all();
        }));
}

tl::expected<std::shared_ptr<FileStorage::AllocatedBatch>, ErrorCode>
FileStorage::AllocateBackupStaging(const std::vector<std::string>& keys,
                                   const std::vector<int64_t>& sizes,
                                   size_t own_held) {
    const auto deadline =
        std::chrono::steady_clock::now() + backup_config_.staging_wait;
    bool waited = false;
    for (;;) {
        ReleaseExpiredBackupQuarantine();
        uint64_t releases;
        {
            std::lock_guard<std::mutex> lock(backup_wait_mutex_);
            releases = backup_staging_releases_;
        }
        auto alloc = AllocateBatch(keys, sizes, *backup_staging_allocator_);
        if (!alloc) BackupStage("staging_alloc_failed", keys.size());
        if (alloc && backup_staging_event_for_test_) {
            // Test observer: report when the memory is back in the
            // allocator, however the batch ends (held, quarantined, dropped).
            StagingEvent("alloc", **alloc);
            AllocatedBatch* raw = alloc->get();
            alloc = std::shared_ptr<AllocatedBatch>(
                raw, [this, owner = std::move(*alloc),
                      span = StagingSpan(*raw)](AllocatedBatch*) mutable {
                    owner.reset();
                    StagingEvent("free", span.first, span.second);
                });
        }
        if (alloc || alloc.error() != ErrorCode::BUFFER_OVERFLOW) return alloc;
        // Full. Worth waiting only while another batch holds staging (the
        // pipeline's other slot, still being written): its release may make
        // room. Allocator rounding means two nominally fitting batches need
        // not fit side by side.
        std::unique_lock<std::mutex> lock(backup_wait_mutex_);
        if (BackupStopRequested()) return alloc;
        // Released since the attempt (possibly the last other holder, so
        // nobody holds staging now): retry before deciding anything.
        if (backup_staging_releases_ != releases) continue;
        const auto others_hold = [&] {
            return backup_staging_held_ > own_held;
        };
        if (!others_hold()) return alloc;
        if (!waited) {
            waited = true;
            backup_staging_alloc_waits_.fetch_add(1);
            BackupStage("staging_wait", keys.size());
        }
        // Wake on a release, stop or drain, or the deadline.
        while (backup_staging_releases_ == releases && others_hold() &&
               !BackupStopRequested()) {
            if (backup_wait_cv_.wait_until(lock, deadline) ==
                std::cv_status::timeout) {
                break;
            }
        }
        if (BackupStopRequested() ||
            (backup_staging_releases_ == releases &&
             std::chrono::steady_clock::now() >= deadline)) {
            return alloc;
        }
    }
}

FileStorage::PreparedBackup FileStorage::ReadBackupBatch(
    const BackupBatch& batch) {
    BackupFailpoint("dispatched", batch);
    PreparedBackup out;
    out.batch = &batch;
    out.start = std::chrono::steady_clock::now();
    // One task per tenant/key. The master merges a later put of a pending
    // key into its task; should a batch still carry two tasks of one key
    // (e.g. from an older master), the later one is the newer put: the read
    // returns its bytes, so it must also carry its version and size.
    std::unordered_map<std::string, std::vector<const BackupTaskItem*>>
        by_tenant;
    std::unordered_map<std::string, size_t> slot;  // scoped key -> index
    for (const auto& task : batch.tasks) {
        auto& list = by_tenant[task.tenant_id];
        auto [it, fresh] = slot.try_emplace(
            TenantId(task.tenant_id).MakeScopedKey(task.key), list.size());
        if (fresh) {
            list.push_back(&task);
        } else {
            list[it->second] = &task;
        }
    }
    for (const auto& [tenant_id, tasks] : by_tenant) {
        BackupStage("before_query", tasks.size());
        // Stop or drain: start no further tenant group (what was already
        // read stays in `out` and goes through the normal write/cleanup).
        if (BackupStopRequested()) {
            out.read_failed += tasks.size();
            continue;
        }
        std::vector<std::string> keys;
        std::vector<int64_t> sizes;
        keys.reserve(tasks.size());
        sizes.reserve(tasks.size());
        for (const auto* t : tasks) {
            keys.push_back(t->key);
            sizes.push_back(t->size);
        }
        // Staging first, before the leased query: a wait for staging must
        // not eat into the read lease.
        auto alloc = AllocateBackupStaging(keys, sizes, out.staging.size());
        if (!alloc) {
            backup_staging_alloc_failed_.fetch_add(1);
            if (ssd_metric_) {
                ssd_metric_->ssd_backup_staging_alloc_failed_total.inc();
            }
            LOG_EVERY_N(WARNING, 20)
                << "SSD backup staging allocation failed: " << alloc.error()
                << ", dropped " << keys.size()
                << " key(s), batch=" << batch.batch_id;
            out.read_failed += keys.size();
            continue;
        }
        if (BackupStopRequested()) {
            out.read_failed += keys.size();
            continue;
        }
        // Read in chunks of about read_chunk_bytes (at least one key), one
        // chunk in flight at a time. A whole group (up to batch_bytes)
        // submitted at once queues that much backup traffic on the
        // transport's lanes and I/O threads ahead of foreground gets and
        // puts of this process and of the source; a bounded chunk bounds how
        // much a foreground transfer can wait behind (a key larger than a
        // chunk still goes as one). One leased query covers up to
        // read_query_bytes, several chunks, so the master sees one query per
        // window rather than per chunk; should a lease end within
        // kBackupReadLeaseMargin of a chunk's read, the rest of the window is
        // queried again first, so a chunk never reads on an aged lease.
        struct ReadKey {
            std::string storage_key;
            std::vector<Slice> slices;
            const BackupTaskItem* task;
        };
        std::vector<ReadKey> group_read;
        size_t group_failed = 0;
        bool unquiesced = false;
        bool stopped = false;
        const int64_t chunk_bytes = backup_config_.read_chunk_bytes;
        const int64_t window_bytes =
            chunk_bytes <= 0
                ? 0
                : std::max(backup_config_.read_query_bytes, chunk_bytes);
        // End of the span from `from` of at most `limit` bytes (0: no limit;
        // at least one key), not past `cap`.
        const auto span_end = [&sizes](size_t from, int64_t limit, size_t cap) {
            size_t end = from;
            int64_t bytes = 0;
            do {
                bytes += sizes[end];
                ++end;
            } while (end < cap && (limit <= 0 || bytes + sizes[end] <= limit));
            return end;
        };
        size_t next = 0;
        size_t requeried_at = keys.size();  // one re-query per position
        // and one more at a window's first chunk (its query was slow)
        size_t refreshed_first_at = keys.size();
        // Estimated time of a chunk from its lease check to the end of its
        // transfer (the last one if slower, else decaying towards recent
        // ones): the lease must outlast at least that much more.
        std::chrono::steady_clock::duration last_chunk_time{};
        // Shortest lease the last query granted (0: none yet / no lease).
        std::chrono::steady_clock::duration last_lease_ttl{};
        while (next < keys.size()) {
            // A window larger than the chunks one lease can cover only gets
            // its tail re-queried (and re-queried again) before it is read:
            // once chunks are slow for the lease, query only as many chunks
            // as fit in it, at least one.
            int64_t window_limit = window_bytes;
            if (chunk_bytes > 0 && last_lease_ttl.count() > 0 &&
                last_chunk_time.count() > 0) {
                const auto usable = last_lease_ttl - 2 * last_chunk_time;
                const int64_t fit =
                    usable.count() > 0 ? 1 + usable / last_chunk_time : 1;
                const int64_t limit = fit > window_bytes / chunk_bytes
                                          ? window_bytes
                                          : fit * chunk_bytes;
                window_limit = std::min(window_limit, limit);
            }
            const size_t wend = span_end(next, window_limit, keys.size());
            if (BackupStopRequested()) {  // stop or drain: read no more
                group_failed += keys.size() - next;
                break;
            }
            const std::vector<std::string> window(keys.begin() + next,
                                                  keys.begin() + wend);
            // Leased query: the source cannot be evicted and reused while we
            // read it.
            const auto queried_at = std::chrono::steady_clock::now();
            auto queries = client_->BatchQuery(window, tenant_id);
            BackupStage("after_query", window.size());
            if (BackupStopRequested()) {  // no transfer after the query either
                group_failed += keys.size() - next;
                break;
            }
            // Only the DRAM copies: Client::BatchGet reads the first complete
            // replica it is given and handles memory replicas only, and the
            // list may put an SSD replica first (e.g. a backup turned into
            // the primary on promotion). No entry: not readable (counted as
            // failed when its chunk comes).
            std::vector<std::optional<QueryResult>> ready(window.size());
            // Shortest lease the query granted (none: 0).
            std::chrono::steady_clock::duration lease_ttl{};
            bool have_lease = false;
            for (size_t i = 0; i < window.size(); ++i) {
                if (!queries[i]) continue;
                std::vector<Replica::Descriptor> memory;
                for (const auto& d : queries[i]->replicas) {
                    if (d.is_memory_replica() &&
                        d.status == ReplicaStatus::COMPLETE) {
                        memory.push_back(d);
                    }
                }
                if (memory.empty()) continue;  // DRAM copy already gone
                ready[i].emplace(std::move(memory), queries[i]->lease_timeout,
                                 queries[i]->object_checksum,
                                 queries[i]->resolved_tenant_id);
                const auto ttl = queries[i]->lease_timeout - queried_at;
                lease_ttl = have_lease ? std::min(lease_ttl, ttl) : ttl;
                have_lease = true;
            }
            last_lease_ttl = lease_ttl;
            // Re-query once a chunk's lease would end within the margin: a
            // quarter of the granted lease (at most kBackupReadLeaseMargin),
            // and at least twice the time the last chunk took, so the lease
            // outlasts the transfer as well as the check. Proportional, so a
            // short lease is still renewed before it runs out and a fresh one
            // is not re-queried at once; with chunks too slow for the lease
            // the next window is cut to the chunks one lease covers. The
            // first chunk of a window was just queried: it is re-queried
            // (once) only if the query left less lease than a chunk takes.
            // No lease (0): nothing to renew, read as is.
            const auto base_margin =
                std::max(std::chrono::steady_clock::duration::zero(),
                         std::min<std::chrono::steady_clock::duration>(
                             kBackupReadLeaseMargin, lease_ttl / 4));
            bool requery = false;
            size_t pos = 0;  // in the window
            while (pos < window.size()) {
                const size_t cend =
                    span_end(next + pos, chunk_bytes, wend) - next;
                if (BackupStopRequested()) {  // stop or drain between chunks
                    group_failed += keys.size() - (next + pos);
                    stopped = true;
                    break;
                }
                const auto now = std::chrono::steady_clock::now();
                const auto margin =
                    base_margin > std::chrono::steady_clock::duration::zero()
                        ? std::max(base_margin, 2 * last_chunk_time)
                        : base_margin;
                bool lease_short = false;
                // A window's first chunk was just queried, so only a lease
                // the query itself used up (a slow query against a short
                // lease) is re-queried: one that would end before a chunk of
                // the estimated time could be read.
                const auto need =
                    pos > 0
                        ? margin
                        : (margin > std::chrono::steady_clock::duration::zero()
                               ? last_chunk_time
                               : std::chrono::steady_clock::duration::zero());
                for (size_t k = pos; k < cend; ++k) {
                    if (need > std::chrono::steady_clock::duration::zero() &&
                        ready[k] && ready[k]->lease_timeout < now + need) {
                        lease_short = true;
                    }
                }
                size_t& mark = pos > 0 ? requeried_at : refreshed_first_at;
                if (lease_short && mark != next + pos) {
                    // Query the rest of the window again before reading on.
                    mark = next + pos;
                    next += pos;
                    requery = true;
                    break;
                }
                std::vector<std::string> read_keys;
                std::vector<QueryResult> read_queries;
                std::vector<const BackupTaskItem*> read_tasks;
                for (size_t k = pos; k < cend; ++k) {
                    // Already re-queried here and the lease is still over:
                    // the read would be refused afterwards, skip it.
                    if (!ready[k] ||
                        (margin > std::chrono::steady_clock::duration::zero() &&
                         ready[k]->lease_timeout <= now)) {
                        ++group_failed;  // best effort
                        continue;
                    }
                    read_keys.push_back(window[k]);
                    read_queries.push_back(std::move(*ready[k]));
                    read_tasks.push_back(tasks[next + k]);
                }
                pos = cend;
                if (read_keys.empty()) continue;
                std::unordered_map<std::string, std::vector<Slice>> slices;
                for (const auto& key : read_keys) {
                    slices[key] = {alloc.value()->slices.at(key)};
                }
                const uint64_t unquiesced_before =
                    UnquiescedTransferFailureCount();
                // Bypass the local hot cache: it is keyed by the bare key and
                // only checks the size, so it could hand us another tenant's
                // or an older version's bytes, which we would then store as
                // this version's backup.
                BackupStage("before_transfer", read_keys.size());
                auto results =
                    client_->BatchGet(read_keys, read_queries, slices,
                                      /*group_by_target=*/false,
                                      /*use_hot_cache=*/false);
                // A slower chunk raises the estimate at once; faster ones
                // lower it by an eighth of the gap per chunk. Windows shrink
                // as soon as chunks slow down but grow back only gradually,
                // so alternating slow and fast chunks do not make the window
                // swing between one chunk and read_query_bytes (re-querying
                // its tail each time).
                const auto took = std::chrono::steady_clock::now() - now;
                last_chunk_time =
                    took >= last_chunk_time
                        ? took
                        : last_chunk_time - (last_chunk_time - took) / 8;
                if (UnquiescedTransferFailureCount() != unquiesced_before) {
                    group_failed +=
                        read_keys.size() + (keys.size() - (next + pos));
                    unquiesced = true;
                    break;
                }
                // Keys refused because their lease ran out during the read
                // (an unforeseen slow query or transfer): query and read
                // them once more into the same staging (their transfers are
                // over), on a fresh lease.
                std::vector<size_t> expired;
                for (size_t i = 0; i < read_keys.size(); ++i) {
                    if (!results[i] &&
                        results[i].error() == ErrorCode::LEASE_EXPIRED) {
                        expired.push_back(i);
                    }
                }
                if (!expired.empty() && !BackupStopRequested()) {
                    std::vector<std::string> again_keys;
                    for (size_t i : expired) again_keys.push_back(read_keys[i]);
                    auto again_q = client_->BatchQuery(again_keys, tenant_id);
                    BackupStage("after_query", again_keys.size());
                    std::vector<size_t> again_idx;
                    std::vector<std::string> again_read;
                    std::vector<QueryResult> again_queries;
                    for (size_t j = 0; j < again_keys.size(); ++j) {
                        if (!again_q[j]) continue;
                        std::vector<Replica::Descriptor> memory;
                        for (const auto& d : again_q[j]->replicas) {
                            if (d.is_memory_replica() &&
                                d.status == ReplicaStatus::COMPLETE) {
                                memory.push_back(d);
                            }
                        }
                        if (memory.empty()) continue;
                        again_idx.push_back(expired[j]);
                        again_read.push_back(again_keys[j]);
                        again_queries.emplace_back(
                            std::move(memory), again_q[j]->lease_timeout,
                            again_q[j]->object_checksum,
                            again_q[j]->resolved_tenant_id);
                    }
                    if (!again_read.empty() && !BackupStopRequested()) {
                        BackupStage("before_transfer", again_read.size());
                        auto again_results =
                            client_->BatchGet(again_read, again_queries, slices,
                                              /*group_by_target=*/false,
                                              /*use_hot_cache=*/false);
                        for (size_t j = 0; j < again_read.size(); ++j) {
                            results[again_idx[j]] = std::move(again_results[j]);
                        }
                        if (UnquiescedTransferFailureCount() !=
                            unquiesced_before) {
                            group_failed +=
                                read_keys.size() + (keys.size() - (next + pos));
                            unquiesced = true;
                            break;
                        }
                    }
                }
                if (backup_transfer_observer_for_test_) {
                    for (size_t i = 0; i < read_keys.size(); ++i) {
                        backup_transfer_observer_for_test_(
                            tenant_id, read_keys[i],
                            results[i] ? ErrorCode::OK : results[i].error(),
                            slices[read_keys[i]].front());
                    }
                }
                for (size_t i = 0; i < read_keys.size(); ++i) {
                    if (!results[i]) {
                        ++group_failed;
                        continue;
                    }
                    group_read.push_back(
                        {TenantId(tenant_id).MakeScopedKey(read_keys[i]),
                         std::move(slices[read_keys[i]]), read_tasks[i]});
                }
            }
            if (requery) continue;
            if (stopped || unquiesced) break;
            next = wend;
        }
        if (unquiesced) {
            // A transfer into this staging may still be running (partial
            // submit, status query failure or wait timeout): write none of
            // this group (the chunks read before share the staging), and
            // keep its memory out of reuse for a while so a late DMA cannot
            // land in the next batch's data.
            out.read_failed += group_failed + group_read.size();
            QuarantineBackupStaging(std::move(alloc.value()));
            LOG_EVERY_N(WARNING, 20)
                << "SSD backup read left transfers unquiesced; dropped "
                << keys.size()
                << " key(s), staging quarantined, batch=" << batch.batch_id;
            continue;
        }
        out.read_failed += group_failed;
        if (group_read.empty()) continue;  // the staging is freed here
        for (auto& r : group_read) {
            out.bytes += r.task->size;
            out.task_by_storage_key.emplace(r.storage_key, *r.task);
            out.batch_object.emplace(std::move(r.storage_key),
                                     std::move(r.slices));
        }
        HoldBackupStaging(out, std::move(alloc.value()));
    }
    return out;
}

bool FileStorage::ThrottleBackupWrite(int64_t bytes) {
    const int64_t rate = backup_config_.max_write_bytes_per_sec;
    if (rate <= 0 || bytes <= 0) return !BackupStopRequested();
    // Simple pacing: each write may not start before the previous writes'
    // bytes have "drained" at `rate`.
    using Clock = std::chrono::steady_clock;
    const auto now = Clock::now();
    if (backup_write_ready_ < now) backup_write_ready_ = now;
    const auto ready = backup_write_ready_;
    // A very low rate with a large batch can put the next deadline beyond
    // what the clock can represent: saturate instead of wrapping into the
    // past (which would silently stop the pacing).
    const auto debt = BackupPacingDebt(bytes, rate);
    backup_write_ready_ = debt >= Clock::time_point::max() - backup_write_ready_
                              ? Clock::time_point::max()
                              : backup_write_ready_ + debt;
    // Interruptible: stop or drain ends the wait (false: do not write).
    return WaitBackupUntil(ready);
}

std::chrono::steady_clock::duration FileStorage::BackupPacingDebt(
    int64_t bytes, int64_t rate) {
    using D = std::chrono::steady_clock::duration;
    // bytes / rate seconds in clock ticks; 128-bit so neither the
    // multiplication nor the unit conversion can overflow.
    const __int128 ticks = static_cast<__int128>(bytes) * D::period::den /
                           (static_cast<__int128>(rate) * D::period::num);
    return ticks >= static_cast<__int128>(D::max().count())
               ? D::max()
               : D(static_cast<D::rep>(ticks));
}

bool FileStorage::IsBackupAckOutcomeUnknown(ErrorCode e) {
    // Transport failures: the request may have reached the master and been
    // applied before the reply was lost.
    return e == ErrorCode::RPC_FAIL || e == ErrorCode::RPC_TIMEOUT;
}

tl::expected<std::vector<uint32_t>, ErrorCode> FileStorage::AckBackupBucket(
    const BackupAckFn& notify, int attempts, std::chrono::milliseconds backoff,
    bool* outcome_unknown) {
    *outcome_unknown = false;
    // NotifyBackupDone is idempotent: a key this client already backs up is
    // skipped, so resending after a lost reply is safe.
    auto res = notify();
    for (int i = 1;
         i < attempts && !res && IsBackupAckOutcomeUnknown(res.error()); ++i) {
        std::this_thread::sleep_for(backoff * i);
        res = notify();
    }
    if (!res && IsBackupAckOutcomeUnknown(res.error())) {
        *outcome_unknown = true;
        return std::vector<uint32_t>{};
    }
    return res;
}

void FileStorage::WriteBackupBatch(PreparedBackup& prepared) {
    const auto& batch = *prepared.batch;
    auto backend =
        std::dynamic_pointer_cast<BucketStorageBackend>(storage_backend_);
    size_t written = 0;
    bool notified = false;
    std::vector<std::string> stale_keys;
    if (!prepared.batch_object.empty()) {
        BackupFailpoint("read_done", batch);
    }
    // Stopping or draining (also while paced): write nothing; the empty ACK
    // below releases the batch's reservation and the staging is freed.
    if (!prepared.batch_object.empty() &&
        !ThrottleBackupWrite(prepared.bytes)) {
        VLOG(1) << "ssd_backup_batch_cancelled id=" << batch.batch_id
                << " keys=" << prepared.batch_object.size();
        prepared.batch_object.clear();
    }
    if (!prepared.batch_object.empty()) {
        auto complete =
            [&](const std::vector<std::string>& keys,
                std::vector<StorageObjectMetadata>& metas) -> ErrorCode {
            std::vector<BackupTaskItem> tasks;
            tasks.reserve(keys.size());
            for (auto& meta : metas) meta.transport_endpoint = local_rpc_addr_;
            for (const auto& key : keys) {
                tasks.push_back(prepared.task_by_storage_key.at(key));
            }
            BackupFailpoint("written",
                            batch);  // bucket on disk, not registered
            bool outcome_unknown = false;
            auto res = AckBackupBucket(
                [&] {
                    auto r =
                        client_->NotifyBackupDone(batch.batch_id, tasks, metas);
                    BackupAcked(batch.batch_id, tasks.size(), r.has_value());
                    return r;
                },
                kBackupAckAttempts, kBackupAckRetryBackoff, &outcome_unknown);
            if (!res) return res.error();  // refused: roll the bucket back
            if (outcome_unknown) {
                // The master may have registered these backups: keep the
                // bytes. If it did not, the copy is never advertised and the
                // scanner demotes it once its keys go cold.
                LOG(WARNING)
                    << "ssd_backup_ack_unknown batch=" << batch.batch_id
                    << " keys=" << keys.size() << " kept";
                return ErrorCode::OK;
            }
            for (uint32_t idx : res.value()) {
                if (idx < keys.size()) stale_keys.push_back(keys[idx]);
            }
            written = keys.size() - stale_keys.size();
            notified = true;
            return ErrorCode::OK;
        };
        // Keys refused as stale (removed or re-put since the task was formed)
        // are undone inside the backend's registration transaction: this
        // copy is unmapped and any backup mapping it displaced (possibly a
        // newer version's, still advertised by the master) is put back.
        auto offload_res = backend->BatchOffloadBackup(
            prepared.batch_object, complete,
            [this](const std::vector<std::string>& evicted_keys) {
                return NotifyEvictedDiskReplicas(evicted_keys);
            },
            &stale_keys);
        if (!offload_res) {
            LOG_EVERY_N(WARNING, 20)
                << "SSD backup write failed: " << offload_res.error()
                << ", batch=" << batch.batch_id;
        }
        if (!stale_keys.empty()) {
            VLOG(1) << "ssd_backup_stale batch=" << batch.batch_id
                    << " refused=" << stale_keys.size();
        }
    }
    if (!notified) {
        // Nothing registered: still release the batch's reservation.
        auto r = client_->NotifyBackupDone(batch.batch_id, {}, {});
        BackupAcked(batch.batch_id, 0, r.has_value());
    }
    // Staging buffers go back before the next read needs them.
    prepared.batch_object.clear();
    prepared.staging.clear();
    VLOG(1) << "ssd_backup_batch id=" << batch.batch_id
            << " tasks=" << batch.tasks.size() << " written=" << written
            << " read_failed=" << prepared.read_failed << " elapsed_ms="
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - prepared.start)
                   .count();
}

void FileStorage::RunBackupBatches(const std::vector<BackupBatch>& batches) {
    if (batches.empty()) return;
    // Two-stage pipeline over the 2-slot staging buffer: read batch i+1
    // (RDMA from its source) while batch i is written to disk.
    // Stop or drain: a read already started is joined (its staging is only
    // released by WriteBackupBatch, which then writes nothing); batches never
    // read are acknowledged empty so the master releases their reservation.
    if (BackupStopRequested()) {  // not even the first read is admitted
        ReleaseUnreadBackupBatches(batches, 0);
        return;
    }
    size_t started = 1;
    auto current = ReadBackupBatch(batches[0]);
    for (size_t i = 0; i < batches.size(); ++i) {
        std::future<PreparedBackup> next;
        if (i + 1 < batches.size() && !BackupStopRequested()) {
            next = std::async(std::launch::async, [this, &batches, i] {
                return ReadBackupBatch(batches[i + 1]);
            });
            started = i + 2;
        }
        WriteBackupBatch(current);
        if (!next.valid()) break;
        current = next.get();
    }
    ReleaseUnreadBackupBatches(batches, started);
}

}  // namespace mooncake
