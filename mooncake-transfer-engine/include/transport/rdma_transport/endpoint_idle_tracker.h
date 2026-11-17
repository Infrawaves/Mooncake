// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef ENDPOINT_IDLE_TRACKER_H
#define ENDPOINT_IDLE_TRACKER_H

#include <cstdint>

namespace mooncake {

// Decides whether an endpoint has been idle, from successive samples of its
// QPs' hardware PSNs. On verified providers, a responder's rq_psn advances
// even for one-sided READ/WRITE that post nothing locally. PSNs can wrap and
// sums can alias, so an unchanged sample is only a candidate for a peer probe,
// never proof that a peer is dead. Live PSN reporting must be verified on the
// deployed NIC/firmware; it is not a generic ibv_query_qp guarantee.
// Not thread-safe: one sampler thread owns it.
class EndpointIdleTracker {
   public:
    // Feed one sample; now_ns must come from a monotonic clock. Returns true
    // when the sum has not changed for at least idle_ns since it was first
    // seen with this value. A clock that went backwards restarts the window.
    bool sample(uint64_t psn_sum, uint64_t now_ns, uint64_t idle_ns) {
        changed_ = has_sample_ && psn_sum != psn_sum_;
        last_sample_ns_ = now_ns;
        if (!has_sample_ || psn_sum != psn_sum_ || now_ns < since_ns_) {
            has_sample_ = true;
            psn_sum_ = psn_sum;
            since_ns_ = now_ns;
            return false;
        }
        return now_ns - since_ns_ >= idle_ns;
    }

    // Drop the window, e.g. while the endpoint is not connected or has
    // outstanding work requests.
    void reset() { has_sample_ = false; }

    // Start a new idle window at the latest sample but keep the PSN baseline,
    // so traffic before the next sample is still seen as a change.
    void restartWindow() {
        if (has_sample_) since_ns_ = last_sample_ns_;
    }

    // Whether psn_sum equals the current baseline (no traffic since it).
    bool matchesBaseline(uint64_t psn_sum) const {
        return has_sample_ && psn_sum == psn_sum_;
    }

    // True when the most recent sample() saw a PSN sum different from the
    // previous one, i.e. the QPs carried traffic in between.
    bool lastSampleChanged() const { return changed_; }

   private:
    bool has_sample_ = false;
    bool changed_ = false;
    uint64_t psn_sum_ = 0;
    uint64_t since_ns_ = 0;
    uint64_t last_sample_ns_ = 0;
};

}  // namespace mooncake

#endif  // ENDPOINT_IDLE_TRACKER_H
