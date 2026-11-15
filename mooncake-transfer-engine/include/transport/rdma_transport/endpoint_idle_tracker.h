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
// QPs' hardware PSNs. A responder QP's rq_psn advances on every request packet
// the peer sends, including one-sided READ/WRITE that post nothing locally,
// so an unchanged sum over a whole window means no traffic in that window.
// Not thread-safe: one sampler thread owns it.
class EndpointIdleTracker {
   public:
    // Feed one sample. Returns true when the sum has not changed for at
    // least idle_ns since it was first seen with this value.
    bool sample(uint64_t psn_sum, uint64_t now_ns, uint64_t idle_ns) {
        if (!has_sample_ || psn_sum != psn_sum_) {
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

   private:
    bool has_sample_ = false;
    uint64_t psn_sum_ = 0;
    uint64_t since_ns_ = 0;
};

}  // namespace mooncake

#endif  // ENDPOINT_IDLE_TRACKER_H
