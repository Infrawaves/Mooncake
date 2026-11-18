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

#ifndef IDLE_PEER_PROBE_H
#define IDLE_PEER_PROBE_H

#include <cstdint>
#include <string>

namespace mooncake {

enum class IdlePeerProbeResult { kReachable, kRefused, kUnknown };

// Probe the actual handshake address from RPC metadata, outside endpoint/store
// locks. Only an explicit TCP refusal is reported as kRefused; a timeout or
// routing failure cannot distinguish a dead process from a network fault and
// is kUnknown (the caller retires only after repeated kUnknown results).
// Numeric IPv4/IPv6 (including a scope ID) only: hostnames return kUnknown so
// DNS resolution cannot stall the reclaim thread. The wait is capped at 200 ms.
IdlePeerProbeResult probeIdlePeer(const std::string &numeric_host,
                                  uint16_t port, int timeout_ms = 100);

}  // namespace mooncake

#endif  // IDLE_PEER_PROBE_H
