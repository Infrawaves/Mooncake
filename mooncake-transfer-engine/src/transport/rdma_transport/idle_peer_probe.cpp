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

#include "transport/rdma_transport/idle_peer_probe.h"

#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <memory>

namespace mooncake {
namespace {

class ScopedSocket {
   public:
    explicit ScopedSocket(int fd) : fd_(fd) {}
    ~ScopedSocket() {
        if (fd_ >= 0) close(fd_);
    }
    ScopedSocket(const ScopedSocket &) = delete;
    ScopedSocket &operator=(const ScopedSocket &) = delete;
    int get() const { return fd_; }

   private:
    int fd_;
};

IdlePeerProbeResult classifyConnectError(int error) {
    if (error == 0) return IdlePeerProbeResult::kReachable;
    if (error == ECONNREFUSED) return IdlePeerProbeResult::kRefused;
    return IdlePeerProbeResult::kUnknown;
}

}  // namespace

IdlePeerProbeResult probeIdlePeer(const std::string &numeric_host,
                                  uint16_t port, int timeout_ms) {
    using Clock = std::chrono::steady_clock;
    if (numeric_host.empty() || numeric_host.find('\0') != std::string::npos ||
        port == 0 || timeout_ms <= 0) {
        return IdlePeerProbeResult::kUnknown;
    }
    const auto deadline =
        Clock::now() + std::chrono::milliseconds(std::min(timeout_ms, 200));
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;
    addrinfo *raw_address = nullptr;
    const auto service = std::to_string(port);
    if (getaddrinfo(numeric_host.c_str(), service.c_str(), &hints,
                    &raw_address) != 0) {
        return IdlePeerProbeResult::kUnknown;
    }
    std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> address(raw_address,
                                                               &freeaddrinfo);
    // A numeric host and fixed TCP socket type yield one concrete address.
    if (!address || address->ai_next) return IdlePeerProbeResult::kUnknown;

    ScopedSocket fd(socket(address->ai_family,
                           SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                           IPPROTO_TCP));
    if (fd.get() < 0) return IdlePeerProbeResult::kUnknown;
    if (connect(fd.get(), address->ai_addr, address->ai_addrlen) == 0) {
        return IdlePeerProbeResult::kReachable;
    }
    const int connect_error = errno;
    if (connect_error != EINPROGRESS) {
        return classifyConnectError(connect_error);
    }

    pollfd pfd{fd.get(), POLLOUT, 0};
    for (;;) {
        const auto now = Clock::now();
        if (now >= deadline) return IdlePeerProbeResult::kUnknown;
        const auto remaining_ms =
            std::chrono::ceil<std::chrono::milliseconds>(deadline - now)
                .count();
        const int rc = poll(&pfd, 1, static_cast<int>(remaining_ms));
        if (rc == 0) return IdlePeerProbeResult::kUnknown;
        if (rc < 0) {
            if (errno == EINTR) continue;
            return IdlePeerProbeResult::kUnknown;
        }
        if (!(pfd.revents & (POLLOUT | POLLERR | POLLHUP))) {
            return IdlePeerProbeResult::kUnknown;
        }
        int error = 0;
        socklen_t length = sizeof(error);
        if (getsockopt(fd.get(), SOL_SOCKET, SO_ERROR, &error, &length) != 0) {
            return IdlePeerProbeResult::kUnknown;
        }
        return classifyConnectError(error);
    }
}

}  // namespace mooncake
