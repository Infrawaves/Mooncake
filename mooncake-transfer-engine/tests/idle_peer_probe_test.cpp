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

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <string>
#include <utility>

#include "transport/rdma_transport/idle_peer_probe.h"

#ifdef MOONCAKE_IDLE_PROBE_TEST_WRAP_CONNECT
// Script failures without depending on public IP routes or firewall settings.
// The ordinary cases below still call the real kernel sockets through __real_.
namespace {
thread_local int forced_connect_error = 0;
thread_local int interrupted_polls = 0;
}  // namespace

extern "C" int __real_connect(int, const sockaddr *, socklen_t);
extern "C" int __real_poll(pollfd *, nfds_t, int);
extern "C" int __wrap_connect(int fd, const sockaddr *address, socklen_t size) {
    if (forced_connect_error) {
        errno = forced_connect_error;
        return -1;
    }
    return __real_connect(fd, address, size);
}
extern "C" int __wrap_poll(pollfd *fds, nfds_t count, int timeout_ms) {
    if (forced_connect_error == EINPROGRESS) {
        if (interrupted_polls-- > 0) {
            errno = EINTR;
            return -1;
        }
        return 0;
    }
    return __real_poll(fds, count, timeout_ms);
}
#endif

namespace mooncake {
namespace {

class LoopbackSocket {
   public:
    explicit LoopbackSocket(int family) {
        fd_ = socket(family, SOCK_STREAM, IPPROTO_TCP);
        if (fd_ < 0) return;
        sockaddr_storage storage{};
        socklen_t size;
        if (family == AF_INET) {
            auto *address = reinterpret_cast<sockaddr_in *>(&storage);
            address->sin_family = AF_INET;
            address->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            size = sizeof(*address);
        } else {
            auto *address = reinterpret_cast<sockaddr_in6 *>(&storage);
            address->sin6_family = AF_INET6;
            address->sin6_addr = in6addr_loopback;
            size = sizeof(*address);
        }
        if (bind(fd_, reinterpret_cast<sockaddr *>(&storage), size) != 0 ||
            getsockname(fd_, reinterpret_cast<sockaddr *>(&storage), &size) !=
                0) {
            return;
        }
        port_ =
            family == AF_INET
                ? ntohs(reinterpret_cast<sockaddr_in *>(&storage)->sin_port)
                : ntohs(reinterpret_cast<sockaddr_in6 *>(&storage)->sin6_port);
    }
    ~LoopbackSocket() {
        if (fd_ >= 0) ::close(fd_);
    }
    LoopbackSocket(const LoopbackSocket &) = delete;
    LoopbackSocket &operator=(const LoopbackSocket &) = delete;
    uint16_t port() const { return port_; }
    bool listen() const { return ::listen(fd_, 16) == 0; }
    void close() {
        if (fd_ >= 0) ::close(fd_);
        fd_ = -1;
    }

   private:
    int fd_ = -1;
    uint16_t port_ = 0;
};

TEST(IdlePeerProbeTest, ListeningPeerIsKeptClosedPortIsRefused) {
    for (const auto &[family, host] :
         {std::pair{AF_INET, "127.0.0.1"}, std::pair{AF_INET6, "::1"}}) {
        LoopbackSocket listening(family);
        if (listening.port() == 0 && family == AF_INET6) continue;  // no IPv6
        ASSERT_NE(listening.port(), 0) << host;
        ASSERT_TRUE(listening.listen());
        EXPECT_EQ(probeIdlePeer(host, listening.port()),
                  IdlePeerProbeResult::kReachable)
            << host;
        LoopbackSocket closed(family);
        closed.close();
        EXPECT_EQ(probeIdlePeer(host, closed.port()),
                  IdlePeerProbeResult::kRefused)
            << host;
    }
}

TEST(IdlePeerProbeTest, NamesAndInvalidArgumentsAreInconclusive) {
    // Never resolve names on this background path, even "localhost".
    const auto started = std::chrono::steady_clock::now();
    for (const std::string host :
         {"localhost", "nonexistent.invalid", "", "127.0.0.1:12345"}) {
        EXPECT_EQ(probeIdlePeer(host, 12345), IdlePeerProbeResult::kUnknown)
            << host;
    }
    EXPECT_LT(std::chrono::steady_clock::now() - started,
              std::chrono::seconds(1));
    EXPECT_EQ(probeIdlePeer("127.0.0.1", 0), IdlePeerProbeResult::kUnknown);
    EXPECT_EQ(probeIdlePeer("127.0.0.1", 12345, 0),
              IdlePeerProbeResult::kUnknown);
}

#ifdef MOONCAKE_IDLE_PROBE_TEST_WRAP_CONNECT
class IdlePeerProbeFailureTest : public ::testing::Test {
   protected:
    void TearDown() override {
        forced_connect_error = 0;
        interrupted_polls = 0;
    }
};

TEST_F(IdlePeerProbeFailureTest, InterruptedWaitThenTimeoutIsInconclusive) {
    forced_connect_error = EINPROGRESS;
    interrupted_polls = 3;
    EXPECT_EQ(probeIdlePeer("127.0.0.1", 12345), IdlePeerProbeResult::kUnknown);
    EXPECT_LT(interrupted_polls, 0);
}

TEST_F(IdlePeerProbeFailureTest, OnlyExplicitRefusalIsRefused) {
    for (const int error :
         {EHOSTUNREACH, ENETUNREACH, ETIMEDOUT, ECONNRESET, EACCES}) {
        forced_connect_error = error;
        EXPECT_EQ(probeIdlePeer("127.0.0.1", 12345),
                  IdlePeerProbeResult::kUnknown)
            << "errno=" << error;
    }
    forced_connect_error = ECONNREFUSED;
    EXPECT_EQ(probeIdlePeer("127.0.0.1", 12345), IdlePeerProbeResult::kRefused);
}
#endif

}  // namespace
}  // namespace mooncake
