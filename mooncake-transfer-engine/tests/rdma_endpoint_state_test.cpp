// Copyright 2026 KVCache.AI
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

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "error.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_endpoint.h"
#include "transport/rdma_transport/rdma_transport.h"

#if defined(__has_feature)
#define MC_HAS_FEATURE(x) __has_feature(x)
#else
#define MC_HAS_FEATURE(x) 0
#endif
#if defined(__SANITIZE_ADDRESS__) || MC_HAS_FEATURE(address_sanitizer)
#include <sanitizer/lsan_interface.h>
#define MC_LSAN_IGNORE_OBJECT(p) __lsan_ignore_object(p)
#else
#define MC_LSAN_IGNORE_OBJECT(p) ((void)(p))
#endif

using namespace mooncake;

namespace mooncake {

class RdmaEndPointTestPeer {
   public:
    static void setStatus(RdmaEndPoint &endpoint, RdmaEndPoint::Status status) {
        endpoint.status_.store(status, std::memory_order_relaxed);
    }

    static void setReadyWaitStartTs(RdmaEndPoint &endpoint, uint64_t start_ts) {
        endpoint.ready_wait_start_ts_.store(start_ts,
                                            std::memory_order_relaxed);
    }

    static void setPeerQpNums(RdmaEndPoint &endpoint,
                              std::vector<uint32_t> peer_qp_nums) {
        endpoint.peer_qp_num_list_ = std::move(peer_qp_nums);
    }

    // Fake PSN reader: every QP reports *psn, or fails with EIO.
    static void setFakePsn(RdmaEndPoint &endpoint, uint64_t psn,
                           bool fail = false) {
        endpoint.psn_reader_for_test_ = [psn, fail](size_t, uint64_t *out) {
            if (fail) return EIO;
            *out = psn;
            return 0;
        };
    }

    static void ensureFakeQp(RdmaEndPoint &endpoint) {
        if (endpoint.qp_list_.empty()) {
            endpoint.qp_list_.push_back(nullptr);
            endpoint.wr_depth_list_ = new std::atomic<int>[1];
            endpoint.wr_depth_list_[0].store(0, std::memory_order_relaxed);
        }
    }

    // Drive the real sampler until the endpoint becomes an idle candidate:
    // an unchanged PSN over one idle window (kIdleNs) of a monotonic clock.
    static void makeIdleCandidate(RdmaEndPoint &endpoint) {
        setStatus(endpoint, RdmaEndPoint::CONNECTED);
        ensureFakeQp(endpoint);
        if (!endpoint.psn_reader_for_test_) setFakePsn(endpoint, 100);
        for (int i = 0; i < 3 && !endpoint.idle_candidate_; ++i)
            endpoint.sampleIdle(clock_ns_ += kIdleNs, kIdleNs);
        ASSERT_TRUE(endpoint.idle_candidate_);
    }

    static bool sample(RdmaEndPoint &endpoint) {
        return endpoint.sampleIdle(clock_ns_ += kIdleNs, kIdleNs);
    }

    static constexpr uint64_t kIdleNs = 10;
    static inline uint64_t clock_ns_ = 0;

    static uint32_t inconclusiveProbes(RdmaEndPoint &endpoint) {
        return endpoint.inconclusive_probes_;
    }

    static void completeActivityAfterSample(RdmaEndPoint &endpoint) {
        // A short WR can be posted AND completed during the TCP probe. The
        // final WR depth is zero, but the generation still records activity.
        ++endpoint.activity_generation_;
    }

    static void setFakeOutstandingWr(RdmaEndPoint &endpoint, int depth) {
        ensureFakeQp(endpoint);
        endpoint.wr_depth_list_[0].store(depth, std::memory_order_relaxed);
    }
};

}  // namespace mooncake

namespace {

class RdmaEndPointStateTest : public ::testing::Test {
   protected:
    void SetUp() override {
        transport_ = new RdmaTransport();
        // Intentional leak: ~RdmaTransport dereferences metadata_, which is
        // null until install(). We only need it as RdmaContext's owner.
        MC_LSAN_IGNORE_OBJECT(transport_);
        context_ = std::make_unique<RdmaContext>(*transport_, "unused");
        endpoint_ = std::make_unique<RdmaEndPoint>(*context_);
    }

    RdmaTransport *transport_ = nullptr;
    std::unique_ptr<RdmaContext> context_;
    std::unique_ptr<RdmaEndPoint> endpoint_;
};

TEST_F(RdmaEndPointStateTest, WaitingReadyAckIsConnectedButNotReadyToSend) {
    RdmaEndPointTestPeer::setStatus(*endpoint_,
                                    RdmaEndPoint::CONNECTED_WAIT_READY_ACK);

    EXPECT_TRUE(endpoint_->connected());
    EXPECT_FALSE(endpoint_->readyToSend());
}

TEST_F(RdmaEndPointStateTest, ConnectedIsReadyToSend) {
    RdmaEndPointTestPeer::setStatus(*endpoint_, RdmaEndPoint::CONNECTED);

    EXPECT_TRUE(endpoint_->connected());
    EXPECT_TRUE(endpoint_->readyToSend());
}

TEST_F(RdmaEndPointStateTest, IdleRetirementRejectsUnsampledEndpoint) {
    RdmaEndPointTestPeer::setStatus(*endpoint_, RdmaEndPoint::CONNECTED);
    EXPECT_FALSE(endpoint_->beginDestroyIfIdle());
    EXPECT_TRUE(endpoint_->readyToSend());
}

TEST_F(RdmaEndPointStateTest, IdleRetirementRejectsWorkCompletedAfterSample) {
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    RdmaEndPointTestPeer::completeActivityAfterSample(*endpoint_);
    EXPECT_FALSE(endpoint_->beginDestroyIfIdle());
    EXPECT_TRUE(endpoint_->readyToSend());
}

TEST_F(RdmaEndPointStateTest, IdleRetirementRechecksOutstandingWork) {
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    RdmaEndPointTestPeer::setFakeOutstandingWr(*endpoint_, 1);
    EXPECT_FALSE(endpoint_->beginDestroyIfIdle());
    EXPECT_TRUE(endpoint_->readyToSend());
    RdmaEndPointTestPeer::setFakeOutstandingWr(*endpoint_, 0);
    // Draining a WR must not revive the rejected candidate without a new
    // sampling window and peer probe.
    EXPECT_FALSE(endpoint_->beginDestroyIfIdle());
}

TEST_F(RdmaEndPointStateTest, IdleRetirementRechecksConnectionState) {
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    RdmaEndPointTestPeer::setStatus(*endpoint_, RdmaEndPoint::CONNECTING);
    EXPECT_FALSE(endpoint_->beginDestroyIfIdle());
    EXPECT_FALSE(endpoint_->retired());
}

TEST_F(RdmaEndPointStateTest, IdleRetirementStopsFurtherPosting) {
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    EXPECT_TRUE(endpoint_->beginDestroyIfIdle());
    EXPECT_TRUE(endpoint_->retired());
    Transport::Slice slice{};
    std::vector<Transport::Slice *> pending{&slice};
    std::vector<Transport::Slice *> failed;
    EXPECT_EQ(endpoint_->submitPostSend(pending, failed), 0);
    EXPECT_TRUE(pending.empty());
    ASSERT_EQ(failed.size(), 1u);
    EXPECT_EQ(failed.front(), &slice);
}

TEST_F(RdmaEndPointStateTest, InconclusiveProbesRetireOnlyAfterLimit) {
    for (uint32_t i = 1; i < 3; ++i) {
        RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
        EXPECT_FALSE(endpoint_->recordInconclusiveProbe(3));
        EXPECT_EQ(RdmaEndPointTestPeer::inconclusiveProbes(*endpoint_), i);
        EXPECT_FALSE(endpoint_->retired());
        EXPECT_TRUE(endpoint_->readyToSend());
    }
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    EXPECT_TRUE(endpoint_->recordInconclusiveProbe(3));
    EXPECT_TRUE(endpoint_->beginDestroyIfIdle());
    EXPECT_TRUE(endpoint_->retired());
}

TEST_F(RdmaEndPointStateTest, DroppedInconclusiveCandidateCannotBeRetired) {
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    EXPECT_FALSE(endpoint_->recordInconclusiveProbe(3));
    // Below the limit the candidate is dropped until a new idle window.
    EXPECT_FALSE(endpoint_->beginDestroyIfIdle());
    EXPECT_FALSE(endpoint_->retired());
}

TEST_F(RdmaEndPointStateTest, ReachableProbeClearsInconclusiveCount) {
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    EXPECT_FALSE(endpoint_->recordInconclusiveProbe(3));
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    EXPECT_FALSE(endpoint_->recordInconclusiveProbe(3));
    endpoint_->resetIdleTimer();
    EXPECT_EQ(RdmaEndPointTestPeer::inconclusiveProbes(*endpoint_), 0u);
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    EXPECT_FALSE(endpoint_->recordInconclusiveProbe(3));
    EXPECT_FALSE(endpoint_->retired());
}

TEST_F(RdmaEndPointStateTest, ActivityCancelsInconclusiveRetirement) {
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    EXPECT_FALSE(endpoint_->recordInconclusiveProbe(2));
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    EXPECT_TRUE(endpoint_->recordInconclusiveProbe(2));
    // Work posted during the probe cancels retirement. (The count itself is
    // cleared by the next sampleIdle(), which sees the generation change.)
    RdmaEndPointTestPeer::completeActivityAfterSample(*endpoint_);
    EXPECT_FALSE(endpoint_->beginDestroyIfIdle());
    EXPECT_FALSE(endpoint_->retired());
    EXPECT_TRUE(endpoint_->readyToSend());
}

TEST_F(RdmaEndPointStateTest, PassiveTrafficAfterInconclusiveProbeClearsCount) {
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    EXPECT_FALSE(endpoint_->recordInconclusiveProbe(6));
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    EXPECT_FALSE(endpoint_->recordInconclusiveProbe(6));
    EXPECT_EQ(RdmaEndPointTestPeer::inconclusiveProbes(*endpoint_), 2u);
    // The peer issues a one-sided READ: nothing is posted locally, only the
    // PSN moves. The next real sample must see it and clear the count.
    RdmaEndPointTestPeer::setFakePsn(*endpoint_, 101);
    EXPECT_FALSE(RdmaEndPointTestPeer::sample(*endpoint_));
    EXPECT_EQ(RdmaEndPointTestPeer::inconclusiveProbes(*endpoint_), 0u);
}

TEST_F(RdmaEndPointStateTest, InconclusiveRetirementNeedsFullWindowEachTime) {
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    EXPECT_FALSE(endpoint_->recordInconclusiveProbe(6));
    // Window restarted at the last sample: the very next sample (one window
    // later) is a candidate again, without a separate baseline sample.
    EXPECT_TRUE(RdmaEndPointTestPeer::sample(*endpoint_));
}

TEST_F(RdmaEndPointStateTest, PsnQueryFailureClearsInconclusiveCount) {
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    EXPECT_FALSE(endpoint_->recordInconclusiveProbe(6));
    RdmaEndPointTestPeer::setFakePsn(*endpoint_, 100, /*fail=*/true);
    EXPECT_FALSE(RdmaEndPointTestPeer::sample(*endpoint_));
    EXPECT_EQ(RdmaEndPointTestPeer::inconclusiveProbes(*endpoint_), 0u);
}

TEST_F(RdmaEndPointStateTest, PassiveTrafficDuringProbeCancelsRetirement) {
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    EXPECT_TRUE(endpoint_->recordInconclusiveProbe(1));
    // The peer resumed one-sided traffic while the TCP probe was pending.
    RdmaEndPointTestPeer::setFakePsn(*endpoint_, 105);
    EXPECT_FALSE(endpoint_->beginDestroyIfIdle());
    EXPECT_FALSE(endpoint_->retired());
    EXPECT_TRUE(endpoint_->readyToSend());
    EXPECT_EQ(RdmaEndPointTestPeer::inconclusiveProbes(*endpoint_), 0u);
}

TEST_F(RdmaEndPointStateTest, PsnQueryFailureAtRetirementCancelsIt) {
    RdmaEndPointTestPeer::makeIdleCandidate(*endpoint_);
    RdmaEndPointTestPeer::setFakePsn(*endpoint_, 100, /*fail=*/true);
    EXPECT_FALSE(endpoint_->beginDestroyIfIdle());
    EXPECT_FALSE(endpoint_->retired());
}

TEST_F(RdmaEndPointStateTest, InconclusiveProbeWithoutCandidateIsIgnored) {
    RdmaEndPointTestPeer::setStatus(*endpoint_, RdmaEndPoint::CONNECTED);
    EXPECT_FALSE(endpoint_->recordInconclusiveProbe(1));
    EXPECT_EQ(RdmaEndPointTestPeer::inconclusiveProbes(*endpoint_), 0u);
}

TEST_F(RdmaEndPointStateTest, ReadyAckTimeoutOnlyAppliesToWaitingState) {
    RdmaEndPointTestPeer::setReadyWaitStartTs(*endpoint_, 1);

    RdmaEndPointTestPeer::setStatus(*endpoint_,
                                    RdmaEndPoint::CONNECTED_WAIT_READY_ACK);
    EXPECT_TRUE(endpoint_->readyAckTimedOut());

    RdmaEndPointTestPeer::setStatus(*endpoint_, RdmaEndPoint::CONNECTED);
    EXPECT_FALSE(endpoint_->readyAckTimedOut());
}

TEST_F(RdmaEndPointStateTest, ReadyAckWithSamePeerQpMarksEndpointReady) {
    endpoint_->setPeerNicPath("peer@nic");
    RdmaEndPointTestPeer::setPeerQpNums(*endpoint_, {11, 22});
    RdmaEndPointTestPeer::setReadyWaitStartTs(*endpoint_, 1);
    RdmaEndPointTestPeer::setStatus(*endpoint_,
                                    RdmaEndPoint::CONNECTED_WAIT_READY_ACK);

    RdmaEndPoint::HandShakeDesc peer_desc;
    peer_desc.ready_ack = true;
    peer_desc.ready_ack_supported = true;
    peer_desc.qp_num = {11, 22};
    RdmaEndPoint::HandShakeDesc local_desc;

    EXPECT_EQ(0, endpoint_->setupConnectionsByPassive(peer_desc, local_desc));
    EXPECT_TRUE(local_desc.reply_msg.empty());
    EXPECT_TRUE(endpoint_->connected());
    EXPECT_TRUE(endpoint_->readyToSend());
    EXPECT_FALSE(endpoint_->readyAckTimedOut());
}

TEST_F(RdmaEndPointStateTest, StaleReadyAckWithDifferentPeerQpDoesNotReset) {
    endpoint_->setPeerNicPath("peer@nic");
    RdmaEndPointTestPeer::setPeerQpNums(*endpoint_, {11, 22});
    RdmaEndPointTestPeer::setReadyWaitStartTs(*endpoint_, 1);
    RdmaEndPointTestPeer::setStatus(*endpoint_,
                                    RdmaEndPoint::CONNECTED_WAIT_READY_ACK);

    RdmaEndPoint::HandShakeDesc peer_desc;
    peer_desc.ready_ack = true;
    peer_desc.ready_ack_supported = true;
    peer_desc.qp_num = {33, 44};
    RdmaEndPoint::HandShakeDesc local_desc;

    EXPECT_EQ(ERR_REJECT_HANDSHAKE,
              endpoint_->setupConnectionsByPassive(peer_desc, local_desc));
    EXPECT_FALSE(local_desc.reply_msg.empty());
    EXPECT_TRUE(endpoint_->connected());
    EXPECT_FALSE(endpoint_->readyToSend());
    EXPECT_TRUE(endpoint_->readyAckTimedOut());
}

}  // namespace
