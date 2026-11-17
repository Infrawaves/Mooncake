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

#include <cstdlib>

#include "config.h"
#include "transport/rdma_transport/endpoint_idle_tracker.h"

namespace mooncake {
namespace {

constexpr uint64_t kSec = 1000000000ull;

TEST(EndpointIdleTrackerTest, FirstSampleIsNeverIdle) {
    EndpointIdleTracker t;
    EXPECT_FALSE(t.sample(0, 100 * kSec, 10 * kSec));
}

TEST(EndpointIdleTrackerTest, IdleOnlyAfterFullWindowUnchanged) {
    EndpointIdleTracker t;
    EXPECT_FALSE(t.sample(42, 0, 10 * kSec));
    EXPECT_FALSE(t.sample(42, 9 * kSec, 10 * kSec));
    EXPECT_TRUE(t.sample(42, 10 * kSec, 10 * kSec));
}

TEST(EndpointIdleTrackerTest, TrafficRestartsWindow) {
    EndpointIdleTracker t;
    EXPECT_FALSE(t.sample(1, 0, 10 * kSec));
    EXPECT_FALSE(t.sample(5, 9 * kSec, 10 * kSec));  // peer sent packets
    EXPECT_FALSE(t.sample(5, 18 * kSec, 10 * kSec));
    EXPECT_TRUE(t.sample(5, 19 * kSec, 10 * kSec));
}

TEST(EndpointIdleTrackerTest, ResetDropsWindow) {
    EndpointIdleTracker t;
    EXPECT_FALSE(t.sample(7, 0, 10 * kSec));
    t.reset();  // e.g. outstanding WRs or not connected at this sample
    EXPECT_FALSE(t.sample(7, 20 * kSec, 10 * kSec));
    EXPECT_TRUE(t.sample(7, 30 * kSec, 10 * kSec));
}

// A re-handshake resets QP PSNs to 0; that is a change, not idleness.
TEST(EndpointIdleTrackerTest, PsnResetCountsAsChange) {
    EndpointIdleTracker t;
    EXPECT_FALSE(t.sample(6800, 0, 10 * kSec));
    EXPECT_FALSE(t.sample(0, 15 * kSec, 10 * kSec));
}

// A clock step back must not underflow into "idle for ages".
TEST(EndpointIdleTrackerTest, BackwardTimeRestartsWindow) {
    EndpointIdleTracker t;
    const uint64_t idle = 4 * 3600 * kSec;
    EXPECT_FALSE(t.sample(9, 3600 * kSec, idle));
    EXPECT_FALSE(t.sample(9, 1800 * kSec, idle));
    EXPECT_FALSE(t.sample(9, 1800 * kSec + idle - kSec, idle));
    EXPECT_TRUE(t.sample(9, 1800 * kSec + idle, idle));
}

class EndpointIdleEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_ENDPOINT_IDLE_RECLAIM"); }
};

TEST_F(EndpointIdleEnvTest, OffByDefaultWithFixedPeriods) {
    ::unsetenv("MC_ENDPOINT_IDLE_RECLAIM");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_FALSE(config.endpoint_idle_reclaim);
    EXPECT_EQ(config.endpoint_idle_reclaim_sec, 14400u);
    EXPECT_EQ(config.endpoint_idle_check_sec, 1800u);
}

TEST_F(EndpointIdleEnvTest, SwitchValues) {
    for (const char *on : {"1", "true", "TRUE"}) {
        ASSERT_EQ(::setenv("MC_ENDPOINT_IDLE_RECLAIM", on, 1), 0);
        GlobalConfig config;
        loadGlobalConfig(config);
        EXPECT_TRUE(config.endpoint_idle_reclaim) << on;
    }
    for (const char *off : {"0", "false"}) {
        ASSERT_EQ(::setenv("MC_ENDPOINT_IDLE_RECLAIM", off, 1), 0);
        GlobalConfig config;
        config.endpoint_idle_reclaim = true;
        loadGlobalConfig(config);
        EXPECT_FALSE(config.endpoint_idle_reclaim) << off;
    }
}

TEST_F(EndpointIdleEnvTest, InvalidValueKeepsDefault) {
    ASSERT_EQ(::setenv("MC_ENDPOINT_IDLE_RECLAIM", "yes-please", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_FALSE(config.endpoint_idle_reclaim);
}

TEST(EndpointIdleTrackerTest, ReportsWhetherLastSampleChanged) {
    EndpointIdleTracker tracker;
    EXPECT_FALSE(tracker.sample(10, 0, 100));
    EXPECT_FALSE(tracker.lastSampleChanged());  // first sample, no baseline
    EXPECT_FALSE(tracker.sample(10, 50, 100));
    EXPECT_FALSE(tracker.lastSampleChanged());
    EXPECT_FALSE(tracker.sample(11, 60, 100));
    EXPECT_TRUE(tracker.lastSampleChanged());
    EXPECT_FALSE(tracker.sample(11, 70, 100));
    EXPECT_FALSE(tracker.lastSampleChanged());
}

TEST(EndpointIdleTrackerTest, RestartWindowKeepsBaseline) {
    EndpointIdleTracker tracker;
    EXPECT_FALSE(tracker.sample(10, 0, 100));
    EXPECT_TRUE(tracker.sample(10, 100, 100));
    tracker.restartWindow();  // new window starts at t=100, baseline kept
    EXPECT_TRUE(tracker.matchesBaseline(10));
    EXPECT_FALSE(tracker.sample(10, 150, 100));
    EXPECT_TRUE(tracker.sample(10, 200, 100));
    tracker.restartWindow();
    // Traffic after the restart is still detected as a change.
    EXPECT_FALSE(tracker.sample(11, 300, 100));
    EXPECT_TRUE(tracker.lastSampleChanged());
    EXPECT_FALSE(tracker.matchesBaseline(10));
}

TEST(EndpointIdleTrackerTest, ResetDropsBaseline) {
    EndpointIdleTracker tracker;
    EXPECT_FALSE(tracker.sample(10, 0, 100));
    tracker.reset();
    EXPECT_FALSE(tracker.matchesBaseline(10));
    tracker.restartWindow();  // no-op without a baseline
    EXPECT_FALSE(tracker.matchesBaseline(10));
}

}  // namespace
}  // namespace mooncake
