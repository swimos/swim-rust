// Copyright 2015-2021 SWIM.AI inc.
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

use crate::agent::lane::channels::update::LaneUpdate;
use crate::agent::lane::channels::uplink::backpressure::{
    KeyedBackpressureConfig, SimpleBackpressureConfig,
};
use crate::agent::lane::channels::uplink::{UplinkAction, UplinkStateMachine};
use crate::meta::log::config::LogConfig;
use crate::meta::metric::config::MetricAggregatorConfig;
use crate::routing::RoutingAddr;
use std::num::NonZeroUsize;
use std::time::Duration;
use swim_utilities::future::retryable::RetryStrategy;

pub mod task;
pub mod update;
pub mod uplink;

/// Configuration parameters controlling how an agent and its lane are executed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExecutionConfig {
    /// Maximum number of pending envelopes in the agent dispatcher.
    pub max_pending_envelopes: usize,
    /// Maximum buffer size, per lane, for accepting uplink actions.
    pub action_buffer: NonZeroUsize,
    /// Maximum buffer size, per lane, for accepting commands.
    pub update_buffer: NonZeroUsize,
    /// Maximum buffer size, per lane, for action lane feedback.
    pub feedback_buffer: NonZeroUsize,
    /// Maximum buffer size, per lane, for reporting errors.
    pub uplink_err_buffer: NonZeroUsize,
    /// Maximum number of fatal uplink errors before the task running a lane will stop.
    pub max_fatal_uplink_errors: usize,
    /// Maximum number of times a lane will attempt to start a new uplink before failing.
    pub max_uplink_start_attempts: NonZeroUsize,
    /// Size of the buffer used by the agent envelope dispatcher to communicate with lanes.
    pub lane_buffer: NonZeroUsize,
    /// Size of the buffer
    pub observation_buffer: NonZeroUsize,
    /// Buffer size for the task that attaches lanes to the dispatcher.
    pub lane_attachment_buffer: NonZeroUsize,
    /// Number of values to process before yielding to the runtime.
    pub yield_after: NonZeroUsize,
    /// Retry strategy to use for transactions on lanes.
    pub retry_strategy: RetryStrategy,
    /// Time to wait for action lane responses when stopping.
    pub cleanup_timeout: Duration,
    /// The buffer size for the MPSC channel used by the agent to schedule events.
    pub scheduler_buffer: NonZeroUsize,
    /// Back-pressure relief configuration for value lane uplinks.
    pub value_lane_backpressure: Option<SimpleBackpressureConfig>,
    /// Back-pressure relief configuration for map lane uplinks.
    pub map_lane_backpressure: Option<KeyedBackpressureConfig>,
    /// Node logging configuration.
    pub node_log: LogConfig,
    /// Metric aggregator configuration
    pub metrics: MetricAggregatorConfig,
    /// The maximum idle time before the agent is terminated.
    pub max_idle_time: Duration,
    /// Maximum number of fatal store errors before the task running a lane will stop.
    pub max_store_errors: usize,
}

const DEFAULT_YIELD_COUNT: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(2048) };

impl AgentExecutionConfig {
    pub fn with(
        default_buffer: NonZeroUsize,
        max_pending_envelopes: usize,
        error_threshold: usize,
        cleanup_timeout: Duration,
        backpressure: Option<KeyedBackpressureConfig>,
        max_idle_time: Duration,
    ) -> Self {
        AgentExecutionConfig {
            max_pending_envelopes,
            action_buffer: default_buffer,
            update_buffer: default_buffer,
            feedback_buffer: default_buffer,
            uplink_err_buffer: default_buffer,
            max_fatal_uplink_errors: error_threshold,
            max_uplink_start_attempts: NonZeroUsize::new(error_threshold + 1).unwrap(),
            lane_buffer: default_buffer,
            observation_buffer: default_buffer,
            lane_attachment_buffer: default_buffer,
            yield_after: DEFAULT_YIELD_COUNT,
            retry_strategy: Default::default(),
            cleanup_timeout,
            scheduler_buffer: default_buffer,
            value_lane_backpressure: backpressure.map(|kc| SimpleBackpressureConfig {
                buffer_size: kc.buffer_size,
                yield_after: kc.buffer_size,
            }),
            map_lane_backpressure: backpressure,
            node_log: LogConfig::default(),
            max_idle_time,
            metrics: Default::default(),
            max_store_errors: error_threshold,
        }
    }
}

impl Default for AgentExecutionConfig {
    fn default() -> Self {
        let default_buffer = NonZeroUsize::new(4).unwrap();

        AgentExecutionConfig {
            max_pending_envelopes: 1,
            action_buffer: default_buffer,
            update_buffer: default_buffer,
            feedback_buffer: default_buffer,
            uplink_err_buffer: default_buffer,
            max_fatal_uplink_errors: 0,
            max_uplink_start_attempts: NonZeroUsize::new(1).unwrap(),
            lane_buffer: default_buffer,
            observation_buffer: default_buffer,
            lane_attachment_buffer: default_buffer,
            yield_after: DEFAULT_YIELD_COUNT,
            retry_strategy: RetryStrategy::default(),
            cleanup_timeout: Duration::from_secs(30),
            scheduler_buffer: default_buffer,
            value_lane_backpressure: None,
            map_lane_backpressure: None,
            node_log: LogConfig::default(),
            metrics: Default::default(),
            max_idle_time: Duration::from_secs(300),
            max_store_errors: 0,
        }
    }
}

/// Creates uplink state machines and update tasks for a lane.
pub trait LaneMessageHandler: Send + Sync {
    type Event: Send;
    type Uplink: UplinkStateMachine<Self::Event> + Send + Sync + 'static;
    type Update: LaneUpdate;

    fn make_uplink(&self, addr: RoutingAddr) -> Self::Uplink;

    fn make_update(&self) -> Self::Update;
}

pub type OutputMessage<Handler> = <<Handler as LaneMessageHandler>::Uplink as UplinkStateMachine<
    <Handler as LaneMessageHandler>::Event,
>>::Msg;

pub type InputMessage<Handler> = <<Handler as LaneMessageHandler>::Update as LaneUpdate>::Msg;

/// An [`UplinkAction`] tagged with the key of the endpoint into routing table from which it originated.
#[derive(Debug)]
pub struct TaggedAction(RoutingAddr, UplinkAction);
