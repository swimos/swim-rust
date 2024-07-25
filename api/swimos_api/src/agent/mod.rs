// Copyright 2015-2024 Swim Inc.
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

//! # SwimOS Agent API
//!
//! Implement the [`Agent`] trait to provide a new kind of agent that can be executed by the SwimOS runtime.
//! The canonical Rust implementation of this trait can be found in the `swimos_agent` crate.

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{future::BoxFuture, ready, Future, FutureExt};
use swimos_utilities::{
    byte_channel::{ByteReader, ByteWriter},
    future::RetryStrategy,
    non_zero_usize,
    routing::RouteUri,
};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::error::{
    AgentInitError, AgentRuntimeError, AgentTaskError, DownlinkRuntimeError, OpenStoreError,
};
use crate::http::{HttpRequest, HttpResponse};

mod downlink;
mod lane;
mod store;

pub use downlink::DownlinkKind;
pub use lane::{LaneKind, LaneKindParseErr, LaneKindRecognizer, WarpLaneKind};
pub use store::StoreKind;

/// Indicates the sub-protocol that a lane uses to communicate its state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UplinkKind {
    Value,
    Map,
    Supply,
}

impl Display for UplinkKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UplinkKind::Value => f.write_str("Value"),
            UplinkKind::Map => f.write_str("Map"),
            UplinkKind::Supply => f.write_str("Supply"),
        }
    }
}

/// Configuration parameters for a lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneConfig {
    /// Size of the input buffer in bytes.
    pub input_buffer_size: NonZeroUsize,
    /// Size of the output buffer in bytes.
    pub output_buffer_size: NonZeroUsize,
    /// A transient lane does not have associated persistent storage.
    pub transient: bool,
}

/// Configuration parameters for a store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreConfig {
    /// Size of buffer to report new values back to the runtime.
    pub buffer_size: NonZeroUsize,
}

const DEFAULT_BUFFER: NonZeroUsize = non_zero_usize!(4096);

impl LaneConfig {
    //TODO: Remove this once const impls are stable.
    const DEFAULT: LaneConfig = LaneConfig {
        input_buffer_size: DEFAULT_BUFFER,
        output_buffer_size: DEFAULT_BUFFER,
        transient: false,
    };
}

impl Default for LaneConfig {
    fn default() -> Self {
        LaneConfig::DEFAULT
    }
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_BUFFER,
        }
    }
}

/// An HTTP response with an uninterpreted body.
pub type RawHttpLaneResponse = HttpResponse<Bytes>;

/// Send half of a single use channel for providing an HTTP response.
#[derive(Debug)]
pub struct HttpResponseSender(oneshot::Sender<RawHttpLaneResponse>);

impl HttpResponseSender {
    pub fn send(self, response: RawHttpLaneResponse) -> Result<(), RawHttpLaneResponse> {
        self.0.send(response)
    }
}

impl Future for HttpResponseReceiver {
    type Output = Result<RawHttpLaneResponse, ()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(ready!(self.as_mut().0.poll_unpin(cx)).map_err(|_| ()))
    }
}

/// Create a channel send back the response to an asynchronously executed HTTP request.
pub fn response_channel() -> (HttpResponseSender, HttpResponseReceiver) {
    let (tx, rx) = oneshot::channel();
    (HttpResponseSender(tx), HttpResponseReceiver(rx))
}

/// Receive half of a single use channel for providing an HTTP response.
#[derive(Debug)]
pub struct HttpResponseReceiver(oneshot::Receiver<RawHttpLaneResponse>);

/// Error type indicating that an agent that made an HTTP request dropped it before the response
/// was received.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq, Default)]
#[error("An HTTP request was dropped before a response was sent.")]
pub struct ReceiveResponseError;

impl HttpResponseReceiver {
    pub fn try_recv(&mut self) -> Result<RawHttpLaneResponse, ReceiveResponseError> {
        self.0.try_recv().map_err(|_| ReceiveResponseError)
    }
}

/// The type of messages sent from the Swim agent runtime to an agent implementation. It includes the
/// request that was received by the server and a single use channel for the agent implementation to
/// provide the response.
#[derive(Debug)]
pub struct HttpLaneRequest {
    pub request: HttpRequest<Bytes>,
    response_tx: HttpResponseSender,
}

impl HttpLaneRequest {
    /// Create a new instance from an HTTP request and provide the receiver that can be
    /// used to wait for the response.
    pub fn new(request: HttpRequest<Bytes>) -> (Self, HttpResponseReceiver) {
        let (tx, rx) = response_channel();
        (
            HttpLaneRequest {
                request,
                response_tx: tx,
            },
            rx,
        )
    }

    /// Split this message into the original request and the channel for sending the response.
    pub fn into_parts(self) -> (HttpRequest<Bytes>, HttpResponseSender) {
        let HttpLaneRequest {
            request,
            response_tx,
        } = self;
        (request, response_tx)
    }
}

/// A channel to make HTTP requests.
pub type HttpLaneRequestChannel = mpsc::Receiver<HttpLaneRequest>;

/// Trait for the context that is passed to an agent to allow it to interact with the runtime.
pub trait AgentContext: Sync {
    /// Open a channel for sending ad-hoc commands. Only one channel can be open at one time
    /// and requesting a second will result in the first being closed.
    fn ad_hoc_commands(&self) -> BoxFuture<'static, Result<ByteWriter, DownlinkRuntimeError>>;

    /// Add a new lane endpoint to the runtime for this agent.
    /// # Arguments
    /// * `name` - The name of the lane.
    /// * `land_kind` - Kind of the lane, determining the protocol that the runtime uses
    ///   to communicate with the lane.
    /// * `config` - Configuration parameters for the lane.
    fn add_lane(
        &self,
        name: &str,
        lane_kind: WarpLaneKind,
        config: LaneConfig,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), AgentRuntimeError>>;

    fn add_http_lane(
        &self,
        name: &str,
    ) -> BoxFuture<'static, Result<HttpLaneRequestChannel, AgentRuntimeError>>;

    /// Open a downlink to a lane on another agent.
    /// # Arguments
    /// * `config` - The configuration for the downlink.
    /// * `host` - The host containing the node.
    /// * `node` - The node URI for the agent.
    /// * `kind` - The kind of the downlink for the runtime.
    fn open_downlink(
        &self,
        host: Option<&str>,
        node: &str,
        lane: &str,
        kind: DownlinkKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), DownlinkRuntimeError>>;

    /// Add a new named store that will persist a (possibly compound) value in the agent state.
    /// # Arguments
    /// * `name` - The name of the store.
    /// * `kind` - The kind of the store.
    fn add_store(
        &self,
        name: &str,
        kind: StoreKind,
    ) -> BoxFuture<'static, Result<(ByteWriter, ByteReader), OpenStoreError>>;
}

#[derive(Debug, Clone, Copy)]
pub struct AgentConfig {
    pub default_lane_config: Option<LaneConfig>,
    pub keep_linked_retry: RetryStrategy,
}

impl AgentConfig {
    //TODO: Remove this once const impls are stable.
    pub const DEFAULT: AgentConfig = AgentConfig {
        default_lane_config: Some(LaneConfig::DEFAULT),
        keep_linked_retry: RetryStrategy::none(),
    };
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Type of the task for a running agent.
pub type AgentTask = BoxFuture<'static, Result<(), AgentTaskError>>;
/// Type of the task to initialize an agent.
pub type AgentInitResult = Result<AgentTask, AgentInitError>;

/// Trait to define a type of agent. Instances of this will be passed to the runtime
/// to be executed. User code should not generally need to implement this directly. It is
/// necessary for this trait to be object safe and any changes to it should take that into
/// account.
pub trait Agent {
    /// Running an agent results in future that will perform the initialization of the agent and
    /// then yield another future that will actually run the agent.
    /// # Arguments
    /// * `route` - The node URI of this agent instance.
    /// * `route_params` - Parameters extracted from the route URI.
    /// * `config` - Configuration parameters for the agent.
    /// * `context` - Context through which the agent can interact with the runtime. If this is
    ///   dropped, then the agent will be terminated.
    fn run(
        &self,
        route: RouteUri,
        route_params: HashMap<String, String>,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult>;
}

static_assertions::assert_obj_safe!(AgentContext, Agent);

/// An [`Agent`] that can be run by dynamic dispatch.
pub type BoxAgent = Box<dyn Agent + Send + 'static>;

impl Agent for BoxAgent {
    fn run(
        &self,
        route: RouteUri,
        route_params: HashMap<String, String>,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        (**self).run(route, route_params, config, context)
    }
}

impl<'a, A> Agent for &'a A
where
    A: Agent,
{
    fn run(
        &self,
        route: RouteUri,
        route_params: HashMap<String, String>,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        (*self).run(route, route_params, config, context)
    }
}
