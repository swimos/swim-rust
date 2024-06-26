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

use std::{collections::HashMap, pin::pin};

use bytes::BytesMut;
use futures::{future::BoxFuture, stream::unfold, FutureExt, SinkExt, Stream, StreamExt};
use swimos_agent_protocol::{
    encoding::lane::{
        RawValueLaneRequestDecoder, RawValueLaneResponseEncoder, ValueLaneRequestDecoder,
        ValueLaneResponseEncoder,
    },
    LaneRequest, LaneResponse,
};
use swimos_api::{
    agent::{Agent, AgentConfig, AgentContext, AgentInitResult, WarpLaneKind},
    error::AgentTaskError,
};
use swimos_form::Form;
use swimos_utilities::{
    byte_channel::{ByteReader, ByteWriter},
    routing::RouteUri,
};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};

pub const LANE: &str = "lane";

#[derive(Form, Debug)]

pub enum TestMessage {
    SetAndReport(i32),
    Event,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentEvent {
    Started,
    Stopped,
}

/// A fake agent that exposes a single lane (with a value type uplink). The lane consumes
/// commands of type [`TestMessage`] and emits events of type [`i32`].
#[derive(Clone)]
pub struct TestAgent {
    reporter: mpsc::UnboundedSender<i32>,
    events: mpsc::UnboundedSender<AgentEvent>,
    check_meta: fn(RouteUri, HashMap<String, String>, AgentConfig),
}

impl TestAgent {
    pub fn new(
        reporter: mpsc::UnboundedSender<i32>, //Reports each time the state changes.
        events: mpsc::UnboundedSender<AgentEvent>, //Reports when the agent is started or stopped.
        check_meta: fn(RouteUri, HashMap<String, String>, AgentConfig), //Check to perform on the agent metadata on startup.
    ) -> Self {
        TestAgent {
            reporter,
            events,
            check_meta,
        }
    }
}

impl Agent for TestAgent {
    fn run(
        &self,
        route: RouteUri,
        route_params: HashMap<String, String>,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        (self.check_meta)(route, route_params, config);
        let events = self.events.clone();
        let reporter = self.reporter.clone();
        async move {
            let lane_conf = config.default_lane_config.unwrap_or_default();
            let (mut tx, mut rx) = context
                .add_lane(LANE, WarpLaneKind::Value, lane_conf)
                .await?;
            if !lane_conf.transient {
                run_lane_initializer(&mut tx, &mut rx).await;
            }

            Ok(run_agent(tx, rx, events, reporter, context).boxed())
        }
        .boxed()
    }
}

pub async fn run_lane_initializer(tx: &mut ByteWriter, rx: &mut ByteReader) {
    let mut stream = pin!(init_stream(rx));
    if stream.next().await.is_some() {
        panic!("Unexpected initial value.")
    } else {
        let mut writer = FramedWrite::new(tx, RawValueLaneResponseEncoder::default());
        writer
            .send(LaneResponse::<BytesMut>::Initialized)
            .await
            .expect("Failed to send initialized message.");
    }
}

fn init_stream(reader: &mut ByteReader) -> impl Stream<Item = BytesMut> + '_ {
    let framed = FramedRead::new(reader, RawValueLaneRequestDecoder::default());
    unfold(Some(framed), |maybe_framed| async move {
        if let Some(mut framed) = maybe_framed {
            match framed.next().await {
                Some(Ok(LaneRequest::Command(body))) => Some((body, Some(framed))),
                Some(Ok(LaneRequest::InitComplete)) => None,
                _ => panic!("Lane init failed."),
            }
        } else {
            None
        }
    })
}

async fn run_agent(
    tx: ByteWriter,
    rx: ByteReader,
    events: mpsc::UnboundedSender<AgentEvent>,
    reporter: mpsc::UnboundedSender<i32>,
    context: Box<dyn AgentContext + Send>,
) -> Result<(), AgentTaskError> {
    events.send(AgentEvent::Started).expect("Channel stopped.");
    let decoder = ValueLaneRequestDecoder::<TestMessage>::default();
    let encoder = ValueLaneResponseEncoder::default();

    let mut input = FramedRead::new(rx, decoder);
    let mut output = FramedWrite::new(tx, encoder);

    let mut state = 0;

    while let Some(result) = input.next().await {
        match result {
            Ok(LaneRequest::Sync(id)) => {
                output
                    .send(LaneResponse::sync_event(id, state))
                    .await
                    .expect("Channel stopped.");
                output
                    .send(LaneResponse::<i32>::synced(id))
                    .await
                    .expect("Channel stopped.");
            }
            Ok(LaneRequest::Command(TestMessage::SetAndReport(n))) => {
                state = n;
                reporter.send(n).expect("Reporter closed.");
            }
            Ok(LaneRequest::Command(TestMessage::Event)) => {
                output
                    .send(LaneResponse::event(state))
                    .await
                    .expect("Channel stopped.");
            }
            Ok(LaneRequest::InitComplete) => {}
            Err(e) => {
                panic!("Bad frame: {}", e);
            }
        }
    }
    drop(input);
    drop(output);
    drop(reporter);
    drop(context);
    events.send(AgentEvent::Stopped).expect("Channel stopped.");
    Ok(())
}
