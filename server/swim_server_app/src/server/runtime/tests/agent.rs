// Copyright 2015-2021 Swim Inc.
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

use futures::{future::BoxFuture, FutureExt, SinkExt, StreamExt};
use swim_api::{
    agent::{Agent, AgentConfig, AgentContext, AgentInitResult, UplinkKind},
    error::AgentTaskError,
    protocol::{
        agent::{LaneRequest, LaneRequestDecoder, LaneResponse, ValueLaneResponseEncoder},
        WithLenRecognizerDecoder,
    },
};
use swim_form::{structural::read::recognizer::RecognizerReadable, Form};
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    routing::uri::RelativeUri,
};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};

pub const LANE: &str = "lane";

#[derive(Form, Debug)]
#[form_root(::swim_form)]
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
    check_meta: fn(RelativeUri, AgentConfig),
}

impl TestAgent {
    pub fn new(
        reporter: mpsc::UnboundedSender<i32>, //Reports each time the state changes.
        events: mpsc::UnboundedSender<AgentEvent>, //Reports when the agent is started or stopped.
        check_meta: fn(RelativeUri, AgentConfig), //Check to perform on the agent metadata on startup.
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
        route: RelativeUri,
        config: AgentConfig,
        context: Box<dyn AgentContext + Send>,
    ) -> BoxFuture<'static, AgentInitResult> {
        (self.check_meta)(route, config);
        let events = self.events.clone();
        let reporter = self.reporter.clone();
        async move {
            let (tx, rx) = context.add_lane(LANE, UplinkKind::Value, None).await?;
            Ok(run_agent(tx, rx, events, reporter).boxed())
        }
        .boxed()
    }
}

async fn run_agent(
    tx: ByteWriter,
    rx: ByteReader,
    events: mpsc::UnboundedSender<AgentEvent>,
    reporter: mpsc::UnboundedSender<i32>,
) -> Result<(), AgentTaskError> {
    events.send(AgentEvent::Started).expect("Channel stopped.");
    let decoder =
        LaneRequestDecoder::new(WithLenRecognizerDecoder::new(TestMessage::make_recognizer()));
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
    events.send(AgentEvent::Stopped).expect("Channel stopped.");
    Ok(())
}
