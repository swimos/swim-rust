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
    protocol::agent::{
        LaneRequest, LaneRequestDecoder, ValueLaneResponse, ValueLaneResponseEncoder,
    },
};
use swim_form::{structural::read::recognizer::RecognizerReadable, Form};
use swim_recon::parser::RecognizerDecoder;
use swim_utilities::{
    io::byte_channel::{ByteReader, ByteWriter},
    routing::uri::RelativeUri,
};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};

const LANE: &str = "lane";

#[derive(Form)]
pub enum Message {
    SetAndReport(i32),
    Event(i32),
}

#[derive(Clone)]
pub struct TestAgent {
    reporter: mpsc::UnboundedSender<i32>,
    check_meta: fn(RelativeUri, AgentConfig),
}

impl TestAgent {
    pub fn new(
        reporter: mpsc::UnboundedSender<i32>,
        check_meta: fn(RelativeUri, AgentConfig),
    ) -> Self {
        TestAgent {
            reporter,
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
        let reporter = self.reporter.clone();
        async move {
            let (tx, rx) = context.add_lane(LANE, UplinkKind::Value, None).await?;
            Ok(run_agent(tx, rx, reporter).boxed())
        }
        .boxed()
    }
}

async fn run_agent(
    tx: ByteWriter,
    rx: ByteReader,
    reporter: mpsc::UnboundedSender<i32>,
) -> Result<(), AgentTaskError> {
    let decoder = LaneRequestDecoder::new(RecognizerDecoder::new(Message::make_recognizer()));
    let encoder = ValueLaneResponseEncoder::default();

    let mut input = FramedRead::new(rx, decoder);
    let mut output = FramedWrite::new(tx, encoder);

    let mut state = 0;

    while let Some(result) = input.next().await {
        match result {
            Ok(LaneRequest::Sync(id)) => {
                output
                    .send(ValueLaneResponse::synced(id, Message::Event(state)))
                    .await
                    .expect("Channel stopped.");
            }
            Ok(LaneRequest::Command(Message::SetAndReport(n))) => {
                state = n;
                reporter.send(n).expect("Reporter closed.");
            }
            Ok(LaneRequest::Command(Message::Event(n))) => {
                output
                    .send(ValueLaneResponse::event(Message::Event(n)))
                    .await
                    .expect("Channel stopped.");
            }
            Err(e) => {
                panic!("Bad frame: {}", e);
            }
        }
    }

    Ok(())
}
