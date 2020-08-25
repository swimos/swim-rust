// Copyright 2015-2020 SWIM.AI inc.
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

use crate::agent::context::AgentExecutionContext;
use crate::agent::{LaneIo, AttachError};
use std::collections::HashMap;
use crate::agent::lane::channels::AgentExecutionConfig;
use crate::routing::{TaggedEnvelope, TaggedClientEnvelope};
use futures::{Stream, StreamExt};
use tokio::sync::{oneshot, mpsc};
use swim_common::warp::envelope::{IncomingLinkMessage, OutgoingLinkMessage};
use swim_common::warp::path::RelativePath;
use pin_utils::pin_mut;
use futures::stream::FuturesUnordered;
use futures::select;
use either::Either;
use crate::agent::lane::channels::uplink::spawn::UplinkErrorReport;
use crate::agent::lane::channels::task::LaneIoError;
use utilities::sync::trigger;
use std::fmt::Display;
use pin_utils::core_reexport::fmt::Formatter;
use std::error::Error;
use parking_lot::Mutex;

pub struct EnvelopeDispatcher<Context> {
    agent_route: String,
    config: AgentExecutionConfig,
    context: Context,
    lanes: HashMap<String, Box<dyn LaneIo<Context>>>,
}

struct OpenRequest {
    name: String,
    callback: oneshot::Sender<mpsc::Sender<TaggedClientEnvelope>>,
}

#[derive(Debug)]
pub enum DispatcherError {
    AttachmentFailed(AttachError),
    LaneTaskFailed(LaneIoError),
}

impl Display for DispatcherError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DispatcherError::AttachmentFailed(err) => write!(f, "{}", err),
            DispatcherError::LaneTaskFailed(err) => write!(f, "{}", err),
        }
    }
}

impl Error for DispatcherError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DispatcherError::AttachmentFailed(err) => Some(err),
            DispatcherError::LaneTaskFailed(err) => Some(err)
        }
    }
}



impl<Context> EnvelopeDispatcher<Context>
where
    Context: AgentExecutionContext + Clone + Send + Sync + 'static,
{

    pub fn new(agent_route: String,
               config: AgentExecutionConfig,
               context: Context,
                lanes: HashMap<String, Box<dyn LaneIo<Context>>>) -> Self {
        EnvelopeDispatcher {
            agent_route, config, context, lanes,
        }
    }

    pub async fn run(self, incoming: impl Stream<Item = TaggedEnvelope>) -> Result<(), DispatcherError> {

        let EnvelopeDispatcher {
            agent_route,
            config,
            context,
            mut lanes
        } = self;

        let (open_tx, mut open_rx) = mpsc::channel::<OpenRequest>(1);

        let open_config = config.clone();
        let open_context: Context = context.clone();

        let (tripwire_tx, tripwire_rx) = trigger::trigger();

        let open_task = async move {

            let _tripwire = tripwire_tx;

            let mut lane_io_tasks = FuturesUnordered::new();

            let requests = open_rx.fuse();
            pin_mut!(requests);

            loop {

                let next: Option<Either<OpenRequest, Result<Vec<UplinkErrorReport>, LaneIoError>>> = if lane_io_tasks.is_empty() {
                    requests.next().await.map(Either::Left)
                } else {
                    select! {
                        request = requests.next() => request.map(Either::Left),
                        completed = lane_io_tasks.next() => completed.map(Either::Right),
                    }
                };

                match next {
                    Some(Either::Left(OpenRequest { name, callback })) => {
                        if let Some(lane_io) = lanes.remove(&name) {
                            let (lane_tx, lane_rx) = mpsc::channel(5);
                            let route = RelativePath::new(agent_route.as_str(), name.as_str());
                            let task_result = lane_io.attach_boxed(
                                route,
                                lane_rx,
                                open_config.clone(),
                                open_context.clone()
                            );
                            match task_result {
                                Ok(task) => {
                                    lane_io_tasks.push(task);
                                    if callback.send(lane_tx).is_err() {
                                        //TODO Log error.
                                    }
                                },
                                Err(e) => {
                                    //TODO Log error.
                                    break Err(DispatcherError::AttachmentFailed(e));
                                }
                            }
                        }
                    },
                    Some(Either::Right(Err(lane_io_err))) => {
                        break Err(DispatcherError::LaneTaskFailed(lane_io_err))
                    },
                    _ => {
                        break Ok(());
                    }
                }

            }


        };

        let mutex_open_tx = Mutex::new(open_tx);
        let lane_senders: HashMap<String, mpsc::Sender<TaggedClientEnvelope>> = HashMap::new();
/*
        incoming.take_until(tripwire_rx).scan(lane_senders, |
            lane_senders,
            TaggedEnvelope(addr, envelope)
        | {

        });





        let dispatch_task = incoming.take_until(tripwire_rx).for_each_concurrent(Some(config.max_concurrency),
                                                            |TaggedEnvelope(addr, envelope)| {

        });*/

        Ok(())
    }
}