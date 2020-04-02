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

use crate::connections::{ConnectionError, ConnectionPoolMessage};
use common::warp::envelope::{Envelope, LaneAddressed};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

//-------------------------------Connection Pool to Downlink------------------------------------

// rx receives messages directly from every open connection in the pool
async fn _receive_all_messages_from_pool(
    mut router_rx: mpsc::Receiver<Result<ConnectionPoolMessage, ConnectionError>>,
    mut sink_request_tx: mpsc::Sender<(url::Url, oneshot::Sender<mpsc::Sender<String>>)>,
) {
    loop {
        let pool_message = router_rx.recv().await.unwrap().unwrap();
        let ConnectionPoolMessage {
            host,
            message: _message,
        } = pool_message;

        //TODO this needs to be implemented
        //We can have multiple sinks (downlinks) for a given host

        let host_url = url::Url::parse(&host).unwrap();
        let (sink_tx, _sink_rx) = oneshot::channel();

        sink_request_tx.send((host_url, sink_tx)).await.unwrap();

        //Todo This should be sent down to the host tasks.
        // sink.send_item(text);
    }
}

async fn _request_sinks(
    mut sink_request_rx: mpsc::Receiver<(url::Url, oneshot::Sender<mpsc::Sender<String>>)>,
) {
    let mut _sinks: HashMap<String, oneshot::Sender<mpsc::Sender<String>>> = HashMap::new();

    loop {
        let (_host, _sink_tx) = sink_request_rx.recv().await.unwrap();

        // Todo Implement this.
        // let sink = pool.request_sink();

        // sink_tx.send(sink);
    }
}

async fn _receive_host_messages_from_pool(
    mut message_rx: mpsc::Receiver<String>,
    downlinks_rxs: Vec<mpsc::Sender<Envelope>>,
) {
    loop {
        //TODO parse the message to an envelope
        let _message = message_rx.recv().await.unwrap();

        let lane_addressed = LaneAddressed {
            node_uri: String::from("node_uri"),
            lane_uri: String::from("lane_uri"),
            body: None,
        };

        let _envelope = Envelope::EventMessage(lane_addressed);

        for mut _downlink_rx in &downlinks_rxs {
            // Todo need clone for envelope
            // downlink_rx.send_item(envelope.clone());
        }
    }
}
