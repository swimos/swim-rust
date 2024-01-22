// Copyright 2015-2023 Swim Inc.
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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use anyhow::{anyhow, Result};
use futures::pin_mut;
use rand::{Rng, SeedableRng};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig as KafkaConfig, Message};
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::yield_now;
use tokio::time::sleep;
use tracing::{error, trace};
use wasmtime::Engine;

use client::{
    ClientHandle, DownlinkOptions, RawMessage, RemotePath, SwimClientBuilder,
    UntypedValueDownlinkView, ValueDownlinkOperationError,
};
use control_ir::KafkaConnectorSpec;
use wasm_ir::connector::ConnectorMessage;

use crate::runtime::{WasmConnector, WasmConnectorFactory, WasmError};

pub async fn run_kafka_connector(
    client: ClientHandle,
    port: usize,
    spec: KafkaConnectorSpec,
    engine: Engine,
) -> Result<()> {
    let KafkaConnectorSpec {
        broker,
        topic,
        group,
        module,
        properties,
    } = spec;

    trace!(broker = ?broker, topic = ?topic, group = ?group, "Starting Kafka connector");

    let consumer: StreamConsumer = KafkaConfig::new()
        .set("group.id", group)
        .set("bootstrap.servers", broker)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()?;

    trace!(?topic, "Created consumer. Subscribing to topic");
    consumer.subscribe(&[topic.as_str()])?;

    let (tx, mut rx) = mpsc::channel(1);
    let mut connector = engine.new_connector(module, tx, properties).await?;
    let mut downlinks = HashMap::default();
    // todo: fix downlink runtime startup bug
    let (client, client_task) = SwimClientBuilder::new(Default::default()).build().await;
    let handle = client.handle();
    let _task = tokio::spawn(client_task);

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                if let Some(bytes) = msg.payload() {
                    trace!("Connector received message. Dispatching to module");

                    suspend(
                        port,
                        connector.dispatch(bytes),
                        &handle,
                        &mut downlinks,
                        &mut rx,
                    )
                    .await?;
                }
            }
            Err(error) => {
                error!(?error, "Kafka connector error");
                return Err(anyhow!(error));
            }
        }
    }
}

async fn suspend<F>(
    port: usize,
    suspended_task: F,
    client: &ClientHandle,
    downlinks: &mut HashMap<RemotePath, UntypedValueDownlinkView>,
    rx: &mut mpsc::Receiver<ConnectorMessage>,
) -> Result<()>
where
    F: Future<Output = Result<(), WasmError>>,
{
    pin_mut!(suspended_task);

    loop {
        let dispatch = select! {
            biased;
            msg = rx.recv() => {
                match msg {
                    Some(request) => request,
                    None => {
                        // it's not possible for the sender to be dropped as we own it
                        unreachable!()
                    }
                }
            },
            r = &mut suspended_task => {
                trace!("Connector suspend complete");
                return Ok(r?)
            }
        };

        trace!("Connector requested to dispatch envelope");

        let ConnectorMessage { node, lane, data } = dispatch;
        let remote = RemotePath::new(format!("ws://127.0.0.1:{port}"), &node, &lane);

        // todo: replace with index lookup instead of cloning path
        match downlinks.entry(remote.clone()) {
            Entry::Occupied(mut entry) => {
                if let Err(ValueDownlinkOperationError::DownlinkStopped) = entry
                    .get_mut()
                    .set(RawMessage::from(data.clone().into_bytes()))
                    .await
                {
                    let view = client
                        .untyped_value_downlink(remote.clone())
                        .options(DownlinkOptions::KEEP_LINKED)
                        .open()
                        .await?;

                    view.set(RawMessage::from(data.into_bytes())).await?;
                    *entry.get_mut() = view;
                }
            }
            Entry::Vacant(entry) => {
                let view = client
                    .untyped_value_downlink(remote)
                    .options(DownlinkOptions::KEEP_LINKED)
                    .open()
                    .await?;
                let view = entry.insert(view);
                view.set(RawMessage::from(data.into_bytes())).await?;
            }
        }
    }
}
