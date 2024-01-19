mod kafka;
mod runtime;

use crate::kafka::run_kafka_connector;
use client::{ClientHandle, SwimClient, SwimClientBuilder};
use control_ir::{ConnectorKind, ConnectorSpec};
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::FutureExt;
use futures_util::StreamExt;
use std::collections::HashMap;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};
use wasmtime::{Config, Engine};

enum Request {
    Deploy { name: String, spec: ConnectorSpec },
}

pub struct ConnectorPool {
    client: SwimClient,
    _task: JoinHandle<()>,
    tx: mpsc::Sender<Request>,
}

impl ConnectorPool {
    pub async fn build(port: usize) -> ConnectorPool {
        let (client, client_task) = SwimClientBuilder::new(Default::default()).build().await;
        let (tx, rx) = mpsc::channel(8);
        let task = tokio::spawn(run(client_task, client.handle(), rx, port));

        ConnectorPool {
            tx,
            client,
            _task: task,
        }
    }

    pub async fn deploy(&self, connectors: HashMap<String, ConnectorSpec>) {
        let ConnectorPool { tx, .. } = self;

        for (name, spec) in connectors {
            tx.send(Request::Deploy { name, spec })
                .await
                .expect("Runtime stopped")
        }
    }
}

struct ConnectorResult {
    kind: ConnectorKind,
    name: String,
    result: anyhow::Result<()>,
}

enum RuntimeEvent {
    ClientStopped,
    ConnectorStopped {
        result: Result<ConnectorResult, JoinError>,
    },
    Request(Request),
}

async fn run(
    mut client_task: BoxFuture<'static, ()>,
    client_handle: ClientHandle,
    mut requests: mpsc::Receiver<Request>,
    port: usize,
) {
    let mut connector_tasks = FuturesUnordered::new();

    let mut config = Config::new();
    config.async_support(true);

    let engine = Engine::new(&config).expect("Failed to build WASM engine");

    loop {
        let event: Option<RuntimeEvent> = select! {
            _ = &mut client_task => Some(RuntimeEvent::ClientStopped),
            request = requests.recv() => request.map(RuntimeEvent::Request),
            result = connector_tasks.next() => result.map(|result| RuntimeEvent::ConnectorStopped { result })
        };

        match event {
            Some(RuntimeEvent::ClientStopped) => {}
            Some(RuntimeEvent::ConnectorStopped { .. }) => {}
            Some(RuntimeEvent::Request(Request::Deploy { name, spec })) => match spec {
                ConnectorSpec::Kafka(spec) => {
                    let handle = client_handle.clone();
                    let engine = engine.clone();
                    let task = tokio::spawn(async move {
                        ConnectorResult {
                            kind: ConnectorKind::Kafka,
                            name,
                            result: run_kafka_connector(handle, port, spec, engine).await,
                        }
                    })
                    .boxed();
                    connector_tasks.push(task);
                }
            },
            None => {}
        }
    }
}
