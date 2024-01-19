use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::str::FromStr;

use futures::StreamExt;
use futures_util::stream::FuturesUnordered;
use futures_util::FutureExt;
use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::{join, select};
use tracing::{debug, error, info, trace, Instrument};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::EnvFilter;
use warp::http::StatusCode;
use warp::{Buf, Filter, Stream};

use client::SwimClientBuilder;
use control_ir::{AgentSpec, DeploySpec};
use swim_server_app::error::ServerError;
use swim_server_app::{Server, ServerBuilder, ServerHandle};
use swim_utilities::routing::route_pattern::RoutePattern;
use swim_wasm_host::wasm::{Config, Engine, Linker};
use swim_wasm_host::{WasmAgentModel, WasmAgentState, WasmModule};
use wasm_connector_host::ConnectorPool;

#[tokio::main]
async fn main() {
    let filter = EnvFilter::default()
        // .add_directive("client=trace".parse().unwrap())
        // .add_directive("swim_client=trace".parse().unwrap())
        // .add_directive("runtime=trace".parse().unwrap())
        // .add_directive("swim_agent=trace".parse().unwrap())
        // .add_directive("swim_messages=trace".parse().unwrap())
        // .add_directive("swim_remote=trace".parse().unwrap())
        // .add_directive("swim_downlink=trace".parse().unwrap())
        .add_directive("wasm_server=trace".parse().unwrap())
        .add_directive(LevelFilter::WARN.into());
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let (tx, rx) = mpsc::channel(128);
    let handle = RuntimeHandle { tx };
    let http_server = warp::serve(build_server(handle)).run(([127, 0, 0, 1], 9090));
    let runtime = run_runtime(rx);
    join!(http_server, runtime);
}

struct EnvHandle {
    server: ServerHandle,
    routes: HashMap<String, watch::Sender<WasmModule<WasmAgentState>>>,
    connectors: ConnectorPool,
}

fn build_runtime(
    port: usize,
    agents: HashMap<String, AgentSpec>,
) -> Result<
    (
        ServerBuilder,
        HashMap<String, watch::Sender<WasmModule<WasmAgentState>>>,
    ),
    String,
> {
    let mut config = Config::new();
    config.async_support(true);

    let engine = Engine::new(&config).unwrap();

    let bind_addr = SocketAddr::from_str(format!("127.0.0.1:{port}").as_str()).unwrap();
    let mut server = ServerBuilder::with_plane_name("main")
        .with_in_memory_store()
        .set_bind_addr(bind_addr);

    let mut routes = HashMap::default();

    for (name, spec) in agents {
        let AgentSpec { route, module } = spec;
        let route = match RoutePattern::parse_str(route.as_str()) {
            Ok(route) => route,
            Err(e) => return Err(e.to_string()),
        };

        let module = match WasmModule::new(&engine, Linker::new(&engine), module) {
            Ok(module) => module,
            Err(e) => return Err(e.to_string()),
        };
        let (watch_tx, watch_rx) = watch::channel(module);
        routes.insert(name, watch_tx);
        server = server.add_route(route, WasmAgentModel::new(watch_rx));
    }

    Ok((server, routes))
}

enum RuntimeEvent {
    Request {
        request: RuntimeRequest,
    },
    TaskComplete {
        name: String,
        result: Result<(), ServerError>,
    },
    ConnectorComplete {
        name: String,
        result: anyhow::Result<()>,
    },
}

async fn run_runtime(mut rx: mpsc::Receiver<RuntimeRequest>) {
    let mut server_tasks = FuturesUnordered::new();
    let mut handles = HashMap::new();

    let (client, mut client_task) = SwimClientBuilder::new(Default::default()).build().await;

    info!("Server started");

    loop {
        let event: Option<RuntimeEvent> = select! {
            request = rx.recv() => request.map(|request| RuntimeEvent::Request { request }),
            exit = server_tasks.next(), if !server_tasks.is_empty() => exit.map(|result: (String, Result<(), ServerError>) | {
                RuntimeEvent::TaskComplete {
                    name: result.0,
                    result: result.1
                }
            }),
            client = &mut client_task => {
                error!("Client stopped");
                return;
            }
        };

        match event {
            Some(RuntimeEvent::Request { request }) => {
                trace!(?request, "Server runtime event received");
                match request {
                    RuntimeRequest::Stop { name, tx } => match handles.remove(&name) {
                        Some(EnvHandle { mut server, .. }) => {
                            server.stop();
                            tx.send(Ok(())).unwrap();
                        }
                        None => tx.send(Err(format!("No server named: {name}"))).unwrap(),
                    },
                    RuntimeRequest::Deploy { tx, spec } => {
                        let DeploySpec {
                            name,
                            port,
                            agents,
                            connectors,
                        } = spec;
                        if handles.contains_key(&name) {
                            tx.send(Err("Duplicate server".to_string())).unwrap();
                            continue;
                        }

                        let (server, routes) = match build_runtime(port, agents) {
                            Ok(o) => o,
                            Err(e) => {
                                tx.send(Err(e)).unwrap();
                                continue;
                            }
                        };

                        match server.build().await {
                            Ok(server) => {
                                let (task, handle) = server.run();
                                let task_name = name.clone();
                                server_tasks.push(async move { (task_name, task.await) });

                                let pool = ConnectorPool::build(port).await;
                                pool.deploy(connectors).await;

                                handles.insert(
                                    name.clone(),
                                    EnvHandle {
                                        server: handle,
                                        routes,
                                        connectors: pool,
                                    },
                                );

                                tx.send(Ok(())).unwrap();

                                debug!(?name, "Deployed server");
                            }
                            Err(e) => {
                                tx.send(Err(e.to_string())).unwrap();
                            }
                        }
                    }
                }
            }
            Some(RuntimeEvent::TaskComplete { name, result }) => {
                debug!(?name, ?result, "Server stopped");
                handles.remove(&name);
            }
            Some(RuntimeEvent::ConnectorComplete { name, result }) => {
                debug!(?name, ?result, "Connector stopped");
            }
            None => break,
        }
    }
}

#[derive(Debug)]
enum RuntimeRequest {
    Stop {
        name: String,
        tx: oneshot::Sender<Result<(), String>>,
    },
    Deploy {
        tx: oneshot::Sender<Result<(), String>>,
        spec: DeploySpec,
    },
}

#[derive(Debug, Clone)]
struct RuntimeHandle {
    tx: mpsc::Sender<RuntimeRequest>,
}

impl RuntimeHandle {
    async fn deploy(&self, spec: DeploySpec) -> bool {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(RuntimeRequest::Deploy { tx, spec })
            .await
            .unwrap();
        rx.await.unwrap().is_ok()
    }
}

fn with_context(
    handle: RuntimeHandle,
) -> impl Filter<Extract = (RuntimeHandle,), Error = Infallible> + Clone {
    warp::any().map(move || handle.clone())
}

fn build_server(
    handle: RuntimeHandle,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path(control_ir::endpoints::DEPLOY_WORKSPACE)
        .and(warp::post())
        .and(warp::body::stream())
        .and(with_context(handle.clone()))
        .and_then(deploy_workspace)
}

async fn deserialize<D>(
    mut body: impl Stream<Item = Result<impl Buf, warp::Error>> + Unpin + Send + Sync,
) -> D
where
    D: DeserializeOwned,
{
    let mut collected: Vec<u8> = vec![];
    while let Some(buf) = body.next().await {
        let mut buf = buf.unwrap();
        while buf.remaining() > 0 {
            let chunk = buf.chunk();
            let chunk_len = chunk.len();
            collected.extend_from_slice(chunk);
            buf.advance(chunk_len);
        }
    }

    serde_json::from_str::<D>(&String::from_utf8(collected).unwrap()).unwrap()
}

async fn deploy_workspace(
    body: impl Stream<Item = Result<impl Buf, warp::Error>> + Unpin + Send + Sync,
    handle: RuntimeHandle,
) -> Result<impl warp::Reply, Infallible> {
    let spec = deserialize(body).await;
    let code = if handle.deploy(spec).await {
        StatusCode::OK
    } else {
        StatusCode::BAD_REQUEST
    };

    Ok(code)
}
