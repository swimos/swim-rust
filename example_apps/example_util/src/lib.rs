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

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    net::SocketAddr,
};

use swimos::server::{until_termination, ServerHandle};
use tokio::{select, sync::oneshot};
use tracing_subscriber::{filter::LevelFilter, EnvFilter};

pub async fn manage_handle(handle: ServerHandle) {
    until_termination(handle, None)
        .await
        .expect("Failed to register interrupt handler.");
}
pub async fn manage_handle_report(
    handle: ServerHandle,
    bound: Option<oneshot::Sender<SocketAddr>>,
) {
    let f = bound.map(|tx| {
        let g: Box<dyn FnOnce(SocketAddr) + Send> = Box::new(move |addr| {
            let _ = tx.send(addr);
        });
        g
    });
    until_termination(handle, f)
        .await
        .expect("Failed to register interrupt handler.");
}

struct FormatMap<'a, K, V>(&'a HashMap<K, V>);

pub fn format_map<K: Display, V: Display>(map: &HashMap<K, V>) -> impl Display + '_ {
    FormatMap(map)
}

impl<'a, K: Display, V: Display> Display for FormatMap<'a, K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        let mut it = self.0.iter();
        if let Some((k, v)) = it.next() {
            write!(f, " ({}, {})", k, v)?;
            for (k, v) in it {
                write!(f, ", ({}, {})", k, v)?;
            }
            write!(f, " ")?;
        }
        write!(f, "}}")?;
        Ok(())
    }
}

pub struct StartDependent {
    pub bound: SocketAddr,
    pub request: oneshot::Sender<ServerHandle>,
}

pub async fn manage_producer_and_consumer(
    mut producer_handle: ServerHandle,
    dep: oneshot::Sender<StartDependent>,
) {
    let mut shutdown_hook = Box::pin(async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to register interrupt handler.");
    });
    let get_addr = producer_handle.bound_addr();

    let maybe_addr = select! {
        _ = &mut shutdown_hook => None,
        maybe_addr = get_addr => maybe_addr,
    };

    let maybe_consumer_handler = if let Some(addr) = maybe_addr {
        println!("Producer bound to: {}", addr);
        let (cb_tx, cb_rx) = oneshot::channel();
        let msg = StartDependent {
            bound: addr,
            request: cb_tx,
        };
        if dep.send(msg).is_err() {
            None
        } else {
            select! {
                _ = &mut shutdown_hook => None,
                consumer_result = cb_rx => consumer_result.ok(),
            }
        }
    } else {
        None
    };

    if let Some(mut consumer_handle) = maybe_consumer_handler {
        let get_addr = consumer_handle.bound_addr();

        let maybe_addr = select! {
            _ = &mut shutdown_hook => None,
            maybe_addr = get_addr => maybe_addr,
        };

        if let Some(addr) = maybe_addr {
            println!("Consumer bound to: {}", addr);
            shutdown_hook.await;
        }

        consumer_handle.stop();
        producer_handle.stop();
    } else {
        println!("Server failed to start.");
        producer_handle.stop();
    }
}

pub fn example_filter() -> Result<EnvFilter, Box<dyn std::error::Error + Send + Sync>> {
    let filter = if let Ok(filter) = EnvFilter::try_from_default_env() {
        filter
    } else {
        EnvFilter::new("")
            .add_directive("swimos_server_app=trace".parse()?)
            .add_directive("swimos_runtime=trace".parse()?)
            .add_directive("swimos_agent=trace".parse()?)
            .add_directive("swimos_messages=trace".parse()?)
            .add_directive("swimos_remote=trace".parse()?)
    };
    Ok(filter)
}

pub fn example_logging() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).map(String::as_str) == Some("--enable-logging") {
        let filter = example_filter()?.add_directive(LevelFilter::WARN.into());
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
    Ok(())
}
