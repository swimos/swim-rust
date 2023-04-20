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

use std::collections::{hash_map::Entry, HashMap};

use bytes::BytesMut;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use ratchet::{
    CloseCode, CloseReason, NoExt, NoExtProvider, ProtocolRegistry, WebSocket, WebSocketConfig,
};
use swim_form::structural::write::StructuralWritable;
use swim_recon::printer::print_recon_compact;
use swim_runtime::net::{BadUrl, SchemeHostPort};
use thiserror::Error;
use tokio::net::TcpStream;

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("Failed to send command: {0}")]
    Ratchet(#[from] ratchet::Error),
    #[error("Invalid URL: {0}")]
    Url(#[from] BadUrl),
}

impl From<std::io::Error> for CommandError {
    fn from(err: std::io::Error) -> Self {
        CommandError::Ratchet(err.into())
    }
}

#[derive(Debug, Default)]
pub struct Commander {
    websockets: HashMap<SchemeHostPort, (BytesMut, WebSocket<TcpStream, NoExt>)>,
}

impl Commander {
    pub async fn send_command(
        &mut self,
        host: impl AsRef<str>,
        node: impl AsRef<str>,
        lane: impl AsRef<str>,
        body: &impl StructuralWritable,
    ) -> Result<(), CommandError> {
        let Commander { websockets } = self;
        let shp = host.as_ref().parse::<SchemeHostPort>()?;
        let (_, ws) = match websockets.entry(shp.clone()) {
            Entry::Occupied(mut entry) => {
                let (buffer, ws) = entry.get_mut();
                ws.write_ping(b"Heartbeat").await?;
                let is_good = loop {
                    match ws.read(buffer).await? {
                        ratchet::Message::Pong(_) => break true,
                        ratchet::Message::Close(_) => break false,
                        _ => {}
                    }
                };
                if is_good {
                    entry.into_mut()
                } else {
                    match open_connection(shp).await {
                        Ok(replacement) => {
                            *ws = replacement;
                            entry.into_mut()
                        }
                        Err(e) => {
                            entry.remove();
                            return Err(e.into());
                        }
                    }
                }
            }
            Entry::Vacant(entry) => entry.insert((BytesMut::new(), open_connection(shp).await?)),
        };
        let envelope = format!(
            "@command(node: \"{}\", lane: \"{}\") {}",
            node.as_ref(),
            lane.as_ref(),
            print_recon_compact(body)
        );

        ws.write_text(envelope).await?;
        Ok(())
    }

    pub async fn close(&mut self) -> Vec<CommandError> {
        let Commander { websockets } = self;
        let mut closes = std::mem::take(websockets)
            .into_iter()
            .map(|(_, (_, mut ws))| {
                Box::pin(async move {
                    ws.close(CloseReason::new(CloseCode::Normal, None))
                        .await
                        .err()
                })
            })
            .collect::<FuturesUnordered<_>>();
        let mut errs = vec![];
        while let Some(maybe) = closes.next().await {
            if let Some(err) = maybe {
                errs.push(err.into());
            }
        }
        errs
    }
}

async fn open_connection(shp: SchemeHostPort) -> Result<WebSocket<TcpStream, NoExt>, CommandError> {
    let SchemeHostPort(scheme, host, port) = shp;
    let remote = format!("{}:{}", host, port);
    let url = format!("{}://{}:{}", scheme, host, port);
    let socket = TcpStream::connect(remote).await?;
    let subprotocols = ProtocolRegistry::new(vec!["warp0"]).unwrap();
    let ws = ratchet::subscribe_with(
        WebSocketConfig::default(),
        socket,
        url,
        NoExtProvider,
        subprotocols,
    )
    .await?
    .into_websocket();
    Ok(ws)
}
