use crate::connections::factory::stream::{build_stream, get_stream_type};
use crate::connections::factory::{async_factory, HostConfig};
use futures::future::BoxFuture;
use futures::FutureExt;
use http::header::SEC_WEBSOCKET_PROTOCOL;
use http::uri::InvalidUri;
use http::Request;
use http::{HeaderValue, Uri};
use ratchet::WebSocketConfig;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use swim_runtime::error::{
    ConnectionError, HttpError, HttpErrorKind, InvalidUriError, InvalidUriErrorKind,
};
use swim_runtime::ws::utils::maybe_resolve_scheme;
use swim_runtime::ws::{Protocol, WebSocketDef, WebsocketFactory};
use url::Url;

const WARP0_PROTO: &str = "warp0";
const MAX_MESSAGE_SIZE: usize = 64 << 20;

pub struct RatchetWebSocketFactory {
    inner: async_factory::AsyncFactory<TungSink, TungStream>,
    host_configurations: HashMap<Url, HostConfig>,
}

impl WebsocketFactory for RatchetWebSocketFactory {
    fn connect(&mut self, url: Url) -> BoxFuture<Result<WebSocketDef, ConnectionError>> {
        let config = match self.host_configurations.entry(url.clone()) {
            Entry::Occupied(o) => o.get().clone(),
            Entry::Vacant(v) => v
                .insert(HostConfig {
                    protocol: Protocol::PlainText,
                    compression_level: WsCompression::None(Some(MAX_MESSAGE_SIZE)),
                })
                .clone(),
        };

        self.inner.connect_using(url, config).boxed()
    }
}

async fn connect(url: Url, config: &mut HostConfig) -> Result<WebSocketDef, ConnectionError> {
    let url = url.as_str();
    let uri: Uri = url.parse().map_err(|e: InvalidUri| {
        ConnectionError::Http(HttpError::new(
            HttpErrorKind::InvalidUri(InvalidUriError::new(
                InvalidUriErrorKind::Malformatted,
                Some(url.to_string()),
            )),
            Some(e.to_string()),
        ))
    })?;
    let mut request = Request::get(uri)
        .body(())
        .map_err(|e| ConnectionError::Http(HttpError::from(e)))?;

    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(WARP0_PROTO),
    );

    let request = maybe_resolve_scheme(request)?;
    let stream_type = get_stream_type(&request, &config.protocol)?;

    let port = request
        .uri()
        .port_u16()
        .unwrap_or_else(|| match request.uri().scheme_str() {
            Some("wss") => 443,
            Some("ws") => 80,
            // resolved by `maybe_resolve_scheme`
            _ => unreachable!(),
        });

    let domain = match request.uri().host() {
        Some(d) => d.to_string(),
        None => {
            return Err(ConnectionError::Http(HttpError::invalid_url(
                request.uri().to_string(),
                Some("Malformatted URI. Missing host".into()),
            )));
        }
    };

    let host = format!("{}:{}", domain, port);
    let stream = build_stream(&host, stream_type).await?;

    match ratchet::subscribe(WebSocketConfig::default(), stream, request).await {
        Ok(sock) => Ok(sock.into_websocket()),
        Err(_) => {
            unimplemented!()
        }
    }
}
