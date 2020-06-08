#[macro_use]
extern crate lazy_static;

use std::sync::Mutex;
use std::time::Duration;

// use crate::wasm::{WasmWsFactory, WasmWsSink, WasmWsStream, WebsocketFactory};
use futures::StreamExt;
use futures_util::SinkExt;
use tokio::sync::mpsc;
use url::Url;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use web_sys::HtmlCanvasElement;
use ws_stream_wasm::{WsMessage, WsMeta};

use swim_client::common::model::Value;
use swim_client::common::warp::path::AbsolutePath;
use swim_client::configuration::downlink::{
    BackpressureMode, ClientParams, ConfigHierarchy, DownlinkParams, OnInvalidMessage,
};
use swim_client::configuration::router::RouterParamBuilder;
use swim_client::connections::factory::wasm::*;
use swim_client::connections::SwimConnPool;
use swim_client::interface::SwimClient;

#[allow(warnings)]
mod request;
mod stock;
#[allow(warnings)]
mod wasm;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

/// Type alias for the result of a drawing function.
pub type DrawResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Type used on the JS side to convert screen coordinates to chart
/// coordinates.
#[wasm_bindgen]
pub struct Chart {
    convert: Box<dyn Fn((i32, i32)) -> Option<(f64, f64)>>,
}

/// Result of screen to chart coordinates conversion.
#[wasm_bindgen]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

#[wasm_bindgen]
impl Chart {
    pub fn coord(&self, x: i32, y: i32) -> Option<Point> {
        (self.convert)((x, y)).map(|(x, y)| Point { x, y })
    }

    pub fn stock(canvas: HtmlCanvasElement) -> Result<Chart, JsValue> {
        let map_coord = stock::draw(canvas).map_err(|err| err.to_string())?;
        Ok(Chart {
            convert: Box::new(move |coord| map_coord(coord).map(|(x, y)| (x.into(), y.into()))),
        })
    }
}

fn config() -> ConfigHierarchy {
    let client_params = ClientParams::new(2, Default::default()).unwrap();
    let default_params = DownlinkParams::new_queue(
        BackpressureMode::Propagate,
        5,
        Duration::from_secs(600),
        5,
        OnInvalidMessage::Terminate,
        10000,
    )
    .unwrap();

    ConfigHierarchy::new(client_params, default_params)
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

#[wasm_bindgen(start)]
pub async fn start() {
    console_error_panic_hook::set_once();
    log("start");

    spawn_local(async {
        log("spawned task");

        let mut fac = WasmWsFactory::new(5);
        let mut client = SwimClient::new(config(), fac).await;
        let path = AbsolutePath::new(
            url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
            "/unit/foo",
            "random",
        );

        log("connecting");

        let (_downlink, mut receiver) = client
            .untyped_value_downlink(path, Value::Extant)
            .await
            .unwrap();

        while let Some(event) = receiver.next().await {
            log(&format!("Client received: {:?}", event));
        }
    });
}
