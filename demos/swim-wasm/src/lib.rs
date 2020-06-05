use url::Url;
use wasm_bindgen::prelude::*;
use web_sys::HtmlCanvasElement;

// use swim_client::configuration::downlink::{
//     BackpressureMode, ClientParams, ConfigHierarchy, DownlinkParams, OnInvalidMessage,
// };
// use swim_client::connections::factory::wasm::*;
// use swim_client::interface::SwimClient;
// use swim_client::connections::factory::WebsocketFactory;
use crate::wasm::{WasmWsFactory, WasmWsSink, WasmWsStream, WebsocketFactory};
use futures::StreamExt;
use futures_util::SinkExt;
use std::sync::Mutex;
use tokio::sync::mpsc;
use wasm_bindgen_futures::spawn_local;
use ws_stream_wasm::{WsMessage, WsMeta};

#[allow(warnings)]
mod request;
mod stock;
#[allow(warnings)]
mod wasm;

#[macro_use]
extern crate lazy_static;

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
//
// fn config() -> ConfigHierarchy {
//     let client_params = ClientParams::new(2, Default::default()).unwrap();
//     let default_params = DownlinkParams::new_queue(
//         BackpressureMode::Propagate,
//         5,
//         Duration::from_secs(600),
//         5,
//         OnInvalidMessage::Terminate,
//         10000,
//     )
//     .unwrap();
//
//     ConfigHierarchy::new(client_params, default_params)
// }

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

// #[tokio::main]
#[wasm_bindgen(start)]
pub async fn start() {
    console_error_panic_hook::set_once();

    log("start");

    // let (mut tx, mut rx) = mpsc::channel(5);
    //
    // tokio::spawn(async move {
    //     log("spawn");
    //
    //     spawn_local(async move {
    //         let url = Url::parse("ws://127.0.0.1:9001/").unwrap();
    //
    //         let (ws, wsio) = WsMeta::connect(url, None).await.unwrap();
    //         let state = ws.ready_state();
    //         log(&format!("State: {:?}", state));
    //
    //         let (mut sink, mut stream) = wsio.split();
    //
    //         sink.send(WsMessage::Text(String::from(
    //             "@link(node: \"unit/foo\", lane: \"random\")",
    //         )))
    //         .await
    //         .unwrap();
    //
    //         sink.send(WsMessage::Text(String::from(
    //             "@sync(node: \"unit/foo\", lane: \"random\")",
    //         )))
    //         .await
    //         .unwrap();
    //
    //         while let Some(e) = stream.next().await {
    //             tx.send(e).await;
    //         }
    //
    //         log("Started client...");
    //     });
    // })
    // .await
    // .unwrap();
    //
    // spawn_local(async move {
    //     while let Some(m) = rx.recv().await {
    //         log(&format!("Msg: {:?}", m));
    //     }
    // });
    // log("exiting");

    spawn_local(async {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            log("hello from tokio");

            let url = Url::parse("ws://127.0.0.1:9001/").unwrap();
            let (ws, wsio) = WsMeta::connect(url, None).await.unwrap();
            // let state = ws.ready_state();
            //
            // log(&format!("State: {:?}", state));
            //
            // let (mut sink, mut stream) = wsio.split();
        });
    });
}
