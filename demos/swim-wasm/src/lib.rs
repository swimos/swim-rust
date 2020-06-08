use std::time::Duration;

use futures::StreamExt;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use web_sys::HtmlCanvasElement;

use swim_client::common::model::Value;
use swim_client::common::warp::path::AbsolutePath;
use swim_client::configuration::downlink::{
    BackpressureMode, ClientParams, ConfigHierarchy, DownlinkParams, OnInvalidMessage,
};
use swim_client::connections::factory::wasm::*;
use swim_client::interface::SwimClient;

mod chart;

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

pub struct DataPoint {
    duration: chrono::Duration,
    data: f64,
}

#[wasm_bindgen]
impl Chart {
    pub fn coord(&self, x: i32, y: i32) -> Option<Point> {
        (self.convert)((x, y)).map(|(x, y)| Point { x, y })
    }

    pub fn init(canvas: HtmlCanvasElement) {
        chart::draw(canvas.clone(), &mut Vec::new());

        spawn_local(async move {
            let fac = WasmWsFactory::new(5);
            let mut client = SwimClient::new(config(), fac).await;
            let path = AbsolutePath::new(
                url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
                "/unit/foo",
                "random",
            );

            let (_downlink, mut receiver) =
                client.value_downlink(path, Value::Extant).await.unwrap();

            log("Opened downlink");

            let mut values = Vec::new();
            let mut averages = Vec::new();
            let window_size = 100;
            let start_epoch = stdweb::web::Date::now();
            let start = chrono::NaiveDateTime::from_timestamp(start_epoch as i64, 0);

            while let Some(event) = receiver.next().await {
                if let Value::Int32Value(i) = *event.action() {
                    log(&format!("Received: {:?}", i));

                    values.push(i.clone());

                    if values.len() > window_size {
                        values.remove(0);
                        averages.remove(0);
                    }

                    if !values.is_empty() {
                        let sum = values.iter().fold(0, |total, x| total + *x);
                        let sum = sum as f64 / values.len() as f64;
                        let now_epoch = stdweb::web::Date::now();
                        let now = chrono::NaiveDateTime::from_timestamp(now_epoch as i64, 0);
                        log(&format!("Average: {:?} @ {:?}", sum, now));
                        let duration = start.signed_duration_since(now);

                        let dp = DataPoint {
                            duration,
                            data: sum,
                        };

                        averages.push(dp);
                    }

                    if averages.len() > 10 {
                        chart::draw(canvas.clone(), &mut averages);
                    }
                } else {
                    panic!("Expected Int32 value");
                }

                // log(&format!("Client received: {:?}", event));
            }
        });
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
    pub fn log(s: &str);
}

#[wasm_bindgen(start)]
pub async fn start() {
    console_error_panic_hook::set_once();
}
