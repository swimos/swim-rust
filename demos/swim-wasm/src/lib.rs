use std::time::Duration;

use futures::StreamExt;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use web_sys::HtmlCanvasElement;

use swim_client::common::model::Value;
use swim_client::common::warp::path::AbsolutePath;
use swim_client::connections::factory::wasm::*;
use swim_client::interface::SwimClient;

mod chart;

#[wasm_bindgen]
pub struct Chart {}

#[derive(Clone)]
pub struct DataPoint {
    duration: chrono::Duration,
    data: f64,
}

#[wasm_bindgen]
impl Chart {
    pub fn init(canvas: HtmlCanvasElement) {
        spawn_local(async move {
            let fac = WasmWsFactory::new(5);
            let mut client = SwimClient::new_default(fac).await;
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
            let window_size = 1000;
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

                    if averages.len() > 2 {
                        let mut avg_clone = averages.clone();
                        avg_clone.reverse();
                        chart::draw(canvas.clone(), avg_clone);
                    }
                } else {
                    panic!("Expected Int32 value");
                }
            }
        });
    }
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
