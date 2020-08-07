// Copyright 2015-2020 SWIM.AI inc.
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

use futures::StreamExt;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use serde::{Deserialize, Serialize};
use swim_client::downlink::model::map::MapEvent;
use swim_client::interface::SwimClient;
use swim_common::model::Value;
use swim_common::warp::path::AbsolutePath;
use swim_wasm::connection::WasmWsFactory;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    pub fn log(s: &str);
}

#[wasm_bindgen(start)]
pub async fn start() {
    console_error_panic_hook::set_once();
}

#[derive(Serialize, Deserialize)]
struct Entry {
    key: i64,
    value: i32,
}

impl Entry {
    fn of(key: &Value, value: &Value) -> Entry {
        let key = if let Value::Int64Value(i) = *key {
            i
        } else {
            panic!("Expected Int64Value. Got: {:?}", key);
        };

        let value = if let Value::Int32Value(i) = *value {
            i
        } else {
            panic!("Expected Int32Value. Got: {:?}", value);
        };

        Entry { key, value }
    }
}

#[wasm_bindgen]
pub struct RustClient;

#[wasm_bindgen]
impl RustClient {
    #[wasm_bindgen(constructor)]
    pub fn run(
        on_sync_callback: js_sys::Function,
        on_update_callback: js_sys::Function,
        on_remove_callback: js_sys::Function,
    ) -> RustClient {
        spawn_local(async move {
            let fac = WasmWsFactory::new(5);
            let mut swim_client = SwimClient::new_with_default(fac).await;

            let (_downlink, mut receiver) = swim_client
                .untyped_map_downlink(Self::path())
                .await
                .unwrap();
            let this = JsValue::NULL;

            while let Some(event) = receiver.next().await {
                let inner = event.get_inner();
                match &inner.event {
                    MapEvent::Initial => {
                        let records: js_sys::Array =
                            inner
                                .view
                                .iter()
                                .fold(js_sys::Array::new(), |vec, (key, value)| {
                                    let entry = Entry::of(key, &*value.as_ref());
                                    let message = JsValue::from_serde(&entry).unwrap();

                                    vec.push(&message);
                                    vec
                                });

                        let _r = on_sync_callback.call1(&this, &records).unwrap();
                    }
                    MapEvent::Insert(key) => {
                        let value = inner.view.get(&key).expect("Missing value");
                        let entry = Entry::of(key, &*value.as_ref());
                        let message = JsValue::from_serde(&entry).unwrap();

                        let _r = on_update_callback.call1(&this, &message).unwrap();
                    }
                    MapEvent::Remove(key) => {
                        let key = if let Value::Int64Value(i) = key {
                            JsValue::from(i.to_string())
                        } else {
                            panic!("Incorrect value type received. {:?}", key);
                        };

                        let _r = on_remove_callback.call1(&this, &key).unwrap();
                    }
                    _ => {
                        // not interested in other events
                    }
                }
            }
        });

        RustClient {}
    }

    fn path() -> AbsolutePath {
        AbsolutePath::new(
            url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
            "/unit/foo",
            "random",
        )
    }
}
