use futures::StreamExt;
use js_sys::Promise;
use serde::{Deserialize, Serialize};

use swim_client::common::warp::envelope::Envelope;
use swim_client::common::warp::path::AbsolutePath;
use swim_client::downlink::model::map::MapEvent;
use swim_client::interface::SwimClient;
use swim_form::*;
use swim_wasm::connection::WasmWsFactory;

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{future_to_promise, spawn_local};

#[wasm_bindgen]
extern "C" {
    fn alert(s: &str);
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

#[wasm_bindgen]
pub struct ChatClient {
    swim_client: SwimClient,
}

#[wasm_bindgen]
#[form]
#[derive(Debug)]
pub struct Message {
    value: String,
    #[serde(rename = "userName")]
    user_name: String,
    uuid: String,
}

#[wasm_bindgen]
impl Message {
    pub fn new(value: String, user_name: String, uuid: String) -> Message {
        Message {
            value,
            user_name,
            uuid,
        }
    }
}

#[wasm_bindgen]
impl ChatClient {
    #[wasm_bindgen(constructor)]
    pub async fn new() -> ChatClient {
        let fac = WasmWsFactory::new(5);
        let swim_client = SwimClient::new_default(fac).await;

        ChatClient { swim_client }
    }

    #[wasm_bindgen]
    pub fn set_callbacks(
        &self,
        on_load_callback: js_sys::Function,
        on_message_callback: js_sys::Function,
    ) {
        let mut client = self.swim_client.clone();

        spawn_local(async move {
            let (_downlink, mut receiver) =
                client.untyped_map_downlink(Self::path()).await.unwrap();
            let this = JsValue::NULL;

            while let Some(msg) = receiver.next().await {
                match &msg.action().event {
                    MapEvent::Initial => {
                        let records: js_sys::Array = msg.action().view.iter().fold(
                            js_sys::Array::new(),
                            |vec, (_key, value)| {
                                let message = Message::try_from_value(&*value).unwrap();
                                let message = JsValue::from_serde(&message).unwrap();

                                vec.push(&message);
                                vec
                            },
                        );

                        let _r = on_load_callback.call1(&this, &records).unwrap();
                    }

                    MapEvent::Insert(key) => {
                        let record = msg.action().view.get(key);
                        let message = Message::try_from_value(&*record.unwrap()).unwrap();
                        let message = JsValue::from_serde(&message).unwrap();

                        let _r = on_message_callback.call1(&this, &message).unwrap();
                    }
                    _ => {}
                }
            }
        });
    }

    fn path() -> AbsolutePath {
        AbsolutePath::new(
            url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
            "/rooms",
            "chats",
        )
    }

    #[wasm_bindgen]
    pub fn send_message(&mut self, msg: Message) -> Promise {
        let mut swim_client = self.swim_client.clone();

        future_to_promise(async move {
            let env = Envelope::make_command("/rooms", "post", Some(msg.as_value()));
            let target =
                AbsolutePath::new(url::Url::parse("ws://127.0.0.1:9001/").unwrap(), "", "");

            match swim_client.send_command(target, env).await {
                Ok(_) => Ok(JsValue::TRUE),
                Err(_) => Err(JsValue::FALSE),
            }
        })
    }
}
