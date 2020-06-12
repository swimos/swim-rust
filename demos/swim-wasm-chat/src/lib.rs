use futures::StreamExt;
use js_sys::Promise;
use serde::{Deserialize, Serialize};
use swim_client::common::warp::envelope::Envelope;
use swim_client::common::warp::path::AbsolutePath;
use swim_client::connections::factory::wasm::*;
use swim_client::downlink::model::map::MapEvent;
use swim_client::interface::SwimClient;
use swim_form::*;

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
    #[serde(rename = "userUuid")]
    user_uuid: String,
}

#[wasm_bindgen]
impl Message {
    pub fn new(value: String, user_name: String, user_uuid: String) -> Message {
        Message {
            value,
            user_name,
            user_uuid,
        }
    }
}

#[wasm_bindgen]
impl ChatClient {
    #[wasm_bindgen(constructor)]
    pub async fn new() -> ChatClient {
        let fac = WasmWsFactory::new(5);
        let swim_client = SwimClient::new_default(fac).await;

        log("Opened client to the server");

        ChatClient { swim_client }
    }

    #[wasm_bindgen]
    pub fn on_message(&self, f: js_sys::Function) {
        let mut client = self.swim_client.clone();

        spawn_local(async move {
            let (_downlink, mut receiver) =
                client.untyped_map_downlink(Self::path()).await.unwrap();
            let this = JsValue::NULL;

            while let Some(msg) = receiver.next().await {
                match &msg.action().event {
                    MapEvent::Insert(key) => {
                        let record = msg.action().view.get(key);
                        let message = Message::try_from_value(&*record.unwrap()).unwrap();

                        let user_name = JsValue::from(&message.user_name);
                        // let time = if let Value::Int64Value(time) = key {
                        //     JsValue::from(time)
                        // } else {
                        //     unreachable!()
                        // };

                        let message = JsValue::from(&message.value);
                        let _r = f.call2(&this, &user_name, &message).unwrap();
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
