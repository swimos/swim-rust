use client::common::model::Value;
use client::common::warp::envelope::Envelope;
use client::common::warp::path::AbsolutePath;
use client::connections::factory::wasm::*;
use client::form::Form;
use client::interface::SwimClient;
use futures::StreamExt;
use js_sys::Promise;
use serde::{Deserialize, Serialize};
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
#[derive(Serialize, Deserialize, Form)]
pub struct Message {
    value: String,
    ip_address: String,
}

#[wasm_bindgen]
impl ChatClient {
    #[wasm_bindgen(constructor)]
    pub async fn new() -> ChatClient {
        let fac = WasmWsFactory::new(5);
        let swim_client = SwimClient::new_default(fac).await;

        log("Opened client to the server");

        let mut cc = swim_client.clone();

        spawn_local(async move {
            let (_downlink, mut receiver) = cc.untyped_map_downlink(Self::path()).await.unwrap();
            while let Some(msg) = receiver.next().await {
                log(&format!("Received: {:?}", msg))
            }
        });

        ChatClient { swim_client }
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
            let env = Envelope::make_command("/rooms", "post", Some(Value::Text(msg.as_value())));
            let target =
                AbsolutePath::new(url::Url::parse("ws://127.0.0.1:9001/").unwrap(), "", "");

            match swim_client.send_command(target, env).await {
                Ok(_) => Ok(JsValue::TRUE),
                Err(_) => Err(JsValue::FALSE),
            }
        })
    }
}
