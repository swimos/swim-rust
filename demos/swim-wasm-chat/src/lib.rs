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

use either::Either;
use futures::{select, StreamExt};
use js_sys::Promise;
use serde::{Deserialize, Serialize};
use swim_client::configuration::downlink::ConfigHierarchy;
use swim_client::downlink::model::map::{MapEvent, ViewWithEvent};
use swim_client::downlink::Event;
use swim_client::interface::SwimClient;
use swim_common::form::Form;
use swim_common::warp::path::AbsolutePath;
use swim_wasm::connection::WasmWsFactory;
use tokio::sync::{mpsc, oneshot};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{future_to_promise, spawn_local};

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
#[derive(Debug, Form, Serialize, Deserialize)]
pub struct Message {
    value: String,
    #[form(rename = "userName")]
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

type MessageRequest = (Message, oneshot::Sender<Result<(), ()>>);

#[wasm_bindgen]
pub struct ChatClient {
    swim_client_tx: mpsc::Sender<MessageRequest>,
}

#[wasm_bindgen]
impl ChatClient {
    #[wasm_bindgen(constructor)]
    pub async fn new(
        on_load_callback: js_sys::Function,
        on_message_callback: js_sys::Function,
    ) -> ChatClient {
        let (tx, rx) = mpsc::channel(10);
        let fac = WasmWsFactory::new(5);
        let swim_client = SwimClient::new(ConfigHierarchy::default(), fac).await;

        spawn_local(ClientTask::new(swim_client, rx, on_load_callback, on_message_callback).run());

        ChatClient { swim_client_tx: tx }
    }

    pub fn send_message(&mut self, msg: Message) -> Promise {
        let mut tx = self.swim_client_tx.clone();

        future_to_promise(async move {
            let (one_tx, one_rx) = oneshot::channel();
            let _ = tx.send((msg, one_tx)).await;

            match one_rx.await {
                Ok(_) => Ok(JsValue::TRUE),
                Err(_) => Err(JsValue::FALSE),
            }
        })
    }
}

struct ClientTask {
    rx: mpsc::Receiver<MessageRequest>,
    swim_client: SwimClient,
    on_load_callback: js_sys::Function,
    on_message_callback: js_sys::Function,
}

impl ClientTask {
    fn new(
        swim_client: SwimClient,
        rx: mpsc::Receiver<MessageRequest>,
        on_load_callback: js_sys::Function,
        on_message_callback: js_sys::Function,
    ) -> ClientTask {
        ClientTask {
            swim_client,
            rx,
            on_load_callback,
            on_message_callback,
        }
    }

    fn path() -> AbsolutePath {
        AbsolutePath::new(
            url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
            "/rooms",
            "chats",
        )
    }

    async fn run(self) {
        let ClientTask {
            rx,
            mut swim_client,
            on_load_callback,
            on_message_callback,
        } = self;

        let (_downlink, receiver) = swim_client
            .untyped_map_downlink(Self::path())
            .await
            .unwrap();
        let this = JsValue::NULL;

        let mut fused_requests = rx.fuse();
        let mut fused_msgs = receiver.fuse();

        loop {
            let req: Either<Option<Event<ViewWithEvent>>, Option<MessageRequest>> = select! {
                event = fused_msgs.next() => Either::Left(event),
                req = fused_requests.next() => Either::Right(req)
            };

            match req {
                Either::Left(Some(event)) => {
                    let inner = event.get_inner();
                    match &inner.event {
                        MapEvent::Initial => {
                            let records: js_sys::Array = inner.view.iter().fold(
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

                        MapEvent::Update(key) => {
                            let record = inner.view.get(&key);
                            let message = Message::try_from_value(&*record.unwrap()).unwrap();
                            let message = JsValue::from_serde(&message).unwrap();

                            let _r = on_message_callback.call1(&this, &message).unwrap();
                        }
                        _ => {}
                    }
                }
                Either::Right(Some((msg, tx))) => {
                    let target = AbsolutePath::new(
                        url::Url::parse("ws://127.0.0.1:9001/").unwrap(),
                        "/rooms",
                        "post",
                    );

                    let res = swim_client.send_command(target, msg).await.map_err(|_| ());
                    let _ = tx.send(res);
                }
                _ => break,
            }
        }
    }
}
