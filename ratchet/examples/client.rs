// Copyright 2015-2021 SWIM.AI inc.
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

#[allow(warnings)]
use futures::SinkExt;
use ratchet::{owned::WebSocketClientBuilder, NoExtProxy};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    let builder = WebSocketClientBuilder::for_extension(NoExtProxy);

    let stream = TcpStream::connect("127.0.0.1:8080").await.unwrap();
    let result = builder.subscribe(stream, "wss://echo.websocket.org").await;

    match result {
        Ok(_) => println!("Ok"),
        Err(e) => println!("{}", e),
    }
}
