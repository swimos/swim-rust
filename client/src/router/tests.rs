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

use std::{thread, time};

use common::sink::item::ItemSink;
use common::warp::envelope::Envelope;
use common::warp::path::AbsolutePath;

use crate::router::{Router, SwimRouter};

#[tokio::test(core_threads = 2)]
async fn foo() {
    let mut router = SwimRouter::new(5).await;

    let path = AbsolutePath::new("ws://127.0.0.1:9001", "foo", "bar");
    let (mut sink, _stream) = router.connection_for(&path).await;

    let sync = Envelope::sync(String::from("node_uri"), String::from("lane_uri"));

    // thread::sleep(time::Duration::from_secs(5));
    sink.send_item(sync).await.unwrap();

    // loop {
    //     println!("{:?}", stream.recv().await.unwrap());
    // }

    thread::sleep(time::Duration::from_secs(5));
    router.close().await;
    thread::sleep(time::Duration::from_secs(5));
}
