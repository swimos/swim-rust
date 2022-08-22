// Copyright 2015-2021 Swim Inc.
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

use swim_api::downlink::Downlink;
use swim_downlink::{value_downlink, DownlinkTask};
use swim_form::Form;
use tokio::sync::mpsc;

use super::DownlinkMessage;

async fn send_linked<T>(tx: &mut mpsc::Sender<DownlinkMessage<T>>) {
    let _ = tx.send(DownlinkMessage::Linked).await;
}

async fn send_synced<'a, T>(tx: &'a mut mpsc::Sender<DownlinkMessage<T>>, _value: &'a T) {
    let _ = tx.send(DownlinkMessage::Synced).await;
}

async fn send_event<'a, T: Clone>(tx: &'a mut mpsc::Sender<DownlinkMessage<T>>, value: &'a T) {
    let _ = tx.send(DownlinkMessage::Event(value.clone())).await;
}

async fn send_unlinked<T>(tx: &mut mpsc::Sender<DownlinkMessage<T>>) {
    let _ = tx.send(DownlinkMessage::Unlinked).await;
}

pub fn make_downlink<T>(
    tx: mpsc::Sender<DownlinkMessage<T>>,
    set_stream: mpsc::Receiver<T>,
) -> impl Downlink + Send
where
    T: Form + Clone + Send + Sync + 'static,
    T::Rec: Send,
{
    let model = value_downlink(set_stream).with_lifecycle(move |lc| {
        lc.with(tx.clone())
            .on_linked(send_linked)
            .on_synced(send_synced)
            .on_unlinked(send_unlinked)
            .on_event(send_event)
    });
    DownlinkTask::new(model)
}
