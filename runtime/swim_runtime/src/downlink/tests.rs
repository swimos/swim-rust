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

use std::num::NonZeroUsize;

use super::{AttachAction, DownlinkOptions};
use futures::{SinkExt, StreamExt};
use swim_api::error::DownlinkTaskError;
use swim_api::protocol::{
    DownlinkNotifiationDecoder, DownlinkNotification, DownlinkOperation, DownlinkOperationEncoder,
};
use swim_form::structural::write::write_by_ref;
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_form::Form;
use swim_model::Text;
use swim_utilities::algebra::non_zero_usize;
use swim_utilities::io::byte_channel;
use swim_utilities::trigger;
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};

#[derive(Debug, Form)]
enum Message {
    Ping,
    CurrentValue(Text),
}

const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(1024);

enum State {
    Unlinked,
    Linked,
    Synced,
}

async fn run_fake_downlink(
    sub: mpsc::Sender<AttachAction>,
    options: DownlinkOptions,
    stop: trigger::Receiver,
) -> Result<(), DownlinkTaskError> {
    let (tx_in, rx_in) = byte_channel::byte_channel(BUFFER_SIZE);
    let (tx_out, rx_out) = byte_channel::byte_channel(BUFFER_SIZE);
    if sub
        .send(AttachAction::new(rx_out, tx_in, options))
        .await
        .is_err()
    {
        return Err(DownlinkTaskError::FailedToStart);
    }
    let mut state = State::Unlinked;
    let mut read = FramedRead::new(
        rx_in,
        DownlinkNotifiationDecoder::new(Message::make_recognizer()),
    )
    .take_until(stop);

    let mut write = FramedWrite::new(tx_out, DownlinkOperationEncoder);

    let mut current = Text::default();

    while let Some(message) = read.next().await.transpose()? {
        match state {
            State::Unlinked => match message {
                DownlinkNotification::Linked => {
                    state = State::Linked;
                }
                DownlinkNotification::Synced => {
                    state = State::Synced;
                }
                _ => {}
            },
            State::Linked => match message {
                DownlinkNotification::Synced => {
                    state = State::Synced;
                }
                DownlinkNotification::Unlinked => {
                    state = State::Unlinked;
                }
                DownlinkNotification::Event {
                    body: Message::CurrentValue(v),
                } => {
                    current = v;
                }
                _ => {}
            },
            State::Synced => match message {
                DownlinkNotification::Unlinked => {
                    state = State::Unlinked;
                }
                DownlinkNotification::Event {
                    body: Message::CurrentValue(v),
                } => {
                    current = v;
                }
                DownlinkNotification::Event {
                    body: Message::Ping,
                } => {
                    if write
                        .send(DownlinkOperation { body: write_by_ref(&current) })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                _ => {}
            },
        }
    }
    Ok(())
}
