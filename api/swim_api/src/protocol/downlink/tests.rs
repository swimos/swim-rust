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

use crate::protocol::downlink::{
    DownlinkOperation, DownlinkOperationDecoder, DownlinkOperationEncoder, SimpleMessageEncoder,
    ValueNotificationDecoder,
};
use bytes::{Buf, Bytes, BytesMut};
use swim_form::structural::read::recognizer::RecognizerReadable;
use swim_form::Form;
use swim_model::Text;
use swim_recon::printer::print_recon_compact;
use tokio_util::codec::{Decoder, Encoder};

use super::{DownlinkNotification, DownlinkNotificationEncoder, EVENT, LINKED, SYNCED, UNLINKED};

fn encode_notification(notification: DownlinkNotification<&[u8]>) -> Bytes {
    let mut buffer = BytesMut::new();
    assert!(DownlinkNotificationEncoder::new(SimpleMessageEncoder)
        .encode(notification, &mut buffer)
        .is_ok());
    buffer.freeze()
}

fn round_trip<T: RecognizerReadable>(
    notification: DownlinkNotification<&[u8]>,
) -> DownlinkNotification<T> {
    let mut buffer = BytesMut::new();
    assert!(DownlinkNotificationEncoder::new(SimpleMessageEncoder)
        .encode(notification, &mut buffer)
        .is_ok());
    let mut decoder = ValueNotificationDecoder::default();
    let result = decoder.decode(&mut buffer);
    match result {
        Ok(Some(value)) => value,
        Ok(None) => {
            panic!("Incomplete.");
        }
        Err(e) => {
            panic!("Bad frame: {}", e);
        }
    }
}

#[test]
fn encode_linked_notification() {
    let mut buffer = encode_notification(DownlinkNotification::Linked);
    assert_eq!(buffer.len(), 1);
    assert_eq!(buffer.get_u8(), LINKED);
}

#[test]
fn encode_synced_notification() {
    let mut buffer = encode_notification(DownlinkNotification::Synced);
    assert_eq!(buffer.len(), 1);
    assert_eq!(buffer.get_u8(), SYNCED);
}

#[test]
fn encode_unlinked_notification() {
    let mut buffer = encode_notification(DownlinkNotification::Unlinked);
    assert_eq!(buffer.len(), 1);
    assert_eq!(buffer.get_u8(), UNLINKED);
}

#[test]
fn encode_event_notification() {
    let content = "content";
    let mut buffer = encode_notification(DownlinkNotification::Event {
        body: content.as_bytes(),
    });
    assert_eq!(buffer.len(), content.len() + 1);
    assert_eq!(buffer.get_u8(), EVENT);
    assert_eq!(buffer.as_ref(), content.as_bytes());
}

#[test]
fn decode_linked_notification() {
    let restored = round_trip::<Text>(DownlinkNotification::Linked);
    assert_eq!(restored, DownlinkNotification::Linked);
}

#[test]
fn decode_synced_notification() {
    let restored = round_trip::<Text>(DownlinkNotification::Synced);
    assert_eq!(restored, DownlinkNotification::Synced);
}

#[test]
fn decode_unlinked_notification() {
    let restored = round_trip::<Text>(DownlinkNotification::Unlinked);
    assert_eq!(restored, DownlinkNotification::Unlinked);
}

#[test]
fn decode_event_notification() {
    let content = "content";
    let event = DownlinkNotification::Event {
        body: content.as_bytes(),
    };
    let restored = round_trip(event);
    assert_eq!(
        restored,
        DownlinkNotification::Event {
            body: Text::new(content)
        }
    );
}

#[derive(Debug, Form, PartialEq, Eq)]
enum Message {
    Ping,
    CurrentValue(Text),
}

#[test]
fn decode_recon_notification() {
    let content = "content";
    let msg = Message::CurrentValue(Text::new(content));
    let recon = format!("{}", print_recon_compact(&msg));
    let event = DownlinkNotification::Event {
        body: recon.as_bytes(),
    };
    let restored = round_trip::<Message>(event);
    assert_eq!(
        restored,
        DownlinkNotification::Event {
            body: Message::CurrentValue(Text::new(content))
        }
    );
}

#[test]
fn decode_recon_notification_twice() {
    let content = "content";
    let msg1 = Message::CurrentValue(Text::new(content));
    let recon1 = format!("{}", print_recon_compact(&msg1));
    let event1 = DownlinkNotification::Event {
        body: recon1.as_bytes(),
    };

    let msg2 = Message::Ping;
    let recon2 = format!("{}", print_recon_compact(&msg2));
    let event2 = DownlinkNotification::Event {
        body: recon2.as_bytes(),
    };

    let mut buffer = BytesMut::new();
    assert!(DownlinkNotificationEncoder::new(SimpleMessageEncoder)
        .encode(event1, &mut buffer)
        .is_ok());
    assert!(DownlinkNotificationEncoder::new(SimpleMessageEncoder)
        .encode(event2, &mut buffer)
        .is_ok());
    let mut decoder = ValueNotificationDecoder::default();

    let restored1 = decoder.decode(&mut buffer);

    match restored1 {
        Ok(Some(value)) => {
            assert_eq!(
                value,
                DownlinkNotification::Event {
                    body: Message::CurrentValue(Text::new(content))
                }
            );
        }
        ow => {
            panic!("Failed: {:?}", ow);
        }
    }

    let restored2 = decoder.decode(&mut buffer);

    match restored2 {
        Ok(Some(value)) => {
            assert_eq!(
                value,
                DownlinkNotification::Event {
                    body: Message::Ping
                }
            );
        }
        ow => {
            panic!("Failed: {:?}", ow);
        }
    }
}

#[test]
fn encode_operation() {
    let content = "body";
    let mut buffer = BytesMut::new();
    assert!(DownlinkOperationEncoder
        .encode(DownlinkOperation::new(Text::new("body")), &mut buffer)
        .is_ok());

    assert_eq!(buffer.len(), content.len() + 8);
    assert_eq!(buffer.get_u64() as usize, content.len());
    assert_eq!(buffer.as_ref(), content.as_bytes());
}

#[test]
fn encode_complex_operation() {
    let content = "body";
    let op = Message::CurrentValue(Text::new(content));
    let recon = format!("{}", print_recon_compact(&op));
    let mut buffer = BytesMut::new();
    assert!(DownlinkOperationEncoder
        .encode(DownlinkOperation::new(op), &mut buffer)
        .is_ok());

    assert_eq!(buffer.len(), recon.len() + 8);
    assert_eq!(buffer.get_u64() as usize, recon.len());
    assert_eq!(buffer.as_ref(), recon.as_bytes());
}

#[test]
fn decode_operation() {
    let content = "body";
    let mut buffer = BytesMut::new();
    assert!(DownlinkOperationEncoder
        .encode(DownlinkOperation::new(Text::new("body")), &mut buffer)
        .is_ok());

    match DownlinkOperationDecoder.decode(&mut buffer) {
        Ok(Some(DownlinkOperation { body })) => {
            assert_eq!(body.as_ref(), content.as_bytes());
        }
        Ok(None) => {
            panic!("Incomplete.")
        }
        Err(e) => {
            panic!("Failed: {}", e);
        }
    }
}

#[test]
fn decode_complex_operation() {
    let content = "body";
    let op = Message::CurrentValue(Text::new(content));
    let recon = format!("{}", print_recon_compact(&op));
    let mut buffer = BytesMut::new();
    assert!(DownlinkOperationEncoder
        .encode(DownlinkOperation::new(op), &mut buffer)
        .is_ok());

    match DownlinkOperationDecoder.decode(&mut buffer) {
        Ok(Some(DownlinkOperation { body })) => {
            assert_eq!(body.as_ref(), recon.as_bytes());
        }
        Ok(None) => {
            panic!("Incomplete.")
        }
        Err(e) => {
            panic!("Failed: {}", e);
        }
    }
}
