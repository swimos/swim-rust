// Copyright 2015-2023 Swim Inc.
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

use bytes::{Buf, BytesMut};
use futures_util::future::{select, Either};
use futures_util::{SinkExt, StreamExt};
use tokio::join;
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

use swim_api::agent::UplinkKind;
use swim_api::lane::WarpLaneKind;
use swim_api::protocol::agent::{
    LaneResponse, LaneResponseDecoder, LaneResponseEncoder, ValueLaneResponseEncoder,
};
use swim_utilities::io::byte_channel::{byte_channel, ByteReader, ByteWriter};
use swim_utilities::non_zero_usize;
use swim_utilities::trigger::trigger;

use crate::channels::{channels, ChannelsSender};

const CLOSED_CHANNEL: &str = "Channel closed early";
const INVALID_DATA: &str = "Channel received invalid data";
const MISSING_CHANNEL: &str = "Missing channel";
const BUFFER_SIZE: NonZeroUsize = non_zero_usize!(128);

struct BytesCodec;

impl Encoder<BytesMut> for BytesCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: BytesMut, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(item.as_ref());
        Ok(())
    }
}

impl Decoder for BytesCodec {
    type Item = BytesMut;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.has_remaining() {
            let rem = src.remaining();
            Ok(Some(src.split_to(rem)))
        } else {
            Ok(None)
        }
    }
}

async fn round_trip(elem: i32, tx: ByteWriter, rx: ByteReader) {
    let mut encoder = FramedWrite::new(tx, ValueLaneResponseEncoder::default());
    encoder
        .send(LaneResponse::StandardEvent(elem))
        .await
        .expect(CLOSED_CHANNEL);

    let mut decoder = FramedRead::new(rx, LaneResponseDecoder::new(BytesCodec));
    let received = decoder
        .next()
        .await
        .expect(CLOSED_CHANNEL)
        .expect(INVALID_DATA);

    assert_eq!(
        received,
        LaneResponse::StandardEvent(BytesMut::from_iter(format!("{}", elem).as_bytes()))
    );
}

async fn push_sender(
    sender: &ChannelsSender,
    name: &str,
    lane_kind: WarpLaneKind,
) -> (ByteWriter, ByteReader) {
    let (tx_in, rx_in) = byte_channel(BUFFER_SIZE);
    let (tx_out, rx_out) = byte_channel(BUFFER_SIZE);

    match lane_kind.uplink_kind() {
        UplinkKind::Value => {
            sender
                .push_value_channel(tx_in, rx_out, name.to_string())
                .await
        }
        UplinkKind::Map => {
            sender
                .push_map_channel(tx_in, rx_out, name.to_string())
                .await
        }
        UplinkKind::Supply => {
            panic!("Unexpected supply uplink")
        }
    }

    (tx_out, rx_in)
}

#[tokio::test]
async fn read_demux() {
    let (sender, mut receiver) = channels();
    let (stop_tx, mut stop_rx) = trigger();

    let agent = async move {
        let (tx, rx) = push_sender(&sender, "mock", WarpLaneKind::Value).await;
        round_trip(13, tx, rx).await;

        let (tx, rx) = push_sender(&sender, "mock2", WarpLaneKind::Value).await;
        round_trip(14, tx, rx).await;

        stop_tx.trigger();
    };

    let peer = async move {
        loop {
            match select(receiver.next(), stop_rx).await {
                Either::Left((Some((uri, response)), stop_receiver)) => {
                    stop_rx = stop_receiver;

                    receiver
                        .with_writer(&uri, |mut writer| async {
                            let mut encoder =
                                FramedWrite::new(&mut writer, LaneResponseEncoder::new(BytesCodec));
                            encoder
                                .send(response.expect_value())
                                .await
                                .expect(CLOSED_CHANNEL);
                            (writer, ())
                        })
                        .await
                        .expect(MISSING_CHANNEL);
                }
                Either::Left((None, _)) | Either::Right(_) => {
                    break;
                }
            }
        }
    };

    join!(agent, peer);
}
