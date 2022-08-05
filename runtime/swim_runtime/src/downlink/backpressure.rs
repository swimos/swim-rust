use swim_api::protocol::{downlink::DownlinkOperationDecoder, map::RawMapOperationDecoder};
use tokio_util::codec::Decoder;

use crate::pressure::{BackpressureStrategy, MapBackpressure, ValueBackpressure};

pub trait DownlinkBackpressure: BackpressureStrategy {
    /// Decoder for operations received from the downlink implementation.
    type Dec: Decoder<Item = Self::Operation>;

    fn make_decoder() -> Self::Dec;
}

impl DownlinkBackpressure for ValueBackpressure {
    type Dec = DownlinkOperationDecoder;

    fn make_decoder() -> Self::Dec {
        Default::default()
    }
}

impl DownlinkBackpressure for MapBackpressure {
    type Dec = RawMapOperationDecoder;
    fn make_decoder() -> Self::Dec {
        Default::default()
    }
}
