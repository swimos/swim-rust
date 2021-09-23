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

mod error;
mod handshake;

use crate::error::DeflateExtensionError;
use crate::handshake::negotiate_client;
use flate2::{Compress, Compression, Decompress};
use ratchet_ext::{
    Extension, ExtensionDecoder, ExtensionEncoder, ExtensionProvider, FrameHeader, Header,
    HeaderMap, HeaderValue, ReunitableExtension, SplittableExtension,
};

/// The minimum size of the LZ77 sliding window size.
const LZ77_MIN_WINDOW_SIZE: u8 = 8;

/// The maximum size of the LZ77 sliding window size. Absence of the `max_window_bits` parameter
/// indicates that the client can receive messages compressed using an LZ77 sliding window of up to
/// 32,768 bytes. RFC 7692 7.1.2.1.
const LZ77_MAX_WINDOW_SIZE: u8 = 15;

#[derive(Default)]
pub struct DeflateExtProvider {
    config: DeflateConfig,
}

impl DeflateExtProvider {
    pub fn with_config(config: DeflateConfig) -> DeflateExtProvider {
        DeflateExtProvider { config }
    }
}

impl ExtensionProvider for DeflateExtProvider {
    type Extension = Deflate;
    type Error = DeflateExtensionError;

    fn apply_headers(&self, _headers: &mut HeaderMap) {
        todo!()
    }

    fn negotiate_client(&self, headers: &[Header]) -> Result<Option<Self::Extension>, Self::Error> {
        negotiate_client(headers, &self.config)
    }

    fn negotiate_server(
        &self,
        _headers: &[Header],
    ) -> Result<Option<(Self::Extension, Option<HeaderValue>)>, Self::Error> {
        unimplemented!()
    }
}

/// A permessage-deflate configuration.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DeflateConfig {
    /// The client's LZ77 sliding window size. Negotiated during the HTTP upgrade. In client mode,
    /// this conforms to RFC 7692 7.1.2.1. In server mode, this conforms to RFC 7692 7.1.2.2. Must
    /// be in range 8..15 inclusive.
    server_max_window_bits: u8,
    /// The client's LZ77 sliding window size. Negotiated during the HTTP upgrade. In client mode,
    /// this conforms to RFC 7692 7.1.2.2. In server mode, this conforms to RFC 7692 7.1.2.2. Must
    /// be in range 8..15 inclusive.
    client_max_window_bits: u8,
    /// Request that the server resets the LZ77 sliding window between messages - RFC 7692 7.1.1.1.
    request_no_context_takeover: bool,
    /// Whether to accept `no_context_takeover`.
    accept_no_context_takeover: bool,
    /// The active compression level. The integer here is typically on a scale of 0-9 where 0 means
    /// "no compression" and 9 means "take as long as you'd like".
    compression_level: Compression,
}

impl Default for DeflateConfig {
    fn default() -> Self {
        DeflateConfig {
            server_max_window_bits: LZ77_MAX_WINDOW_SIZE,
            client_max_window_bits: LZ77_MAX_WINDOW_SIZE,
            request_no_context_takeover: false,
            accept_no_context_takeover: true,
            compression_level: Compression::best(),
        }
    }
}

/// A permessage-deflate configuration.
#[derive(Debug, PartialEq)]
struct InitialisedDeflateConfig {
    server_max_window_bits: u8,
    client_max_window_bits: u8,
    compress_reset: bool,
    decompress_reset: bool,
    compression_level: Compression,
}

#[derive(Debug)]
pub struct Deflate {
    encoder: DeflateEncoder,
    decoder: DeflateDecoder,
}

impl Deflate {
    fn initialise_from(config: InitialisedDeflateConfig) -> Deflate {
        Deflate {
            decoder: DeflateDecoder::new(config.server_max_window_bits),
            encoder: DeflateEncoder::new(config.compression_level, config.client_max_window_bits),
        }
    }
}

impl Extension for Deflate {
    type Encoder = DeflateEncoder;
    type Decoder = DeflateDecoder;

    fn encoder(&mut self) -> &mut Self::Encoder {
        &mut self.encoder
    }

    fn decoder(&mut self) -> &mut Self::Decoder {
        &mut self.decoder
    }
}

impl SplittableExtension for Deflate {
    type SplitEncoder = DeflateEncoder;
    type SplitDecoder = DeflateDecoder;

    fn split(self) -> (Self::Encoder, Self::Decoder) {
        let Deflate { encoder, decoder } = self;
        (encoder, decoder)
    }
}

impl ReunitableExtension for Deflate {
    fn reunite(encoder: Self::Encoder, decoder: Self::Decoder) -> Self {
        Deflate { encoder, decoder }
    }
}

#[derive(Debug)]
pub struct DeflateEncoder {
    compress: Compress,
}

impl DeflateEncoder {
    fn new(compression: Compression, mut window_size: u8) -> DeflateEncoder {
        // https://github.com/madler/zlib/blob/cacf7f1d4e3d44d871b605da3b647f07d718623f/deflate.c#L303
        if window_size == 8 {
            window_size = 9;
        }

        DeflateEncoder {
            compress: Compress::new_with_window_bits(compression, false, window_size),
        }
    }
}

impl ExtensionEncoder for DeflateEncoder {
    type Error = DeflateExtensionError;

    fn encode<A>(&mut self, _payload: A, _header: &mut FrameHeader) -> Result<(), Self::Error>
    where
        A: AsMut<[u8]>,
    {
        todo!()
    }
}

#[derive(Debug)]
pub struct DeflateDecoder {
    decompress: Decompress,
}

impl DeflateDecoder {
    fn new(mut window_size: u8) -> DeflateDecoder {
        // https://github.com/madler/zlib/blob/cacf7f1d4e3d44d871b605da3b647f07d718623f/deflate.c#L303
        if window_size == 8 {
            window_size = 9;
        }

        DeflateDecoder {
            decompress: Decompress::new_with_window_bits(false, window_size),
        }
    }
}

impl ExtensionDecoder for DeflateDecoder {
    type Error = DeflateExtensionError;

    fn decode<A>(&mut self, _payload: A, _header: &mut FrameHeader) -> Result<(), Self::Error>
    where
        A: AsMut<[u8]>,
    {
        todo!()
    }
}

#[test]
fn t() {}
