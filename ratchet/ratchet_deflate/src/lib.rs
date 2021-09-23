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
use crate::handshake::{apply_headers, negotiate_client, negotiate_server};
use bytes::BytesMut;
use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};
use ratchet_ext::{
    ExtensionDecoder, ExtensionEncoder, ExtensionProvider, FrameHeader, Header, HeaderMap,
    HeaderValue, ReunitableExtension, SplittableExtension,
};
use std::slice;

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

    fn apply_headers(&self, headers: &mut HeaderMap) {
        apply_headers(headers, &self.config);
    }

    fn negotiate_client(&self, headers: &[Header]) -> Result<Option<Self::Extension>, Self::Error> {
        negotiate_client(headers, &self.config)
    }

    fn negotiate_server(
        &self,
        headers: &[Header],
    ) -> Result<Option<(Self::Extension, HeaderValue)>, Self::Error> {
        negotiate_server(headers, &self.config)
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

impl InitialisedDeflateConfig {
    fn from_config(config: &DeflateConfig) -> InitialisedDeflateConfig {
        InitialisedDeflateConfig {
            server_max_window_bits: config.server_max_window_bits,
            client_max_window_bits: config.client_max_window_bits,
            compress_reset: false,
            decompress_reset: false,
            compression_level: config.compression_level,
        }
    }
}

#[derive(Debug)]
pub struct Deflate {
    encoder: DeflateEncoder,
    decoder: DeflateDecoder,
}

impl Deflate {
    fn initialise_from(config: InitialisedDeflateConfig) -> Deflate {
        Deflate {
            decoder: DeflateDecoder::new(config.server_max_window_bits, config.decompress_reset),
            encoder: DeflateEncoder::new(
                config.compression_level,
                config.client_max_window_bits,
                config.compress_reset,
            ),
        }
    }
}

impl SplittableExtension for Deflate {
    type SplitEncoder = DeflateEncoder;
    type SplitDecoder = DeflateDecoder;

    fn split(self) -> (Self::SplitEncoder, Self::SplitDecoder) {
        let Deflate { encoder, decoder } = self;
        (encoder, decoder)
    }
}

impl ReunitableExtension for Deflate {
    fn reunite(encoder: Self::SplitEncoder, decoder: Self::SplitDecoder) -> Self {
        Deflate { encoder, decoder }
    }
}

#[derive(Debug)]
pub struct DeflateEncoder {
    buf: BytesMut,
    compress: Compress,
    compress_reset: bool,
}

impl DeflateEncoder {
    fn new(compression: Compression, mut window_size: u8, compress_reset: bool) -> DeflateEncoder {
        // https://github.com/madler/zlib/blob/cacf7f1d4e3d44d871b605da3b647f07d718623f/deflate.c#L303
        if window_size == 8 {
            window_size = 9;
        }

        DeflateEncoder {
            buf: BytesMut::default(),
            compress: Compress::new_with_window_bits(compression, false, window_size),
            compress_reset,
        }
    }
}

impl ExtensionEncoder for Deflate {
    type Error = DeflateExtensionError;

    fn encode(
        &mut self,
        payload: &mut BytesMut,
        header: &mut FrameHeader,
    ) -> Result<(), Self::Error> {
        self.encoder.encode(payload, header)
    }
}

impl ExtensionEncoder for DeflateEncoder {
    type Error = DeflateExtensionError;

    fn encode(
        &mut self,
        payload: &mut BytesMut,
        header: &mut FrameHeader,
    ) -> Result<(), Self::Error> {
        let DeflateEncoder {
            buf,
            compress,
            compress_reset,
        } = self;

        buf.clear();
        buf.reserve(payload.len());

        while compress.total_in() < payload.len() as u64 {
            let at = compress.total_in() as usize;

            let cap = buf.capacity();
            let len = buf.len();

            let compress_result = unsafe {
                let before = compress.total_out();
                let ret = {
                    let ptr = buf.as_mut_ptr().offset(len as isize);
                    let out = slice::from_raw_parts_mut(ptr, cap - len);
                    compress.compress(&payload[at..], out, FlushCompress::None)
                };
                buf.set_len((compress.total_out() - before) as usize + len);
                ret
            }?;

            match compress_result {
                Status::BufError => buf.reserve(2048),
                Status::Ok => continue,
                Status::StreamEnd => break,
            }
        }

        buf.truncate(buf.len() - 4);
        std::mem::swap(payload, buf);

        if *compress_reset {
            compress.reset();
        }

        header.rsv1 = true;

        Ok(())
    }
}

#[derive(Debug)]
pub struct DeflateDecoder {
    buf: BytesMut,
    decompress: Decompress,
    decompress_reset: bool,
}

impl DeflateDecoder {
    fn new(mut window_size: u8, decompress_reset: bool) -> DeflateDecoder {
        // https://github.com/madler/zlib/blob/cacf7f1d4e3d44d871b605da3b647f07d718623f/deflate.c#L303
        if window_size == 8 {
            window_size = 9;
        }

        DeflateDecoder {
            buf: BytesMut::default(),
            decompress: Decompress::new_with_window_bits(false, window_size),
            decompress_reset,
        }
    }
}

impl ExtensionDecoder for Deflate {
    type Error = DeflateExtensionError;

    fn decode(
        &mut self,
        payload: &mut BytesMut,
        header: &mut FrameHeader,
    ) -> Result<(), Self::Error> {
        self.decoder.decode(payload, header)
    }
}

impl ExtensionDecoder for DeflateDecoder {
    type Error = DeflateExtensionError;

    fn decode(
        &mut self,
        payload: &mut BytesMut,
        header: &mut FrameHeader,
    ) -> Result<(), Self::Error> {
        let DeflateDecoder {
            buf,
            decompress,
            decompress_reset,
        } = self;
        payload.extend_from_slice(&[0, 0, 0xFF, 0xFF]);

        buf.clear();
        buf.reserve(payload.len());

        while decompress.total_in() < payload.len() as u64 {
            let at = decompress.total_in() as usize;

            let cap = buf.capacity();
            let len = buf.len();

            let decompress_result = unsafe {
                let before = decompress.total_out();
                let ret = {
                    let ptr = buf.as_mut_ptr().offset(len as isize);
                    let out = slice::from_raw_parts_mut(ptr, cap - len);
                    decompress.decompress(&payload[at..], out, FlushDecompress::None)
                };
                buf.set_len((decompress.total_out() - before) as usize + len);
                ret
            }?;

            match decompress_result {
                Status::BufError => buf.reserve(2048),
                Status::Ok => continue,
                Status::StreamEnd => break,
            }
        }

        std::mem::swap(payload, buf);

        if *decompress_reset {
            decompress.reset(true);
        }

        header.rsv1 = true;
        Ok(())
    }
}

#[test]
fn t() {}
