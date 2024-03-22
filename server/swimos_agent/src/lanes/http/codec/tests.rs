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

use std::fmt::Debug;

use bytes::{Buf, BufMut, BytesMut};
use frunk::{hlist, HNil};
use mime::Mime;
use swimos_form::Form;
use thiserror::Error;

use crate::lanes::http::{content_type::recon, CodecError};

use super::{HttpLaneCodec, HttpLaneCodecSupport, Recon};

#[test]
fn recon_supports() {
    let recon_codec = Recon;
    assert!(recon_codec.supports(recon()));
    assert!(!recon_codec.supports(&mime::APPLICATION_JSON));
}

#[test]
fn recon_select() {
    let recon_ct = recon().clone();
    let app_star: Mime = "application/*".parse().unwrap();
    let any = mime::STAR_STAR.clone();
    let json = mime::APPLICATION_JSON.clone();

    let recon_codec = Recon;
    assert_eq!(recon_codec.select_codec(&[]), Some(recon()));
    assert_eq!(recon_codec.select_codec(&[recon_ct]), Some(recon()));
    assert_eq!(recon_codec.select_codec(&[app_star]), Some(recon()));
    assert_eq!(recon_codec.select_codec(&[any]), Some(recon()));
    assert_eq!(recon_codec.select_codec(&[json]), None);
}

#[test]
fn unsupported_encoding() {
    let recon_codec = Recon;
    let mut buffer = BytesMut::new();
    assert!(matches!(
        recon_codec.encode(&mime::APPLICATION_JSON, &3, &mut buffer),
        Err(CodecError::UnsupportedContentType(_))
    ));
}

#[test]
fn recon_encoding() {
    recon_round_trip(&56i32);
    recon_round_trip(&"hello".to_string());
}

fn recon_round_trip<T>(value: &T)
where
    T: Form + Eq + Debug,
{
    let recon_codec = Recon;
    let mut buffer = BytesMut::new();
    assert!(recon_codec.encode(recon(), value, &mut buffer).is_ok());

    let restored: T = recon_codec
        .decode(recon(), buffer.as_ref())
        .expect("Decode failed.");
    assert_eq!(&restored, value);
}

fn test_ct1() -> mime::Mime {
    "example/a".parse().unwrap()
}

fn test_ct2() -> mime::Mime {
    "example/b".parse().unwrap()
}

#[derive(Clone, Debug)]
struct CodecA(mime::Mime);

impl Default for CodecA {
    fn default() -> Self {
        Self(test_ct1())
    }
}

#[derive(Clone, Debug)]
struct CodecB(mime::Mime);

impl Default for CodecB {
    fn default() -> Self {
        Self(test_ct2())
    }
}

impl HttpLaneCodecSupport for CodecA {
    fn supports(&self, content_type: &Mime) -> bool {
        content_type == &self.0
    }

    fn select_codec<'a>(&self, accepts: &'a [Mime]) -> Option<&'a Mime> {
        accepts.iter().find(|ct| **ct == self.0)
    }
}

#[derive(Debug, Error)]
#[error("Bad data.")]
struct BadData;

impl HttpLaneCodec<i32> for CodecA {
    fn encode(
        &self,
        content_type: &Mime,
        value: &i32,
        buffer: &mut BytesMut,
    ) -> Result<(), CodecError> {
        if self.supports(content_type) {
            buffer.reserve(5);
            buffer.put_u8(0);
            buffer.put_i32(*value);
            Ok(())
        } else {
            Err(CodecError::UnsupportedContentType(content_type.clone()))
        }
    }

    fn decode(&self, content_type: &Mime, mut buffer: &[u8]) -> Result<i32, CodecError> {
        if self.supports(content_type) {
            if buffer.len() == 5 {
                if buffer.get_u8() == 0 {
                    Ok(buffer.get_i32())
                } else {
                    Err(CodecError::EncodingError(Box::new(BadData)))
                }
            } else {
                Err(CodecError::EncodingError(Box::new(BadData)))
            }
        } else {
            Err(CodecError::UnsupportedContentType(content_type.clone()))
        }
    }
}

impl HttpLaneCodecSupport for CodecB {
    fn supports(&self, content_type: &Mime) -> bool {
        content_type == &self.0
    }

    fn select_codec<'a>(&self, accepts: &'a [Mime]) -> Option<&'a Mime> {
        accepts.iter().find(|ct| **ct == self.0)
    }
}

impl HttpLaneCodec<i32> for CodecB {
    fn encode(
        &self,
        content_type: &Mime,
        value: &i32,
        buffer: &mut BytesMut,
    ) -> Result<(), CodecError> {
        if self.supports(content_type) {
            buffer.reserve(5);
            buffer.put_u8(1);
            buffer.put_i32(*value);
            Ok(())
        } else {
            Err(CodecError::UnsupportedContentType(content_type.clone()))
        }
    }

    fn decode(&self, content_type: &Mime, mut buffer: &[u8]) -> Result<i32, CodecError> {
        if self.supports(content_type) {
            if buffer.len() == 5 {
                if buffer.get_u8() == 1 {
                    Ok(buffer.get_i32())
                } else {
                    Err(CodecError::EncodingError(Box::new(BadData)))
                }
            } else {
                Err(CodecError::EncodingError(Box::new(BadData)))
            }
        } else {
            Err(CodecError::UnsupportedContentType(content_type.clone()))
        }
    }
}

#[test]
fn hnil_codec() {
    let ct1 = test_ct1();
    let ct2 = test_ct2();
    let codec = HNil;
    assert!(!codec.supports(&ct1));
    assert!(!codec.supports(&ct2));

    let mut buffer = BytesMut::new();
    assert!(matches!(
        codec.encode(&ct1, &1, &mut buffer),
        Err(CodecError::UnsupportedContentType(_))
    ));
    assert!(matches!(
        codec.encode(&ct2, &1, &mut buffer),
        Err(CodecError::UnsupportedContentType(_))
    ));
}

#[test]
fn hcons_codec_support() {
    let ct1 = test_ct1();
    let ct2 = test_ct2();
    let codec = hlist![CodecA::default(), CodecB::default()];

    assert!(codec.supports(&ct1));
    assert!(codec.supports(&ct2));
    assert!(!codec.supports(recon()));
}

#[test]
fn hcons_codec_encoding() {
    let ct1 = test_ct1();
    let ct2 = test_ct2();
    let codec = hlist![CodecA::default(), CodecB::default()];

    let mut buffer = BytesMut::new();

    assert!(codec.encode(&ct1, &5, &mut buffer).is_ok());
    let bytes = buffer.as_ref();
    assert_eq!(bytes.len(), 5);
    assert_eq!(bytes[0], 0);
    let restored = codec.decode(&ct1, bytes).expect("Decode failed.");
    assert_eq!(restored, 5);

    buffer.clear();

    assert!(codec.encode(&ct2, &7, &mut buffer).is_ok());
    let bytes = buffer.as_ref();
    assert_eq!(bytes.len(), 5);
    assert_eq!(bytes[0], 1);
    let restored = codec.decode(&ct2, bytes).expect("Decode failed.");
    assert_eq!(restored, 7);
}
