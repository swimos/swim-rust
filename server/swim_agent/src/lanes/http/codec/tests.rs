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

use bytes::BytesMut;
use mime::Mime;
use swim_form::Form;

use crate::lanes::http::{content_type::recon, CodecError};

use super::{Recon, HttpLaneCodec, HttpLaneCodecSupport};

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
    assert!(matches!(recon_codec.encode(&mime::APPLICATION_JSON, &3, &mut buffer), Err(CodecError::UnsupportedContentType(_))));
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

    let restored: T = recon_codec.decode(recon(), buffer.as_ref()).expect("Decode failed.");
    assert_eq!(&restored, value);
}