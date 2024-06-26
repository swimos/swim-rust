// Copyright 2015-2024 Swim Inc.
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
use serde::{Deserialize, Serialize};

use crate::lanes::http::{content_type::recon, CodecError, HttpLaneCodec, HttpLaneCodecSupport};

use super::Json;

#[test]
fn json_supports() {
    let json_codec = Json;
    assert!(json_codec.supports(&mime::APPLICATION_JSON));
    assert!(!json_codec.supports(recon()));
}

#[test]
fn json_select() {
    let recon_ct = recon().clone();
    let app_star: Mime = "application/*".parse().unwrap();
    let any = mime::STAR_STAR.clone();
    let json = mime::APPLICATION_JSON.clone();

    let json_codec = Json;
    assert_eq!(json_codec.select_codec(&[]), Some(&mime::APPLICATION_JSON));
    assert_eq!(
        json_codec.select_codec(&[json]),
        Some(&mime::APPLICATION_JSON)
    );
    assert_eq!(
        json_codec.select_codec(&[app_star]),
        Some(&mime::APPLICATION_JSON)
    );
    assert_eq!(
        json_codec.select_codec(&[any]),
        Some(&mime::APPLICATION_JSON)
    );
    assert_eq!(json_codec.select_codec(&[recon_ct]), None);
}

#[test]
fn unsupported_encoding() {
    let json_codec = Json;
    let mut buffer = BytesMut::new();
    assert!(matches!(
        json_codec.encode(recon(), &3, &mut buffer),
        Err(CodecError::UnsupportedContentType(_))
    ));
}

#[test]
fn json_encoding() {
    json_round_trip(&56i32);
    json_round_trip(&"hello".to_string());
}

fn json_round_trip<T>(value: &T)
where
    T: Serialize + for<'a> Deserialize<'a> + Eq + Debug,
{
    let json_codec = Json;
    let mut buffer = BytesMut::new();
    assert!(json_codec
        .encode(&mime::APPLICATION_JSON, value, &mut buffer)
        .is_ok());

    let restored: T = json_codec
        .decode(&mime::APPLICATION_JSON, buffer.as_ref())
        .expect("Decode failed.");
    assert_eq!(&restored, value);
}
