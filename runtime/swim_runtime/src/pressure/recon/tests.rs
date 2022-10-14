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

use super::MapOperationReconEncoder;
use bytes::{Bytes, BytesMut};
use swim_api::protocol::map::{MapOperation, RawMapOperation, RawMapOperationMut};
use tokio_util::codec::Encoder;

#[test]
fn recon_encode_clear() {
    let op: RawMapOperationMut = MapOperation::Clear;
    let mut encoder = MapOperationReconEncoder;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(op, &mut buffer).is_ok());

    assert_eq!(buffer.as_ref(), b"@clear");
}

#[test]
fn recon_encode_remove() {
    let key = Bytes::from_static(b"5");
    let op: RawMapOperation = MapOperation::Remove { key };
    let mut encoder = MapOperationReconEncoder;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(op, &mut buffer).is_ok());

    assert_eq!(buffer.as_ref(), b"@remove(key:5)");
}

#[test]
fn recon_encode_update() {
    let key = Bytes::from_static(b"5");
    let value = BytesMut::from(b"data".as_slice());
    let op: RawMapOperation = MapOperation::Update { key, value };
    let mut encoder = MapOperationReconEncoder;
    let mut buffer = BytesMut::new();

    assert!(encoder.encode(op, &mut buffer).is_ok());

    assert_eq!(buffer.as_ref(), b"@update(key:5) data");
}
