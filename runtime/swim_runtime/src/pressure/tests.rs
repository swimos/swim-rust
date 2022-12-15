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

use bytes::{Bytes, BytesMut};
use swim_api::protocol::downlink::DownlinkOperation;

use crate::pressure::{BackpressureStrategy, SupplyBackpressure};

use super::ValueBackpressure;

#[test]
fn value_backpressure_empty() {
    let mut backpressure = ValueBackpressure::default();

    assert!(!BackpressureStrategy::has_data(&backpressure));

    let mut buffer = BytesMut::new();
    backpressure.prepare_write(&mut buffer);
    assert!(buffer.is_empty());
}

#[test]
fn supply_backpressure_empty() {
    let mut backpressure = SupplyBackpressure::default();

    assert!(!BackpressureStrategy::has_data(&backpressure));

    let mut buffer = BytesMut::new();
    backpressure.prepare_write(&mut buffer);
    assert!(buffer.is_empty());
}

#[test]
fn value_backpressure_write_direct() {
    let mut backpressure = ValueBackpressure::default();

    assert!(!BackpressureStrategy::has_data(&backpressure));

    let mut buffer = BytesMut::new();
    backpressure.write_direct(
        DownlinkOperation::new(Bytes::copy_from_slice(&[1, 2, 3])),
        &mut buffer,
    );

    assert_eq!(buffer.as_ref(), &[1, 2, 3]);
}

#[test]
fn supply_backpressure_write_direct() {
    let mut backpressure = SupplyBackpressure::default();

    assert!(!BackpressureStrategy::has_data(&backpressure));

    let mut buffer = BytesMut::new();
    backpressure.write_direct(
        DownlinkOperation::new(Bytes::copy_from_slice(&[1, 2, 3])),
        &mut buffer,
    );

    assert_eq!(buffer.as_ref(), &[1, 2, 3]);
}

#[test]
fn value_backpressure_push_data() {
    let mut backpressure = ValueBackpressure::default();

    assert!(!BackpressureStrategy::has_data(&backpressure));

    backpressure
        .push_operation(DownlinkOperation::new(Bytes::copy_from_slice(&[1, 2, 3])))
        .unwrap();
    assert!(BackpressureStrategy::has_data(&backpressure));

    let mut buffer = BytesMut::new();

    backpressure.prepare_write(&mut buffer);
    assert_eq!(buffer.as_ref(), &[1, 2, 3]);
    assert!(!BackpressureStrategy::has_data(&backpressure));
}

#[test]
fn supply_backpressure_push_data() {
    let mut backpressure = SupplyBackpressure::default();

    assert!(!BackpressureStrategy::has_data(&backpressure));

    backpressure
        .push_operation(DownlinkOperation::new(Bytes::copy_from_slice(&[1, 2, 3])))
        .unwrap();
    assert!(BackpressureStrategy::has_data(&backpressure));

    let mut buffer = BytesMut::new();

    backpressure.prepare_write(&mut buffer);
    assert_eq!(buffer.as_ref(), &[1, 2, 3]);
    assert!(!BackpressureStrategy::has_data(&backpressure));
}

#[test]
fn value_backpressure_push_multiple() {
    let mut backpressure = ValueBackpressure::default();

    assert!(!BackpressureStrategy::has_data(&backpressure));

    backpressure
        .push_operation(DownlinkOperation::new(Bytes::copy_from_slice(&[1, 2, 3])))
        .unwrap();
    backpressure
        .push_operation(DownlinkOperation::new(Bytes::copy_from_slice(&[4, 5, 6])))
        .unwrap();
    backpressure
        .push_operation(DownlinkOperation::new(Bytes::copy_from_slice(&[7, 8, 9])))
        .unwrap();
    assert!(BackpressureStrategy::has_data(&backpressure));

    let mut buffer = BytesMut::new();

    backpressure.prepare_write(&mut buffer);
    assert_eq!(buffer.as_ref(), &[7, 8, 9]);
    assert!(!BackpressureStrategy::has_data(&backpressure));
}

#[test]
fn supply_backpressure_push_multiple() {
    let mut backpressure = SupplyBackpressure::default();

    assert!(!BackpressureStrategy::has_data(&backpressure));

    backpressure
        .push_operation(DownlinkOperation::new(Bytes::copy_from_slice(&[1, 2, 3])))
        .unwrap();
    backpressure
        .push_operation(DownlinkOperation::new(Bytes::copy_from_slice(&[4, 5, 6])))
        .unwrap();
    backpressure
        .push_operation(DownlinkOperation::new(Bytes::copy_from_slice(&[7, 8, 9])))
        .unwrap();
    assert!(BackpressureStrategy::has_data(&backpressure));

    let mut buffer = BytesMut::new();

    backpressure.prepare_write(&mut buffer);
    assert_eq!(buffer.as_ref(), &[1, 2, 3]);
    assert!(BackpressureStrategy::has_data(&backpressure));

    backpressure.prepare_write(&mut buffer);
    assert_eq!(buffer.as_ref(), &[4, 5, 6]);
    assert!(BackpressureStrategy::has_data(&backpressure));

    backpressure.prepare_write(&mut buffer);
    assert_eq!(buffer.as_ref(), &[7, 8, 9]);
    assert!(!BackpressureStrategy::has_data(&backpressure));
}
