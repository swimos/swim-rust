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

use bytes::{BufMut, Bytes, BytesMut};

use super::MapOperationQueue;
use swim_api::protocol::map::RawMapOperation;

fn bytes_of(text: &str) -> Bytes {
    let mut buf = BytesMut::with_capacity(text.len());
    buf.put(text.as_bytes());
    buf.freeze()
}

#[test]
fn empty_queue() {
    let mut queue = MapOperationQueue::default();
    assert!(queue.is_empty());
    assert_eq!(queue.len(), 0);
    assert!(queue.pop().is_none());
}

#[test]
fn simple_push_pop_upd() {
    let mut queue = MapOperationQueue::default();
    let upd = RawMapOperation::Update {
        key: bytes_of("key1"),
        value: bytes_of("value"),
    };
    assert!(queue.push(upd.clone()).is_ok());
    assert!(!queue.is_empty());
    assert_eq!(queue.len(), 1);

    assert_eq!(queue.pop(), Some(upd));
    assert!(queue.is_empty());
    assert_eq!(queue.len(), 0);
}

#[test]
fn simple_push_pop_rem() {
    let mut queue = MapOperationQueue::default();
    let rem = RawMapOperation::Remove {
        key: bytes_of("key1"),
    };
    assert!(queue.push(rem.clone()).is_ok());
    assert!(!queue.is_empty());
    assert_eq!(queue.len(), 1);

    assert_eq!(queue.pop(), Some(rem));
    assert!(queue.is_empty());
    assert_eq!(queue.len(), 0);
}

#[test]
fn simple_push_pop_clear() {
    let mut queue = MapOperationQueue::default();
    let clear = RawMapOperation::Clear;
    assert!(queue.push(clear.clone()).is_ok());
    assert!(!queue.is_empty());
    assert_eq!(queue.len(), 1);

    assert_eq!(queue.pop(), Some(clear));
    assert!(queue.is_empty());
    assert_eq!(queue.len(), 0);
}

#[test]
fn push_pop_different_keys() {
    let mut queue = MapOperationQueue::default();
    let op1 = RawMapOperation::Update {
        key: bytes_of("key1"),
        value: bytes_of("value1"),
    };
    let op2 = RawMapOperation::Remove {
        key: bytes_of("key2"),
    };
    let op3 = RawMapOperation::Update {
        key: bytes_of("key3"),
        value: bytes_of("value3"),
    };

    assert!(queue.push(op1.clone()).is_ok());
    assert!(!queue.is_empty());
    assert_eq!(queue.len(), 1);

    assert!(queue.push(op2.clone()).is_ok());
    assert!(!queue.is_empty());
    assert_eq!(queue.len(), 2);

    assert!(queue.push(op3.clone()).is_ok());
    assert!(!queue.is_empty());
    assert_eq!(queue.len(), 3);

    assert_eq!(queue.pop(), Some(op1));
    assert!(!queue.is_empty());
    assert_eq!(queue.len(), 2);

    assert_eq!(queue.pop(), Some(op2));
    assert!(!queue.is_empty());
    assert_eq!(queue.len(), 1);

    assert_eq!(queue.pop(), Some(op3));
    assert!(queue.is_empty());
    assert_eq!(queue.len(), 0);
}

#[test]
fn replace_upd() {
    let mut queue = MapOperationQueue::default();
    let upd1 = RawMapOperation::Update {
        key: bytes_of("key"),
        value: bytes_of("value1"),
    };
    let upd2 = RawMapOperation::Update {
        key: bytes_of("key"),
        value: bytes_of("value2"),
    };
    assert!(queue.push(upd1).is_ok());
    assert!(!queue.is_empty());
    assert_eq!(queue.len(), 1);

    assert!(queue.push(upd2.clone()).is_ok());
    assert!(!queue.is_empty());
    assert_eq!(queue.len(), 1);

    assert_eq!(queue.pop(), Some(upd2));
    assert!(queue.is_empty());
    assert_eq!(queue.len(), 0);
}

#[test]
fn remove_over_upd() {
    let mut queue = MapOperationQueue::default();
    let op1 = RawMapOperation::Update {
        key: bytes_of("key"),
        value: bytes_of("value1"),
    };
    let op2 = RawMapOperation::Remove {
        key: bytes_of("key"),
    };
    assert!(queue.push(op1).is_ok());
    assert!(!queue.is_empty());
    assert_eq!(queue.len(), 1);

    assert!(queue.push(op2.clone()).is_ok());
    assert!(!queue.is_empty());
    assert_eq!(queue.len(), 1);

    assert_eq!(queue.pop(), Some(op2));
    assert!(queue.is_empty());
    assert_eq!(queue.len(), 0);
}

#[test]
fn update_over_remove() {
    let mut queue = MapOperationQueue::default();

    let op1 = RawMapOperation::Remove {
        key: bytes_of("key"),
    };
    let op2 = RawMapOperation::Update {
        key: bytes_of("key"),
        value: bytes_of("value1"),
    };
    assert!(queue.push(op1).is_ok());
    assert!(!queue.is_empty());
    assert_eq!(queue.len(), 1);

    assert!(queue.push(op2.clone()).is_ok());
    assert!(!queue.is_empty());
    assert_eq!(queue.len(), 1);

    assert_eq!(queue.pop(), Some(op2));
    assert!(queue.is_empty());
    assert_eq!(queue.len(), 0);
}

fn update(key: &str, value: &str) -> RawMapOperation {
    RawMapOperation::Update {
        key: bytes_of(key),
        value: bytes_of(value),
    }
}

#[test]
fn overwrite_in_middle() {
    let mut queue = MapOperationQueue::default();

    assert!(queue.push(update("key0", "value0")).is_ok());
    queue.pop();

    for i in 0..5 {
        let j = i + 1;
        let key = format!("key{}", j);
        let value = format!("value{}", j);
        let upd = update(&key, &value);
        assert!(queue.push(upd).is_ok());
    }
    assert_eq!(queue.len(), 5);

    let upd = update("key3", "replaced");
    assert!(queue.push(upd).is_ok());
    assert_eq!(queue.len(), 5);

    assert_eq!(queue.pop(), Some(update("key1", "value1")));
    assert_eq!(queue.pop(), Some(update("key2", "value2")));
    assert_eq!(queue.pop(), Some(update("key3", "replaced")));
    assert_eq!(queue.pop(), Some(update("key4", "value4")));
    assert_eq!(queue.pop(), Some(update("key5", "value5")));
    assert!(queue.is_empty());
}

#[test]
fn clear_when_filled() {
    let mut queue = MapOperationQueue::default();

    for i in 0..5 {
        let j = i + 1;
        let key = format!("key{}", j);
        let upd = if i % 2 == 0 {
            let value = format!("value{}", j);
            update(&key, &value)
        } else {
            RawMapOperation::Remove {
                key: bytes_of(&key),
            }
        };
        assert!(queue.push(upd).is_ok());
    }
    assert_eq!(queue.len(), 5);

    assert!(queue.push(RawMapOperation::Clear).is_ok());
    assert_eq!(queue.len(), 1);

    assert_eq!(queue.pop(), Some(RawMapOperation::Clear));
    assert!(queue.is_empty());
}

// Tests what happens when the internal epoch counter overflows. In practice this will never
// actually ocurr but we should be robust against it.
#[test]
fn epoch_overflow() {
    let mut queue = MapOperationQueue::default();
    queue.head_epoch = usize::MAX - 2;

    for i in 0..5 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        let upd = update(&key, &value);
        assert!(queue.push(upd).is_ok());
    }
    assert_eq!(queue.len(), 5);

    for i in 0..5 {
        let key = format!("key{}", i);
        let value = format!("replaced{}", i);
        let upd = update(&key, &value);
        assert!(queue.push(upd).is_ok());
    }

    assert_eq!(queue.len(), 5);

    for i in 0..5 {
        let key = format!("key{}", i);
        let value = format!("replaced{}", i);
        assert_eq!(queue.pop(), Some(update(&key, &value)));
    }

    assert!(queue.is_empty());
}
