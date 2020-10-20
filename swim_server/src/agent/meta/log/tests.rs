// Copyright 2015-2020 SWIM.AI inc.
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

use crate::agent::meta::log::{LogEntry, LogLevel};
use std::str::FromStr;
use swim_common::form::Form;
use swim_common::model::time::Timestamp;
use swim_common::model::{Attr, Item, Value};
use utilities::uri::RelativeUri;

#[test]
fn log_entry_from_value() {
    let now = Timestamp::now();

    let value = Value::Record(
        vec![Attr::of((
            "trace",
            Value::from_vec(vec![
                Item::of(("time", now.into_value())),
                Item::of(("node", "/node")),
                Item::of(("lane", "/lane")),
            ]),
        ))],
        vec![Item::ValueItem(Value::text("something interesting"))],
    );

    assert_eq!(
        LogEntry::try_from_value(&value),
        Ok(LogEntry::with_timestamp(
            now,
            "something interesting".to_string(),
            LogLevel::Trace,
            RelativeUri::from_str("/lane").unwrap(),
            RelativeUri::from_str("/node").unwrap()
        ))
    )
}

#[test]
fn log_entry_as_value() {
    let now = Timestamp::now();

    let entry = LogEntry::with_timestamp(
        now,
        "something interesting".to_string(),
        LogLevel::Trace,
        RelativeUri::from_str("/lane").unwrap(),
        RelativeUri::from_str("/node").unwrap(),
    );

    assert_eq!(
        entry.as_value(),
        Value::Record(
            vec![Attr::of((
                "trace",
                Value::from_vec(vec![
                    Item::of(("time", now.into_value())),
                    Item::of(("node", "/node")),
                    Item::of(("lane", "/lane")),
                ]),
            ))],
            vec![Item::ValueItem(Value::text("something interesting"))],
        )
    );
}
