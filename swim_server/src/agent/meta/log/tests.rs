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

use crate::agent::meta::log::LogEntry;
use crate::agent::meta::LogLevel;
use std::str::FromStr;
use swim_common::form::Form;
use swim_common::model::Value;
use utilities::uri::RelativeUri;

#[test]
fn test_log_entry() {
    let entry = LogEntry::make(
        String::from("message"),
        LogLevel::Error,
        RelativeUri::from_str("/lane").unwrap(),
    );

    match entry.into_value() {
        Value::Record(mut attrs, _items) => {
            assert!(matches!(attrs.pop(), Some(attr) if attr.name == "error"));
        }
        _ => {
            panic!("Expected a record");
        }
    }
}
