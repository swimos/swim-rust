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

use crate::form::{Form, FormErr, ValidatedForm};
use crate::model::schema::StandardSchema;
use crate::model::{Value, ValueKind};
use chrono::{DateTime, LocalResult, TimeZone, Utc};
use std::fmt::Display;

/// A structure representing the time that it was created.
#[derive(Copy, Clone, PartialEq, Eq, Debug, Ord, PartialOrd, Hash)]
pub struct Timestamp(DateTime<Utc>);

impl AsRef<DateTime<Utc>> for Timestamp {
    fn as_ref(&self) -> &DateTime<Utc> {
        &self.0
    }
}

impl<TZ> From<DateTime<TZ>> for Timestamp
where
    TZ: TimeZone,
{
    fn from(dt: DateTime<TZ>) -> Self {
        Timestamp(dt.with_timezone(&Utc))
    }
}

impl From<Timestamp> for DateTime<Utc> {
    fn from(ts: Timestamp) -> Self {
        ts.0
    }
}

impl Timestamp {
    /// Returns a new Timestamp representing the current date.
    pub fn now() -> Timestamp {
        Timestamp(Utc::now())
    }

    /// Returns the number of non-leap-nanoseconds since January 1, 1970 UTC.
    pub fn nanos(&self) -> i64 {
        self.0.timestamp_nanos()
    }
}

fn check_parse_time_result<T, V>(me: LocalResult<T>, ts: &V) -> Result<T, FormErr>
where
    V: Display,
{
    match me {
        LocalResult::Single(val) => Ok(val),
        _ => Err(FormErr::Message(format!(
            "Failed to parse timestamp: {}",
            ts
        ))),
    }
}

impl Form for Timestamp {
    fn as_value(&self) -> Value {
        Value::Int64Value(self.0.timestamp_nanos())
    }

    fn into_value(self) -> Value {
        Value::Int64Value(self.0.timestamp_nanos())
    }

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::UInt64Value(n) => {
                let inner = check_parse_time_result(
                    Utc.timestamp_opt((n / 1_000_000_000) as i64, (n % 1_000_000_000) as u32),
                    n,
                )?;
                Ok(Timestamp(inner))
            }
            Value::Int64Value(n) => {
                let inner = check_parse_time_result(
                    Utc.timestamp_opt(n / 1_000_000_000, (n % 1_000_000_000) as u32),
                    n,
                )?;

                Ok(Timestamp(inner))
            }
            v => Err(FormErr::incorrect_type("Value::Int64Value", v)),
        }
    }

    fn try_convert(value: Value) -> Result<Self, FormErr> {
        Form::try_from_value(&value)
    }
}

impl ValidatedForm for Timestamp {
    fn schema() -> StandardSchema {
        StandardSchema::Or(vec![
            StandardSchema::OfKind(ValueKind::Int64),
            StandardSchema::OfKind(ValueKind::UInt64),
        ])
    }
}

#[test]
fn test_local_time() {
    let now = Timestamp(Utc::now());
    let value = now.as_value();

    assert_eq!(Value::Int64Value(now.nanos()), value);
    assert_eq!(Timestamp::try_convert(value), Ok(now))
}
