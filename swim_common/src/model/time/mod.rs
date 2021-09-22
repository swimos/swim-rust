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

use crate::form::structural::read::error::ExpectedEvent;
use crate::form::structural::read::event::{NumericValue, ReadEvent};
use crate::form::structural::read::recognizer::{
    Recognizer, RecognizerReadable, SimpleAttrBody, SimpleRecBody,
};
use crate::form::structural::read::ReadError;
use crate::form::structural::write::{PrimitiveWriter, StructuralWritable, StructuralWriter};
use crate::form::ValueSchema;
use crate::model::schema::StandardSchema;
use crate::model::ValueKind;
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

fn check_parse_time_result<T, V>(me: LocalResult<T>, ts: &V) -> Result<T, ReadError>
where
    V: Display,
{
    match me {
        LocalResult::Single(val) => Ok(val),
        _ => Err(ReadError::Message(
            format!("Failed to parse timestamp: {}", ts).into(),
        )),
    }
}

impl StructuralWritable for Timestamp {
    fn write_with<W: StructuralWriter>(
        &self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        writer.write_i64(self.as_ref().timestamp_nanos())
    }

    fn write_into<W: StructuralWriter>(
        self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        self.write_with(writer)
    }

    fn num_attributes(&self) -> usize {
        0
    }
}

impl RecognizerReadable for Timestamp {
    type Rec = TimestampRecognizer;
    type AttrRec = SimpleAttrBody<TimestampRecognizer>;
    type BodyRec = SimpleRecBody<TimestampRecognizer>;

    fn make_recognizer() -> Self::Rec {
        TimestampRecognizer
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(TimestampRecognizer)
    }

    fn make_body_recognizer() -> Self::BodyRec {
        SimpleRecBody::new(TimestampRecognizer)
    }
}

pub struct TimestampRecognizer;

impl Recognizer for TimestampRecognizer {
    type Target = Timestamp;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match input {
            ReadEvent::Number(NumericValue::Int(n)) => {
                let result = check_parse_time_result(
                    Utc.timestamp_opt(n / 1_000_000_000, (n % 1_000_000_000) as u32),
                    &n,
                )
                .map(Timestamp)
                .map_err(|_| ReadError::NumberOutOfRange);

                Some(result)
            }
            ReadEvent::Number(NumericValue::UInt(n)) => {
                let result = check_parse_time_result(
                    Utc.timestamp_opt((n / 1_000_000_000) as i64, (n % 1_000_000_000) as u32),
                    &n,
                )
                .map(Timestamp)
                .map_err(|_| ReadError::NumberOutOfRange);
                Some(result)
            }
            ow => Some(Err(
                ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::UInt64))
            )),
        }
    }

    fn reset(&mut self) {}
}

impl ValueSchema for Timestamp {
    fn schema() -> StandardSchema {
        StandardSchema::Or(vec![
            StandardSchema::OfKind(ValueKind::Int64),
            StandardSchema::OfKind(ValueKind::UInt64),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::form::Form;
    use crate::model::Value;

    #[test]
    fn test_local_time() {
        let now = Timestamp(Utc::now());
        let value = now.as_value();

        assert_eq!(Value::Int64Value(now.nanos()), value);
        assert_eq!(Timestamp::try_convert(value), Ok(now))
    }
}
