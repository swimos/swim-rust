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

use chrono::{DateTime, TimeZone, Utc};
use std::fmt::{Display, Formatter};

/// A structure representing the time that it was created.
#[derive(Copy, Clone, PartialEq, Eq, Debug, Ord, PartialOrd, Hash)]
pub struct Timestamp(DateTime<Utc>);

impl AsRef<DateTime<Utc>> for Timestamp {
    fn as_ref(&self) -> &DateTime<Utc> {
        &self.0
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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
