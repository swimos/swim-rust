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

use chrono::{Duration, Utc};
use swim_common::model::time::Timestamp;

#[derive(Clone, PartialEq, Eq, Debug, Ord, PartialOrd, Hash)]
pub struct Token {
    id: String,
    expires: Option<Timestamp>,
}

impl Token {
    /// Creates a new token that will expire at the provided timestamp if it is `Some(_)` or which
    /// will never expire if it is `None`.
    pub fn new(id: String, expires: Option<Timestamp>) -> Token {
        Token { id, expires }
    }

    pub fn empty() -> Token {
        Token {
            id: String::new(),
            expires: None,
        }
    }
}

pub trait Expired {
    fn expired(&self, skew: i64) -> bool;
}

impl Expired for Token {
    fn expired(&self, skew: i64) -> bool {
        match self.expires {
            Some(expires) => expires.as_ref().lt(&(Utc::now() + Duration::seconds(skew))),
            None => false,
        }
    }
}
