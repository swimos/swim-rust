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

#![allow(clippy::match_wild_err_arm)]

use std::error::Error;
use std::fmt::{Display, Formatter};

pub mod clock;
pub mod errors;
pub mod future;
pub mod iteratee;
pub mod lru_cache;
#[macro_use]
pub mod ptr;
pub mod sync;
pub mod trace;

/// Error thrown by methods that required a usize to be positive.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ZeroUsize;

impl Display for ZeroUsize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Zero Usize")
    }
}

impl Error for ZeroUsize {}
