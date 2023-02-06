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

use uuid::Uuid;

const REMOTE: u8 = 0;
const PLANE: u8 = 1;
const CLIENT: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdKind {
    Remote,
    Plane,
    Client,
}

impl IdKind {
    fn tag(&self) -> u8 {
        match self {
            IdKind::Remote => REMOTE,
            IdKind::Plane => PLANE,
            IdKind::Client => CLIENT,
        }
    }
}

#[derive(Debug)]
pub struct IdIssuer {
    kind: IdKind,
    count: u64,
}

impl IdIssuer {
    pub const fn new(kind: IdKind) -> Self {
        IdIssuer { kind, count: 0 }
    }

    pub fn next_id(&mut self) -> Uuid {
        let IdIssuer { kind, count } = self;
        let c = *count;
        *count += 1;
        make_id(kind.tag(), c)
    }
}

const fn make_id(tag: u8, count: u64) -> Uuid {
    let mut uuid_as_int = count as u128;
    uuid_as_int |= (tag as u128) << 120;
    Uuid::from_u128(uuid_as_int)
}
