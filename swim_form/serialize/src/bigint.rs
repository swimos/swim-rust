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

use crate::serializer::{BIG_INT_PREFIX, BIG_UINT_PREFIX};

use num_bigint::{BigInt, BigUint};
use serde::Serializer;

pub fn serialize_bigint<S>(bi: &BigInt, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&(BIG_INT_PREFIX.to_owned() + &bi.to_string()))
}

pub fn serialize_big_uint<S>(bi: &BigUint, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&(BIG_UINT_PREFIX.to_owned() + &bi.to_string()))
}
