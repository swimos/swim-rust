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

use core::fmt;
use std::ops::Deref;
use std::str::FromStr;

use serde::de::{Error, Visitor};
use serde::export::Formatter;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
struct BigInt {
    inner: num_bigint::BigInt,
}

impl From<num_bigint::BigInt> for BigInt {
    fn from(bi: num_bigint::BigInt) -> Self {
        BigInt { inner: bi }
    }
}

impl From<BigInt> for num_bigint::BigInt {
    fn from(bi: BigInt) -> Self {
        bi.inner
    }
}

impl Deref for BigInt {
    type Target = num_bigint::BigInt;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Serialize for BigInt {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&*self.inner.to_string())
    }
}

struct BigIntVisitor;

impl<'de> Visitor<'de> for BigIntVisitor {
    type Value = BigInt;

    fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "A valid big integer")
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match num_bigint::BigInt::from_str(v) {
            Ok(bi) => Ok(BigInt { inner: bi }),
            Err(e) => Err(E::custom(format!("{:?}", e))),
        }
    }
}

impl<'de> Deserialize<'de> for BigInt {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(BigIntVisitor)
    }
}

#[test]
fn test_serialize_deserialize() {
    use num_bigint::RandBigInt;

    #[derive(Serialize, Debug, Deserialize, PartialEq)]
    struct S {
        bi: BigInt,
    }

    let mut rng = rand::thread_rng();

    let input = S {
        bi: BigInt {
            inner: rng.gen_bigint(1000),
        },
    };

    let json = serde_json::to_string(&input).unwrap();
    let r: S = serde_json::from_str(&json).unwrap();

    assert_eq!(input, r);
}
