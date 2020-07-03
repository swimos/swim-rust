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

use crate::ValueDeserializer;
use common::model::Value;
use core::fmt;
use num_bigint::{BigInt as RemoteBigInt, BigInt};
use serde::de::{Error, Visitor};
use serde::export::Formatter;
use serde::{Deserialize, Deserializer};
use std::str::FromStr;

#[allow(dead_code)]
fn from_value<'de, T>(value: &'de Value) -> super::Result<T>
where
    T: Deserialize<'de>,
{
    let mut deserializer = match value {
        Value::Record(_, _) => ValueDeserializer::for_values(value),
        _ => ValueDeserializer::for_single_value(value),
    };

    let t = T::deserialize(&mut deserializer)?;
    Ok(t)
}

#[allow(dead_code)]
pub fn deserialize<'de, D>(deserializer: D) -> Result<RemoteBigInt, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(BigIntVisitor)
}

pub struct BigIntVisitor;

impl<'de> Visitor<'de> for BigIntVisitor {
    type Value = BigInt;

    fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "a valid Big Integer")
    }

    fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BigInt::from(v))
    }

    fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BigInt::from(v))
    }

    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BigInt::from(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BigInt::from(v))
    }

    fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BigInt::from(v))
    }

    fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BigInt::from(v))
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BigInt::from(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BigInt::from(v))
    }

    fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BigInt::from(v as i32))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BigInt::from(v as i64))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match BigInt::from_str(v) {
            Ok(bi) => Ok(bi),
            Err(e) => Err(E::custom(format!("{:?}", e))),
        }
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match BigInt::from_str(v) {
            Ok(bi) => Ok(bi),
            Err(e) => Err(E::custom(format!("{:?}", e))),
        }
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match BigInt::from_str(&v) {
            Ok(bi) => Ok(bi),
            Err(e) => Err(E::custom(format!("{:?}", e))),
        }
    }
}

#[test]
fn t() {
    use common::model::{Attr, Item};

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    struct S {
        #[serde(deserialize_with = "deserialize")]
        bi: RemoteBigInt,
    }

    let record = Value::Record(
        vec![Attr::of("S")],
        vec![Item::from(("bi", Value::Int64Value(1)))],
    );

    let parsed_value = from_value::<S>(&record).unwrap();
    println!("{:#?}", parsed_value);
}
