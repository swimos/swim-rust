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
use num_bigint::BigUint;
use num_traits::FromPrimitive;
use serde::de::{Error, Visitor};
use serde::export::Formatter;
use serde::Deserializer;
use std::convert::TryFrom;
use std::str::FromStr;

#[allow(dead_code)]
pub fn deserialize<'de, D>(deserializer: D) -> Result<BigUint, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(BigIntVisitor)
}

pub struct BigIntVisitor;
impl<'de> Visitor<'de> for BigIntVisitor {
    type Value = BigUint;

    fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "a valid Big Integer")
    }

    fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
    where
        E: Error,
    {
        BigUint::from_i8(v).ok_or_else(|| E::custom(format!("invalid big integer: {:?}", v)))
    }

    fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
    where
        E: Error,
    {
        BigUint::from_i16(v).ok_or_else(|| E::custom(format!("invalid big integer: {:?}", v)))
    }

    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        BigUint::from_i32(v).ok_or_else(|| E::custom(format!("invalid big integer: {:?}", v)))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        BigUint::from_i64(v).ok_or_else(|| E::custom(format!("invalid big integer: {:?}", v)))
    }

    fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BigUint::try_from(v).expect("infailable"))
    }

    fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BigUint::try_from(v).expect("infailable"))
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BigUint::try_from(v).expect("infailable"))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(BigUint::try_from(v).expect("infailable"))
    }

    fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        BigUint::from_f32(v).ok_or_else(|| E::custom(format!("invalid big integer: {:?}", v)))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        BigUint::from_f64(v).ok_or_else(|| E::custom(format!("invalid big integer: {:?}", v)))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match BigUint::from_str(v) {
            Ok(bi) => Ok(bi),
            Err(e) => Err(E::custom(format!("{:?}", e))),
        }
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match BigUint::from_str(v) {
            Ok(bi) => Ok(bi),
            Err(e) => Err(E::custom(format!("{:?}", e))),
        }
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match BigUint::from_str(&v) {
            Ok(bi) => Ok(bi),
            Err(e) => Err(E::custom(format!("{:?}", e))),
        }
    }
}
