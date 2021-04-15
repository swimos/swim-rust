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

pub mod lane;
pub mod node;
pub mod plane;

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use swim_common::model::text::Text;

/// A lane key that is either a map lane key or a value lane key.
pub enum LaneKey<'l> {
    /// A map lane key.
    ///
    /// Within plane stores, map lane keys are defined in the format of `/node_uri/lane_uri/key`
    /// where `key` is the key of a lane's map data structure.
    Map {
        /// The lane URI.
        lane_uri: &'l Text,
        /// An optional, serialized, key. This is optional as ranged snapshots to not require the
        /// key.
        key: Option<Vec<u8>>,
    },
    /// A value lane key.
    Value {
        /// The lane URI.
        lane_uri: &'l Text,
    },
}

/// A storage key used for either map or value lanes.
#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, PartialEq)]
pub enum StoreKey<'n, 'l> {
    #[serde(borrow)]
    Map(MapStorageKey<'n, 'l>),
    #[serde(borrow)]
    Value(ValueStorageKey<'n, 'l>),
}

/// A storage key for map lanes.
#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, PartialEq)]
pub struct MapStorageKey<'n, 'l> {
    /// The node URI that this key corresponds to.
    #[serde(with = "text_serde", borrow)]
    pub node_uri: Cow<'n, Text>,
    /// The lane URI that this key corresponds to.
    #[serde(with = "text_serde", borrow)]
    pub lane_uri: Cow<'l, Text>,
    /// An optional serialized key for the key within the lane.
    ///
    /// This is optional as it is not required for executing ranged snapshots on a storage engine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Vec<u8>>,
}

/// A storage key for value lanes.
#[derive(Serialize, Deserialize, Clone, Debug, PartialOrd, PartialEq)]
pub struct ValueStorageKey<'n, 'l> {
    #[serde(with = "text_serde", borrow)]
    pub node_uri: Cow<'n, Text>,
    #[serde(with = "text_serde", borrow)]
    pub lane_uri: Cow<'l, Text>,
}

/// Cow<'_, Text> serde implementation
mod text_serde {
    use serde::de::{Error, Visitor};
    use serde::{Deserializer, Serializer};
    use std::borrow::Cow;
    use std::fmt::Formatter;
    use std::str::FromStr;
    use swim_common::model::text::Text;

    pub fn serialize<S>(text: &Text, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(text.as_str())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Cow<'de, Text>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TextVisitor;

        impl<'de> Visitor<'de> for TextVisitor {
            type Value = Cow<'de, Text>;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("Expected a string literal")
            }

            fn visit_borrowed_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                match Text::from_str(v) {
                    Ok(s) => Ok(Cow::Owned(s)),
                    Err(e) => Err(E::custom(e)),
                }
            }
        }

        deserializer.deserialize_str(TextVisitor)
    }
}
