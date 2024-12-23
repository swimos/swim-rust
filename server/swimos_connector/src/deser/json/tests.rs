// Copyright 2015-2024 Swim Inc.
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

use swimos_model::{Item, Value};

use crate::deser::{JsonDeserializer, MessageDeserializer};

#[test]
fn json_deserializer() {
    let deserializer = JsonDeserializer.boxed();

    let bytes: &[u8] = "{ \"a\": 1, \"b\": true}".as_bytes();
    let expected = Value::Record(vec![], vec![Item::slot("a", 1), Item::slot("b", true)]);

    assert_eq!(deserializer.deserialize(bytes).expect("Failed."), expected);
}
