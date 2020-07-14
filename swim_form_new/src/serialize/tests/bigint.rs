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

use num_bigint::BigInt;

use common::model::{Attr, Item, Value};

use crate::deserialize::FormDeserializeErr;
use crate::serialize::as_value;
use crate::serialize::serializer::ValueSerializer;
use crate::{Form, SerializeToValue};

#[test]
fn test_bigint() {
    struct S {
        bi: BigInt,
    }

    impl Form for S {
        fn as_value(&self) -> Value {
            as_value(self)
        }

        fn try_from_value(_value: &Value) -> Result<Self, FormDeserializeErr> {
            unimplemented!()
        }
    }

    impl SerializeToValue for S {
        fn serialize(&self, serializer: &mut ValueSerializer) {
            serializer.serialize_struct("S", 1);
            serializer.serialize_field(Some("bi"), &self.bi);
            serializer.exit_nested();
        }
    }

    let s = S {
        bi: BigInt::from(100),
    };

    let value = as_value(&s);
    let expected = Value::Record(
        vec![Attr::of("S")],
        vec![Item::slot("bi", Value::Text(String::from("100")))],
    );

    assert_eq!(value, expected);
}
