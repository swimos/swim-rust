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

use crate::reader::{ValueReadError, ValueReader};
use crate::writer::as_value;
use crate::writer::writer::ValueWriter;
use crate::{Form, TransmuteValue};
use common::model::{Attr, Item, Value};
use num_bigint::BigInt;

#[test]
fn test_bigint() {
    struct S {
        bi: BigInt,
    }

    impl Form for S {
        fn as_value(&self) -> Value {
            as_value(self)
        }

        fn try_from_value(_value: &Value) -> Result<Self, ValueReadError> {
            unimplemented!()
        }
    }

    impl TransmuteValue for S {
        fn transmute_to_value(&self, writer: &mut ValueWriter) {
            writer.transmute_struct("S", 1);
            writer.transmute_field(Some("bi"), &self.bi);
            writer.exit_nested();
        }

        fn transmute_from_value(&self, reader: &mut ValueReader) -> Result<Self, ValueReadError> {
            unimplemented!()
        }
    }

    let s = S {
        bi: BigInt::from(100),
    };

    let value = as_value(&s);
    let expected = Value::Record(
        vec![Attr::of("S")],
        vec![Item::slot("bi", BigInt::from(100))],
    );

    assert_eq!(value, expected);
}
