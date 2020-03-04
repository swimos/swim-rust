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

use serde::Deserialize;

use crate::model::Value;
use crate::structure::form::Form;

#[cfg(test)]
mod structs {
    use crate::model::{Attr, Item};

    use super::*;

    #[test]
    fn simple_struct() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct Test {
            a: i32,
            b: i64,
        }

        let _test = Test { a: 1, b: 2 };

        let mut record = Value::Record(
            vec![Attr::from("Test")],
            vec![
                Item::from(("a", 1)),
                Item::from(("b", Value::Int64Value(2))),
            ],
        );
        let parsed_value = Form::default().from_value::<Test>(&mut record);

        println!("{:?}", parsed_value);
    }
}

#[cfg(test)]
mod valid_types {
    use super::*;

    #[test]
    fn test_extant() {
        let parsed_value = Form::default()
            .from_value::<Option<String>>(&mut Value::Extant)
            .unwrap();
        let expected = None;

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_i32() {
        let parsed_value = Form::default()
            .from_value::<i32>(&mut Value::Int32Value(1))
            .unwrap();
        let expected = 1;

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_i64() {
        let parsed_value = Form::default()
            .from_value::<i64>(&mut Value::Int64Value(2))
            .unwrap();
        let expected = 2;

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_f64() {
        let parsed_value = Form::default()
            .from_value::<f64>(&mut Value::Float64Value(1.0))
            .unwrap();
        let expected = 1.0;

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_bool() {
        let parsed_value = Form::default()
            .from_value::<bool>(&mut Value::BooleanValue(true))
            .unwrap();
        let expected = true;

        assert_eq!(parsed_value, expected);
    }

    #[test]
    fn test_text() {
        let parsed_value = Form::default()
            .from_value::<String>(&mut Value::Text(String::from("swim.ai")))
            .unwrap();
        let expected = String::from("swim.ai");

        assert_eq!(parsed_value, expected);
    }
}
