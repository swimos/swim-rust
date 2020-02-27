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



use serde::Serialize;

use crate::structure::form::compound::{SerializerError, to_value};
use crate::structure::form::tests::assert_err;

#[cfg(test)]
mod compound_types {
    use super::*;

    #[test]
    fn illegal_struct() {
        #[derive(Serialize)]
        struct Parent {
            a: i32,
            b: Child,
        }

        #[derive(Serialize)]
        struct Child {
            a: u64,
        }

        let test = Parent {
            a: 0,
            b: Child {
                a: 0
            },
        };

        let parsed_value = to_value(&test);
        assert_err(parsed_value, SerializerError::UnsupportedType(String::from("u64")));
    }

    #[test]
    fn valid_struct() {
        #[derive(Serialize)]
        struct Parent {
            a: i32,
            b: Child,
            c: Child,
        }

        #[derive(Serialize)]
        struct Child {
            a: i64,
            b: String,
        }

        let test = Parent {
            a: 0,
            b: Child {
                a: 1,
                b: String::from("child1"),
            },
            c: Child {
                a: 2,
                b: String::from("child2"),
            },
        };

        let parsed_value = to_value(&test);
        println!("{:?}", parsed_value);
    }
}
