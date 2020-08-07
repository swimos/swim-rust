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

#[cfg(test)]
#[allow(warnings)]
mod t {

    use crate::form::Form;
    use crate::model::{Attr, Item, Value};
    use crate::warp::envelope::Envelope;
    use form_derive::*;

    const TEST_PRIO: f64 = 0.5;
    const TEST_RATE: f64 = 1.0;
    const TEST_NODE: &str = "node_uri";
    const TEST_LANE: &str = "lane_uri";
    const TEST_TAG: &str = "test";

    #[test]
    fn test_transmute() {
        let env = Envelope::make_sync(
            TEST_NODE,
            TEST_LANE,
            Some(TEST_RATE),
            Some(TEST_PRIO),
            Some(1.into_value()),
        );
        let as_value = env.as_value();
        let into_value = env.into_value();

        println!("{:?}", as_value);
        println!("{:?}", into_value);

        println!("{}", as_value);
        println!("{}", into_value);
    }
}
