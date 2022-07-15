// Copyright 2015-2021 Swim Inc.
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

use std::collections::HashMap;

use swim_agent::lanes::{MapLane, ValueLane};
use swim_agent::projections;
use swim_model::Text;

// Projections should work for an empty struct.
#[projections]
#[allow(dead_code)]
struct NoLanes {}

#[test]
fn projections_single_lane() {
    #[projections]
    struct SingleLane {
        first: ValueLane<i32>,
    }

    let agent = SingleLane {
        first: ValueLane::new(0, 56),
    };

    assert_eq!(SingleLane::FIRST(&agent).read(|n| *n), 56);
}

#[test]
fn projections_two_lanes() {
    #[projections]
    struct TwoLanes {
        first: ValueLane<i32>,
        second: MapLane<i32, Text>,
    }

    let mut init = HashMap::new();
    init.insert(67, Text::new("hello"));

    let agent = TwoLanes {
        first: ValueLane::new(0, 77),
        second: MapLane::new(1, init),
    };

    assert_eq!(TwoLanes::FIRST(&agent).read(|n| *n), 77);
    assert_eq!(
        TwoLanes::SECOND(&agent).get(&67, |v| v.cloned()),
        Some(Text::new("hello"))
    );
}

#[test]
fn projections_three_lanes() {
    #[projections]
    struct ThreeLanes {
        first: ValueLane<i32>,
        second: MapLane<i32, Text>,
        third_lane: ValueLane<String>,
    }

    let mut init = HashMap::new();
    init.insert(67, Text::new("hello"));

    let agent = ThreeLanes {
        first: ValueLane::new(0, 77),
        second: MapLane::new(1, init),
        third_lane: ValueLane::new(2, "".to_string()),
    };

    assert_eq!(ThreeLanes::FIRST(&agent).read(|n| *n), 77);
    assert_eq!(
        ThreeLanes::SECOND(&agent).get(&67, |v| v.cloned()),
        Some(Text::new("hello"))
    );
    assert!(ThreeLanes::THIRD_LANE(&agent).read(|s| s.is_empty()));
}

#[test]
fn projections_generic() {
    #[projections]
    struct GenericAgent<S, T> {
        first: ValueLane<S>,
        second: ValueLane<T>,
    }

    let agent = GenericAgent {
        first: ValueLane::new(0, 77),
        second: ValueLane::new(1, Text::new("hello")),
    };

    assert_eq!(GenericAgent::FIRST(&agent).read(|n| *n), 77);
    assert_eq!(
        GenericAgent::SECOND(&agent).read(|s| s.clone()),
        Text::new("hello")
    );
}
