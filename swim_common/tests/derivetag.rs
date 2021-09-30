// Copyright 2015-2021 SWIM.AI inc.
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

use std::str::FromStr;
use swim_common::form::structural::Tag;
use swim_model::Text;

const EMPTY: [&'static str; 0] = [];

#[test]
fn derive_enum_tag_empty() {
    #[derive(Tag, PartialEq, Eq, Debug)]
    enum TagEnum {}

    assert_eq!(TagEnum::VARIANTS, &EMPTY);
}

#[test]
fn derive_enum_tag_one_option() {
    #[derive(Tag, PartialEq, Eq, Debug)]
    enum TagEnum {
        First,
    }

    assert_eq!(TagEnum::VARIANTS, &["First"]);

    let first = TagEnum::First;
    assert_eq!(first.as_ref(), "First");
    assert_eq!(TagEnum::from_str("First"), Ok(TagEnum::First));

    assert_eq!(
        TagEnum::from_str("Other"),
        Err(Text::new("Possible values are: 'First'."))
    )
}

#[test]
fn derive_enum_tag_two_options() {
    #[derive(Tag, PartialEq, Eq, Debug)]
    enum TagEnum {
        First,
        Second,
    }

    assert_eq!(TagEnum::VARIANTS, &["First", "Second"]);

    let first = TagEnum::First;
    assert_eq!(first.as_ref(), "First");
    assert_eq!(TagEnum::from_str("First"), Ok(TagEnum::First));

    let second = TagEnum::Second;
    assert_eq!(second.as_ref(), "Second");
    assert_eq!(TagEnum::from_str("Second"), Ok(TagEnum::Second));

    assert_eq!(
        TagEnum::from_str("Other"),
        Err(Text::new("Possible values are: 'First', 'Second'."))
    )
}
