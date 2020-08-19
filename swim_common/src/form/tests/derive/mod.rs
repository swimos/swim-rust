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

use trybuild::TestCases;

#[test]
fn test_derive() {
    let t = TestCases::new();

    t.compile_fail("src/form/tests/derive/borrowed_field.rs");
    t.compile_fail("src/form/tests/derive/empty_tag.rs");
    t.compile_fail("src/form/tests/derive/double_kind.rs");
    t.compile_fail("src/form/tests/derive/double_body_attr.rs");
    t.compile_fail("src/form/tests/derive/double_header_body_attr.rs");
    t.compile_fail("src/form/tests/derive/incorrect_rename_type.rs");
    t.compile_fail("src/form/tests/derive/incorrect_tag_type.rs");
    t.compile_fail("src/form/tests/derive/incorrect_tag_type_enum.rs");
    t.compile_fail("src/form/tests/derive/invalid_tag_placement_enum.rs");
    t.compile_fail("src/form/tests/derive/unknown_field_attr.rs");
    t.compile_fail("src/form/tests/derive/unknown_container_attr.rs");
    t.compile_fail("src/form/tests/derive/union.rs");
}
