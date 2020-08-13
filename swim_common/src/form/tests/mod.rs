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

mod enumeration;
mod impls;
mod structure;
mod validated;

use trybuild::TestCases;

#[test]
fn test_derive() {
    let t = TestCases::new();

    t.compile_fail("src/form/tests/derive/form/empty_tag.rs");
    t.compile_fail("src/form/tests/derive/form/double_kind.rs");
    t.compile_fail("src/form/tests/derive/form/double_body_attr.rs");
    t.compile_fail("src/form/tests/derive/form/double_header_body_attr.rs");
    t.compile_fail("src/form/tests/derive/form/incorrect_rename_type.rs");
    t.compile_fail("src/form/tests/derive/form/incorrect_tag_type.rs");
    t.compile_fail("src/form/tests/derive/form/incorrect_tag_type_enum.rs");
    t.compile_fail("src/form/tests/derive/form/invalid_tag_placement_enum.rs");
    t.compile_fail("src/form/tests/derive/form/unknown_field_attr.rs");
    t.compile_fail("src/form/tests/derive/form/unknown_container_attr.rs");
    t.compile_fail("src/form/tests/derive/form/union.rs");

    t.compile_fail("src/form/tests/derive/validated_form/all_items_op_arg_count.rs");
    t.compile_fail("src/form/tests/derive/validated_form/all_items_unit.rs");
    t.compile_fail("src/form/tests/derive/validated_form/and_op_arg_count.rs");
    t.compile_fail("src/form/tests/derive/validated_form/or_op_arg_count.rs");
    t.compile_fail("src/form/tests/derive/validated_form/not_op_arg_count.rs");
    t.compile_fail("src/form/tests/derive/validated_form/invalid_container_of_kind.rs");
    t.compile_fail("src/form/tests/derive/validated_form/trailing_schema.rs");
    t.compile_fail("src/form/tests/derive/validated_form/nothing_attr_on_field.rs");
    t.compile_fail("src/form/tests/derive/validated_form/invalid_range.rs");
}
