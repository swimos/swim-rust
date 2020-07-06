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

#[cfg(test)]
mod traits;

#[test]
fn test_derive() {
    let t = TestCases::new();

    t.pass("src/tests/derive/struct/single_derive.rs");
    t.pass("src/tests/derive/struct/nested_derives.rs");
    t.pass("src/tests/derive/struct/tuple_struct.rs");
    t.pass("src/tests/derive/struct/newtype.rs");
    t.pass("src/tests/derive/struct/unit_struct.rs");

    t.pass("src/tests/integration/struct/serialize_ok.rs");
    t.pass("src/tests/integration/struct/deserialize_ok.rs");
    t.pass("src/tests/integration/struct/deserialize_err.rs");
    t.pass("src/tests/integration/struct/tuple_ser_ok.rs");
    t.pass("src/tests/integration/struct/tuple_de_err.rs");
    t.pass("src/tests/integration/struct/tuple_de_ok.rs");
    t.pass("src/tests/integration/struct/newtype_ser_ok.rs");
    t.pass("src/tests/integration/struct/newtype_de_ok.rs");
    t.pass("src/tests/integration/struct/newtype_de_err.rs");
    t.pass("src/tests/integration/struct/unit_ser_ok.rs");
    t.pass("src/tests/integration/struct/unit_de_ok.rs");
    t.pass("src/tests/integration/struct/unit_de_err.rs");

    t.pass("src/tests/integration/enum/enum_ser_ok.rs");
    t.pass("src/tests/integration/enum/enum_ser_struct_ok.rs");

    t.pass("src/tests/derive/enum/single_enum.rs");
    t.pass("src/tests/derive/enum/nested_enum.rs");

    t.pass("src/tests/derive/collection/simple_vector.rs");
    t.pass("src/tests/derive/collection/vector_with_compound.rs");

    t.compile_fail("src/tests/derive/unimplemented/unimplemented_compound.rs");
    t.compile_fail("src/tests/derive/unimplemented/unimplemented_nested.rs");
    t.compile_fail("src/tests/derive/unimplemented/unimplemented_primitive.rs");
    t.compile_fail("src/tests/derive/unimplemented/unimplemented_vector.rs");

    t.compile_fail("src/tests/derive/attributes/invalid_bigint_attr.rs");
    t.compile_fail("src/tests/derive/attributes/invalid_biguint_attr.rs");
    t.compile_fail("src/tests/derive/attributes/missing_arg.rs");
    t.compile_fail("src/tests/derive/attributes/too_many_args.rs");

    t.pass("src/tests/integration/bigint/bigint_ser_ok.rs");
    t.pass("src/tests/integration/bigint/bigint_deser_err.rs");
}
