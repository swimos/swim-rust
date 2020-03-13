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

// There has been some file contention when the test cases are in their own functions (grouped) and
// so, for now, the cases are together.
#[test]
fn test_derive() {
    let t = TestCases::new();

    t.pass("src/tests/derive/single_derive.rs");
    t.pass("src/tests/derive/nested_derives.rs");
    t.pass("src/tests/derive/simple_vector.rs");
    t.pass("src/tests/derive/vector_with_compound.rs");

    t.pass("src/tests/integration/serialize_ok.rs");
    t.pass("src/tests/integration/deserialize_ok.rs");
    t.pass("src/tests/integration/deserialize_err.rs");

    t.compile_fail("src/tests/derive/unimplemented_compound.rs");
    t.compile_fail("src/tests/derive/unimplemented_nested.rs");
    t.compile_fail("src/tests/derive/unimplemented_primitive.rs");
    t.compile_fail("src/tests/derive/unimplemented_vector.rs");
}

