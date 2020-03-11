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
mod traits;

#[test]
fn test_compiles() {
    let t = trybuild::TestCases::new();

    t.pass("src/tests/compiles/single_derive.rs");
    t.pass("src/tests/compiles/nested_derives.rs");

    t.compile_fail("src/tests/compiles/unimplemented_compound.rs");
    t.compile_fail("src/tests/compiles/unimplemented_nested.rs");
    t.compile_fail("src/tests/compiles/unimplemented_primitive.rs");
}
