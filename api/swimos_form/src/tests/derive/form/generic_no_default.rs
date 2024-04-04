// Copyright 2015-2023 Swim Inc.
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

use swimos_form::Form;

fn main() {
    #[derive(Form)]
    struct Valid;

    struct Skipped;

    #[derive(Form, Debug, PartialEq, Clone)]
    struct S<A, B> {
        a: A,
        #[form(skip)]
        b: B,
    }

    let s = S {
        a: Valid,
        b: Skipped,
    };

    let _ = s.as_value();
}
