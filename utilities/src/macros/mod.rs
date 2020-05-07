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

/// Shortcuts writing a crate-only function, resolving warnings and hiding documentation.
/// Doesn't support lifetimes at present.
#[macro_export]
macro_rules! crate_only {
    // Matches a function with no return value or body
    ($(#[$attr:meta])* fn $name:ident ($($arg:ident : $argtype:ty),*) { }) => {
        $(#[$attr])*
        #[doc(hidden)]
        #[allow(dead_code)]
        pub(crate) fn $name($($arg : $argtype),*) {}
    };
    // Matches a function with no return value but a body
    ($(#[$attr:meta])* fn $name:ident ($($arg:ident : $argtype:ty),*) { $body:expr }) => {
        $(#[$attr])*
        #[doc(hidden)]
        #[allow(dead_code)]
        pub(crate) fn $name($($arg : $argtype),*) {
            $body
        }
    };
    // Matches a function with a return value and body
    ($(#[$attr:meta])* fn $name:ident ($($arg:ident : $argtype:ty),*) -> $ret:ty { $body:expr }) => {
        $(#[$attr])*
        #[doc(hidden)]
        #[allow(dead_code)]
        pub(crate) fn $name($($arg : $argtype),*) -> $ret {
            $body
        }
    };
}
