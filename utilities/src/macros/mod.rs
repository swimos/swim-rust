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

// #[macro_export]
// macro_rules! unpack_enum {
//     ($variant:path, $value:expr) => {
//         match $value {
//             $variant(x) => x,
//             v => panic!("Mismatched enum. Expected: {:?} found {:?}", $variant, v),
//         }
//     };
// }
//
// #[cfg(test)]
// mod test_enum_unpack {
//
//     #[derive(Debug)]
//     enum MyEnum {
//         A,
//         B(i32),
//     }
//
//     #[test]
//     fn unpack_enum_ok() {
//         let e = MyEnum::B(1);
//         let r = unpack_enum!(MyEnum::B, e);
//
//         assert_eq!(r, 1);
//     }
//
//     #[test]
//     #[should_panic] //(expected = "Mismatched enum. Expected: {:?} found {:?}")]
//     fn unpack_enum_fail() {
//         let e = MyEnum::B(1);
//         let r = unpack_enum!(MyEnum::B, e);
//
//         assert_eq!(r, 1);
//     }
// }
