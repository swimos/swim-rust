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

use std::cmp::Ordering;

/// Compares an [`i64`] and a [`u64`], returning whether the left-hand side is less than, equal to
/// or greater than the right-hand side.
pub fn cmp_i64_u64(lhs: i64, rhs: u64) -> Ordering {
    if lhs < 0 {
        Ordering::Less
    } else {
        let lhs = lhs as u64;
        lhs.cmp(&rhs)
    }
}

/// Compares an [`u64`] and a [`i64`], returning whether the left-hand side is less than, equal to
/// or greater than the right-hand side.
pub fn cmp_u64_i64(lhs: u64, rhs: i64) -> Ordering {
    if rhs < 0 {
        Ordering::Greater
    } else {
        let rhs = rhs as u64;
        lhs.cmp(&rhs)
    }
}

/// Compares an [`i32`] and a [`u32`], returning whether the left-hand side is less than, equal to
/// or greater than the right-hand side.
pub fn cmp_i32_u32(lhs: i32, rhs: u32) -> Ordering {
    if lhs < 0 {
        Ordering::Less
    } else {
        let lhs = lhs as u32;
        lhs.cmp(&rhs)
    }
}

/// Compares an [`u32`] and a [`i32`], returning whether the left-hand side is less than, equal to
/// or greater than the right-hand side.
pub fn cmp_u32_i32(lhs: u32, rhs: i32) -> Ordering {
    if rhs < 0 {
        Ordering::Greater
    } else {
        let rhs = rhs as u32;
        lhs.cmp(&rhs)
    }
}

#[cfg(test)]
mod tests {
    mod test_32 {
        use super::super::{cmp_i32_u32, cmp_u32_i32};
        use std::cmp::Ordering;

        #[test]
        fn less() {
            assert_eq!(cmp_i32_u32(-10, 10), Ordering::Less);
            assert_eq!(cmp_u32_i32(0, 10), Ordering::Less);
        }

        #[test]
        fn greater() {
            assert_eq!(cmp_i32_u32(10, 0), Ordering::Greater);
            assert_eq!(cmp_u32_i32(10, -10), Ordering::Greater);
        }

        #[test]
        fn eq() {
            assert_eq!(cmp_i32_u32(10, 10), Ordering::Equal);
            assert_eq!(cmp_u32_i32(10, 10), Ordering::Equal);
        }

        #[test]
        fn much_less() {
            assert_eq!(cmp_i32_u32(10, u32::max_value()), Ordering::Less);
            assert_eq!(cmp_u32_i32(10, i32::max_value()), Ordering::Less);
        }

        #[test]
        fn much_greater() {
            assert_eq!(
                cmp_i32_u32(i32::max_value(), u32::min_value()),
                Ordering::Greater
            );
            assert_eq!(
                cmp_u32_i32(u32::max_value(), i32::min_value()),
                Ordering::Greater
            );
        }
    }

    mod test_64 {
        use super::super::{cmp_i64_u64, cmp_u64_i64};
        use std::cmp::Ordering;

        #[test]
        fn less() {
            assert_eq!(cmp_i64_u64(-10, 10), Ordering::Less);
            assert_eq!(cmp_u64_i64(0, 10), Ordering::Less);
        }

        #[test]
        fn greater() {
            assert_eq!(cmp_i64_u64(10, 0), Ordering::Greater);
            assert_eq!(cmp_u64_i64(10, -10), Ordering::Greater);
        }

        #[test]
        fn eq() {
            assert_eq!(cmp_i64_u64(10, 10), Ordering::Equal);
            assert_eq!(cmp_u64_i64(10, 10), Ordering::Equal);
        }

        #[test]
        fn much_less() {
            assert_eq!(cmp_i64_u64(10, u64::max_value()), Ordering::Less);
            assert_eq!(cmp_u64_i64(10, i64::max_value()), Ordering::Less);
        }

        #[test]
        fn much_greater() {
            assert_eq!(
                cmp_i64_u64(i64::max_value(), u64::min_value()),
                Ordering::Greater
            );
            assert_eq!(
                cmp_u64_i64(u64::max_value(), i64::min_value()),
                Ordering::Greater
            );
        }
    }
}
