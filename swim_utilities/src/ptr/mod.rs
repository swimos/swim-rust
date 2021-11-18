// Copyright 2015-2021 Swim Inc.
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

/// Tests two trait objects for equality. This is useful when dealing with boxed trait objects and
/// equality tests are still required.
///
/// Example:
/// ```
/// use std::sync::Arc;
/// use swim_utilities::ptr::data_ptr_eq;
///
/// trait TestTrait {}
///
/// struct S;
/// impl TestTrait for S {}
///
/// #[derive(Clone)]
/// pub struct Sender {
///     task: Arc<dyn TestTrait>,
/// }
///
/// let s1 = Sender { task: Arc::new(S) };
/// let s2 = s1.clone();
///
/// assert!(data_ptr_eq(&*s1.task, &*s2.task));
/// ```
pub fn data_ptr_eq<T: ?Sized>(p1: &T, p2: &T) -> bool {
    let l = p1 as *const T as *const u8;
    let r = p2 as *const T as *const u8;

    std::ptr::eq(l, r)
}

#[cfg(test)]
mod trait_eq_tests {
    use crate::ptr::data_ptr_eq;
    use std::sync::Arc;

    trait TestTrait {}

    #[test]
    fn eq() {
        #[derive(Clone)]
        pub struct Sender {
            task: Arc<dyn TestTrait>,
        }

        struct S;

        impl TestTrait for S {}

        let s1 = Sender { task: Arc::new(S) };
        let s2 = s1.clone();

        assert!(data_ptr_eq(&*s1.task, &*s2.task));
    }

    #[test]
    fn not_eq() {
        struct A;

        impl TestTrait for A {}

        let a = A;
        let b = A;

        assert!(!data_ptr_eq(&a, &b));
    }
}
