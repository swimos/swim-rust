// Copyright 2015-2024 Swim Inc.
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

//! # Algebraic traits
//!
//! A selection of traits representing abstract algebras. These can be used to abstract over types
//! that have differing concepts of 'combination'.
//!
//! # Examples
//!
//! Consider the following function to combine maps:
//!
//! ```
//! use std::hash::Hash;
//! use std::collections::{HashMap, HashSet};
//!
//! use swimos_algebra::Semigroup;
//!
//! fn combine_maps<K: Eq + Hash + Clone, T: Semigroup>(
//!     mut left: HashMap<K, T>,
//!     mut right: HashMap<K, T>) -> HashMap<K, T> {
//!    
//!     let keys = left.keys().chain(right.keys()).cloned().collect::<HashSet<_>>();
//!
//!     keys.into_iter().filter_map(|k| {
//!         match (left.remove(&k), right.remove(&k)) {
//!            (None, Some(r)) => Some((k, r)),
//!            (Some(l), None) => Some((k, l)),
//!            (Some(l), Some(r)) => Some((k, Semigroup::op(l, r))),
//!            _ => None,
//!        }
//!     }).collect()
//! }
//! ```
//!
//! This can be applied to a map with integer values,
//!
//!
//! ```
//! # use std::hash::Hash;
//! # use std::collections::{HashMap, HashSet};
//! #
//! # use swimos_algebra::Semigroup;
//! #
//! # fn combine_maps<K: Eq + Hash + Clone, T: Semigroup>(
//! #   mut left: HashMap<K, T>,
//! #   mut right: HashMap<K, T>) -> HashMap<K, T> {
//! #   
//! # let keys = left.keys().chain(right.keys()).cloned().collect::<HashSet<_>>();
//! #
//! # keys.into_iter().filter_map(|k| {
//! #    match (left.remove(&k), right.remove(&k)) {
//! #           (None, Some(r)) => Some((k, r)),
//! #           (Some(l), None) => Some((k, l)),
//! #           (Some(l), Some(r)) => Some((k, Semigroup::op(l, r))),
//! #           _ => None,
//! #       }
//! #    }).collect()
//! # }
//! let left_ints = [("red", 3), ("green", 23), ("blue", 1)]
//!     .into_iter()
//!     .collect::<HashMap<_, _>>();
//!
//! let right_ints = [("red", 4), ("blue", 1)]
//!     .into_iter()
//!     .collect::<HashMap<_, _>>();
//!
//! let expected_ints = [("red", 7), ("green", 23), ("blue", 2)]
//!     .into_iter()
//!     .collect::<HashMap<_, _>>();
//!
//! assert_eq!(combine_maps(left_ints, right_ints), expected_ints);
//! ```
//!
//! or a map with vectors as values:
//!
//! ```
//! # use std::hash::Hash;
//! # use std::collections::{HashMap, HashSet};
//! #
//! # use swimos_algebra::Semigroup;
//! #
//! # fn combine_maps<K: Eq + Hash + Clone, T: Semigroup>(
//! #   mut left: HashMap<K, T>,
//! #   mut right: HashMap<K, T>) -> HashMap<K, T> {
//! #   
//! # let keys = left.keys().chain(right.keys()).cloned().collect::<HashSet<_>>();
//! #
//! # keys.into_iter().filter_map(|k| {
//! #    match (left.remove(&k), right.remove(&k)) {
//! #           (None, Some(r)) => Some((k, r)),
//! #           (Some(l), None) => Some((k, l)),
//! #           (Some(l), Some(r)) => Some((k, Semigroup::op(l, r))),
//! #           _ => None,
//! #       }
//! #    }).collect()
//! # }
//! let left_vecs = [
//!     ("red", vec![12, -5, 6]),
//!     ("green", vec![1]),
//!     ("blue", vec![5, 8, 11]),
//! ]
//! .into_iter()
//! .collect::<HashMap<_, _>>();
//!
//! let right_vecs = [
//!     ("red", vec![4, 1]),
//!     ("blue", vec![-7])
//! ]
//! .into_iter()
//! .collect::<HashMap<_, _>>();
//!
//! let expected_vecs = [
//!     ("red", vec![12, -5, 6, 4, 1]),
//!     ("green", vec![1]),
//!     ("blue", vec![5, 8, 11, -7]),
//! ]
//! .into_iter()
//! .collect::<HashMap<_, _>>();
//!
//! assert_eq!(combine_maps(left_vecs, right_vecs), expected_vecs);
//! ```

mod monoid;
mod semigroup;
mod zero;

pub use monoid::Monoid;
pub use semigroup::Semigroup;
pub use zero::Zero;
