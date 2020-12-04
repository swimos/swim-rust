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

use crate::model::schema::{
    as_big_int, as_f64, as_i64, as_u64, combine_orderings, float_endpoint_to_slot,
    int_endpoint_to_slot,
};
use crate::model::{Attr, Value};
use num_bigint::{BigInt, ToBigInt};
use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Bound<T> {
    value: T,
    inclusive: bool,
}

impl<T> Bound<T> {
    pub fn new(value: T, inclusive: bool) -> Self {
        Bound { value, inclusive }
    }

    pub fn inclusive(value: T) -> Self {
        Bound::new(value, true)
    }

    pub fn exclusive(value: T) -> Self {
        Bound::new(value, false)
    }

    pub fn get_value(&self) -> &T {
        &self.value
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Range<T: Clone + PartialOrd> {
    min: Option<Bound<T>>,
    max: Option<Bound<T>>,
}

impl Range<i64> {
    fn new(min: Option<Bound<i64>>, max: Option<Bound<i64>>) -> Self {
        let min = min.map(|Bound { value, inclusive }| {
            if !inclusive {
                Bound::new(value + 1, true)
            } else {
                Bound::new(value, inclusive)
            }
        });

        let max = max.map(|Bound { value, inclusive }| {
            if !inclusive {
                Bound::new(value - 1, true)
            } else {
                Bound::new(value, inclusive)
            }
        });

        Range { min, max }
    }

    pub fn upper_bounded(max: Bound<i64>) -> Self {
        Range::<i64>::new(None, Some(max))
    }

    pub fn lower_bounded(min: Bound<i64>) -> Self {
        Range::<i64>::new(Some(min), None)
    }

    pub fn bounded(min: Bound<i64>, max: Bound<i64>) -> Self {
        Range::<i64>::new(Some(min), Some(max))
    }
}

impl Range<u64> {
    fn new(min: Option<Bound<u64>>, max: Option<Bound<u64>>) -> Self {
        let min = min.map(|Bound { value, inclusive }| {
            if !inclusive {
                Bound::new(value + 1, true)
            } else {
                Bound::new(value, inclusive)
            }
        });

        let max = max.map(|Bound { value, inclusive }| {
            if !inclusive {
                Bound::new(value - 1, true)
            } else {
                Bound::new(value, inclusive)
            }
        });

        Range { min, max }
    }

    pub fn upper_bounded(max: Bound<u64>) -> Self {
        Range::<u64>::new(None, Some(max))
    }

    pub fn lower_bounded(min: Bound<u64>) -> Self {
        Range::<u64>::new(Some(min), None)
    }

    pub fn bounded(min: Bound<u64>, max: Bound<u64>) -> Self {
        Range::<u64>::new(Some(min), Some(max))
    }
}

impl Range<BigInt> {
    fn new(min: Option<Bound<BigInt>>, max: Option<Bound<BigInt>>) -> Self {
        let min = min.map(|Bound { value, inclusive }| {
            if !inclusive {
                Bound::new(value + 1, true)
            } else {
                Bound::new(value, inclusive)
            }
        });

        let max = max.map(|Bound { value, inclusive }| {
            if !inclusive {
                Bound::new(value - 1, true)
            } else {
                Bound::new(value, inclusive)
            }
        });

        Range { min, max }
    }

    pub fn upper_bounded(max: Bound<BigInt>) -> Self {
        Range::<BigInt>::new(None, Some(max))
    }

    pub fn lower_bounded(min: Bound<BigInt>) -> Self {
        Range::<BigInt>::new(Some(min), None)
    }

    pub fn bounded(min: Bound<BigInt>, max: Bound<BigInt>) -> Self {
        Range::<BigInt>::new(Some(min), Some(max))
    }
}

impl Range<f64> {
    fn new(min: Option<Bound<f64>>, max: Option<Bound<f64>>) -> Self {
        Range { min, max }
    }

    pub fn upper_bounded(max: Bound<f64>) -> Self {
        Range::<f64>::new(None, Some(max))
    }

    pub fn lower_bounded(min: Bound<f64>) -> Self {
        Range::<f64>::new(Some(min), None)
    }

    pub fn bounded(min: Bound<f64>, max: Bound<f64>) -> Self {
        Range::<f64>::new(Some(min), Some(max))
    }
}

impl<T: Clone + PartialOrd> Range<T> {
    pub fn unbounded() -> Self {
        Range {
            min: None,
            max: None,
        }
    }

    fn has_upper_bound(&self) -> bool {
        self.min.is_none() && self.max.is_some()
    }

    fn has_lower_bound(&self) -> bool {
        self.min.is_some() && self.max.is_none()
    }

    pub(crate) fn is_bounded(&self) -> bool {
        self.min.is_some() && self.max.is_some()
    }

    pub(crate) fn is_doubly_unbounded(&self) -> bool {
        self.min.is_none() && self.max.is_none()
    }

    pub fn get_min(&self) -> &Option<Bound<T>> {
        &self.min
    }

    pub fn get_max(&self) -> &Option<Bound<T>> {
        &self.max
    }
}

impl<T: Clone + PartialOrd> PartialOrd for Range<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.eq(other) {
            Some(Ordering::Equal)
        } else if self.is_doubly_unbounded() {
            Some(Ordering::Greater)
        } else if other.is_doubly_unbounded() {
            Some(Ordering::Less)
        } else if self.is_bounded() && other.is_bounded() {
            let lower =
                partial_cmp_bounds(self.min.clone()?, other.min.clone()?, BoundType::Lower)?;
            let upper =
                partial_cmp_bounds(self.max.clone()?, other.max.clone()?, BoundType::Upper)?;
            combine_orderings(lower, upper)
        } else if other.is_bounded() {
            cmp_bounded_and_half_bounded_range(self, other)
        } else if self.is_bounded() {
            Some(cmp_bounded_and_half_bounded_range(other, self)?.reverse())
        } else if self.has_lower_bound() && other.has_lower_bound() {
            partial_cmp_bounds(self.min.clone()?, other.min.clone()?, BoundType::Lower)
        } else if self.has_upper_bound() && other.has_upper_bound() {
            partial_cmp_bounds(self.max.clone()?, other.max.clone()?, BoundType::Upper)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum BoundType {
    Upper,
    Lower,
}

fn partial_cmp_bounds<T: Clone + PartialOrd>(
    this: Bound<T>,
    other: Bound<T>,
    cmp_type: BoundType,
) -> Option<Ordering> {
    if this.value == other.value && this.inclusive != other.inclusive {
        if this.inclusive {
            Some(Ordering::Greater)
        } else {
            Some(Ordering::Less)
        }
    } else if cmp_type == BoundType::Upper {
        Some(this.value.partial_cmp(&other.value)?)
    } else {
        Some(this.value.partial_cmp(&other.value)?.reverse())
    }
}

fn cmp_bounded_and_half_bounded_range<T: Clone + PartialOrd>(
    half_bounded: &Range<T>,
    bounded: &Range<T>,
) -> Option<Ordering> {
    if (half_bounded.has_upper_bound()
        && partial_cmp_bounds(
            half_bounded.max.clone()?,
            bounded.max.clone()?,
            BoundType::Upper,
        )? != Ordering::Less)
        || (half_bounded.has_lower_bound()
            && partial_cmp_bounds(
                half_bounded.min.clone()?,
                bounded.min.clone()?,
                BoundType::Lower,
            )? != Ordering::Less)
    {
        Some(Ordering::Greater)
    } else {
        None
    }
}

pub fn in_int_range(value: &Value, range: &Range<i64>) -> bool {
    match as_i64(&value) {
        Some(n) => in_range(n, range),
        _ => false,
    }
}

pub fn in_uint_range(value: &Value, range: &Range<u64>) -> bool {
    match as_u64(&value) {
        Some(n) => in_range(n, range),
        _ => false,
    }
}

pub fn in_float_range(value: &Value, range: &Range<f64>) -> bool {
    match as_f64(&value) {
        Some(x) => in_range(x, range),
        _ => false,
    }
}

pub fn in_big_int_range(value: &Value, range: &Range<BigInt>) -> bool {
    match as_big_int(&value) {
        Some(n) => in_range(n, range),
        _ => false,
    }
}

pub fn in_range<T: Clone + PartialOrd>(value: T, range: &Range<T>) -> bool {
    let lower = range
        .min
        .clone()
        .map(|bound| {
            let Bound {
                value: lb,
                inclusive: incl,
            } = bound;

            if incl {
                lb <= value
            } else {
                lb < value
            }
        })
        .unwrap_or(true);

    let upper = range
        .max
        .clone()
        .map(|bound| {
            let Bound {
                value: ub,
                inclusive: incl,
            } = bound;
            if incl {
                ub >= value
            } else {
                ub > value
            }
        })
        .unwrap_or(true);
    lower && upper
}

pub fn i32_range_as_i64() -> Range<i64> {
    Range::<i64>::new(
        Some(Bound::inclusive(i32::MIN as i64)),
        Some(Bound::inclusive(i32::MAX as i64)),
    )
}

pub fn i32_range_as_big_int() -> Range<BigInt> {
    Range::<BigInt>::new(
        Some(Bound::inclusive(BigInt::from(i32::MIN))),
        Some(Bound::inclusive(BigInt::from(i32::MAX))),
    )
}

pub fn i64_range_as_i64() -> Range<i64> {
    Range::<i64>::bounded(Bound::inclusive(i64::MIN), Bound::inclusive(i64::MAX))
}

pub fn i64_range_as_big_int() -> Range<BigInt> {
    Range::<BigInt>::new(
        Some(Bound::inclusive(BigInt::from(i64::MIN))),
        Some(Bound::inclusive(BigInt::from(i64::MAX))),
    )
}

pub fn u32_range_as_i64() -> Range<i64> {
    Range::<i64>::new(
        Some(Bound::inclusive(u32::MIN as i64)),
        Some(Bound::inclusive(u32::MAX as i64)),
    )
}

pub fn u32_range_as_u64() -> Range<u64> {
    Range::<u64>::new(
        Some(Bound::inclusive(u32::MIN as u64)),
        Some(Bound::inclusive(u32::MAX as u64)),
    )
}

pub fn u32_range_as_big_int() -> Range<BigInt> {
    Range::<BigInt>::new(
        Some(Bound::inclusive(BigInt::from(u32::MIN))),
        Some(Bound::inclusive(BigInt::from(u32::MAX))),
    )
}

pub fn u64_range_as_u64() -> Range<u64> {
    Range::<u64>::new(
        Some(Bound::inclusive(u64::MIN)),
        Some(Bound::inclusive(u64::MAX)),
    )
}

pub fn u64_range_as_big_int() -> Range<BigInt> {
    Range::<BigInt>::new(
        Some(Bound::inclusive(BigInt::from(u64::MIN))),
        Some(Bound::inclusive(BigInt::from(u64::MAX))),
    )
}

pub fn float_64_range() -> Range<f64> {
    Range::<f64>::bounded(Bound::inclusive(f64::MIN), Bound::inclusive(f64::MAX))
}

pub fn i64_range_to_big_int_range(range: &Range<i64>) -> Range<BigInt> {
    let min = range.get_min().map(|Bound { value, inclusive }| {
        Bound::new(value.to_bigint().expect("infallible"), inclusive)
    });
    let max = range.get_max().map(|Bound { value, inclusive }| {
        Bound::new(value.to_bigint().expect("infallible"), inclusive)
    });

    Range::<BigInt>::new(min, max)
}

pub fn u64_range_to_big_int_range(range: &Range<u64>) -> Range<BigInt> {
    let min = range.get_min().map(|Bound { value, inclusive }| {
        Bound::new(value.to_bigint().expect("infallible"), inclusive)
    });
    let max = range.get_max().map(|Bound { value, inclusive }| {
        Bound::new(value.to_bigint().expect("infallible"), inclusive)
    });

    Range::<BigInt>::new(min, max)
}

// Create a Value from an int range schema.
pub fn int_range_to_value<N: Into<Value> + Clone + PartialOrd>(
    tag: &str,
    range: Range<N>,
) -> Value {
    let mut slots = vec![];
    let Range { min, max } = range;

    if let Some(Bound {
        value,
        inclusive: _,
    }) = min
    {
        slots.push(int_endpoint_to_slot("min", value))
    }
    if let Some(Bound {
        value,
        inclusive: _,
    }) = max
    {
        slots.push(int_endpoint_to_slot("max", value))
    }
    Attr::with_items(tag, slots).into()
}

// Create a Value from a float range schema.
pub fn float_range_to_value<N: Into<Value> + Copy + PartialOrd>(
    tag: &str,
    range: Range<N>,
) -> Value {
    let mut slots = vec![];
    let Range { min, max } = range;

    if let Some(Bound { value, inclusive }) = min {
        slots.push(float_endpoint_to_slot("min", value, inclusive))
    }
    if let Some(Bound { value, inclusive }) = max {
        slots.push(float_endpoint_to_slot("max", value, inclusive))
    }
    Attr::with_items(tag, slots).into()
}
