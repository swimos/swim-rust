use crate::model::schema::{
    as_f64, as_i64, combine_orderings, float_endpoint_to_slot, int_endpoint_to_slot,
};
use crate::model::{Attr, Value};
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
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Range<T: Copy + PartialOrd> {
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

impl<T: Copy + PartialOrd> Range<T> {
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
}

impl<T: Copy + PartialOrd> PartialOrd for Range<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.eq(other) {
            Some(Ordering::Equal)
        } else if self.is_doubly_unbounded() {
            Some(Ordering::Greater)
        } else if other.is_doubly_unbounded() {
            Some(Ordering::Less)
        } else if self.is_bounded() && other.is_bounded() {
            let lower = partial_cmp_bounds(self.min?, other.min?, BoundType::Lower)?;
            let upper = partial_cmp_bounds(self.max?, other.max?, BoundType::Upper)?;
            combine_orderings(lower, upper)
        } else if other.is_bounded() {
            cmp_bounded_and_half_bounded_range(self, other)
        } else if self.is_bounded() {
            Some(cmp_bounded_and_half_bounded_range(other, self)?.reverse())
        } else if self.has_lower_bound() && other.has_lower_bound() {
            partial_cmp_bounds(self.min?, other.min?, BoundType::Lower)
        } else if self.has_upper_bound() && other.has_upper_bound() {
            partial_cmp_bounds(self.max?, other.max?, BoundType::Upper)
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

fn partial_cmp_bounds<T: Copy + PartialOrd>(
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

fn cmp_bounded_and_half_bounded_range<T: Copy + PartialOrd>(
    half_bounded: &Range<T>,
    bounded: &Range<T>,
) -> Option<Ordering> {
    if (half_bounded.has_upper_bound()
        && partial_cmp_bounds(half_bounded.max?, bounded.max?, BoundType::Upper)? != Ordering::Less)
        || (half_bounded.has_lower_bound()
            && partial_cmp_bounds(half_bounded.min?, bounded.min?, BoundType::Lower)?
                != Ordering::Less)
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

pub fn in_float_range(value: &Value, range: &Range<f64>) -> bool {
    match as_f64(&value) {
        Some(x) => in_range(x, range),
        _ => false,
    }
}

pub fn in_range<T: Copy + PartialOrd>(value: T, range: &Range<T>) -> bool {
    let lower = range
        .min
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

pub fn int_32_range() -> Range<i64> {
    Range::<i64>::bounded(
        Bound::inclusive(i32::MIN as i64),
        Bound::inclusive(i32::MAX as i64),
    )
}

pub fn int_64_range() -> Range<i64> {
    Range::<i64>::bounded(Bound::inclusive(i64::MIN), Bound::inclusive(i64::MAX))
}

pub fn float_64_range() -> Range<f64> {
    Range::<f64>::bounded(Bound::inclusive(f64::MIN), Bound::inclusive(f64::MAX))
}

// Create a Value from an int range schema.
pub fn int_range_to_value<N: Into<Value> + Copy + PartialOrd>(tag: &str, range: Range<N>) -> Value {
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
