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

/// Creates a value from the provided items.
///
/// For example:
/// ```
/// use swim_model::Value;
/// use swim_model::value;
///
/// let value = value!(1i32);
/// assert_eq!(value, Value::Int32Value(1));
/// ```
///
/// ## Extant value:
/// ```
/// use swim_model::{Value, value};
///
/// let value = value!();
/// assert_eq!(value, Value::Extant);
/// ```
///
/// For multiple values the `record!` macro is used internally and uses the same syntax, see the
/// documentation for the syntax.
#[macro_export]
macro_rules! value {
    () => {
        $crate::Value::Extant
    };
    ($x:expr) => {
        $crate::Value::from($x)
    };
    ($($items:expr),+ $(,)?) => (
        $crate::record!($($items),*)
    );
    (items => [$($items:expr),+ $(,)?]) => (
        $crate::record!(items => [$($items),*])
    );
    (attrs => [$($attrs:expr),+ $(,)?]) => (
        $crate::record!(attrs => [$($attrs),*])
    );
    (attrs => [$($attrs:expr),+ $(,)?], items => [$($items:expr),+ $(,)?]) => (
        $crate::record!(attrs => [$($attrs),*], items => [$($items),*])
    );
}

/// Creates a record from the provided items.
///
/// ## An empty record:
/// ```
/// use swim_model::{Value, record};
///
/// let value = record!();
/// assert_eq!(value, Value::Record(vec![], vec![]));
/// ```
/// ## From multiple items:
///
/// ```
/// use swim_model::{Value, record};
///
/// let value = record!(1i32, 2i32, 3i32);
/// assert_eq!(value, Value::from_vec(vec![1, 2, 3]));
/// ```
///
/// ```
/// use swim_model::{Value, record};
///
/// let value = record!(items => [1i32, 2i32, 3i32]);
/// assert_eq!(value, Value::from_vec(vec![1, 2, 3]));
/// ```
///
/// ## From multiple attributes:
///
/// ```
/// use swim_model::{Attr, Value, record};
///
/// let value = record!(attrs => [("first", 1), ("second", 2)]);
/// assert_eq!(value, Value::of_attrs(vec![Attr::of(("first", 1)), Attr::of(("second", 2))]));
/// ```
///
/// ## From attributes and items
/// ```
/// use swim_model::{Attr, Item, Value, record};
///
/// let value = record! {
///     attrs => [("first", 1), ("second", 2)],
///     items => [1i32, 2i32, 3i32]
/// };
/// assert_eq!(value, Value::Record(
///     vec![Attr::of(("first", 1)), Attr::of(("second", 2))],
///     vec![
///         Item::ValueItem(Value::Int32Value(1)),
///         Item::ValueItem(Value::Int32Value(2)),
///         Item::ValueItem(Value::Int32Value(3))
///    ])
/// );
/// ```
///
#[macro_export]
macro_rules! record {
    () => {
        $crate::Value::Record(vec![], vec![])
    };
    ($($items:expr),+ $(,)?) => (
        $crate::Value::Record(Vec::new(), vec![$($items.into()),+])
    );
    (items => [$($items:expr),+ $(,)?]) => (
        $crate::Value::Record(Vec::new(), vec![$($items.into()),+])
    );
    (attrs => [$($attrs:expr),+ $(,)?]) => (
        $crate::Value::of_attrs(vec![$($attrs.into()),+])
    );
    (attrs => [$($attrs:expr),+ $(,)?], items => [$($items:expr),+ $(,)?]) => (
        $crate::Value::Record(vec![$($attrs.into()),+], vec![$($items.into()),+])
    );
}

#[cfg(test)]
mod tests {
    use crate::{Attr, Item, Value};

    #[test]
    fn test_extant() {
        let value = value!();
        assert_eq!(value, Value::Extant);
    }

    #[test]
    fn test_i32() {
        let value = value!(100i32);
        assert_eq!(value, Value::Int32Value(100));
    }

    #[test]
    fn test_record() {
        {
            let expected = Value::from_vec(vec![100i32, 200i32, 300i32]);
            assert_eq!(value!(100i32, 200i32, 300i32), expected);
            assert_eq!(record!(100i32, 200i32, 300i32), expected);
        }
        {
            let expected = Value::from_vec(vec![
                Value::Int32Value(100),
                Value::Int64Value(200),
                Value::from("Hello"),
            ]);
            assert_eq!(
                value!(
                    Value::Int32Value(100),
                    Value::Int64Value(200),
                    Value::from("Hello")
                ),
                expected
            );
            assert_eq!(
                record!(
                    Value::Int32Value(100),
                    Value::Int64Value(200),
                    Value::from("Hello")
                ),
                expected
            );
        }
        {
            let expected = Value::Record(
                vec![Attr::of(("hello", Value::Int32Value(200))), Attr::of("a")],
                vec![
                    Item::ValueItem(Value::Int64Value(100)),
                    Item::ValueItem(Value::Extant),
                ],
            );
            assert_eq!(
                value! {
                    attrs => [("hello", 200), "a"],
                    items => [100i64, Value::Extant]
                },
                expected
            );
            assert_eq!(
                record! {
                    attrs => [("hello", 200), "a"],
                    items => [100i64, Value::Extant]
                },
                expected
            );
        }
        {
            let expected = Value::Record(
                vec![],
                vec![
                    Item::ValueItem(Value::Int64Value(100)),
                    Item::ValueItem(Value::Extant),
                ],
            );
            assert_eq!(
                value! {
                    items => [100i64, Value::Extant]
                },
                expected
            );
            assert_eq!(
                record! {
                    items => [100i64, Value::Extant]
                },
                expected
            );
        }
        {
            let expected = Value::Record(
                vec![Attr::of(("hello", Value::Int32Value(200))), Attr::of("a")],
                vec![],
            );
            assert_eq!(
                value! {
                    attrs => [("hello", 200), "a"]
                },
                expected
            );
            assert_eq!(
                record! {
                    attrs => [("hello", 200), "a"]
                },
                expected
            );
        }
    }
}
