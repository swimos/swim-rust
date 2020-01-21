use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::iter;

pub mod parser;

#[cfg(test)]
mod tests;

/// The core Swim model type. A recursive data type that can be represented in text as a Recon
/// document.
#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    /// A defined but empty value.
    Extant,

    /// A 32-bit integer wrapped as a [`Value`].
    Int32Value(i32),

    /// A 64-bit integer wrapped as a [`Value`].
    Int64Value(i64),

    /// A 64-bit floating point number wrapped as a [`Value`].
    Float64Value(f64),

    /// A boolean wrapped as a [`Value`].
    BooleanValue(bool),

    /// A textual value. A text can either be an identifier or a string literal. A literal
    /// consists of underscores, digits and most characters from the basic multilingual plane and
    /// may not start with a digit.
    ///
    /// Literals will be printed "as is" whereas string literals will be quoted and escaped using
    /// Java conventions.
    ///
    /// Additionally, the strings `true` and `false` are not identifiers.
    ///
    /// # Examples
    ///
    /// ```
    /// use swim_rust::model::Value;
    ///
    /// assert_eq!(Value::text("an_identifier").to_string(), "an_identifier");
    /// assert_eq!(Value::text("2morrow").to_string(), r#""2morrow""#);
    /// assert_eq!(Value::text("\t\r\n").to_string(), r#""\t\r\n""#);
    /// assert_eq!(Value::text("true").to_string(), r#""true""#);
    /// ```
    ///
    Text(String),

    ///
    /// A compound [`Value`] consisting of any number of [`Attr`]s and [`Item`]s.
    ///
    Record(Vec<Attr>, Vec<Item>),
}

impl Value {
    /// Create a text value from anything that can be converted to a ['String'].
    pub fn text<T: ToString>(x: T) -> Value {
        Value::Text(x.to_string())
    }

    /// Create a record from a vector of ['Item']s.
    pub fn record(items: Vec<Item>) -> Value {
        Value::Record(vec![], items)
    }

    /// Create a singleton record from anything that can be converted to an ['Item'].
    pub fn singleton<I: Into<Item>>(value: I) -> Value {
        Value::record(vec![value.into()])
    }

    /// Create an empty record.
    pub fn empty_record() -> Value {
        Value::Record(vec![], vec![])
    }

    /// Create a record from a vector of anything that can be converted to ['Item']s.
    pub fn from_vec<I: Into<Item>>(items: Vec<I>) -> Value {
        Value::Record(
            vec![],
            items.into_iter().map(|item| Item::of(item)).collect(),
        )
    }

    /// Create a record consisting of only a single ['Attr'].
    pub fn of_attr(attr: Attr) -> Value {
        Value::Record(vec![attr], vec![])
    }

    /// Create a record from a vector of ['Attr']s.
    pub fn of_attrs(attrs: Vec<Attr>) -> Value {
        Value::Record(attrs, vec![])
    }
}

impl From<i32> for Value {
    fn from(n: i32) -> Self {
        Value::Int32Value(n)
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Int64Value(n)
    }
}

impl From<f64> for Value {
    fn from(x: f64) -> Self {
        Value::Float64Value(x)
    }
}

impl From<bool> for Value {
    fn from(p: bool) -> Self {
        Value::BooleanValue(p)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::Text(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::Text(s.to_owned())
    }
}

/// An attribute that can be applied to a record ['Value']. A key value pair where the key is
/// a ['String'] and the value can be any ['Value'].
#[derive(Clone, PartialEq, Debug)]
pub struct Attr {
    pub name: String,
    pub value: Value,
}

impl Attr {
    /// Create an ['Attr'] from anything that can be converted to one.
    ///
    /// #Examples
    ///
    /// ```
    /// use swim_rust::model::*;
    ///
    /// assert_eq!(Attr::of("name"), Attr { name: String::from("name"), value: Value::Extant });
    /// assert_eq!(Attr::of(("key", 1)), Attr { name: String::from("key"), value: Value::Int32Value(1) });
    /// ```
    pub fn of<T: Into<Attr>>(rep: T) -> Attr {
        rep.into()
    }
}

impl From<&str> for Attr {
    fn from(s: &str) -> Self {
        Attr {
            name: s.to_owned(),
            value: Value::Extant,
        }
    }
}

impl From<String> for Attr {
    fn from(name: String) -> Self {
        Attr {
            name,
            value: Value::Extant,
        }
    }
}

impl<V: Into<Value>> From<(&str, V)> for Attr {
    fn from(pair: (&str, V)) -> Self {
        let (name_str, v) = pair;
        Attr {
            name: name_str.to_owned(),
            value: v.into(),
        }
    }
}

impl<V: Into<Value>> From<(String, V)> for Attr {
    fn from(pair: (String, V)) -> Self {
        let (name, v) = pair;
        Attr {
            name,
            value: v.into(),
        }
    }
}

/// An item that may occur in the body of record ['Value'].
#[derive(Clone, PartialEq, Debug)]
pub enum Item {
    /// An item consisting of a single ['Value'].
    ValueItem(Value),

    /// An item that is a key value pair where both are ['Value']s.
    Slot(Value, Value),
}

impl Item {
    /// Create an ['Item'] from anything that can be converted to one.
    ///
    /// #Examples
    ///
    /// ```
    /// use swim_rust::model::*;
    ///
    /// assert_eq!(Item::of("name"), Item::ValueItem(Value::text("name")));
    /// assert_eq!(Item::of(("key", 1)), Item::Slot(Value::text("key"), Value::Int32Value(1)));
    /// assert_eq!(Item::of((true, -1i64)), Item::Slot(Value::BooleanValue(true), Value::Int64Value(-1)));
    /// ```
    pub fn of<I: Into<Item>>(item: I) -> Item {
        item.into()
    }

    /// Create a slot ['Item'] from a pair of things that can be converted to ['Value']s.
    ///
    /// #Examples
    ///
    /// ```
    /// use swim_rust::model::*;
    /// assert_eq!(Item::slot("key", 1), Item::Slot(Value::text("key"), Value::Int32Value(1)));
    /// ```
    pub fn slot<K: Into<Value>, V: Into<Value>>(key: K, value: V) -> Item {
        Item::Slot(key.into(), value.into())
    }
}

impl<V: Into<Value>> From<V> for Item {
    fn from(v: V) -> Self {
        Item::ValueItem(v.into())
    }
}

impl<K: Into<Value>, V: Into<Value>> From<(K, V)> for Item {
    fn from(pair: (K, V)) -> Self {
        let (key, value) = pair;
        Item::Slot(key.into(), value.into())
    }
}

fn write_string_literal(literal: &str, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
    if parser::is_identifier(literal) {
        f.write_str(literal)
    } else if needs_escape(literal) {
        write!(f, "\"{}\"", escape_text(literal))
    } else {
        write!(f, "\"{}\"", literal)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Value::Extant => f.write_str(""),
            Value::Int32Value(n) => write!(f, "{}", n),
            Value::Int64Value(n) => write!(f, "{}", n),
            Value::Float64Value(x) => write!(f, "{:e}", x),
            Value::BooleanValue(p) => write!(f, "{}", p),
            Value::Text(s) => write_string_literal(s, f),
            Value::Record(attrs, body) => {
                if attrs.is_empty() && body.is_empty() {
                    f.write_str("{}")
                } else {
                    for attr in attrs {
                        write!(f, "{}", attr)?;
                    }
                    if !body.is_empty() {
                        f.write_str("{")?;
                        let mut first = true;
                        for elem in body.iter() {
                            if !first {
                                f.write_str(",")?;
                            }
                            write!(f, "{}", elem)?;
                            first = false;
                        }
                        f.write_str("}")
                    } else {
                        Result::Ok(())
                    }
                }
            }
        }
    }
}

impl Display for Attr {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match &self.value {
            Value::Record(attrs, body) if attrs.is_empty() && body.len() > 1 => {
                write!(f, "@{}(", self.name)?;
                let mut first = true;
                for elem in body.iter() {
                    if !first {
                        f.write_str(",")?;
                    }
                    write!(f, "{}", elem)?;
                    first = false;
                }
                f.write_str(")")
            }
            Value::Record(attrs, body) if attrs.is_empty() && body.len() == 1 => {
                f.write_str("@")?;
                write_string_literal(&self.name, f)?;
                match body.first() {
                    Some(slot @ Item::Slot(_, _)) => write!(f, "({})", slot),
                    _ => write!(f, "({})", &self.value),
                }
            }
            Value::Extant => {
                f.write_str("@")?;
                write_string_literal(&self.name, f)
            }
            ow => {
                f.write_str("@")?;
                write_string_literal(&self.name, f)?;
                write!(f, "({})", ow)
            }
        }
    }
}

impl Display for Item {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Item::ValueItem(value) => write!(f, "{}", value),
            Item::Slot(key, value) => write!(f, "{}:{}", key, value),
        }
    }
}

fn needs_escape(text: &str) -> bool {
    text.chars().any(|c| c < '\u{20}' || c == '"' || c == '\\')
}

static DIGITS: [char; 16] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
];

fn escape_text(text: &str) -> String {
    let mut output = Vec::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '"' => {
                output.push('\\');
                output.push('\"');
            }
            '\\' => {
                output.push('\\');
                output.push('\\');
            }
            '\r' => {
                output.push('\\');
                output.push('r');
            }
            '\n' => {
                output.push('\\');
                output.push('n');
            }
            '\t' => {
                output.push('\\');
                output.push('t');
            }
            '\u{08}' => {
                output.push('\\');
                output.push('b');
            }
            '\u{0c}' => {
                output.push('\\');
                output.push('f');
            }
            cp if cp < '\u{20}' => {
                let n = cp as usize;
                output.push('\\');
                output.push('u');
                output.push(DIGITS[(n >> 12) & 0xf]);
                output.push(DIGITS[(n >> 8) & 0xf]);
                output.push(DIGITS[(n >> 4) & 0xf]);
                output.push(DIGITS[n & 0xf]);
            }
            _ => output.push(c),
        }
    }
    output.iter().collect()
}
