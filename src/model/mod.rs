use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::iter;

pub mod parser;

#[cfg(test)]
mod tests;

#[derive(Clone,PartialEq)]
pub enum Value {
    Extant,
    Int32Value(i32),
    Int64Value(i64),
    Float64Value(f64),
    BooleanValue(bool),
    Text(String),
    Record(Vec<Attr>, Vec<Item>),
}

impl Value {

    pub fn text<T: ToString>(x: T) -> Value {
        Value::Text(x.to_string())
    }

    pub fn record(items: Vec<Item>) -> Value {
        Value::Record(vec![], items)
    }

    pub fn singleton<I: ToItem>(value: I) -> Value {
        Value::record(vec![value.item()])
    }

    pub fn empty_record() -> Value {
        Value::Record(vec![], vec![])
    }

    pub fn from_vec<V: ToValue>(items: Vec<V>) -> Value {
        Value::Record(vec![],
                      items.into_iter()
                          .map(|item| Item::of(item))
                          .collect())
    }

    pub fn of_attr(attr: Attr) -> Value {
        Value::Record(vec![attr], vec![])
    }

    pub fn of_attrs(attrs: Vec<Attr>) -> Value {
        Value::Record(attrs, vec![])
    }

}

pub trait ToValue {

    fn val(self) -> Value;

}

impl ToValue for i32 {
    fn val(self) -> Value {
        Value::Int32Value(self)
    }
}

impl ToValue for i64 {
    fn val(self) -> Value {
        Value::Int64Value(self)
    }
}

impl ToValue for f64 {

    fn val(self) -> Value {
        Value::Float64Value(self)
    }

}

impl ToValue for bool {

    fn val(self) -> Value {
        Value::BooleanValue(self)
    }

}

impl ToValue for String {

    fn val(self) -> Value {
        Value::Text(self)
    }

}

impl ToValue for &str {

    fn val(self) -> Value {
        Value::Text(self.to_owned())
    }

}

impl ToValue for Value {
    fn val(self) -> Value {
        self
    }
}

#[derive(Clone,PartialEq)]
pub struct Attr {
    name: String,
    value: Value,
}

impl Attr {

    pub fn of<T: ToAttr>(rep : T) -> Attr {
        rep.attr()
    }

}

pub trait ToAttr {

    fn attr(self) -> Attr;

}

impl ToAttr for &str {
    fn attr(self) -> Attr {
        Attr { name: self.to_owned(), value: Value::Extant }
    }
}

impl ToAttr for String {
    fn attr(self) -> Attr {
        Attr { name: self, value: Value::Extant }
    }
}

impl<V: ToValue> ToAttr for (&str, V) {

    fn attr(self) -> Attr {
        let (name_str, v) = self;
        Attr { name: name_str.to_owned(), value: v.val() }
    }

}

impl<V: ToValue> ToAttr for (String, V) {

    fn attr(self) -> Attr {
        let (name, v) = self;
        Attr { name, value: v.val() }
    }

}

#[derive(Clone,PartialEq)]
pub enum Item {
    ValueItem(Value),
    Slot(Value, Value),
}

impl Item {

    pub fn of<I: ToItem>(item: I) -> Item {
        item.item()
    }

    pub fn slot<K: ToValue, V : ToValue>(key : K, value : V) -> Item {
        Item::Slot(key.val(), value.val())
    }
}

pub trait ToItem {

    fn item(self) -> Item;

}

pub trait ToSlot {

    fn slot(self) -> Item;

}

impl<V: ToValue> ToItem for V {
    fn item(self) -> Item {
        Item::ValueItem(self.val())
    }
}

impl ToItem for Item {
    fn item(self) -> Item {
        self
    }
}

impl<K: ToValue, V: ToValue> ToSlot for (K, V) {

    fn slot(self) -> Item {
        let (key, value) = self;
        Item::Slot(key.val(), value.val())
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
            },
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
            },
            Value::Record(attrs, body)if attrs.is_empty() && body.len() == 1 => {
                f.write_str("@")?;
                write_string_literal(&self.name, f)?;
                match body.first() {
                    Some(slot@ Item::Slot(_, _)) => write!(f, "({})", slot),
                    _ => write!(f, "({})", &self.value),
                }
            },
            Value::Extant => {
                f.write_str("@")?;
                write_string_literal(&self.name, f)
            },
            ow => {
                f.write_str("@")?;
                write_string_literal(&self.name, f)?;
                write!(f, "({})", ow)
            },
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

static DIGITS: [char; 16] = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'];

fn escape_text(text: &str) -> String {
    let mut output = Vec::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '"' => {
                output.push('\\');
                output.push('\"');
            },
            '\\' => {
                output.push('\\');
                output.push('\\');
            },
            '\r' => {
                output.push('\\');
                output.push('r');
            },
            '\n' => {
                output.push('\\');
                output.push('n');
            },
            '\t' => {
                output.push('\\');
                output.push('t');
            },
            '\u{08}' => {
                output.push('\\');
                output.push('b');
            },
            '\u{0c}' => {
                output.push('\\');
                output.push('f');
            },
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





