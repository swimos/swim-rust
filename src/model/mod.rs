use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::iter;

pub mod parser;

pub enum Value {
    Extant,
    Int32Value(i32),
    Int64Value(i64),
    Float64Value(f64),
    BooleanValue(bool),
    Text(String),
    Record(Vec<Attr>, Vec<Item>),
}

pub struct Attr {
    name: String,
    value: Value,
}

pub enum Item {
    ValueItem(Value),
    Slot(Value, Value),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Value::Extant => f.write_str(""),
            Value::Int32Value(n) => write!(f, "{}", n),
            Value::Int64Value(n) => write!(f, "{}", n),
            Value::Float64Value(x) => write!(f, "{}", x),
            Value::BooleanValue(p) => write!(f, "{}", p),
            Value::Text(s) if parser::is_identifier(s) => f.write_str(s),
            Value::Text(s) if needs_escape(s) => write!(f, "\"{}\"", escape_text(s)),
            Value::Text(s) => write!(f, "\"{}\"", s),
            Value::Record(attrs, body) => {
                if attrs.is_empty() && body.is_empty() {
                    f.write_str("{}")
                } else {
                    for attr in attrs {
                        write!(f, "{}", attr)?;
                    }
                    if !body.is_empty() {
                        f.write_str("{")?;
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
                    first = true;
                }
                Result::Ok(())
            },
            Value::Extant => write!(f, "@{}", self.name),
            ow => write!(f, "@{}({})", self.name, ow),
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





