use super::*;
use crate::model::parser::ValueParseState::AfterAttribute;

pub fn is_identifier_start(c: char) -> bool {
    c >= 'A' && c <= 'Z' ||
        c == '_' ||
        c >= 'a' && c <= 'z' ||
        c == '\u{b7}' ||
        c >= '\u{c0}' && c <= '\u{d6}' ||
        c >= '\u{d8}' && c <= '\u{f6}' ||
        c >= '\u{f8}' && c <= '\u{37d}' ||
        c >= '\u{37f}' && c <= '\u{1fff}' ||
        c >= '\u{200c}' && c <= '\u{200d}' ||
        c >= '\u{203f}' && c <= '\u{2040}' ||
        c >= '\u{2070}' && c <= '\u{218f}' ||
        c >= '\u{2c00}' && c <= '\u{2fef}' ||
        c >= '\u{3001}' && c <= '\u{d7ff}' ||
        c >= '\u{f900}' && c <= '\u{fdcf}' ||
        c >= '\u{fdf0}' && c <= '\u{fffd}' ||
        c >= '\u{10000}' && c <= '\u{effff}'
}

pub fn is_identifier_char(c: char) -> bool {
    is_identifier_start(c) || c == '-' || c >= '0' && c <= '9'
}

pub fn is_identifier(name: &str) -> bool {
    if name == "true" || name == "false" {
        false
    } else {
        let mut name_chars = name.chars();
        match name_chars.next() {
            Some(c) if is_identifier_start(c) =>
                name_chars.all(is_identifier_char),
            _ => false,
        }
    }
}

#[derive(PartialEq)]
enum ReconToken<'a> {
    AttrMarker,
    AttrBodyStart,
    AttrBodyEnd,
    RecordBodyStart,
    RecordBodyEnd,
    SlotDivider,
    EntrySep,
    NewLine,
    Identifier(&'a str),
    StringLiteral(&'a str),
    Int32Literal(i32),
    Int64Literal(i64),
    Float64Literal(f64),
    BoolLiteral(bool),
}

impl ReconToken<'_> {

    fn is_value(&self) -> bool {
        match self {
            ReconToken::Identifier(_) |
            ReconToken::StringLiteral(_) |
            ReconToken::Int32Literal(_) |
            ReconToken::Int64Literal(_) |
            ReconToken::Float64Literal(_) |
            ReconToken::BoolLiteral(_) => true,
            _ => false
        }
    }

    fn to_value(&self) -> Option<Result<Value, String>> {
        match *self {
            ReconToken::Identifier(name) => Some(Ok(Value::Text(name.to_owned()))),
            ReconToken::StringLiteral(name) => Some(
                unescape(name).map(|unesc| Value::Text(unesc))),
            ReconToken::Int32Literal(n) => Some(Ok(Value::Int32Value(n))),
            ReconToken::Int64Literal(n) => Some(Ok(Value::Int64Value(n))),
            ReconToken::Float64Literal(x) => Some(Ok(Value::Float64Value(x))),
            ReconToken::BoolLiteral(p) => Some(Ok(Value::BooleanValue(p))),
            _ => None
        }
    }

}

struct LocatedReconToken<'a>(ReconToken<'a>, usize);

fn loc(token: ReconToken, offset: usize) -> LocatedReconToken {
    LocatedReconToken(token, offset)
}

enum TokenParseState {
    None,
    ReadingIdentifier(usize),
    ReadingStringLiteral(usize, bool),
    ReadingInteger(usize),
    ReadingMantissa(usize),
    StartExponent(usize),
    ReadingExponent(usize),
    ReadingNewLine(usize),
    Failed(usize),
}


fn tokenize_update<'a>(source: &'a str,
                       state: &mut TokenParseState,
                       index: usize,
                       current: char,
                       next: Option<char>) -> Option<Result<LocatedReconToken<'a>, FailedAt>> {
    match state {
        TokenParseState::None => token_start(source, state, index, current, next),
        TokenParseState::ReadingIdentifier(off) => {
            match next {
                Some(c) if is_identifier_char(c) => {
                    None
                },
                _ => {
                    let start = *off;
                    let content = &source[start..index + 1];
                    let token = if content == "true" {
                        ReconToken::BoolLiteral(true)
                    } else if content == "false" {
                        ReconToken::BoolLiteral(false)
                    } else {
                        ReconToken::Identifier(content)
                    };
                    *state = TokenParseState::None;
                    Some(Result::Ok(loc(token, start)))
                },
            }
        },
        TokenParseState::ReadingStringLiteral(off, escape) => {
            if current == '\"' {
                let start = *off;
                let token = ReconToken::StringLiteral(&source[start..index]);
                *state = TokenParseState::None;
                Some(Result::Ok(loc(token, start)))
            } else {
                match next {
                    Some(_) => {
                        if current == '\\' && !*escape {
                            *escape = true;
                            None
                        } else {
                            *escape = false;
                            None
                        }
                    },
                    _ => {
                        *state = TokenParseState::Failed(index);
                        Some(Result::Err(FailedAt(index)))
                    },
                }
            }
        },
        TokenParseState::ReadingInteger(off) => {
            match next {
                Some(c) if is_numeric_char(c) => {
                    if current == '.' {
                        *state = TokenParseState::ReadingMantissa(*off)
                    } else if current == 'e' || current == 'E' {
                        *state = TokenParseState::StartExponent(*off)
                    }
                    None
                },
                _ => {
                    let start = *off;
                    Some(parse_int_token(state, start, &source[start..index + 1]))
                },
            }
        },
        TokenParseState::ReadingMantissa(off) => {
            match next {
                Some(c) if is_mantissa_char(c) => {
                    if current == 'e' || current == 'E' {
                        *state = TokenParseState::StartExponent(*off)
                    }
                    None
                },
                _ => {
                    let start = *off;
                    Some(parse_float_token(state, start, &source[start..index + 1]))
                },
            }
        },
        TokenParseState::StartExponent(off) => {
            match next {
                Some(c) if c.is_digit(10) => {
                    if current == '-' || current.is_digit(10) {
                        *state = TokenParseState::ReadingExponent(*off);
                        None
                    } else {
                        let start = *off;
                        *state = TokenParseState::Failed(start);
                        Some(Result::Err(FailedAt(start)))
                    }
                },
                _ => {
                    let start = *off;
                    Some(parse_float_token(state, start, &source[start..index + 1]))
                },
            }
        },
        TokenParseState::ReadingExponent(off) => {
            match next {
                Some(c) if c.is_digit(10) => {
                    if current.is_digit(10) {
                        *state = TokenParseState::ReadingExponent(*off);
                        None
                    } else {
                        let start = *off;
                        *state = TokenParseState::Failed(start);
                        Some(Result::Err(FailedAt(start)))
                    }
                },
                _ => {
                    let start = *off;
                    Some(parse_float_token(state, start, &source[start..index + 1]))
                },
            }
        }
        TokenParseState::ReadingNewLine(off) => {
            match next {
                Some(w) if w.is_whitespace() => {
                    None
                },
                Some(_) => {
                    let start = *off;
                    *state = TokenParseState::None;
                    Some(Ok(loc(ReconToken::NewLine, start)))
                },
                _ => {
                    *state = TokenParseState::None;
                    None
                },
            }
        },
        TokenParseState::Failed(i) => {
            let off = *i;
            *state = TokenParseState::Failed(off);
            Some(Result::Err(FailedAt(off)))
        },
    }
}

fn is_numeric_char(c: char) -> bool {
    c == '.' || is_mantissa_char(c)
}

fn is_mantissa_char(c: char) -> bool {
    c == '-' || c == 'e' || c == 'E' || c.is_digit(10)
}

fn parse_int_token<'a>(state: &mut TokenParseState,
                       offset: usize,
                       rep: &str) -> Result<LocatedReconToken<'a>, FailedAt> {
    match rep.parse::<i64>() {
        Ok(n) => {
            *state = TokenParseState::None;
            Ok(loc(match i32::try_from(n) {
                Ok(m) => {
                    ReconToken::Int32Literal(m)
                },
                Err(_) => ReconToken::Int64Literal(n),
            }, offset))
        },
        Err(_) => {
            *state = TokenParseState::Failed(offset);
            Err(FailedAt(offset))
        },
    }
}

fn parse_float_token<'a>(state: &mut TokenParseState,
                         offset: usize,
                         rep: &str) -> Result<LocatedReconToken<'a>, FailedAt> {
    match rep.parse::<f64>() {
        Ok(x) => {
            *state = TokenParseState::None;
            Ok(loc(ReconToken::Float64Literal(x), offset))
        },
        Err(_) => {
            *state = TokenParseState::Failed(offset);
            Err(FailedAt(offset))
        },
    }
}

fn token_start<'a>(source: &'a str,
                   state: &mut TokenParseState,
                   index: usize,
                   current: char,
                   next: Option<char>) -> Option<Result<LocatedReconToken<'a>, FailedAt>> {
    match current {
        '@' => Some(Result::Ok(loc(ReconToken::AttrMarker, index))),
        '(' => Some(Result::Ok(loc(ReconToken::AttrBodyStart, index))),
        ')' => Some(Result::Ok(loc(ReconToken::AttrBodyEnd, index))),
        '{' => Some(Result::Ok(loc(ReconToken::RecordBodyStart, index))),
        '}' => Some(Result::Ok(loc(ReconToken::RecordBodyEnd, index))),
        ':' => Some(Result::Ok(loc(ReconToken::SlotDivider, index))),
        ',' | ';' => Some(Result::Ok(loc(ReconToken::EntrySep, index))),
        '\r' | '\n' => {
            match next {
                Some(c) if c.is_whitespace() => {
                    *state = TokenParseState::ReadingNewLine(index);
                    None
                },
                _ => {
                    Some(Result::Ok(loc(ReconToken::NewLine, index)))
                },
            }
        },
        w if w.is_whitespace() => None,
        '\"' => {
            *state = TokenParseState::ReadingStringLiteral(index + 1, false);
            None
        },
        c if is_identifier_start(c) => {
            match next {
                Some(c) if is_identifier_char(c) => {
                    *state = TokenParseState::ReadingIdentifier(index);
                    None
                },
                _ => {
                    Some(Result::Ok(
                        loc(ReconToken::Identifier(&source[index..index + 1]), index)))
                },
            }
        },
        '-' => {
            match next {
                Some(c) if c == '.' || c.is_digit(10) => {
                    *state = TokenParseState::ReadingInteger(index);
                    None
                },
                _ => {
                    *state = TokenParseState::Failed(index);
                    Some(Result::Err(FailedAt(index)))
                }
            }
        },
        c if c.is_digit(10) => {
            match next {
                Some(c) if is_numeric_char(c) => {
                    *state = TokenParseState::ReadingInteger(index);
                    None
                },
                _ => {
                    Some(Result::Ok(
                        loc(ReconToken::Int32Literal(c.to_digit(10).unwrap() as i32), index)))
                }
            }
        },
        '.' => {
            match next {
                Some(c) if c.is_digit(10) => {
                    *state = TokenParseState::ReadingMantissa(index);
                    None
                },
                _ => {
                    *state = TokenParseState::Failed(index);
                    Some(Result::Err(FailedAt(index)))
                }
            }
        }
        _ => {
            *state = TokenParseState::Failed(index);
            Some(Result::Err(FailedAt(index)))
        },
    }
}

fn tokenize_str<'a>(repr: &'a str) -> impl Iterator<Item=Result<LocatedReconToken<'a>, FailedAt>> + 'a {

    let following = repr.chars()
        .skip(1)
        .map(|c| Some(c))
        .chain(iter::once(None));

    repr.char_indices()
        .zip(following)
        .scan(TokenParseState::None,
              move |state, ((i, current), next)| {
                  tokenize_update(repr, state, i, current, next)
              })
}

pub struct FailedAt(usize);


enum ValueParseState {
    AttributeStart,
    ReadingAttribute(String),
    AfterAttribute,
    RecordStart(bool),
    InsideBody(bool, bool),
    Single(bool, Value),
    ReadingSlot(bool, Value),
    AfterSlot(bool),
}

impl ValueParseState {

    fn new_record(attr_body: bool) -> (Vec<Attr>, Vec<Item>, ValueParseState) {
        (vec![], vec![], ValueParseState::RecordStart(attr_body))
    }

}

pub enum ParseFailure {
    BadToken(FailedAt),
    InvalidToken(FailedAt),
    IncompleteRecord,
    UnconsumedInput,
}

pub enum ParseTermination {
    Failed(ParseFailure),
    EarlyTermination(Value),
}

fn make_singleton(attrs: Vec<Attr>, value: Value) -> Value {
    Value::Record(attrs, vec![Item::ValueItem(value)])
}

enum EscapeState {
    None,
    Escape,
    UnicodeEscape0,
    UnicodeEscape1(u32),
    UnicodeEscape2(u32, u32),
    UnicodeEscape3(u32, u32, u32),
    Failed,
}

fn is_escape(c: char) -> bool {
    c == '\\' || c == '\"' || c == 'b' || c == 'f' || c == 'n' || c == 'r' || c == '\t'
}

fn unescape(literal: &str) -> Result<String, String> {
    let mut failed = false;
    let unescaped_string = literal.chars().scan(EscapeState::None, |state, c| {
        match state {
            EscapeState::None => {
                if c == '\\' {
                    *state = EscapeState::Escape;
                    None
                } else {
                    Some(c)
                }
            },
            EscapeState::Escape if is_escape(c) => {
                *state = EscapeState::None;
                Some(match c {
                    '\\' => '\\',
                    '\"' => '\"',
                    'b' => '\u{08}',
                    'f' => '\u{0c}',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    ow => ow
                })
            },
            EscapeState::Escape if c == 'u' => {
                *state = EscapeState::UnicodeEscape0;
                None
            },
            EscapeState::UnicodeEscape0 if c == 'u' => None,
            EscapeState::UnicodeEscape0 if c.is_digit(16) => {
                *state = EscapeState::UnicodeEscape1(c.to_digit(16).unwrap());
                None
            },
            EscapeState::UnicodeEscape1(d1) if c.is_digit(16) => {
                *state = EscapeState::UnicodeEscape2(*d1, c.to_digit(16).unwrap());
                None
            },
            EscapeState::UnicodeEscape2(d1, d2) if c.is_digit(16) => {
                *state = EscapeState::UnicodeEscape3(*d1, *d2, c.to_digit(16).unwrap());
                None
            },
            EscapeState::UnicodeEscape3(d1, d2, d3) if c.is_digit(16) => {
                let uc: char = char::try_from(
                    (*d1 << 12) | (*d2 << 8) | (*d3 << 4) | c.to_digit(16).unwrap())
                    .unwrap();
                *state = EscapeState::None;
                Some(uc)
            },
            EscapeState::Failed => None,
            _ => {
                *state = EscapeState::Failed;
                failed = true;
                None
            },
        }
    }).collect();
    if failed {
        Err(literal.to_owned())
    } else {
        Ok(unescaped_string)
    }
}

pub fn parse_single(repr: &str) -> Result<Value, ParseFailure> {
    let mut state = vec![];
    let mut tokens = tokenize_str(repr);
    let mut result: Option<Result<Value, ParseFailure>> = None;
    while let Some(token) = tokens.next() {
        match consume_token(&mut state, token) {
            Some(ParseTermination::Failed(err)) => {
                result = Some(Err(err));
                break
            },
            Some(ParseTermination::EarlyTermination(value)) => {
                result = Some(Ok(value));
                break
            }
            None => {},
        }
    }
    match result {
        Some(e @Err(_)) => e,
        Some(Ok(value)) => {
          match tokens.next() {
              Some(_) => Err(ParseFailure::UnconsumedInput),
              _ => Ok(value)
          }
        },
        _ => {
            let depth = state.len();
            match state.pop() {
                Some((mut attrs, items, parse_state)) if depth == 1 => {
                    match parse_state {
                        ValueParseState::ReadingAttribute(name) => {
                            attrs.push(Attr{ name: name.to_owned(), value: Value::Extant });
                            Ok(Value::Record(attrs, items))
                        },
                        ValueParseState::AfterAttribute => {
                            Ok(Value::Record(attrs, items))
                        },
                        _ => Err(ParseFailure::IncompleteRecord)
                    }
                },
                _ => Err(ParseFailure::IncompleteRecord)
            }
        },
    }

}

fn push_down(state: &mut Vec<(Vec<Attr>, Vec<Item>, ValueParseState)>,
             value : Value,
             offset: usize) -> Option<Result<Value, FailedAt>> {
    if let Some((mut attrs, mut items, mut parse_state)) = state.pop() {
        match parse_state {
            ValueParseState::ReadingAttribute(name) => {
                match value {
                    Value::Record(_, mut body_items) if body_items.len() <= 1 => {
                        match body_items.pop() {
                            Some(Item::ValueItem(single)) => attrs.push(Attr { name, value: single }),
                            Some(slot) => attrs.push(Attr { name, value: Value::Record(vec![], vec![slot]) }),
                            _ => attrs.push(Attr { name, value: Value::Extant }),
                        }
                    },
                    rec @ Value::Record(_, _) => {
                        attrs.push(Attr { name, value: rec })
                    },
                    ow => {
                        attrs.push(Attr { name, value: ow})
                    },
                }
                parse_state = AfterAttribute;
                state.push((attrs, items, parse_state));
                None
            },
            ValueParseState::RecordStart(p) | ValueParseState::InsideBody(p, _) => {
                parse_state = ValueParseState::Single(p, value);
                state.push((attrs, items, parse_state));
                None
            },
            ValueParseState::ReadingSlot(p, key) => {
                items.push(Item::Slot(key, value));
                parse_state = ValueParseState::AfterSlot(p);
                state.push((attrs, items, parse_state));
                None
            },
            _ => Some(Err(FailedAt(offset))),
        }

    } else {
        Some(Ok(value))
    }
}

fn push_down_and_close(state: &mut Vec<(Vec<Attr>, Vec<Item>, ValueParseState)>,
                       value: Value,
                       is_attr: bool,
                       offset: usize) -> Option<Result<Value, FailedAt>> {
    if let Some((attrs, mut items, parse_state)) = state.pop() {
        match parse_state {
            ValueParseState::RecordStart(p) | ValueParseState::InsideBody(p, _) if p == is_attr => {
                items.push(Item::ValueItem(value));
                let record = Value::Record(attrs, items);
                push_down(state, record, offset)
            },
            ValueParseState::ReadingSlot(p, key) if p == is_attr => {
                items.push(Item::Slot(key, value));
                let record = Value::Record(attrs, items);
                push_down(state, record, offset)
            },
            _ => Some(Err(FailedAt(offset))),
        }

    } else {
        Some(Err(FailedAt(offset)))
    }
}

fn consume_token(state: &mut Vec<(Vec<Attr>, Vec<Item>, ValueParseState)>,
                 loc_token: Result<LocatedReconToken, FailedAt>) -> Option<ParseTermination> {
    use ValueParseState::*;
    use ReconToken::*;

    let LocatedReconToken(token, offset) = match loc_token {
        Ok(t) => t,
        Err(at) => return Some(ParseTermination::Failed(ParseFailure::BadToken(at))),
    };

    let result : Option<Result<Value, FailedAt>> =
        if let Some((mut attrs,
                        mut items,
                        mut parse_state)) = state.pop() {

        match (parse_state, token) {
            (AttributeStart, Identifier(name))  => {
                parse_state = ReadingAttribute(name.to_owned());
                state.push((attrs, items, parse_state));
                None
            },
            (AttributeStart, StringLiteral(name))  => {
                match unescape(name) {
                    Ok(unesc) =>  {
                        parse_state = ReadingAttribute(unesc);
                        state.push((attrs, items, parse_state));
                        None
                    },
                    Err(_) => Some(Err(FailedAt(offset))),
                }
            },

            //AFTER ATTRIBUTE NAME

            (ReadingAttribute(name), AttrMarker)  => {
                attrs.push(Attr { name: name.to_owned(), value: Value::Extant });
                parse_state = AttributeStart;
                state.push((attrs, items, parse_state));
                None
            },
            (st @ ReadingAttribute(_), AttrBodyStart)  => {
                state.push((attrs, items, st));
                state.push(ValueParseState::new_record(true));
                None
            },
            (ReadingAttribute(name), RecordBodyStart)  => {
                attrs.push(Attr { name: name.to_owned(), value: Value::Extant });
                parse_state = RecordStart(false);
                state.push((attrs, items, parse_state));
                None
            },
            (ReadingAttribute(attr_name), tok) if tok.is_value() =>{
                let attr = Attr { name: attr_name.to_owned(), value: Value::Extant };
                attrs.push(attr);
                match tok.to_value() {
                    Some(Ok(value)) => {
                        let record = make_singleton(attrs, value);
                        push_down(state, record, offset)
                    },
                    _ => Some(Err(FailedAt(offset))),
                }

            },
            (ReadingAttribute(attr_name), EntrySep) => {
                let attr = Attr { name: attr_name.to_owned(), value: Value::Extant };
                attrs.push(attr);
                let record = Value::Record(attrs, items);
                push_down(state, record, offset)
            },
            (ReadingAttribute(attr_name), tok @ AttrBodyEnd) |
            (ReadingAttribute(attr_name), tok @ RecordBodyEnd) => {
                let attr = Attr { name: attr_name.to_owned(), value: Value::Extant };
                attrs.push(attr);
                let record = Value::Record(attrs, items);
                push_down_and_close(state, record, tok == AttrBodyEnd, offset)
            },

            //AFTER ATTRIBUTE BODY

            (AfterAttribute, AttrMarker)  => {
                parse_state = AttributeStart;
                state.push((attrs, items, parse_state));
                None
            },
            (AfterAttribute, RecordBodyStart)  => {
                parse_state = RecordStart(false);
                state.push((attrs, items, parse_state));
                None
            },
            (AfterAttribute, tok) if tok.is_value() => {
                match tok.to_value() {
                    Some(Ok(value)) => {
                        let record = make_singleton(attrs, value);
                        push_down(state, record, offset)
                    },
                    _ => Some(Err(FailedAt(offset))),
                }
            },
            (AfterAttribute, EntrySep) => {
                let record = Value::Record(attrs, items);
                push_down(state, record, offset)
            },
            (AfterAttribute, tok @ AttrBodyEnd) |
            (AfterAttribute, tok @ RecordBodyEnd) => {
                let record = Value::Record(attrs, items);
                push_down_and_close(state, record, tok == AttrBodyEnd, offset)
            },

            //CLOSING RECORD BODY

            (RecordStart(false), tok @ RecordBodyEnd) |
            (RecordStart(true), tok @ AttrBodyEnd) |
            (AfterSlot(false), tok @ RecordBodyEnd) |
            (AfterSlot(true), tok @ AttrBodyEnd) => {
                let record = Value::Record(attrs, items);
                push_down_and_close(state, record, tok == AttrBodyEnd, offset)
            },
            (Single(false, v), tok @ RecordBodyEnd) |
            (Single(true, v), tok @ AttrBodyEnd) => {
                items.push(Item::ValueItem(v));
                let record = Value::Record(attrs, items);
                push_down_and_close(state, record, tok == AttrBodyEnd, offset)
            },
            (InsideBody(false, required), tok@ RecordBodyEnd) |
            (InsideBody(true, required), tok @ AttrBodyEnd) => {
                if required {
                    items.push(Item::ValueItem(Value::Extant));
                }
                let record = Value::Record(attrs, items);
                push_down_and_close(state, record, tok == AttrBodyEnd, offset)
            },

            //RECORD NESTING

            (st @ RecordStart(_), RecordBodyStart) |
            (st @ ReadingSlot(_, _), RecordBodyStart) |
            (st @ InsideBody(_, _), RecordBodyStart) => {
                state.push((attrs, items, st));
                state.push(ValueParseState::new_record(true));
                None
            },

            (st @ RecordStart(_), AttrMarker) |
            (st @ ReadingSlot(_, _), AttrMarker) |
            (st @ InsideBody(_, _), AttrMarker) => {
                state.push((attrs, items, st));
                state.push((vec![], vec![], AttributeStart));
                None
            },

            //VALUES

            (RecordStart(p), tok) if tok.is_value() => {
                match tok.to_value() {
                    Some(Ok(value)) => {
                        parse_state = Single(p, value);
                        state.push((attrs, items, parse_state));
                        None
                    },
                    _ => Some(Err(FailedAt(offset))),
                }
            },
            (ReadingSlot(p, key), tok) if tok.is_value() => {
                match tok.to_value() {
                    Some(Ok(value)) => {
                        parse_state = AfterSlot(p);
                        items.push(Item::Slot(key, value));
                        state.push((attrs, items, parse_state));
                        None
                    },
                    _ => Some(Err(FailedAt(offset))),
                }
            },
            (Single(p, value), tok) if tok == EntrySep || tok == NewLine => {
                parse_state = InsideBody(p, tok == EntrySep);
                items.push(Item::ValueItem(value));
                state.push((attrs, items, parse_state));
                None
            },

            //SLOTS
            (Single(p, key), SlotDivider) => {
                parse_state = ReadingSlot(p, key);
                state.push((attrs, items, parse_state));
                None
            },
            (AfterSlot(p), tok) if tok == EntrySep || tok == NewLine => {
                parse_state = InsideBody(p, tok == EntrySep);
                state.push((attrs, items, parse_state));
                None
            },

            //FALLBACKS
            (_, NewLine) => None,
            _ => Some(Err(FailedAt(offset))),
        }
    } else {
        match token {
            AttrMarker => {
                state.push((vec![], vec![], AttributeStart));
                None
            },
            RecordBodyStart => {
                state.push(ValueParseState::new_record(false));
                None
            },
            NewLine => None,
            tok if tok.is_value() => {
                match tok.to_value() {
                    Some(Ok(value)) => Some(Ok(value)),
                    _ => Some(Err(FailedAt(offset)))
                }
            },
            _ => Some(Err(FailedAt(offset))),
        }
    };

    match result {
        Some(Ok(value)) => Some(ParseTermination::EarlyTermination(value)),
        Some(Err(failed)) => Some(ParseTermination::Failed(ParseFailure::InvalidToken(failed))),
        _ => None,
    }
}

