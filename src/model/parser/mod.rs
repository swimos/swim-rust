use super::*;
use std::borrow::Borrow;

#[cfg(test)]
mod tests;

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

#[derive(Eq,Clone,Copy,Hash,Debug,PartialEq,Ord,PartialOrd)]
pub struct FailedAt(usize);

#[derive(Eq,Clone,Copy,Hash,Debug,PartialEq,Ord,PartialOrd)]
enum TokenError {
    NoClosingQuote,
    InvalidInteger,
    InvalidFloat,
    BadStartChar,
}

#[derive(Eq,Clone,Hash,Debug,PartialEq,Ord,PartialOrd)]
pub struct BadToken(FailedAt, TokenError);


#[derive(Eq,Clone,Hash,Debug,PartialEq)]
pub enum ParseFailure {
    TokenizationFailure(BadToken),
    InvalidToken(FailedAt),
    IncompleteRecord,
    UnconsumedInput,
}

#[derive(Clone,Debug,PartialEq)]
pub enum ParseTermination {
    Failed(ParseFailure),
    EarlyTermination(Value),
}

pub fn parse_single(repr: &str) -> Result<Value, ParseFailure> {
    let mut state = vec![];
    let mut tokens = tokenize_str(repr);
    let mut result: Option<Result<Value, ParseFailure>> = None;
    while let Some(maybe_token) = tokens.next() {
        match maybe_token {
            Ok(token) => {
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
            },
            Err(failed) => {
                result = Some(Err(ParseFailure::TokenizationFailure(failed)));
                break
            },
        }

    }
    match result {
        Some(e @ Err(_)) => e,
        Some(Ok(value)) => {
            let mut rem = tokens.filter(|t| {
                match t {
                    Ok(LocatedReconToken(ReconToken::NewLine, _)) => false,
                    _ => true,
                }
            });
            match rem.next() {
                Some(_) => Err(ParseFailure::UnconsumedInput),
                _ => Ok(value)
            }
        },
        _ => {
            let depth = state.len();
            match state.pop() {
                Some(Frame { mut attrs, items, parse_state}) if depth == 1 => {
                    match parse_state {
                        ValueParseState::ReadingAttribute(name) => {
                            attrs.push(Attr { name: name.to_owned(), value: Value::Extant });
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

trait TokenStr: PartialEq + Borrow<str> + Into<String> {

}

impl<'a> TokenStr for &'a str {

}

impl<'a> TokenStr for String {

}

#[derive(PartialEq,Debug)]
enum ReconToken<S> {
    AttrMarker,
    AttrBodyStart,
    AttrBodyEnd,
    RecordBodyStart,
    RecordBodyEnd,
    SlotDivider,
    EntrySep,
    NewLine,
    Identifier(S),
    StringLiteral(S),
    Int32Literal(i32),
    Int64Literal(i64),
    Float64Literal(f64),
    BoolLiteral(bool),
}

impl<S: TokenStr> ReconToken<S> {
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

    fn unwrap_value(self) -> Option<Result<Value, String>> {
        match self {
            ReconToken::Identifier(name) => Some(Ok(Value::Text(name.into()))),
            ReconToken::StringLiteral(name) => Some(
                unescape(name.borrow()).map(|unesc| Value::Text(unesc))),
            ReconToken::Int32Literal(n) => Some(Ok(Value::Int32Value(n))),
            ReconToken::Int64Literal(n) => Some(Ok(Value::Int64Value(n))),
            ReconToken::Float64Literal(x) => Some(Ok(Value::Float64Value(x))),
            ReconToken::BoolLiteral(p) => Some(Ok(Value::BooleanValue(p))),
            _ => None
        }
    }
}

#[derive(PartialEq,Debug)]
struct LocatedReconToken<S>(ReconToken<S>, usize);

fn loc<S>(token: ReconToken<S>, offset: usize) -> LocatedReconToken<S> {
    LocatedReconToken(token, offset)
}

#[derive(PartialEq,Eq,Debug)]
enum TokenParseState {
    None,
    ReadingIdentifier(usize),
    ReadingStringLiteral(usize, bool),
    ReadingInteger(usize),
    ReadingMantissa(usize),
    StartExponent(usize),
    ReadingExponent(usize),
    ReadingNewLine(usize),
    Failed(usize, TokenError),
}

fn tokenize_update<'a>(source: &'a str,
                       state: &mut TokenParseState,
                       index: usize,
                       current: char,
                       next: Option<(usize, char)>) -> Option<Result<LocatedReconToken<&'a str>, BadToken>> {
    match state {
        TokenParseState::None => token_start(source, state, index, current, next),
        TokenParseState::ReadingIdentifier(off) => {
            match next {
                Some((_, c)) if is_identifier_char(c) => {
                    None
                },
                Some((next_off, _)) => {
                    let tok = extract_identifier(&source, next_off, *off);
                    *state = TokenParseState::None;
                    tok
                },
                _ => {
                    let tok = extract_identifier(&source, source.len(), *off);
                    *state = TokenParseState::None;
                    tok
                },
            }
        },
        TokenParseState::ReadingStringLiteral(off, escape) => {
            if current == '\"' {
                if *escape {
                    *escape = false;
                    None
                } else {
                    let start = *off;
                    let token = ReconToken::StringLiteral(&source[start..index]);
                    *state = TokenParseState::None;
                    Some(Result::Ok(loc(token, start)))
                }
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
                        *state = TokenParseState::Failed(index, TokenError::NoClosingQuote);
                        Some(Result::Err(BadToken(FailedAt(index), TokenError::NoClosingQuote)))
                    },
                }
            }
        },
        TokenParseState::ReadingInteger(off) => {
            match next {
                Some((_, c)) if is_numeric_char(c) => {
                    if current == '.' {
                        *state = TokenParseState::ReadingMantissa(*off)
                    } else if current == 'e' || current == 'E' {
                        *state = TokenParseState::StartExponent(*off)
                    }
                    None
                },
                Some((next_off, _)) => {
                    let start = *off;
                    Some(parse_int_token(state, start, &source[start..next_off]))
                },
                _ => {
                    let start = *off;
                    Some(parse_int_token(state, start, &source[start..source.len()]))
                },
            }
        },
        TokenParseState::ReadingMantissa(off) => {
            match next {
                Some((_, c)) if is_mantissa_char(c) => {
                    if current == 'e' || current == 'E' {
                        *state = TokenParseState::StartExponent(*off)
                    }
                    None
                },
                Some((next_off, _)) => {
                    let start = *off;
                    Some(parse_float_token(state, start, &source[start..next_off]))
                },
                _ => {
                    let start = *off;
                    Some(parse_float_token(state, start, &source[start..source.len()]))
                },
            }
        },
        TokenParseState::StartExponent(off) => {
            match next {
                Some((_, c)) if c.is_digit(10) => {
                    if current == '-' || current.is_digit(10) {
                        *state = TokenParseState::ReadingExponent(*off);
                        None
                    } else {
                        let start = *off;
                        *state = TokenParseState::Failed(start, TokenError::InvalidFloat);
                        Some(Result::Err(BadToken(FailedAt(start), TokenError::InvalidFloat)))
                    }
                },
                Some((next_off, _)) => {
                    if current.is_digit(10) {
                        let start = *off;
                        Some(parse_float_token(state, start, &source[start..next_off]))
                    } else {
                        Some(Err(BadToken(FailedAt(*off), TokenError::InvalidFloat)))
                    }
                },
                _ => {
                    let start = *off;
                    Some(parse_float_token(state, start, &source[start..source.len()]))
                },
            }
        },
        TokenParseState::ReadingExponent(off) => {
            match next {
                Some((_, c)) if c.is_digit(10) => {
                    if current.is_digit(10) {
                        *state = TokenParseState::ReadingExponent(*off);
                        None
                    } else {
                        let start = *off;
                        *state = TokenParseState::Failed(start, TokenError::InvalidFloat);
                        Some(Result::Err(BadToken(FailedAt(start), TokenError::InvalidFloat)))
                    }
                },
                Some((next_off, _)) => {
                    let start = *off;
                    Some(parse_float_token(state, start, &source[start..next_off]))
                },
                _ => {
                    let start = *off;
                    Some(parse_float_token(state, start, &source[start..source.len()]))
                },
            }
        }
        TokenParseState::ReadingNewLine(off) => {
            match next {
                Some((_, w)) if w.is_whitespace() => {
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
        TokenParseState::Failed(i, err) => {

            Some(Err(BadToken(FailedAt(*i), *err)))
        },
    }
}

fn extract_identifier(source: &str,
                      next_index: usize,
                      start: usize) -> Option<Result<LocatedReconToken<&str>, BadToken>> {
    let content = &source[start..next_index];
    let token = if content == "true" {
        ReconToken::BoolLiteral(true)
    } else if content == "false" {
        ReconToken::BoolLiteral(false)
    } else {
        ReconToken::Identifier(content)
    };
    Some(Result::Ok(loc(token, start)))
}

fn is_numeric_char(c: char) -> bool {
    c == '.' || is_mantissa_char(c)
}

fn is_mantissa_char(c: char) -> bool {
    c == '-' || c == 'e' || c == 'E' || c.is_digit(10)
}

fn parse_int_token<'a>(state: &mut TokenParseState,
                       offset: usize,
                       rep: &str) -> Result<LocatedReconToken<&'a str>, BadToken> {
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
            *state = TokenParseState::Failed(offset, TokenError::InvalidInteger);
            Err(BadToken(FailedAt(offset), TokenError::InvalidInteger))
        },
    }
}

fn parse_float_token<'a>(state: &mut TokenParseState,
                         offset: usize,
                         rep: &str) -> Result<LocatedReconToken<&'a str>, BadToken> {
    match rep.parse::<f64>() {
        Ok(x) => {
            *state = TokenParseState::None;
            Ok(loc(ReconToken::Float64Literal(x), offset))
        },
        Err(_) => {
            *state = TokenParseState::Failed(offset, TokenError::InvalidFloat);
            Err(BadToken(FailedAt(offset), TokenError::InvalidFloat))
        },
    }
}

fn token_start<'a>(source: &'a str,
                   state: &mut TokenParseState,
                   index: usize,
                   current: char,
                   next: Option<(usize, char)>) -> Option<Result<LocatedReconToken<&'a str>, BadToken>> {
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
                Some((_, c)) if c.is_whitespace() => {
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
            match next {
                Some((next_index, _)) => {
                    *state = TokenParseState::ReadingStringLiteral(next_index, false);
                    None
                }
                _ => {
                    *state = TokenParseState::Failed(index, TokenError::NoClosingQuote);
                    Some(Err(BadToken(FailedAt(index), TokenError::NoClosingQuote)))
                }
            }
        },
        c if is_identifier_start(c) => {
            match next {
                Some((_, c)) if is_identifier_char(c) => {
                    *state = TokenParseState::ReadingIdentifier(index);
                    None
                },
                Some((next_off, _)) => {
                    Some(Ok(loc(ReconToken::Identifier(&source[index..next_off]), index)))
                },
                _ => {
                    Some(Ok(loc(ReconToken::Identifier(&source[index..source.len()]), index)))
                },
            }
        },
        '-' => {
            match next {
                Some((_, c)) if c == '.' || c.is_digit(10) => {
                    *state = TokenParseState::ReadingInteger(index);
                    None
                },
                _ => {
                    *state = TokenParseState::Failed(index, TokenError::InvalidInteger);
                    Some(Result::Err(BadToken(FailedAt(index), TokenError::InvalidInteger)))
                }
            }
        },
        c if c.is_digit(10) => {
            match next {
                Some((_, c)) if is_numeric_char(c) => {
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
                Some((_, c)) if c.is_digit(10) => {
                    *state = TokenParseState::ReadingMantissa(index);
                    None
                },
                _ => {
                    *state = TokenParseState::Failed(index, TokenError::InvalidFloat);
                    Some(Result::Err(BadToken(FailedAt(index), TokenError::InvalidFloat)))
                }
            }
        }
        _ => {
            *state = TokenParseState::Failed(index, TokenError::BadStartChar);
            Some(Result::Err(BadToken(FailedAt(index), TokenError::BadStartChar)))
        },
    }
}

fn final_token<'a>(source: &'a str,
                   state: &mut TokenParseState) -> Option<Result<LocatedReconToken<&'a str>, BadToken>> {
    match state {
        TokenParseState::ReadingIdentifier(off) =>
            extract_identifier(source, source.len(), *off),
        TokenParseState::ReadingInteger(off) => {
            let start = *off;
            Some(parse_int_token(state, start, &source[start..source.len()]))
        },
        TokenParseState::ReadingMantissa(off) => {
            let start = *off;
            Some(parse_float_token(state, start, &source[start..source.len()]))
        },
        TokenParseState::StartExponent(off) =>
            Some(Err(BadToken(FailedAt(*off), TokenError::InvalidFloat))),
        TokenParseState::ReadingExponent(off) => {
            let start = *off;
            Some(parse_float_token(state, start, &source[start..source.len()]))
        },
        TokenParseState::ReadingNewLine(off) =>
            Some(Ok(LocatedReconToken(ReconToken::NewLine, *off))),
        TokenParseState::Failed(off, err) =>
            Some(Err(BadToken(FailedAt(*off), *err))),
        _ => None,
    }
}

fn tokenize_str<'a>(repr: &'a str) -> impl Iterator<Item=Result<LocatedReconToken<&'a str>, BadToken>> + 'a {
    let following = repr.char_indices()
        .skip(1)
        .map(|ci| Some(ci))
        .chain(iter::once(None));

    repr.char_indices()
        .zip(following)
        .scan(TokenParseState::None,
              move |state, ((i, current), next)| {
                  let current_token = tokenize_update(repr, state, i, current, next);
                  match (&current_token, next) {
                      (Some(_), _) => Some(current_token),
                      (None, None) => Some(final_token(repr, state)),
                      _ => Some(None)
                  }
              })
        .flatten()
}

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
    c == '\\' || c == '\"' || c == 'b' || c == 'f' || c == 'n' || c == 'r' || c == 't'
}

/// Unescape a string using Java conventions. Returns the input as a failure if the string
/// contains an invalid escape.
///
/// TODO Handle escaped UTF-16 surrogate pairs.
pub fn unescape(literal: &str) -> Result<String, String> {
    let mut failed = false;
    let unescaped_string = literal.chars().scan(EscapeState::None, |state, c| {
        Some(match state {
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
        })
    }).flatten().collect();
    if failed {
        Err(literal.to_owned())
    } else {
        Ok(unescaped_string)
    }
}

fn push_down(state: &mut Vec<Frame>,
             value: Value,
             offset: usize) -> Option<Result<Value, FailedAt>> {
    if let Some(Frame { mut attrs, mut items, mut parse_state }) = state.pop() {
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
                        attrs.push(Attr { name, value: ow })
                    },
                }
                parse_state = ValueParseState::AfterAttribute;
                state.push(Frame { attrs, items, parse_state });
                None
            },
            ValueParseState::RecordStart(p) | ValueParseState::InsideBody(p, _) => {
                parse_state = ValueParseState::Single(p, value);
                state.push(Frame { attrs, items, parse_state });
                None
            },
            ValueParseState::ReadingSlot(p, key) => {
                items.push(Item::Slot(key, value));
                parse_state = ValueParseState::AfterSlot(p);
                state.push(Frame { attrs, items, parse_state });
                None
            },
            _ => Some(Err(FailedAt(offset))),
        }
    } else {
        Some(Ok(value))
    }
}

fn push_down_and_close(state: &mut Vec<Frame>,
                       value: Value,
                       is_attr: bool,
                       offset: usize) -> Option<Result<Value, FailedAt>> {
    if let Some(Frame { attrs, mut items, parse_state }) = state.pop() {
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

enum StartAt {
    Attrs,
    AttrBody,
    RecordBody,
}

struct Frame {
    attrs: Vec<Attr>,
    items: Vec<Item>,
    parse_state: ValueParseState,
}

impl Frame {
    fn new_record(start: StartAt) -> Frame {
        Frame {
            attrs: vec![],
            items: vec![],
            parse_state: match start {
                StartAt::Attrs => ValueParseState::AttributeStart,
                StartAt::AttrBody => ValueParseState::RecordStart(true),
                StartAt::RecordBody => ValueParseState::RecordStart(false),
            },
        }
    }
}

enum StateModification {
    Fail,
    RePush(Frame),
    OpenNew(Frame, StartAt),
    PushDown(Value),
    PushDownAndClose(Value, bool),

}

impl StateModification {
    fn apply(self, state: &mut Vec<Frame>,
             offset: usize) -> Option<Result<Value, FailedAt>> {
        match self {
            StateModification::Fail => Some(Err(FailedAt(offset))),
            StateModification::RePush(frame) => {
                state.push(frame);
                None
            },
            StateModification::OpenNew(frame, start) => {
                state.push(frame);
                match start {
                    StartAt::Attrs => {},
                    StartAt::AttrBody => {},
                    StartAt::RecordBody => {},
                }
                state.push(Frame::new_record(start));
                None
            },
            StateModification::PushDown(value) => {
                push_down(state, value, offset)
            }
            StateModification::PushDownAndClose(value, attr_body) => {
                push_down_and_close(state, value, attr_body, offset)
            }
        }
    }
}

fn repush(attrs: Vec<Attr>, items: Vec<Item>, parse_state: ValueParseState) -> StateModification {
    StateModification::RePush(Frame {
        attrs,
        items,
        parse_state,
    })
}

fn open_new(attrs: Vec<Attr>, items: Vec<Item>, parse_state: ValueParseState, start_at: StartAt) -> StateModification {
    StateModification::OpenNew(Frame {
        attrs,
        items,
        parse_state,
    }, start_at)
}

fn consume_token<S: TokenStr>(state: &mut Vec<Frame>,
                 loc_token: LocatedReconToken<S>) -> Option<ParseTermination> {
    use ValueParseState::*;

    let LocatedReconToken(token, offset) = loc_token;

    if let Some(Frame { attrs, items, parse_state }) = state.pop() {
        let state_mod: StateModification = match parse_state {
            AttributeStart => update_attr_start(token, attrs, items),
            ReadingAttribute(name) => update_reading_attr(token, name, attrs, items),
            AfterAttribute => update_after_attr(token, attrs, items),
            RecordStart(attr_body) => update_record_start(token, attr_body, attrs, items),
            InsideBody(attr_body, required) => update_inside_body(token, attr_body, required, attrs, items),
            Single(attr_body, value) => update_single(token, attr_body, value, attrs, items),
            ReadingSlot(attr_body, key) => update_reading_slot(token, attr_body, key, attrs, items),
            AfterSlot(attr_body) => update_after_slot(token, attr_body, attrs, items),
        };
        match state_mod.apply(state, offset) {
            Some(Ok(value)) => Some(ParseTermination::EarlyTermination(value)),
            Some(Err(failed)) => Some(ParseTermination::Failed(ParseFailure::InvalidToken(failed))),
            _ => None,
        }
    } else {
        use ReconToken::*;
        match token {
            AttrMarker => {
                state.push(Frame::new_record(StartAt::Attrs));
                None
            },
            RecordBodyStart => {
                state.push(Frame::new_record(StartAt::RecordBody));
                None
            },
            NewLine => None,
            tok if tok.is_value() => {
                match tok.unwrap_value() {
                    Some(Ok(value)) => Some(ParseTermination::EarlyTermination(value)),
                    _ => Some(ParseTermination::Failed(ParseFailure::InvalidToken(FailedAt(offset))))
                }
            },
            _ => Some(ParseTermination::Failed(ParseFailure::InvalidToken(FailedAt(offset)))),
        }
    }
}

fn update_attr_start<S: TokenStr>(token: ReconToken<S>,
                     attrs: Vec<Attr>,
                     items: Vec<Item>) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        Identifier(name) => repush(attrs, items, ReadingAttribute(name.into())),
        StringLiteral(name) => {
            match unescape(name.borrow()) {
                Ok(unesc) => {
                    repush(attrs, items, ReadingAttribute(unesc))
                },
                Err(_) => StateModification::Fail,
            }
        },
        _ => StateModification::Fail,
    }
}

fn update_reading_attr<S: TokenStr>(token: ReconToken<S>,
                       name: String,
                       mut attrs: Vec<Attr>,
                       items: Vec<Item>) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        AttrMarker => {
            attrs.push(Attr { name: name.to_owned(), value: Value::Extant });
            repush(attrs, items, AttributeStart)
        },
        AttrBodyStart => open_new(attrs, items, ReadingAttribute(name), StartAt::AttrBody),
        RecordBodyStart => {
            attrs.push(Attr { name, value: Value::Extant });
            repush(attrs, items, RecordStart(false))
        },
        tok if tok.is_value() => {
            let attr = Attr { name, value: Value::Extant };
            attrs.push(attr);
            match tok.unwrap_value() {
                Some(Ok(value)) => {
                    let record = make_singleton(attrs, value);
                    StateModification::PushDown(record)
                },
                _ => StateModification::Fail,
            }
        },
        EntrySep => {
            let attr = Attr { name, value: Value::Extant };
            attrs.push(attr);
            let record = Value::Record(attrs, items);
            StateModification::PushDown(record)
        },
        tok @ AttrBodyEnd | tok @ RecordBodyEnd => {
            let attr = Attr { name, value: Value::Extant };
            attrs.push(attr);
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, tok == AttrBodyEnd)
        },
        NewLine => repush(attrs, items, ReadingAttribute(name)),
        _ => StateModification::Fail,
    }
}

fn update_after_attr<S: TokenStr>(token: ReconToken<S>,
                     attrs: Vec<Attr>,
                     items: Vec<Item>) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        AttrMarker => repush(attrs, items, AttributeStart),
        RecordBodyStart => repush(attrs, items, RecordStart(false)),
        tok if tok.is_value() => {
            match tok.unwrap_value() {
                Some(Ok(value)) => {
                    let record = make_singleton(attrs, value);
                    StateModification::PushDown(record)
                },
                _ => StateModification::Fail,
            }
        },
        EntrySep => {
            let record = Value::Record(attrs, items);
            StateModification::PushDown(record)
        },
        tok @ AttrBodyEnd | tok @ RecordBodyEnd => {
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, tok == AttrBodyEnd)
        },
        NewLine => repush(attrs, items, AfterAttribute),
        _ => StateModification::Fail,
    }
}

fn update_record_start<S: TokenStr>(token: ReconToken<S>,
                       attr_body: bool,
                       attrs: Vec<Attr>,
                       items: Vec<Item>) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        RecordBodyEnd if !attr_body => {
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, false)
        },
        AttrBodyEnd if attr_body => {
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, true)
        },
        RecordBodyStart => open_new(attrs, items, RecordStart(attr_body), StartAt::RecordBody),
        AttrMarker => open_new(attrs, items, RecordStart(attr_body), StartAt::Attrs),
        tok if tok.is_value() => {
            match tok.unwrap_value() {
                Some(Ok(value)) => repush(attrs, items, Single(attr_body, value)),
                _ => StateModification::Fail,
            }
        },
        NewLine => repush(attrs, items, RecordStart(attr_body)),
        _ => StateModification::Fail,
    }
}

fn update_inside_body<S: TokenStr>(token: ReconToken<S>,
                      attr_body: bool,
                      required: bool,
                      attrs: Vec<Attr>,
                      mut items: Vec<Item>) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        RecordBodyEnd if !attr_body => {
            if required {
                items.push(Item::ValueItem(Value::Extant));
            }
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, false)
        },
        AttrBodyEnd if attr_body => {
            if required {
                items.push(Item::ValueItem(Value::Extant));
            }
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, true)
        },
        RecordBodyStart => open_new(attrs, items, InsideBody(attr_body, required), StartAt::RecordBody),
        AttrMarker => open_new(attrs, items, InsideBody(attr_body, required), StartAt::Attrs),
        NewLine => repush(attrs, items, InsideBody(attr_body, required)),
        _ => StateModification::Fail,
    }
}

fn update_single<S: TokenStr>(token: ReconToken<S>,
                 attr_body: bool,
                 value: Value,
                 attrs: Vec<Attr>,
                 mut items: Vec<Item>) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        RecordBodyEnd if !attr_body => {
            items.push(Item::ValueItem(value));
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, false)
        },
        AttrBodyEnd if attr_body => {
            items.push(Item::ValueItem(value));
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, true)
        },
        tok @ EntrySep | tok @ NewLine => {
            items.push(Item::ValueItem(value));
            repush(attrs, items, InsideBody(attr_body, tok == EntrySep))
        },
        SlotDivider => repush(attrs, items, ReadingSlot(attr_body, value)),
        _ => StateModification::Fail,
    }
}

fn update_reading_slot<S: TokenStr>(token: ReconToken<S>,
                       attr_body: bool,
                       key: Value,
                       attrs: Vec<Attr>,
                       mut items: Vec<Item>) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        RecordBodyStart => open_new(attrs, items, ReadingSlot(attr_body, key), StartAt::RecordBody),
        AttrMarker => open_new(attrs, items, ReadingSlot(attr_body, key), StartAt::Attrs),
        tok if tok.is_value() => {
            match tok.unwrap_value() {
                Some(Ok(value)) => {
                    items.push(Item::Slot(key, value));
                    repush(attrs, items, AfterSlot(attr_body))
                },
                _ => StateModification::Fail,
            }
        },
        RecordBodyEnd if !attr_body => {
            items.push(Item::Slot(key, Value::Extant));
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, false)
        },
        AttrBodyEnd if attr_body => {
            items.push(Item::Slot(key, Value::Extant));
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, true)
        },
        tok @ EntrySep | tok @ NewLine => {
            items.push(Item::Slot(key, Value::Extant));
            repush(attrs, items, InsideBody(attr_body, tok == EntrySep))
        },
        _ => StateModification::Fail,
    }
}


fn update_after_slot<S: TokenStr>(token: ReconToken<S>,
                     attr_body: bool,
                     attrs: Vec<Attr>,
                     items: Vec<Item>) -> StateModification {
    use ReconToken::*;
    use ValueParseState::*;

    match token {
        RecordBodyEnd if !attr_body => {
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, false)
        },
        AttrBodyEnd if attr_body => {
            let record = Value::Record(attrs, items);
            StateModification::PushDownAndClose(record, true)
        },
        tok @ EntrySep | tok @ NewLine =>
            repush(attrs, items, InsideBody(attr_body, tok == EntrySep)),
        _ => StateModification::Fail,
    }
}


