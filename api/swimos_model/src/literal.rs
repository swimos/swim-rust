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

use std::{borrow::Cow, fmt::Formatter};

/// Format a string as a Recon string literal, escaping it where necessary.
///
/// # Arguments
/// * `literal` - The string to format as a literal.
/// * `f` - The output formatter.
pub fn write_string_literal(literal: &str, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
    if crate::identifier::is_identifier(literal) {
        f.write_str(literal)
    } else if needs_escape(literal) {
        write!(f, "\"{}\"", escape_text(literal))
    } else {
        write!(f, "\"{}\"", literal)
    }
}

/// Escape a string so that it can be written as a Recon string literal. If the string does not
/// require escapes a reference to the original string will be returned an no data will be copied.
///
/// # Arguments
/// * `text` - The string to escape.
pub fn escape_if_needed(text: &str) -> Cow<str> {
    if needs_escape(text) {
        Cow::Owned(escape_text(text))
    } else {
        Cow::Borrowed(text)
    }
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

fn needs_escape(text: &str) -> bool {
    text.chars().any(|c| c < '\u{20}' || c == '"' || c == '\\')
}
