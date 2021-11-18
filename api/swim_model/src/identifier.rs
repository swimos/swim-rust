// Copyright 2015-2021 Swim Inc.
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

/// Determine if a character is valid at the start of an identifier.
///
/// #Examples
///
/// ```
/// use swim_model::identifier::is_identifier_start;
///
/// assert!(is_identifier_start('a'));
/// assert!(is_identifier_start('ℵ'));
/// assert!(is_identifier_start('_'));
/// assert!(!is_identifier_start('2'));
/// assert!(!is_identifier_start('-'));
/// assert!(!is_identifier_start('@'));
///
/// ```
pub fn is_identifier_start(c: char) -> bool {
    ('A'..='Z').contains(&c)
        || c == '_'
        || ('a'..='z').contains(&c)
        || c == '\u{b7}'
        || ('\u{c0}'..='\u{d6}').contains(&c)
        || ('\u{d8}'..='\u{f6}').contains(&c)
        || ('\u{f8}'..='\u{37d}').contains(&c)
        || ('\u{37f}'..='\u{1fff}').contains(&c)
        || ('\u{200c}'..='\u{200d}').contains(&c)
        || ('\u{203f}'..='\u{2040}').contains(&c)
        || ('\u{2070}'..='\u{218f}').contains(&c)
        || ('\u{2c00}'..='\u{2fef}').contains(&c)
        || ('\u{3001}'..='\u{d7ff}').contains(&c)
        || ('\u{f900}'..='\u{fdcf}').contains(&c)
        || ('\u{fdf0}'..='\u{fffd}').contains(&c)
        || ('\u{10000}'..='\u{effff}').contains(&c)
}

/// Determine if a character is valid after the start of an identifier.
///
/// #Examples
///
/// ```
///
/// use swim_model::identifier::is_identifier_char;
/// assert!(is_identifier_char('a'));
/// assert!(is_identifier_char('ℵ'));
/// assert!(is_identifier_char('_'));
/// assert!(is_identifier_char('2'));
/// assert!(is_identifier_char('-'));
/// assert!(!is_identifier_char('@'));
///
/// ```
pub fn is_identifier_char(c: char) -> bool {
    is_identifier_start(c) || c == '-' || ('0'..='9').contains(&c)
}

/// Determine if a string is a valid Recon identifier.
///
/// #Examples
///
/// ```
/// use swim_model::identifier::is_identifier;
///
/// assert!(is_identifier("name"));
/// assert!(is_identifier("name2"));
/// assert!(is_identifier("_name"));
/// assert!(is_identifier("two_parts"));
/// assert!(!is_identifier("2morrow"));
/// assert!(!is_identifier("@tag"));
///
/// ```
///
pub fn is_identifier(name: &str) -> bool {
    if name == "true" || name == "false" {
        false
    } else {
        let mut name_chars = name.chars();
        match name_chars.next() {
            Some(c) if is_identifier_start(c) => name_chars.all(is_identifier_char),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::is_identifier;

    #[test]
    fn identifiers() {
        assert!(is_identifier("name"));
        assert!(is_identifier("اسم"));
        assert!(is_identifier("name2"));
        assert!(is_identifier("first_second"));
        assert!(is_identifier("_name"));

        assert!(!is_identifier(""));
        assert!(!is_identifier("2name"));
        assert!(!is_identifier("true"));
        assert!(!is_identifier("false"));
        assert!(!is_identifier("two words"));
        assert!(!is_identifier("£%^$&*"));
        assert!(!is_identifier("\r\n\t"));
        assert!(!is_identifier("\"\\\""));
    }
}
