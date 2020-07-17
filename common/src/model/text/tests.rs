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

use crate::model::text::{Text, TextInner};
use std::borrow::{Borrow, BorrowMut};
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

#[test]
fn empty_text() {
    let text = Text::empty();
    let Text(inner) = text;
    assert!(matches!(inner, TextInner::Small(arr) if arr[0] == 0));
}

const SMALL: &str = "word";
const LARGE: &str = "aaaa aaaa aaaa aaaa aaaa aaaa aaaa aaaa aaaa aaaa ";
const BORDERLINE: &str = "borderline string                            !";

#[test]
fn create_text() {
    let small = Text::new(SMALL);
    assert!(small.is_small());
    assert_eq!(small, SMALL);

    let borderline = Text::new(BORDERLINE);
    assert!(borderline.is_small());
    assert_eq!(borderline, BORDERLINE);

    let large = Text::new(LARGE);
    assert!(!large.is_small());
    assert_eq!(large, LARGE);
}

#[test]
fn text_from_string() {
    let small = Text::from_string(SMALL.to_string());
    assert!(small.is_small());
    assert_eq!(small, SMALL);

    let borderline = Text::from_string(BORDERLINE.to_string());
    assert!(borderline.is_small());
    assert_eq!(borderline, BORDERLINE);

    let large = Text::from_string(LARGE.to_string());
    assert!(!large.is_small());
    assert_eq!(large, LARGE);
}

#[test]
fn text_as_str() {
    let mut empty = Text::empty();
    assert_eq!(empty.as_str(), "");
    assert_eq!(empty.as_str_mut(), "");

    let mut small = Text::new(SMALL);
    assert_eq!(small.as_str(), SMALL);
    assert_eq!(small.as_str_mut(), SMALL);

    let mut borderline = Text::new(BORDERLINE);
    assert_eq!(borderline.as_str(), BORDERLINE);
    assert_eq!(borderline.as_str_mut(), BORDERLINE);

    let mut large = Text::new(LARGE);
    assert_eq!(large.as_str(), LARGE);
    assert_eq!(large.as_str_mut(), LARGE);
}

#[test]
fn text_len() {
    let empty = Text::empty();
    assert_eq!(empty.len(), 0);

    let small = Text::new(SMALL);
    assert_eq!(small.len(), SMALL.len());

    let borderline = Text::new(BORDERLINE);
    assert_eq!(borderline.len(), BORDERLINE.len());

    let large = Text::new(LARGE);
    assert_eq!(large.len(), LARGE.len());
}

#[test]
fn text_is_empty() {
    let empty = Text::empty();
    assert!(empty.is_empty());

    let small = Text::new(SMALL);
    assert!(!small.is_empty());

    let borderline = Text::new(BORDERLINE);
    assert!(!borderline.is_empty());

    let large = Text::new(LARGE);
    assert!(!large.is_empty());
}

#[test]
fn text_push_char() {
    let mut empty = Text::empty();
    empty.push('🐳');
    assert_eq!(empty, "🐳");
    assert!(empty.is_small());

    let mut small = Text::new(SMALL);
    small.push('🐳');
    assert_eq!(small, SMALL.to_string() + "🐳");
    assert!(small.is_small());

    let mut borderline = Text::new(BORDERLINE);
    borderline.push('🐳');
    assert_eq!(borderline, BORDERLINE.to_string() + "🐳");
    assert!(!borderline.is_small());

    let mut large = Text::new(LARGE);
    large.push('🐳');
    assert_eq!(large, LARGE.to_string() + "🐳");
    assert!(!large.is_small());
}

#[test]
fn text_push_str() {
    let mut empty = Text::empty();
    empty.push_str("stuff");
    assert_eq!(empty, "stuff");
    assert!(empty.is_small());

    let mut small = Text::new(SMALL);
    small.push_str("stuff");
    assert_eq!(small, SMALL.to_string() + "stuff");
    assert!(small.is_small());

    let mut borderline = Text::new(BORDERLINE);
    borderline.push_str("stuff");
    assert_eq!(borderline, BORDERLINE.to_string() + "stuff");
    assert!(!borderline.is_small());

    let mut large = Text::new(LARGE);
    large.push_str("stuff");
    assert_eq!(large, LARGE.to_string() + "stuff");
    assert!(!large.is_small());
}

#[test]
fn text_as_bytes() {
    let empty = Text::empty();
    assert!(empty.as_bytes().is_empty());

    let small = Text::new(SMALL);
    assert_eq!(small.as_bytes(), SMALL.as_bytes());

    let borderline = Text::new(BORDERLINE);
    assert_eq!(borderline.as_bytes(), BORDERLINE.as_bytes());

    let large = Text::new(LARGE);
    assert_eq!(large.as_bytes(), LARGE.as_bytes());
}

#[test]
fn text_as_ref_bytes() {
    let empty = Text::empty();
    let bytes: &[u8] = empty.as_ref();
    assert!(bytes.is_empty());

    let small = Text::new(SMALL);
    let bytes: &[u8] = small.as_ref();
    assert_eq!(bytes, SMALL.as_bytes());

    let borderline = Text::new(BORDERLINE);
    let bytes: &[u8] = borderline.as_ref();
    assert_eq!(bytes, BORDERLINE.as_bytes());

    let large = Text::new(LARGE);
    let bytes: &[u8] = large.as_ref();
    assert_eq!(bytes, LARGE.as_bytes());
}

#[test]
fn text_as_ref_str() {
    let empty = Text::empty();
    let as_str: &str = empty.as_ref();
    assert!(as_str.is_empty());

    let small = Text::new(SMALL);
    let as_str: &str = small.as_ref();
    assert_eq!(as_str, SMALL);

    let borderline = Text::new(BORDERLINE);
    let as_str: &str = borderline.as_ref();
    assert_eq!(as_str, BORDERLINE);

    let large = Text::new(LARGE);
    let as_str: &str = large.as_ref();
    assert_eq!(as_str, LARGE);
}

#[test]
fn text_as_mut_str() {
    let mut empty = Text::empty();
    let as_str: &mut str = empty.as_mut();
    assert!(as_str.is_empty());

    let mut small = Text::new(SMALL);
    let as_str: &mut str = small.as_mut();
    assert_eq!(as_str, SMALL);

    let mut borderline = Text::new(BORDERLINE);
    let as_str: &mut str = borderline.as_mut();
    assert_eq!(as_str, BORDERLINE);

    let mut large = Text::new(LARGE);
    let as_str: &mut str = large.as_mut();
    assert_eq!(as_str, LARGE);
}

#[test]
fn text_borrow() {
    let empty = Text::empty();
    let empty_borrowed: &str = empty.borrow();
    assert!(empty_borrowed.is_empty());

    let small = Text::new(SMALL);
    let small_borrowed: &str = small.borrow();
    assert_eq!(small_borrowed, SMALL);

    let borderline = Text::new(BORDERLINE);
    let borderline_borrowed: &str = borderline.borrow();
    assert_eq!(borderline_borrowed, BORDERLINE);

    let large = Text::new(LARGE);
    let large_borrowed: &str = large.borrow();
    assert_eq!(large_borrowed, LARGE);
}

#[test]
fn text_borrow_mut() {
    let mut empty = Text::empty();
    let empty_borrowed: &mut str = empty.borrow_mut();
    assert!(empty_borrowed.is_empty());

    let mut small = Text::new(SMALL);
    let small_borrowed: &mut str = small.borrow_mut();
    assert_eq!(small_borrowed, SMALL);

    let mut borderline = Text::new(BORDERLINE);
    let borderline_borrowed: &mut str = borderline.borrow_mut();
    assert_eq!(borderline_borrowed, BORDERLINE);

    let mut large = Text::new(LARGE);
    let large_borrowed: &mut str = large.borrow_mut();
    assert_eq!(large_borrowed, LARGE);
}

fn text_from_impls_for(string: &str) {
    let text: Text = string.into();
    assert_eq!(text, string);
    let text: Text = string.to_string().into();
    assert_eq!(text, string);
    let text: Text = (&string.to_string()).into();
    assert_eq!(text, string);
    let text: Text = (&mut string.to_string()).into();
    assert_eq!(text, string);
    let text: Text = string.to_string().as_mut_str().into();
    assert_eq!(text, string);

    let other = Text::new(string);
    let text: Text = (&other).into();
    assert_eq!(text, string);

    let mut other = Text::new(string);
    let text: Text = (&mut other).into();
    assert_eq!(text, string);

    let boxed: Box<str> = string.to_string().into_boxed_str();
    let text: Text = boxed.into();
    assert_eq!(text, string);

    let boxed = Box::new(string.to_string());
    let text: Text = boxed.into();
    assert_eq!(text, string);

    let boxed = Box::new(Text::new(string));
    let text: Text = boxed.into();
    assert_eq!(text, string);
}

#[test]
fn text_from_impls() {
    text_from_impls_for("");
    text_from_impls_for(SMALL);
    text_from_impls_for(BORDERLINE);
    text_from_impls_for(LARGE);
}

#[test]
fn text_from_str() {
    let small = Text::from_str(SMALL);
    assert_eq!(small, Ok(SMALL.into()));

    let borderline = Text::from_str(BORDERLINE);
    assert_eq!(borderline, Ok(BORDERLINE.into()));

    let large = Text::from_str(LARGE);
    assert_eq!(large, Ok(LARGE.into()));
}

const OTHER: &str = "other";

fn text_eq_for(text: Text, string: &str) {
    assert_eq!(text, string);
    assert_eq!(text, string.to_string());
    assert_eq!(text, &string.to_string());
    assert_eq!(text, &mut string.to_string());
    assert_eq!(text, string.to_string().as_mut_str());

    let other = Text::new(string);
    assert_eq!(text, &other);

    let mut other = Text::new(string);
    assert_eq!(text, &mut other);

    let boxed: Box<str> = string.to_string().into_boxed_str();
    assert_eq!(text, boxed);

    let boxed = Box::new(string.to_string());
    assert_eq!(text, boxed);

    let boxed = Box::new(Text::new(string));
    assert_eq!(text, boxed);

    assert_ne!(text, OTHER);
    assert_ne!(text, OTHER.to_string());
    assert_ne!(text, &OTHER.to_string());
    assert_ne!(text, &mut OTHER.to_string());
    assert_ne!(text, OTHER.to_string().as_mut_str());

    let other = Text::new(OTHER);
    assert_ne!(text, &other);

    let mut other = Text::new(OTHER);
    assert_ne!(text, &mut other);

    let boxed: Box<str> = OTHER.to_string().into_boxed_str();
    assert_ne!(text, boxed);

    let boxed = Box::new(OTHER.to_string());
    assert_ne!(text, boxed);

    let boxed = Box::new(Text::new(OTHER));
    assert_ne!(text, boxed);
}

#[test]
fn text_eq() {
    text_eq_for(Text::new(""), "");
    text_eq_for(Text::new(SMALL), SMALL);
    text_eq_for(Text::new(BORDERLINE), BORDERLINE);
    text_eq_for(Text::new(LARGE), LARGE);
}

fn hash_for<T: Hash>(t: T) -> u64 {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn text_hash() {
    let small = Text::new(SMALL);
    assert_eq!(hash_for(small), hash_for(SMALL));

    let borderline = Text::new(BORDERLINE);
    assert_eq!(hash_for(borderline), hash_for(BORDERLINE));

    let large = Text::new(LARGE);
    assert_eq!(hash_for(large), hash_for(LARGE));
}

#[test]
fn text_partial_order() {
    let small = Text::new(SMALL);
    let borderline = Text::new(BORDERLINE);
    let large = Text::new(LARGE);

    assert_eq!(small.partial_cmp(&small), Some(Ordering::Equal));
    assert_eq!(borderline.partial_cmp(&borderline), Some(Ordering::Equal));
    assert_eq!(large.partial_cmp(&large), Some(Ordering::Equal));

    assert_eq!(
        small.partial_cmp(&borderline),
        SMALL.partial_cmp(BORDERLINE)
    );
    assert_eq!(small.partial_cmp(&large), SMALL.partial_cmp(LARGE));

    assert_eq!(
        borderline.partial_cmp(&small),
        BORDERLINE.partial_cmp(SMALL)
    );
    assert_eq!(
        borderline.partial_cmp(&large),
        BORDERLINE.partial_cmp(LARGE)
    );

    assert_eq!(large.partial_cmp(&small), LARGE.partial_cmp(SMALL));
    assert_eq!(
        large.partial_cmp(&borderline),
        LARGE.partial_cmp(BORDERLINE)
    );
}

#[test]
fn text_order() {
    let small = Text::new(SMALL);
    let borderline = Text::new(BORDERLINE);
    let large = Text::new(LARGE);

    assert_eq!(small.cmp(&small), Ordering::Equal);
    assert_eq!(borderline.cmp(&borderline), Ordering::Equal);
    assert_eq!(large.cmp(&large), Ordering::Equal);

    assert_eq!(small.cmp(&borderline), SMALL.cmp(BORDERLINE));
    assert_eq!(small.cmp(&large), SMALL.cmp(LARGE));

    assert_eq!(borderline.cmp(&small), BORDERLINE.cmp(SMALL));
    assert_eq!(borderline.cmp(&large), BORDERLINE.cmp(LARGE));

    assert_eq!(large.cmp(&small), LARGE.cmp(SMALL));
    assert_eq!(large.cmp(&borderline), LARGE.cmp(BORDERLINE));
}

#[test]
fn text_debug() {
    let small = Text::new(SMALL);
    assert_eq!(format!("{:?}", small), "Text(Small, \"word\")");

    let borderline = Text::new(BORDERLINE);
    assert_eq!(
        format!("{:?}", borderline),
        "Text(Small, \"borderline string                            !\")"
    );

    let large = Text::new(LARGE);
    assert_eq!(
        format!("{:?}", large),
        "Text(Large, \"aaaa aaaa aaaa aaaa aaaa aaaa aaaa aaaa aaaa aaaa \")"
    );
}

#[test]
fn text_display() {
    let small = Text::new(SMALL);
    assert_eq!(format!("{}", small), format!("{}", SMALL));

    let borderline = Text::new(BORDERLINE);
    assert_eq!(format!("{}", borderline), format!("{}", BORDERLINE));

    let large = Text::new(LARGE);
    assert_eq!(format!("{}", large), format!("{}", LARGE));
}

#[test]
fn text_default() {
    assert_eq!(Text::default(), Text::empty());
}

#[test]
fn text_clone() {
    let small = Text::new(SMALL);
    let borderline = Text::new(BORDERLINE);
    let large = Text::new(LARGE);

    assert_eq!(small.clone(), small);
    assert_eq!(borderline.clone(), borderline);
    assert_eq!(large.clone(), large);
}

#[test]
fn text_clone_from() {
    let small = Text::new(SMALL);
    let borderline = Text::new(BORDERLINE);
    let large = Text::new(LARGE);

    let mut target = Text::empty();
    target.clone_from(&small);
    assert_eq!(target, small);

    let mut target = Text::empty();
    target.clone_from(&borderline);
    assert_eq!(target, borderline);

    let mut target = Text::empty();
    target.clone_from(&large);
    assert_eq!(target, large);

    let mut target = Text::new(LARGE);
    target.clone_from(&small);
    assert_eq!(target, small);

    let mut target = Text::new(LARGE);
    target.clone_from(&borderline);
    assert_eq!(target, borderline);

    let mut target = Text::new(LARGE);
    target.clone_from(&large);
    assert_eq!(target, large);
}
