// Copyright 2015-2023 Swim Inc.
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

use std::str::FromStr;

use mime::Mime;
use swim_model::http::{Header, HeaderValue, StandardHeaderName};

pub struct Headers<'a> {
    headers: &'a [Header],
}

impl<'a> Headers<'a> {
    pub fn new(headers: &'a [Header]) -> Self {
        Headers { headers }
    }
}

pub struct InvalidHeader;

impl<'a> Headers<'a> {
    pub fn content_type(&self) -> Result<Option<Mime>, InvalidHeader> {
        let Headers { headers } = self;
        if let Some(header) = headers
            .iter()
            .find(|Header { name, .. }| name == &StandardHeaderName::ContentType)
        {
            if let Some(value) = header.value.as_str() {
                match Mime::from_str(value) {
                    Ok(mime) => Ok(Some(mime)),
                    Err(_) => Err(InvalidHeader),
                }
            } else {
                Err(InvalidHeader)
            }
        } else {
            Ok(None)
        }
    }

    pub fn accept(&self) -> impl Iterator<Item = Result<Mime, InvalidHeader>> + '_ {
        let Headers { headers } = self;
        headers
            .iter()
            .filter(|Header { name, .. }| name == &StandardHeaderName::Accept)
            .map(|Header { value, .. }| value)
            .flat_map(extract_accept)
    }
}

#[derive(Default)]
enum AcceptState<'a, I> {
    Init(&'a HeaderValue),
    ConsumingPart(I),
    #[default]
    Done,
}

fn extract_accept(value: &HeaderValue) -> impl Iterator<Item = Result<Mime, InvalidHeader>> + '_ {
    let mut state = AcceptState::Init(value);
    std::iter::from_fn(move || loop {
        match std::mem::take(&mut state) {
            AcceptState::Init(value) => {
                if let Some(value_str) = value.as_str() {
                    let it = value_str.split(';').map(|s| s.trim());
                    state = AcceptState::ConsumingPart(it);
                } else {
                    state = AcceptState::Done;
                    break Some(Err(InvalidHeader));
                }
            }
            AcceptState::ConsumingPart(mut it) => match it.next() {
                Some(part) if !part.is_empty() => {
                    break Some(Mime::from_str(part).map_err(|_| InvalidHeader));
                }
                _ => break None,
            },
            AcceptState::Done => break None,
        }
    })
}

pub fn content_type_header(mime: &Mime) -> Header {
    Header {
        name: StandardHeaderName::ContentType.into(),
        value: HeaderValue::from(mime.as_ref()),
    }
}
