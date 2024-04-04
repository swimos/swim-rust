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

use std::collections::HashSet;

use mime::Mime;
use swimos_model::http::{Header, StandardHeaderName};
use swimos_utilities::format::join;

use super::Headers;

fn make_headers(content_type: Option<&Mime>, accept: Vec<Vec<&Mime>>) -> Vec<Header> {
    let mut headers = vec![];

    if let Some(content_type) = content_type {
        let header = Header::new(StandardHeaderName::ContentType, content_type.to_string());
        headers.push(header);
    }
    for content_types in accept {
        let value = join(&content_types, "; ").to_string();
        let header = Header::new(StandardHeaderName::Accept, value);
        headers.push(header);
    }

    headers
}

#[test]
fn absent_content_type() {
    let header_vec = make_headers(None, vec![vec![&mime::APPLICATION_JSON, &mime::TEXT_PLAIN]]);
    let headers = Headers::new(&header_vec);
    assert!(matches!(headers.content_type(), Ok(None)));
}

#[test]
fn invalid_content_type() {
    let header_vec = vec![Header::new(StandardHeaderName::ContentType, "////")];
    let headers = Headers::new(&header_vec);
    assert!(headers.content_type().is_err());
}

#[test]
fn extract_content_type() {
    let header_vec = make_headers(
        Some(&mime::APPLICATION_JSON),
        vec![vec![&mime::APPLICATION_JSON, &mime::TEXT_PLAIN]],
    );
    let headers = Headers::new(&header_vec);

    match headers.content_type() {
        Ok(Some(ct)) => assert_eq!(ct, mime::APPLICATION_JSON),
        ow => panic!("Failed to get content type: {:?}", ow),
    }
}

#[test]
fn extract_accept() {
    let header_vec = make_headers(
        Some(&mime::APPLICATION_JSON),
        vec![
            vec![&mime::APPLICATION_JSON, &mime::TEXT_PLAIN],
            vec![&mime::APPLICATION_PDF],
        ],
    );
    let headers = Headers::new(&header_vec);

    let accept = headers
        .accept()
        .map(|h| h.expect("Invalid accept entry."))
        .collect::<HashSet<_>>();

    let expected = [
        &mime::APPLICATION_JSON,
        &mime::TEXT_PLAIN,
        &mime::APPLICATION_PDF,
    ]
    .into_iter()
    .cloned()
    .collect::<HashSet<_>>();

    assert_eq!(accept, expected);
}

#[test]
fn skip_faulty_accept_entries() {
    let headers_vec = vec![Header::new(
        StandardHeaderName::Accept,
        "application/json; ///; text/plain",
    )];

    let headers = Headers::new(&headers_vec);

    let accept = headers
        .accept()
        .filter_map(|r| r.ok())
        .collect::<HashSet<_>>();

    let expected = [&mime::APPLICATION_JSON, &mime::TEXT_PLAIN]
        .into_iter()
        .cloned()
        .collect::<HashSet<_>>();

    assert_eq!(accept, expected);
}
