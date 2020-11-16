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

use biscuit::jwk::JWKSet;
use biscuit::Empty;
use chrono::{DateTime, FixedOffset, Utc};
use http::header::{CACHE_CONTROL, EXPIRES};
use http::{HeaderMap, HeaderValue};
use serde::export::Formatter;
use std::fmt::Display;
use tokio::time::delay_for;
use url::Url;
use utilities::future::retryable::strategy::RetryStrategy;

const MAX_AGE_DIRECTIVE: &str = "max-age";
const NO_STORE_CACHEABILITY: &str = "no-store";
pub const DEFAULT_CERT_SKEW: i64 = 30;
pub const GOOGLE_JWK_CERTS_URL: &str = "https://www.googleapis.com/oauth2/v3/certs";

#[derive(Debug, PartialEq)]
enum KeyStoreStrategy {
    /// Don't cache anything and always fetch the latest value when it's accessed.
    NoStore,
    /// Revalidate the entry at the provided time.
    RevalidateAt(DateTime<FixedOffset>),
}

#[derive(Debug, PartialEq)]
pub struct GoogleKeyStore {
    strategy: KeyStoreStrategy,
    /// Number of seconds before the public key certificate expiry time before forcing a refresh.
    permitted_cert_exp_skew: i64,
    /// The retry strategy to use after failing to refresh the public keys.
    retry_strategy: RetryStrategy,
    public_key_url: Url,
    certs: JWKSet<Empty>,
}

impl Default for GoogleKeyStore {
    fn default() -> Self {
        GoogleKeyStore {
            strategy: KeyStoreStrategy::NoStore,
            permitted_cert_exp_skew: DEFAULT_CERT_SKEW,
            retry_strategy: Default::default(),
            public_key_url: Url::parse(GOOGLE_JWK_CERTS_URL)
                .expect("Failed to parse default Google certificate URL"),
            certs: JWKSet { keys: vec![] },
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum GoogleKeyStoreError {
    ServerError,
    UpdateError(String),
}

impl Display for GoogleKeyStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GoogleKeyStoreError::ServerError => write!(
                f,
                "The server responded with an error when attempting to update the certificate"
            ),
            GoogleKeyStoreError::UpdateError(e) => {
                write!(f, "Failed to update certificates: {}", e)
            }
        }
    }
}

impl GoogleKeyStore {
    pub fn new(public_key_url: Url, cert_skew: i64) -> GoogleKeyStore {
        GoogleKeyStore {
            strategy: KeyStoreStrategy::NoStore,
            permitted_cert_exp_skew: cert_skew,
            retry_strategy: Default::default(),
            public_key_url,
            certs: JWKSet { keys: vec![] },
        }
    }

    pub(crate) fn cert_skew(&self) -> i64 {
        self.permitted_cert_exp_skew
    }

    pub(crate) fn key_url(&self) -> Url {
        self.public_key_url.clone()
    }

    fn stale(&self) -> bool {
        match self.strategy {
            KeyStoreStrategy::NoStore => true,
            KeyStoreStrategy::RevalidateAt(expires) => {
                expires.lt(&Into::<DateTime<FixedOffset>>::into(Utc::now()))
            }
        }
    }

    fn parse_response(&self, headers: &HeaderMap) -> Result<KeyStoreStrategy, GoogleKeyStoreError> {
        let cache_control_headers = headers.get_all(CACHE_CONTROL);

        for cache_control_header in cache_control_headers {
            let cache_control_header = cache_control_header.to_str().unwrap();
            let directives = cache_control_header.split(',');

            for directive in directives {
                match directive.trim() {
                    NO_STORE_CACHEABILITY => return Ok(KeyStoreStrategy::NoStore),
                    dir if dir.starts_with(MAX_AGE_DIRECTIVE) => {
                        let mut value_opt = dir.split('=').skip(1);

                        return match value_opt.next() {
                            Some(time) => match time.parse::<i64>() {
                                Ok(seconds) => {
                                    let expires = Utc::now()
                                        + chrono::Duration::seconds(
                                            seconds - self.permitted_cert_exp_skew,
                                        );
                                    Ok(KeyStoreStrategy::RevalidateAt(expires.into()))
                                }
                                Err(e) => Err(GoogleKeyStoreError::UpdateError(format!(
                                    "Failed to parse max-age value: {}",
                                    e
                                ))),
                            },
                            None => Err(GoogleKeyStoreError::UpdateError(
                                "Missing max-age value".into(),
                            )),
                        };
                    }
                    _ => {}
                }
            }
        }

        match headers.get(EXPIRES) {
            Some(time) => match time.to_str() {
                Ok(time) => match DateTime::parse_from_rfc2822(time) {
                    Ok(expires) => Ok(KeyStoreStrategy::RevalidateAt(expires)),
                    Err(e) => Err(GoogleKeyStoreError::UpdateError(format!(
                        "Failed to parse expiry time: {}",
                        e
                    ))),
                },
                Err(e) => Err(GoogleKeyStoreError::UpdateError(format!(
                    "Failed to parse expiry time: {}",
                    e
                ))),
            },
            None => Ok(KeyStoreStrategy::NoStore),
        }
    }

    pub async fn refresh(&mut self) -> Result<(), GoogleKeyStoreError> {
        if !self.stale() && !self.certs.keys.is_empty() {
            return Ok(());
        }

        let mut has_errored = false;

        loop {
            let get_result = reqwest::get(self.public_key_url.as_str()).await;

            match get_result {
                Ok(response) if response.status().is_success() => {
                    if has_errored {
                        self.retry_strategy = RetryStrategy::default();
                    }

                    self.strategy = self.parse_response(response.headers())?;

                    let response_body = response.text().await?;

                    self.certs = serde_json::from_str(&response_body)?;

                    return Ok(());
                }
                Ok(response) if response.status().is_server_error() => {
                    match self.retry_strategy.next() {
                        Some(Some(duration)) => {
                            has_errored = true;
                            delay_for(duration).await;
                        }
                        _ => return Err(GoogleKeyStoreError::ServerError),
                    }
                }
                _ => return Err(GoogleKeyStoreError::ServerError),
            }
        }
    }

    pub async fn keys(&mut self) -> Result<&JWKSet<Empty>, GoogleKeyStoreError> {
        self.refresh().await?;
        Ok(&self.certs)
    }
}

impl From<reqwest::Error> for GoogleKeyStoreError {
    fn from(e: reqwest::Error) -> Self {
        GoogleKeyStoreError::UpdateError(e.to_string())
    }
}

impl From<serde_json::Error> for GoogleKeyStoreError {
    fn from(e: serde_json::Error) -> Self {
        GoogleKeyStoreError::UpdateError(e.to_string())
    }
}

#[test]
fn store_no_cache() {
    let authenticator = GoogleKeyStore::new(Url::parse(GOOGLE_JWK_CERTS_URL).unwrap(), 30);

    let mut header_map = HeaderMap::new();
    header_map.insert(
        CACHE_CONTROL,
        HeaderValue::from_static(NO_STORE_CACHEABILITY),
    );

    let parse_result = authenticator.parse_response(&header_map);
    assert_eq!(parse_result, Ok(KeyStoreStrategy::NoStore));
}

#[test]
fn store_revalidate() {
    let authenticator = GoogleKeyStore::new(Url::parse(GOOGLE_JWK_CERTS_URL).unwrap(), 30);
    let grace_period = 30;

    let mut header_map = HeaderMap::new();
    header_map.insert(
        CACHE_CONTROL,
        HeaderValue::from_str(&format!("{}={}", MAX_AGE_DIRECTIVE, grace_period)).unwrap(),
    );

    let start = Utc::now();

    let parse_result = authenticator.parse_response(&header_map);
    match parse_result {
        Ok(KeyStoreStrategy::RevalidateAt(expires)) => {
            assert!(start < expires && expires < Utc::now() + chrono::Duration::seconds(30));
        }
        _ => panic!("Expected a revalidation strategy"),
    }
}

#[test]
fn store_unknown_directive() {
    let authenticator = GoogleKeyStore::new(Url::parse(GOOGLE_JWK_CERTS_URL).unwrap(), 30);

    let mut header_map = HeaderMap::new();
    header_map.insert(CACHE_CONTROL, HeaderValue::from_static("only-if-cached"));

    let parse_result = authenticator
        .parse_response(&header_map)
        .expect("Expected a valid key store strategy");

    assert_eq!(parse_result, KeyStoreStrategy::NoStore)
}
