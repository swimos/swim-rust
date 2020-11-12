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

use crate::{AuthenticationError, Authenticator};
use biscuit::jwk::JWKSet;
use biscuit::Empty;
use chrono::{DateTime, FixedOffset, Utc};
use futures::FutureExt;
use futures_util::future::BoxFuture;
use http::header::{CACHE_CONTROL, EXPIRES};
use im::HashSet;
use reqwest::header::HeaderMap;

use url::Url;
use utilities::future::retryable::strategy::RetryStrategy;

const GOOGLE_JWK_CERTS_URL: &str = "https://www.googleapis.com/oauth2/v3/certs";
const MAX_AGE_DIRECTIVE: &str = "max-age";
const NO_STORE_CACHEABILITY: &str = "no-store";

#[derive(Debug, PartialEq, Eq, Hash)]
struct PublicKeyDef;

#[derive(Debug)]
pub enum KeyCacheStrategy {
    NoStore,
    RevalidateAt(DateTime<FixedOffset>),
}

impl KeyCacheStrategy {
    fn from_headers(headers: &HeaderMap) -> Result<KeyCacheStrategy, AuthenticationError> {
        let cache_control_headers = headers.get_all(CACHE_CONTROL);

        for cache_control_header in cache_control_headers {
            let cache_control_header = cache_control_header.to_str().unwrap();
            let directives = cache_control_header.split(',');

            for directive in directives {
                match directive.trim() {
                    NO_STORE_CACHEABILITY => return Ok(KeyCacheStrategy::NoStore),
                    dir if dir.starts_with(MAX_AGE_DIRECTIVE) => {
                        let mut value_opt = dir.split('=').skip(1);

                        match value_opt.next() {
                            Some(time) => {
                                return match time.parse() {
                                    Ok(seconds) => {
                                        let expires =
                                            Utc::now() + chrono::Duration::seconds(seconds);
                                        Ok(KeyCacheStrategy::RevalidateAt(expires.into()))
                                    }
                                    Err(e) => Err(AuthenticationError::malformatted(
                                        "Failed to parse max-age value",
                                        e,
                                    )),
                                }
                            }
                            None => {
                                return Err(AuthenticationError::MalformattedResponse(
                                    "Missing max-age value".into(),
                                ))
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        match headers.get(EXPIRES) {
            Some(time) => match time.to_str() {
                Ok(time) => match DateTime::parse_from_rfc2822(time) {
                    Ok(expires) => Ok(KeyCacheStrategy::RevalidateAt(expires)),
                    Err(e) => {
                        return Err(AuthenticationError::malformatted(
                            "Failed to parse expiry time",
                            e,
                        ))
                    }
                },
                Err(e) => {
                    return Err(AuthenticationError::malformatted(
                        "Failed to parse expiry time",
                        e,
                    ))
                }
            },
            None => Ok(KeyCacheStrategy::NoStore),
        }
    }
}

#[derive(Debug)]
pub struct GoogleIdAuthenticator {
    last_refresh: u32,
    key_cache_strategy: KeyCacheStrategy,
    audiences: HashSet<String>,
    emails: HashSet<String>,
    keys: JWKSet<Empty>,
    public_key_uri: Url,
    retry_strategy: RetryStrategy,
}

impl From<reqwest::Error> for AuthenticationError {
    fn from(e: reqwest::Error) -> Self {
        AuthenticationError::MalformattedResponse(e.to_string())
    }
}

impl From<serde_json::Error> for AuthenticationError {
    fn from(e: serde_json::Error) -> Self {
        AuthenticationError::MalformattedResponse(e.to_string())
    }
}

impl GoogleIdAuthenticator {
    pub fn new() -> GoogleIdAuthenticator {
        GoogleIdAuthenticator {
            last_refresh: 0,
            key_cache_strategy: KeyCacheStrategy::NoStore,
            audiences: Default::default(),
            emails: Default::default(),
            keys: JWKSet { keys: Vec::new() },
            public_key_uri: Url::parse(GOOGLE_JWK_CERTS_URL)
                .expect("Failed to parse Google JWK certificate URL"),
            retry_strategy: RetryStrategy::default(),
        }
    }

    pub fn using_key_url(public_key_uri: Url) -> GoogleIdAuthenticator {
        GoogleIdAuthenticator {
            last_refresh: 0,
            key_cache_strategy: KeyCacheStrategy::NoStore,
            audiences: Default::default(),
            emails: Default::default(),
            keys: JWKSet { keys: Vec::new() },
            public_key_uri,
            retry_strategy: RetryStrategy::default(),
        }
    }

    // todo: refactor into a key store with a channel to prevent multiple, simultaneous refreshes
    async fn refresh_keys(&mut self) -> Result<(), AuthenticationError> {
        let mut has_errored = false;

        loop {
            let get_result = reqwest::get(self.public_key_uri.as_str()).await;

            match get_result {
                Ok(response) if response.status().is_success() => {
                    if has_errored {
                        self.retry_strategy = RetryStrategy::default();
                    }

                    self.key_cache_strategy = KeyCacheStrategy::from_headers(response.headers())?;

                    let response_body = response.text().await?;

                    self.keys = serde_json::from_str(&response_body)?;

                    return Ok(());
                }
                Ok(response) if response.status().is_server_error() => {
                    match self.retry_strategy.next() {
                        Some(Some(duration)) => {
                            has_errored = true;
                            tokio::time::delay_for(duration).await;
                        }
                        _ => return Err(AuthenticationError::ServerError),
                    }
                }
                _ => return Err(AuthenticationError::ServerError),
            }
        }
    }
}

pub struct GoogleId;

impl Authenticator for GoogleIdAuthenticator {
    type Token = GoogleId;
    type AuthenticateFuture = BoxFuture<'static, Result<Self::Token, AuthenticationError>>;

    fn authenticate(&self) -> Self::AuthenticateFuture {
        async { Err(AuthenticationError::MalformattedResponse("".into())) }.boxed()
    }
}

#[cfg(test)]
mod tests {
    use crate::googleid::GoogleIdAuthenticator;

    #[tokio::test]
    async fn t() {
        let mut authenticator = GoogleIdAuthenticator::new();
        let _r = authenticator.refresh_keys().await;

        println!("{:?}", authenticator);
    }
}
