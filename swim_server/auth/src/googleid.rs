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
use biscuit::{CompactJson, Empty};
use chrono::{DateTime, FixedOffset, Utc};
use futures::FutureExt;
use futures_util::future::BoxFuture;
use http::header::{CACHE_CONTROL, EXPIRES};
use im::HashSet;
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use swim_common::form::Form;

use crate::policy::PolicyDirective;
use biscuit::jws::Compact;
use swim_common::model::Value;
use tokio::time::delay_for;
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

                        return match value_opt.next() {
                            Some(time) => match time.parse() {
                                Ok(seconds) => {
                                    let expires = Utc::now() + chrono::Duration::seconds(seconds);
                                    Ok(KeyCacheStrategy::RevalidateAt(expires.into()))
                                }
                                Err(e) => Err(AuthenticationError::malformatted(
                                    "Failed to parse max-age value",
                                    e,
                                )),
                            },
                            None => Err(AuthenticationError::MalformattedResponse(
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
                    Ok(expires) => Ok(KeyCacheStrategy::RevalidateAt(expires)),
                    Err(e) => {
                        return Err(AuthenticationError::malformatted(
                            "Failed to parse expiry time",
                            e,
                        ));
                    }
                },
                Err(e) => {
                    return Err(AuthenticationError::malformatted(
                        "Failed to parse expiry time",
                        e,
                    ));
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

    // todo: refactor into a key store with a channel to prevent multiple, simultaneous, refreshes
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
                            delay_for(duration).await;
                        }
                        _ => return Err(AuthenticationError::ServerError),
                    }
                }
                _ => return Err(AuthenticationError::ServerError),
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct GoogleId {
    email_verified: bool,
    #[serde(rename = "azp")]
    authorised_party: String,
    #[serde(rename = "hd")]
    hosted_domain: Url,
    email: String,
    name: String,
    picture: Url,
    given_name: String,
    family_name: String,
    locale: String,
}

impl CompactJson for GoogleId {}

#[derive(Form)]
#[form(tag = "googleId")]
pub struct GoogleIdCredentials(String);

impl<'s> Authenticator<'s> for GoogleIdAuthenticator {
    type Credentials = GoogleIdCredentials;
    type AuthenticateFuture = BoxFuture<'s, Result<PolicyDirective, AuthenticationError>>;

    fn authenticate(&'s mut self, credentials: GoogleIdCredentials) -> Self::AuthenticateFuture {
        async move {
            self.refresh_keys().await?;

            let compact: Compact<GoogleId, Empty> = Compact::new_encoded(&credentials.0);
            let decode_result = compact.decode_with_jwks(&self.keys);

            println!("{:?}", decode_result);

            Ok(PolicyDirective::allow(Value::Extant))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use crate::googleid::{GoogleId, GoogleIdCredentials};
    use biscuit::jwa::SignatureAlgorithm;
    use biscuit::jws::{Compact, RegisteredHeader, Secret};
    use biscuit::{ClaimsSet, Empty, RegisteredClaims, SingleOrMultiple};
    use std::str::FromStr;

    fn expected_biscuit() -> Compact<ClaimsSet<GoogleId>, Empty> {
        let expected_claims = ClaimsSet::<GoogleId> {
            registered: RegisteredClaims {
                issuer: Some(FromStr::from_str("accounts.google.com").unwrap()),
                subject: Some(FromStr::from_str("117614620700092979612").unwrap()),
                audience: Some(SingleOrMultiple::Single(
                    FromStr::from_str(
                        "339656303991-hjc1rr2vv0lclnqg0jq76r4qar9c8p62.apps.googleusercontent.com",
                    )
                    .unwrap(),
                )),
                not_before: None,
                ..Default::default()
            },
            private: GoogleId {
                email_verified: false,
                authorised_party: "".to_string(),
                hosted_domain: "https://swim.ai/".parse().unwrap(),
                email: "tom@swim.ai".to_string(),
                name: "Tom".to_string(),
                picture: "https://www.swim.ai/images/marlin-swim-blue.svg"
                    .parse()
                    .unwrap(),
                given_name: "Tom".to_string(),
                family_name: "Tom".to_string(),
                locale: "EN".to_string(),
            },
        };

        Compact::new_decoded(
            From::from(RegisteredHeader {
                algorithm: SignatureAlgorithm::RS256,
                ..Default::default()
            }),
            expected_claims,
        )
    }

    fn expected_token() -> String {
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhY2NvdW50cy5nb29nbGUuY29tIiwic3ViIjoiMTE3N\
        jE0NjIwNzAwMDkyOTc5NjEyIiwiYXVkIjoiMzM5NjU2MzAzOTkxLWhqYzFycjJ2djBsY2xucWcwanE3NnI0cWFyOWM4\
        cDYyLmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJhenAiOiIiLCJoZCI\
        6Imh0dHBzOi8vc3dpbS5haS8iLCJlbWFpbCI6InRvbUBzd2ltLmFpIiwibmFtZSI6IlRvbSIsInBpY3R1cmUiOiJodH\
        RwczovL3d3dy5zd2ltLmFpL2ltYWdlcy9tYXJsaW4tc3dpbS1ibHVlLnN2ZyIsImdpdmVuX25hbWUiOiJUb20iLCJmY\
        W1pbHlfbmFtZSI6IlRvbSIsImxvY2FsZSI6IkVOIn0.USLIq07s92Ah4tMdJuxG-eiGwVxedFz8OBaI7r4GLLLzmJxq\
        FDdPbX_RuS7yejU9L22C45X7siqsWXcFwqenkNcwKcDArAxdyR5rjL8iOoC5i8s6i754zbLsxdqq5yuWBG18vqXFl7D\
        A0m2r8U60lUbdJh7kXDQNWn9hGfn4YjCnSEgCGkBVJQop_yIip-aTVzhahFEF4OFdMGBROHkUBeO5_FY_gV4PG5Qkc3\
        tNvt6GNZU6PkeSu0dhuivnmNkqBwyAjywnL4_nqNmRivvv2YYviKug80xR-n-jSO3Sm0Nj-8x44gLDzQAbxrYJwOzsu\
        MIe7Z7BTEC4lsNfpMfJWA"
            .into()
    }

    #[test]
    fn test_signing() {
        let private_key = Secret::rsa_keypair_from_file("test/private_key.der").unwrap();
        let biscuit = expected_biscuit();
        let encoded_biscuit = biscuit.into_encoded(&private_key).expect("Encode error");
        let token = encoded_biscuit.unwrap_encoded().to_string();

        assert_eq!(token, expected_token())
    }

    #[tokio::test]
    async fn test_decode() {
        let secret = Secret::public_key_from_file("test/public_key.der").unwrap();
        let credentials = GoogleIdCredentials(expected_token());

        let compact: Compact<ClaimsSet<GoogleId>, Empty> =
            biscuit::jws::Compact::new_encoded(&credentials.0);
        let algorithm = SignatureAlgorithm::RS256;
        let decoded_biscuit = compact.decode(&secret, algorithm).expect("Decode failure");

        assert_eq!(decoded_biscuit, expected_biscuit());
    }
}
