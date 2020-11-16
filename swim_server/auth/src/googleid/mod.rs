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

mod store;
#[cfg(test)]
mod tests;

use crate::{AuthenticationError, Authenticator, Token, TokenStore};
use biscuit::{ClaimsSet, CompactJson, Empty, SingleOrMultiple};
use chrono::{Duration, Utc};
use futures::FutureExt;
use futures_util::future::BoxFuture;
use im::HashSet;
use serde::{Deserialize, Serialize};
use swim_common::form::Form;

use crate::googleid::store::{
    GoogleKeyStore, GoogleKeyStoreError, DEFAULT_CERT_SKEW, GOOGLE_JWK_CERTS_URL,
};
use crate::policy::PolicyDirective;
use biscuit::jws::Compact;
use std::num::NonZeroI64;
use std::ops::Deref;
use swim_common::model::Value;
use url::Url;

const GOOGLE_ISS1: &str = "accounts.google.com";
const GOOGLE_ISS2: &str = "https://accounts.google.com";
const DEFAULT_TOKEN_SKEW: i64 = 5;

/// An authenticator for validating Google ID tokens.
/// See https://developers.google.com/identity/sign-in/web/ for more information
#[derive(Debug)]
pub struct GoogleIdAuthenticator {
    /// Number of seconds beyond the token's expiry time that are permitted.
    token_skew: i64,
    key_store: GoogleKeyStore,
    emails: HashSet<String>,
    audiences: HashSet<String>,
    tokens_store: TokenStore,
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Form)]
pub struct GoogleId {
    email_verified: bool,
    #[serde(rename = "azp")]
    authorised_party: String,
    #[serde(rename = "hd")]
    hosted_domain: Option<Url>,
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

    /// Verifies that the provided Google ID token is valid.
    ///
    /// Verifies that the RS256 signature was signed using the public keys from the provided
    /// `public_key_uri`, that the time that the expiry time of the token is valid (providing a
    /// pre-configured clock skew grace period), that the issuer is one of `accounts.google.com`
    /// or `https://accounts.google.com`, that the audience ID is one of the permitted values, and
    /// that the email address associated with the token is also permitted.
    ///
    /// Returns `Ok` and an associated `PolicyDirective` or an `Err` if there was an error when
    /// decoding the JWT.
    fn authenticate(&'s mut self, credentials: GoogleIdCredentials) -> Self::AuthenticateFuture {
        async move {
            if let Some(directive) = self.tokens_store.get(&credentials.0) {
                return Ok(directive.policy.clone());
            }

            let jwt: Compact<ClaimsSet<GoogleId>, Empty> = Compact::new_encoded(&credentials.0);
            let keys = self.key_store.keys().await?;

            match jwt.decode_with_jwks(keys) {
                Ok(token) => {
                    let (_header, token) = token.unwrap_decoded();
                    let ClaimsSet {
                        registered,
                        private,
                        ..
                    } = token;

                    // Check that the token has not expired
                    let expiry_timestamp = match &registered.expiry {
                        Some(expiry_time) => {
                            if expiry_time.lt(&(Utc::now() + Duration::seconds(self.token_skew))) {
                                return Ok(PolicyDirective::deny(Value::Extant));
                            } else {
                                let expiry_timestamp = expiry_time.deref().clone().into();
                                expiry_timestamp
                            }
                        }
                        None => return Ok(PolicyDirective::deny(Value::Extant)),
                    };

                    // Check that the issuer of the token is one of the expected Google issuers
                    match &registered.issuer {
                        Some(uri) => {
                            let uri = uri.as_ref();
                            if ![GOOGLE_ISS1, GOOGLE_ISS2].contains(&uri) {
                                return Ok(PolicyDirective::deny(Value::Extant));
                            }
                        }
                        None => return Ok(PolicyDirective::deny(Value::Extant)),
                    }

                    // Check the token's audience ID matches ours
                    match &registered.audience {
                        Some(SingleOrMultiple::Single(audience)) => {
                            if !self.audiences.contains(&audience.as_ref().to_string()) {
                                return Ok(PolicyDirective::forbid(Value::Extant));
                            }
                        }
                        Some(SingleOrMultiple::Multiple(audiences)) => {
                            let matched_audience = audiences
                                .iter()
                                .map(|a| a.as_ref().to_string())
                                .any(|aud| self.audiences.contains(&aud));

                            if !matched_audience {
                                return Ok(PolicyDirective::forbid(Value::Extant));
                            }
                        }
                        None => return Ok(PolicyDirective::forbid(Value::Extant)),
                    }

                    // Check that the email of the token matches one of the configured emails
                    return if self.emails.contains(&private.email) {
                        let policy = PolicyDirective::allow(private.into_value());
                        let token = Token::new(credentials.0, expiry_timestamp);
                        self.tokens_store.insert(token, policy.clone());

                        Ok(policy)
                    } else {
                        Ok(PolicyDirective::deny(Value::Extant))
                    };
                }
                Err(_) => Err(AuthenticationError::MalformattedResponse(
                    "Failed to decode JWT".into(),
                )),
            }
        }
        .boxed()
    }
}

impl From<GoogleKeyStoreError> for AuthenticationError {
    fn from(e: GoogleKeyStoreError) -> Self {
        AuthenticationError::KeyStoreError(e.to_string())
    }
}

#[derive(Default)]
pub struct GoogleIdAuthenticatorBuilder {
    /// Number of seconds before the public key certificate expiry time before forcing a refresh.
    token_skew: Option<i64>,
    /// Number of seconds beyond the token's expiry time that are permitted.
    cert_skew: Option<i64>,
    /// Permitted emails for this resource.
    emails: Option<HashSet<String>>,
    /// Permitted audiences for this resource.
    audiences: Option<HashSet<String>>,
    /// An optional override for the public key URL.
    key_issuer_url: Option<Url>,
}

impl GoogleIdAuthenticatorBuilder {
    pub fn token_skew(mut self, duration: NonZeroI64) -> Self {
        self.token_skew = Some(duration.get());
        self
    }

    pub fn cert_skew(mut self, duration: NonZeroI64) -> Self {
        self.cert_skew = Some(duration.get());
        self
    }

    pub fn emails(mut self, emails: HashSet<String>) -> Self {
        self.emails = Some(emails);
        self
    }

    pub fn audiences(mut self, audiences: HashSet<String>) -> Self {
        self.audiences = Some(audiences);
        self
    }

    pub fn key_issuer_url(mut self, url: Url) -> Self {
        self.key_issuer_url = Some(url);
        self
    }

    pub fn build(self) -> GoogleIdAuthenticator {
        let public_key_uri = self.key_issuer_url.unwrap_or_else(|| {
            Url::parse(GOOGLE_JWK_CERTS_URL)
                .expect("Failed to parse default Google JWK certificate URL")
        });
        let token_skew = self.token_skew.unwrap_or(DEFAULT_TOKEN_SKEW);
        let permitted_cert_exp_skew = self.cert_skew.unwrap_or(DEFAULT_CERT_SKEW);

        GoogleIdAuthenticator {
            token_skew,
            key_store: GoogleKeyStore::new(public_key_uri, permitted_cert_exp_skew),
            emails: self.emails.expect("At least one email must be provided"),
            audiences: self
                .audiences
                .expect("At least one audience must be provided"),
            tokens_store: TokenStore::new(token_skew),
        }
    }
}
