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

use std::num::NonZeroI64;

use biscuit::jws::Compact;
use biscuit::{ClaimsSet, CompactJson, Empty, SingleOrMultiple};
use chrono::{Duration, Utc};
use futures::FutureExt;
use futures_util::future::BoxFuture;
use im::HashSet;
use serde::{Deserialize, Serialize};
use url::Url;

use swim_common::form::{Form, FormErr};
use swim_common::model::{Attr, Item, Value};
use swim_common::ok;

use crate::googleid::store::{
    GoogleKeyStore, GoogleKeyStoreError, DEFAULT_CERT_SKEW, GOOGLE_JWK_CERTS_URL,
};
use crate::policy::{IssuedPolicy, PolicyDirective};
use crate::token::Token;
use crate::{AuthenticationError, Authenticator};
use std::ops::Deref;

mod store;
#[cfg(test)]
mod tests;

const GOOGLE_AUTH_TAG: &str = "googleId";
const GOOGLE_ISS1: &str = "accounts.google.com";
const GOOGLE_ISS2: &str = "https://accounts.google.com";
const DEFAULT_TOKEN_SKEW: i64 = 5;

/// An authenticator for validating Google ID tokens.
/// See https://developers.google.com/identity/sign-in/web/ for more information
#[derive(Debug, PartialEq)]
pub struct GoogleIdAuthenticator {
    /// Number of seconds beyond the token's expiry time that are permitted.
    token_skew: i64,
    key_store: GoogleKeyStore,
    emails: HashSet<String>,
    audiences: HashSet<String>,
}

impl Form for GoogleIdAuthenticator {
    fn as_value(&self) -> Value {
        Value::Record(
            vec![Attr::of(GOOGLE_AUTH_TAG)],
            vec![
                Item::Slot("token_skew".into(), Value::Int64Value(self.token_skew)),
                Item::Slot(
                    "cert_skew".into(),
                    Value::Int64Value(self.key_store.cert_skew()),
                ),
                Item::Slot("publicKeyUri".into(), self.key_store.key_url().as_value()),
                Item::Slot("emails".into(), self.emails.as_value()),
                Item::Slot("audiences".into(), self.audiences.as_value()),
            ],
        )
    }

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::Record(attrs, items) => {
                if attrs.len() > 1 {
                    return Err(FormErr::Malformatted);
                }

                match attrs.iter().next() {
                    Some(Attr { name, .. }) if name == GOOGLE_AUTH_TAG => {}
                    _ => return Err(FormErr::MismatchedTag),
                }

                let mut opt_token_skew = None;
                let mut opt_cert_skew = None;
                let mut opt_public_key_uri = None;
                let mut opt_emails = None;
                let mut opt_audiences = None;

                for item in items {
                    match item {
                        Item::Slot(Value::Text(name), Value::Int64Value(skew))
                            if name == "token_skew" =>
                        {
                            opt_token_skew = Some(*skew);
                        }
                        Item::Slot(Value::Text(name), Value::Int64Value(skew))
                            if name == "cert_skew" =>
                        {
                            opt_cert_skew = Some(*skew);
                        }
                        Item::Slot(Value::Text(name), url) if name == "publicKeyUri" => {
                            opt_public_key_uri = Some(Url::try_from_value(url)?);
                        }
                        Item::Slot(Value::Text(name), emails) if name == "emails" => {
                            opt_emails = Some(Form::try_from_value(emails)?);
                        }
                        Item::Slot(Value::Text(name), audiences) if name == "audiences" => {
                            opt_audiences = Some(Form::try_from_value(audiences)?);
                        }
                        i => return Err(FormErr::Message(format!("Unexpected item: {:?}", i))),
                    }
                }

                Ok(GoogleIdAuthenticator {
                    token_skew: ok!(opt_token_skew),
                    key_store: GoogleKeyStore::new(ok!(opt_public_key_uri), ok!(opt_cert_skew)),
                    emails: ok!(opt_emails),
                    audiences: ok!(opt_audiences),
                })
            }
            v => Err(FormErr::incorrect_type("Value::Record", v)),
        }
    }
}

impl From<reqwest::Error> for AuthenticationError {
    fn from(e: reqwest::Error) -> Self {
        AuthenticationError::Malformatted(e.to_string())
    }
}

impl From<serde_json::Error> for AuthenticationError {
    fn from(e: serde_json::Error) -> Self {
        AuthenticationError::Malformatted(e.to_string())
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
    type AuthenticateFuture = BoxFuture<'s, Result<IssuedPolicy, AuthenticationError>>;

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
                                return Ok(IssuedPolicy::deny(Value::Extant));
                            } else {
                                let expiry_timestamp = expiry_time.deref().clone().into();
                                expiry_timestamp
                            }
                        }
                        None => return Ok(IssuedPolicy::deny(Value::Extant)),
                    };

                    // Check that the issuer of the token is one of the expected Google issuers
                    match &registered.issuer {
                        Some(uri) => {
                            let uri = uri.as_ref();
                            if ![GOOGLE_ISS1, GOOGLE_ISS2].contains(&uri) {
                                return Ok(IssuedPolicy::deny(Value::Extant));
                            }
                        }
                        None => return Ok(IssuedPolicy::deny(Value::Extant)),
                    }

                    // Check the token's audience ID matches ours
                    match &registered.audience {
                        Some(SingleOrMultiple::Single(audience)) => {
                            if !self.audiences.contains(&audience.as_ref().to_string()) {
                                return Ok(IssuedPolicy::forbid(Value::Extant));
                            }
                        }
                        Some(SingleOrMultiple::Multiple(audiences)) => {
                            let matched_audience = audiences
                                .iter()
                                .map(|a| a.as_ref().to_string())
                                .any(|aud| self.audiences.contains(&aud));

                            if !matched_audience {
                                return Ok(IssuedPolicy::forbid(Value::Extant));
                            }
                        }
                        None => return Ok(IssuedPolicy::forbid(Value::Extant)),
                    }

                    // Check that the email of the token matches one of the configured emails
                    return if self.emails.contains(&private.email) {
                        let token = Token::new(private.email.clone(), Some(expiry_timestamp));
                        let policy = PolicyDirective::allow(private.into_value());

                        Ok(IssuedPolicy::new(token, policy))
                    } else {
                        Ok(IssuedPolicy::deny(Value::Extant))
                    };
                }
                Err(_) => Err(AuthenticationError::Malformatted(
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
        }
    }
}
