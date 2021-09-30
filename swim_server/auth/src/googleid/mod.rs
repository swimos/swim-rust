// Copyright 2015-2021 SWIM.AI inc.
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

use swim_common::form::Form;
use swim_model::{Value, ValueKind};

use crate::googleid::store::{
    GoogleKeyStore, GoogleKeyStoreError, DEFAULT_CERT_SKEW, GOOGLE_JWK_CERTS_URL,
};
use crate::policy::{IssuedPolicy, PolicyDirective};
use crate::token::Token;
use crate::{AuthenticationError, Authenticator};
use biscuit::jwa::SignatureAlgorithm;
use std::borrow::Borrow;
use std::convert::TryFrom;
use swim_common::form::structural::read::error::ExpectedEvent;
use swim_common::form::structural::read::event::{NumericValue, ReadEvent};
use swim_common::form::structural::read::recognizer::primitive::StringRecognizer;
use swim_common::form::structural::read::recognizer::{
    Recognizer, RecognizerReadable, SimpleAttrBody, VecRecognizer,
};
use swim_common::form::structural::read::ReadError;
use swim_common::form::structural::write::{
    BodyWriter, HeaderWriter, PrimitiveWriter, RecordBodyKind, StructuralWritable, StructuralWriter,
};
use swim_model::Text;

mod store;
#[cfg(test)]
mod tests;

const GOOGLE_AUTH_TAG: &str = "googleId";
const GOOGLE_ISS1: &str = "accounts.google.com";
const GOOGLE_ISS2: &str = "https://accounts.google.com";
const DEFAULT_TOKEN_SKEW: i64 = 5;

/// An authenticator for validating Google ID tokens.
/// See <https://developers.google.com/identity/sign-in/web/> for more information
#[derive(Debug, PartialEq)]
pub struct GoogleIdAuthenticator {
    /// Number of seconds beyond the token's expiry time that are permitted.
    token_skew: i64,
    key_store: GoogleKeyStore,
    emails: HashSet<String>,
    audiences: HashSet<String>,
}

impl StructuralWritable for GoogleIdAuthenticator {
    fn write_with<W: StructuralWriter>(
        &self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        let header_writer = writer.record(1)?;
        let mut body_writer = header_writer
            .write_extant_attr(GOOGLE_AUTH_TAG)?
            .complete_header(RecordBodyKind::MapLike, 5)?;
        body_writer = body_writer.write_i64_slot("token_skew", self.token_skew)?;
        body_writer = body_writer.write_i64_slot("cert_skew", self.key_store.cert_skew())?;
        body_writer = body_writer.write_slot_into("publicKeyUri", self.key_store.key_url())?;
        let emails = self.emails.iter().cloned().collect::<Vec<_>>();
        body_writer = body_writer.write_slot_into("emails", emails)?;
        let audiences = self.audiences.iter().cloned().collect::<Vec<_>>();
        body_writer = body_writer.write_slot_into("audiences", audiences)?;
        body_writer.done()
    }

    fn write_into<W: StructuralWriter>(
        self,
        writer: W,
    ) -> Result<<W as PrimitiveWriter>::Repr, <W as PrimitiveWriter>::Error> {
        let header_writer = writer.record(1)?;
        let mut body_writer = header_writer
            .write_extant_attr(GOOGLE_AUTH_TAG)?
            .complete_header(RecordBodyKind::MapLike, 5)?;
        body_writer = body_writer.write_i64_slot("token_skew", self.token_skew)?;
        body_writer = body_writer.write_i64_slot("cert_skew", self.key_store.cert_skew())?;
        body_writer = body_writer.write_slot_into("publicKeyUri", self.key_store.key_url())?;
        let emails = self.emails.into_iter().collect::<Vec<_>>();
        body_writer = body_writer.write_slot_into("emails", emails)?;
        let audiences = self.audiences.into_iter().collect::<Vec<_>>();
        body_writer = body_writer.write_slot_into("audiences", audiences)?;
        body_writer.done()
    }

    fn num_attributes(&self) -> usize {
        1
    }
}

#[derive(Clone, Copy)]
enum AuthRecogField {
    TokenSkew,
    CertSkew,
    PublicKeyUri,
    Emails,
    Audiences,
}

enum AuthRecogState {
    Init,
    Tag,
    AfterTag,
    InBody,
    Slot(AuthRecogField),
    Field(AuthRecogField),
}

pub struct GoogleIdAuthenticatorRecognizer {
    state: AuthRecogState,
    token_skew: Option<i64>,
    cert_skew: Option<i64>,
    public_key_uri: Option<Url>,
    emails: Option<Vec<String>>,
    audiences: Option<Vec<String>>,
    emails_recognizer: VecRecognizer<String, StringRecognizer>,
    audiences_recognizer: VecRecognizer<String, StringRecognizer>,
}

impl GoogleIdAuthenticatorRecognizer {
    fn try_done(&mut self) -> Result<GoogleIdAuthenticator, ReadError> {
        let GoogleIdAuthenticatorRecognizer {
            token_skew,
            cert_skew,
            public_key_uri,
            emails,
            audiences,
            ..
        } = self;
        let mut missing = vec![];
        if token_skew.is_none() {
            missing.push(Text::new("token_skew"));
        }
        if cert_skew.is_none() {
            missing.push(Text::new("cert_skew"));
        }
        if public_key_uri.is_none() {
            missing.push(Text::new("publicKeyUri"));
        }
        if emails.is_none() {
            missing.push(Text::new("emails"));
        }
        if audiences.is_none() {
            missing.push(Text::new("audiences"));
        }
        if let (
            Some(token_skew),
            Some(cert_skew),
            Some(public_key_uri),
            Some(emails),
            Some(audiences),
        ) = (
            token_skew.take(),
            cert_skew.take(),
            public_key_uri.take(),
            emails.take(),
            audiences.take(),
        ) {
            Ok(GoogleIdAuthenticator {
                token_skew,
                key_store: GoogleKeyStore::new(public_key_uri, cert_skew),
                emails: emails.into_iter().collect(),
                audiences: audiences.into_iter().collect(),
            })
        } else {
            Err(ReadError::MissingFields(missing))
        }
    }
}

impl Default for GoogleIdAuthenticatorRecognizer {
    fn default() -> Self {
        GoogleIdAuthenticatorRecognizer {
            state: AuthRecogState::Init,
            token_skew: None,
            cert_skew: None,
            public_key_uri: None,
            emails: None,
            audiences: None,
            emails_recognizer: <Vec<String>>::make_recognizer(),
            audiences_recognizer: <Vec<String>>::make_recognizer(),
        }
    }
}

impl Recognizer for GoogleIdAuthenticatorRecognizer {
    type Target = GoogleIdAuthenticator;

    fn feed_event(&mut self, input: ReadEvent<'_>) -> Option<Result<Self::Target, ReadError>> {
        match &self.state {
            AuthRecogState::Init => {
                if let ReadEvent::StartAttribute(name) = input {
                    if name == GOOGLE_AUTH_TAG {
                        self.state = AuthRecogState::Tag;
                        None
                    } else {
                        Some(Err(ReadError::MissingTag))
                    }
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Attribute(Some(
                        Text::new(GOOGLE_AUTH_TAG),
                    )))))
                }
            }
            AuthRecogState::Tag => match input {
                ReadEvent::Extant => None,
                ReadEvent::EndAttribute => {
                    self.state = AuthRecogState::AfterTag;
                    None
                }
                ow => Some(Err(ow.kind_error(ExpectedEvent::EndOfAttribute))),
            },
            AuthRecogState::AfterTag => {
                if matches!(&input, ReadEvent::StartBody) {
                    self.state = AuthRecogState::InBody;
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::RecordBody)))
                }
            }
            AuthRecogState::InBody => match input {
                ReadEvent::TextValue(slot_name) => match slot_name.borrow() {
                    "token_skew" => {
                        self.state = AuthRecogState::Slot(AuthRecogField::TokenSkew);
                        None
                    }
                    "cert_skew" => {
                        self.state = AuthRecogState::Slot(AuthRecogField::CertSkew);
                        None
                    }
                    "publicKeyUri" => {
                        self.state = AuthRecogState::Slot(AuthRecogField::PublicKeyUri);
                        None
                    }
                    "emails" => {
                        self.state = AuthRecogState::Slot(AuthRecogField::Emails);
                        None
                    }
                    "audiences" => {
                        self.state = AuthRecogState::Slot(AuthRecogField::Audiences);
                        None
                    }
                    ow => Some(Err(ReadError::UnexpectedField(Text::new(ow)))),
                },
                ReadEvent::EndRecord => Some(self.try_done()),
                ow => Some(Err(ow.kind_error(ExpectedEvent::Or(vec![
                    ExpectedEvent::ValueEvent(ValueKind::Text),
                    ExpectedEvent::EndOfRecord,
                ])))),
            },
            AuthRecogState::Slot(fld) => {
                if matches!(&input, ReadEvent::Slot) {
                    self.state = AuthRecogState::Field(*fld);
                    None
                } else {
                    Some(Err(input.kind_error(ExpectedEvent::Slot)))
                }
            }
            AuthRecogState::Field(AuthRecogField::TokenSkew) => match input {
                ReadEvent::Number(NumericValue::Int(n)) => {
                    self.token_skew = Some(n);
                    self.state = AuthRecogState::InBody;
                    None
                }
                ReadEvent::Number(NumericValue::UInt(n)) => {
                    if let Ok(m) = i64::try_from(n) {
                        self.token_skew = Some(m);
                        self.state = AuthRecogState::InBody;
                        None
                    } else {
                        Some(Err(ReadError::NumberOutOfRange))
                    }
                }
                ow => Some(Err(
                    ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::UInt64))
                )),
            },
            AuthRecogState::Field(AuthRecogField::CertSkew) => match input {
                ReadEvent::Number(NumericValue::Int(n)) => {
                    self.cert_skew = Some(n);
                    self.state = AuthRecogState::InBody;
                    None
                }
                ReadEvent::Number(NumericValue::UInt(n)) => {
                    if let Ok(m) = i64::try_from(n) {
                        self.cert_skew = Some(m);
                        self.state = AuthRecogState::InBody;
                        None
                    } else {
                        Some(Err(ReadError::NumberOutOfRange))
                    }
                }
                ow => Some(Err(
                    ow.kind_error(ExpectedEvent::ValueEvent(ValueKind::UInt64))
                )),
            },
            AuthRecogState::Field(AuthRecogField::PublicKeyUri) => {
                if let ReadEvent::TextValue(text) = input {
                    if let Ok(url) = Url::parse(text.borrow()) {
                        self.public_key_uri = Some(url);
                        self.state = AuthRecogState::InBody;
                        None
                    } else {
                        Some(Err(ReadError::Malformatted {
                            text: text.into(),
                            message: Text::new("No a valid URL."),
                        }))
                    }
                } else {
                    Some(Err(
                        input.kind_error(ExpectedEvent::ValueEvent(ValueKind::Text))
                    ))
                }
            }
            AuthRecogState::Field(AuthRecogField::Emails) => {
                match self.emails_recognizer.feed_event(input)? {
                    Ok(v) => {
                        self.emails = Some(v);
                        self.state = AuthRecogState::InBody;
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            }
            AuthRecogState::Field(AuthRecogField::Audiences) => {
                match self.audiences_recognizer.feed_event(input)? {
                    Ok(v) => {
                        self.audiences = Some(v);
                        self.state = AuthRecogState::InBody;
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            }
        }
    }

    fn reset(&mut self) {
        let GoogleIdAuthenticatorRecognizer {
            state,
            token_skew,
            cert_skew,
            public_key_uri,
            emails,
            audiences,
            emails_recognizer,
            audiences_recognizer,
        } = self;
        *state = AuthRecogState::Init;
        *token_skew = None;
        *cert_skew = None;
        *public_key_uri = None;
        *emails = None;
        *audiences = None;
        emails_recognizer.reset();
        audiences_recognizer.reset();
    }
}

impl RecognizerReadable for GoogleIdAuthenticator {
    type Rec = GoogleIdAuthenticatorRecognizer;
    type AttrRec = SimpleAttrBody<GoogleIdAuthenticatorRecognizer>;
    type BodyRec = Self::Rec;

    fn make_recognizer() -> Self::Rec {
        GoogleIdAuthenticatorRecognizer::default()
    }

    fn make_attr_recognizer() -> Self::AttrRec {
        SimpleAttrBody::new(Self::make_recognizer())
    }

    fn make_body_recognizer() -> Self::BodyRec {
        Self::make_recognizer()
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

            match jwt.decode_with_jwks(keys, Some(SignatureAlgorithm::RS256)) {
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
                                (**expiry_time).into()
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
                            if !self.audiences.contains(audience) {
                                return Ok(IssuedPolicy::forbid(Value::Extant));
                            }
                        }
                        Some(SingleOrMultiple::Multiple(audiences)) => {
                            let matched_audience =
                                audiences.iter().any(|aud| self.audiences.contains(aud));

                            if !matched_audience {
                                return Ok(IssuedPolicy::forbid(Value::Extant));
                            }
                        }
                        None => return Ok(IssuedPolicy::forbid(Value::Extant)),
                    }

                    // Check that the email of the token matches one of the configured emails
                    if self.emails.contains(&private.email) {
                        let token = Token::new(private.email.clone(), Some(expiry_timestamp));
                        let policy = PolicyDirective::allow(private.into_value());

                        Ok(IssuedPolicy::new(token, policy))
                    } else {
                        Ok(IssuedPolicy::deny(Value::Extant))
                    }
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
