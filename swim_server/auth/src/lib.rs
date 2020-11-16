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

use crate::googleid::GoogleIdAuthenticator;
use crate::policy::PolicyDirective;
use chrono::{Duration, Utc};
use futures::future::ready;
use futures::Future;
use futures_util::future::Ready;
use std::collections::HashMap;
use std::fmt::Display;
use swim_common::form::{Form, FormErr};
use swim_common::model::time::Timestamp;
use swim_common::model::{Attr, Value};

pub mod googleid;
pub mod policy;

pub enum AgentAuthenticator {
    AlwaysAllow(AlwaysAllowAuthenticator),
    AlwaysDeny(AlwaysDenyAuthenticator),
    AlwaysForbid(AlwaysForbidAuthenticator),
    GoogleId(Box<GoogleIdAuthenticator>),
}

#[derive(Debug, Eq, PartialEq)]
pub enum AuthenticationError {
    KeyStoreError(String),
    ServerError,
    MalformattedResponse(String),
}

impl AuthenticationError {
    pub fn malformatted<A: Display>(msg: &str, cause: A) -> AuthenticationError {
        AuthenticationError::MalformattedResponse(format!("{}: {}", msg, cause))
    }
}

/// A trait for defining authenticators that validates a remote host's credentials against some
/// authentication implementation.
pub trait Authenticator<'s>: Form {
    /// The type of the structure that this authenticator requires.
    type Credentials: Form;
    /// A future that resolves to either a policy for the remote host or an associated error.
    type AuthenticateFuture: Future<Output = Result<PolicyDirective, AuthenticationError>> + 's;

    /// Attempt to authenticate the remote host.
    fn authenticate(&'s mut self, credentials: Self::Credentials) -> Self::AuthenticateFuture;
}

/// An authenticator that will always allow the remote host.
pub struct AlwaysAllowAuthenticator;
impl<'s> Authenticator<'s> for AlwaysAllowAuthenticator {
    type Credentials = Value;
    type AuthenticateFuture = Ready<Result<PolicyDirective, AuthenticationError>>;

    fn authenticate(&'s mut self, credentials: Self::Credentials) -> Self::AuthenticateFuture {
        ready(Ok(PolicyDirective::allow(credentials)))
    }
}

impl Form for AlwaysAllowAuthenticator {
    fn as_value(&self) -> Value {
        Value::of_attr("allow")
    }

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::Record(attrs, items) if items.is_empty() => match attrs.first() {
                Some(Attr { name, .. }) if name == "allow" => Ok(AlwaysAllowAuthenticator),
                _ => Err(FormErr::MismatchedTag),
            },
            v => Err(FormErr::incorrect_type("Value::Record", v)),
        }
    }
}

/// An authenticator that will always deny the remote host.
pub struct AlwaysDenyAuthenticator;
impl<'s> Authenticator<'s> for AlwaysDenyAuthenticator {
    type Credentials = Value;
    type AuthenticateFuture = Ready<Result<PolicyDirective, AuthenticationError>>;

    fn authenticate(&'s mut self, credentials: Self::Credentials) -> Self::AuthenticateFuture {
        ready(Ok(PolicyDirective::deny(credentials)))
    }
}

impl Form for AlwaysDenyAuthenticator {
    fn as_value(&self) -> Value {
        Value::of_attr("deny")
    }

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::Record(attrs, items) if items.is_empty() => match attrs.first() {
                Some(Attr { name, .. }) if name == "deny" => Ok(AlwaysDenyAuthenticator),
                _ => Err(FormErr::MismatchedTag),
            },
            v => Err(FormErr::incorrect_type("Value::Record", v)),
        }
    }
}

/// An authenticator that will always forbid the remote host.
pub struct AlwaysForbidAuthenticator;
impl<'s> Authenticator<'s> for AlwaysForbidAuthenticator {
    type Credentials = Value;
    type AuthenticateFuture = Ready<Result<PolicyDirective, AuthenticationError>>;

    fn authenticate(&'s mut self, credentials: Self::Credentials) -> Self::AuthenticateFuture {
        ready(Ok(PolicyDirective::forbid(credentials)))
    }
}

impl Form for AlwaysForbidAuthenticator {
    fn as_value(&self) -> Value {
        Value::of_attr("forbid")
    }

    fn try_from_value(value: &Value) -> Result<Self, FormErr> {
        match value {
            Value::Record(attrs, items) if items.is_empty() => match attrs.first() {
                Some(Attr { name, .. }) if name == "forbid" => Ok(AlwaysForbidAuthenticator),
                _ => Err(FormErr::MismatchedTag),
            },
            v => Err(FormErr::incorrect_type("Value::Record", v)),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Ord, PartialOrd, Hash)]
pub struct Token {
    id: String,
    expires: Timestamp,
}

impl Token {
    pub fn new(id: String, expires: Timestamp) -> Token {
        Token { id, expires }
    }
}

pub trait Expired {
    fn expired(&self, skew: i64) -> bool;
}

impl Expired for Token {
    fn expired(&self, skew: i64) -> bool {
        self.expires
            .as_ref()
            .lt(&(Utc::now() + Duration::seconds(skew)))
    }
}

#[derive(Debug, PartialEq)]
pub struct TokenDirective {
    token: Token,
    policy: PolicyDirective,
}

impl Expired for TokenDirective {
    fn expired(&self, skew: i64) -> bool {
        self.token.expired(skew)
    }
}

/// A store that contains mapped identifiers to a given directive. The store is lazy and will only
/// check if tokens have expired following a get invocation.
#[derive(Debug, PartialEq)]
struct TokenStore {
    /// An allowed time expiry skew for the tokens.
    skew: i64,
    tokens: HashMap<String, TokenDirective>,
}

impl TokenStore {
    /// Creates a new token store that permits the provided time skew on token expiration.
    pub fn new(skew: i64) -> TokenStore {
        TokenStore {
            skew,
            tokens: Default::default(),
        }
    }
}

impl TokenStore {
    /// Insert (and overwrite) the token at the provided key.
    pub fn insert(&mut self, token: Token, policy: PolicyDirective) {
        let id = token.id.clone();
        let token_directive = TokenDirective { token, policy };

        self.tokens.insert(id, token_directive);
    }

    /// Gets a reference to the provided `TokenDirective` using the key.
    ///
    /// This operation will also prune any expired tokens.
    pub fn get(&mut self, key: &str) -> Option<&TokenDirective> {
        let TokenStore { skew, tokens } = self;

        tokens.retain(|_k, token| !token.expired(*skew));
        tokens.get(key)
    }
}
