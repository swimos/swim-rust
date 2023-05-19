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

use rustls::client::InvalidDnsNameError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("Error reading PEM file: {0}")]
    InvalidPem(std::io::Error),
    #[error("The provided input did not contain a valid private key.")]
    InvalidPrivateKey,
    #[error("Invalid certificate: {0}")]
    BadCertificate(#[from] webpki::Error),
    #[error("Invalid DNS host name: {0}")]
    BadHostName(#[from] InvalidDnsNameError),
    #[error("TLS handshake failed: {0}")]
    HandshakeFailed(std::io::Error),
}
