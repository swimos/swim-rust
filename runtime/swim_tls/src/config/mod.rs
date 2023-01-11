// Copyright 2015-2021 Swim Inc.
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

/// Supported certificate formats for TLS connections.
pub enum CertFormat {
    Pem,
    Der,
}

/// An (unvalidated)) TLS certificate (or list of certificates for a PEM file).
pub struct CertificateFile {
    pub format: CertFormat,
    pub body: Vec<u8>,
}

/// A chain of TLS certificates (starting with the server certificate and ending with the CA).
pub struct CertChain(pub Vec<CertificateFile>);

/// An (unvalidated) private key for a server.
pub struct PrivateKey {
    pub format: CertFormat,
    pub body: Vec<u8>,
}

/// Combined TLS configuration (both server and client)/
pub struct TlsConfig {
    pub client: ClientConfig,
    pub server: ServerConfig,
}

/// Configuration parameters for a TLS server.
pub struct ServerConfig {
    pub chain: CertChain,
    pub key: PrivateKey,
    pub enable_log_file: bool,
}

/// Configuration parameters for a TLS client.
pub struct ClientConfig {
    pub use_webpki_roots: bool,
    pub custom_roots: Vec<CertificateFile>,
}
