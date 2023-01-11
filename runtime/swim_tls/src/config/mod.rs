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

pub enum CertFormat {
    Pem,
    Der,
}

pub struct Certificate {
    pub format: CertFormat,
    pub body: Vec<u8>,
}

pub struct CertChain(pub Vec<Certificate>);

pub struct PrivateKey {
    pub format: CertFormat,
    pub body: Vec<u8>,
}

pub struct TlsConfig {
    pub client: ClientConfig,
    pub server: ServerConfig,
}

pub struct ServerConfig {
    pub chain: CertChain,
    pub key: PrivateKey,
    pub enable_log_file: bool,
}

pub struct ClientConfig {
    pub use_webpki_roots: bool,
    pub custom_roots: Vec<Certificate>,
}