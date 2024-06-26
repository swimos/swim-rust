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

mod config;
mod errors;
mod maybe;
mod net;

pub use config::{
    CertChain, CertFormat, CertificateFile, ClientConfig, PrivateKey, ServerConfig, TlsConfig,
};
pub use errors::TlsError;
pub use maybe::MaybeTlsStream;
pub use net::{RustlsClientNetworking, RustlsListener, RustlsNetworking, RustlsServerNetworking};
use rustls::crypto::CryptoProvider;
use std::sync::Arc;

#[derive(Default)]
pub enum CryptoProviderConfig {
    ProcessDefault,
    #[default]
    FromFeatureFlags,
    Provided(Arc<CryptoProvider>),
}

impl CryptoProviderConfig {
    pub fn build(self) -> Arc<CryptoProvider> {
        match self {
            CryptoProviderConfig::ProcessDefault => CryptoProvider::get_default()
                .expect("No default cryptographic provider specified")
                .clone(),
            CryptoProviderConfig::FromFeatureFlags => {
                #[cfg(all(feature = "ring_provider", not(feature = "aws_lc_rs_provider")))]
                {
                    return Arc::new(rustls::crypto::ring::default_provider());
                }

                #[cfg(all(feature = "aws_lc_rs_provider", not(feature = "ring_provider")))]
                {
                    return Arc::new(rustls::crypto::aws_lc_rs::default_provider());
                }

                #[allow(unreachable_code)]
                {
                    panic!("Ambiguous cryptographic provider feature flags specified. Only \"ring_provider\" or \"aws_lc_rs_provider\" may be specified")
                }
            }
            CryptoProviderConfig::Provided(provider) => provider,
        }
    }
}
