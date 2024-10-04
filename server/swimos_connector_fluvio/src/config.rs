// Copyright 2015-2024 Swim Inc.
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

use fluvio::config::{TlsCerts, TlsConfig, TlsPaths, TlsPolicy as FluvioTlsPolicy};
use fluvio::{FluvioConfig, Offset};
use std::path::PathBuf;
use swimos_connector::config::format::DataFormat;
use swimos_connector::config::{IngressMapLaneSpec, IngressValueLaneSpec};
use swimos_connector::{RelaySpecification, Relays};
use swimos_form::Form;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Configuration parameters for the Fluvio connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "fluvio")]
pub struct FluvioIngressSpecification {
    /// The topic to consume from.
    pub topic: String,
    /// Fluvio library configuration.
    pub fluvio: Option<FluvioSpecification>,
    /// The partition to consume from.
    pub partition: u32,
    /// The offset to start consuming from.
    #[form(name = "offset")]
    pub offset: OffsetSpecification,
    /// Specifications for the value lanes to define for the connector. This includes a pattern to
    /// define a selector that will pick out values to set to that lane, from a Fluvio message.
    pub value_lanes: Vec<IngressValueLaneSpec>,
    /// Specifications for the map lanes to define for the connector. This includes a pattern to
    /// define a selector that will pick out updates to apply to that lane, from a Fluvio message.
    pub map_lanes: Vec<IngressMapLaneSpec>,
    /// Deserialization format to use to interpret the contents of the keys of the Fluvio messages.
    pub key_deserializer: DataFormat,
    /// Deserialization format to use to interpret the contents of the payloads of the Fluvio
    /// messages.
    pub payload_deserializer: DataFormat,
    /// Collection of relays used for forwarding messages to lanes on agents.
    pub relays: Vec<RelaySpecification>,
}

impl FluvioIngressSpecification {
    pub fn build(self) -> Result<FluvioIngressConfiguration, BoxError> {
        let FluvioIngressSpecification {
            topic,
            fluvio,
            partition,
            offset,
            value_lanes,
            map_lanes,
            key_deserializer,
            payload_deserializer,
            relays,
        } = self;

        let fluvio = match fluvio {
            Some(native) => native.build()?,
            None => FluvioConfig::load()?,
        };
        Ok(FluvioIngressConfiguration {
            topic,
            fluvio,
            partition,
            offset: offset.build()?,
            value_lanes,
            map_lanes,
            key_deserializer,
            payload_deserializer,
            relays: Relays::try_from(relays)?,
        })
    }
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub enum OffsetSpecification {
    Beginning(Option<u32>),
    End(Option<u32>),
    Absolute(i64),
}

impl OffsetSpecification {
    fn build(self) -> Result<Offset, BoxError> {
        match self {
            OffsetSpecification::Beginning(Some(n)) => Ok(Offset::from_beginning(n)),
            OffsetSpecification::Beginning(None) => Ok(Offset::beginning()),
            OffsetSpecification::End(Some(n)) => Ok(Offset::from_end(n)),
            OffsetSpecification::End(None) => Ok(Offset::end()),
            OffsetSpecification::Absolute(n) => Ok(Offset::absolute(n)?),
        }
    }
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "Native")]
pub struct FluvioSpecification {
    pub addr: String,
    pub use_spu_local_address: Option<bool>,
    pub tls: Option<TlsPolicy>,
    pub client_id: Option<String>,
}

impl FluvioSpecification {
    fn build(self) -> Result<fluvio::FluvioConfig, BoxError> {
        let FluvioSpecification {
            addr,
            use_spu_local_address,
            tls,
            client_id,
        } = self;

        let tls = match tls {
            Some(tls) => tls.build(),
            None => FluvioTlsPolicy::default(),
        };

        let mut config = fluvio::FluvioConfig::new(addr).with_tls(tls);
        config.client_id = client_id;

        if let Some(use_spu_local_address) = use_spu_local_address {
            config.use_spu_local_address = use_spu_local_address;
        }

        Ok(config)
    }
}

/// Describes whether or not to use TLS and how
#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub enum TlsPolicy {
    Disabled,
    Anonymous,
    Verified(FluvioTlsConfig),
}

impl TlsPolicy {
    fn build(self) -> FluvioTlsPolicy {
        match self {
            TlsPolicy::Disabled => FluvioTlsPolicy::Disabled,
            TlsPolicy::Anonymous => FluvioTlsPolicy::Anonymous,
            TlsPolicy::Verified(FluvioTlsConfig::Inline(paths)) => {
                let FluvioTlsCerts {
                    domain,
                    key,
                    cert,
                    ca_cert,
                } = paths;
                FluvioTlsPolicy::Verified(TlsConfig::Inline(TlsCerts {
                    domain,
                    key,
                    cert,
                    ca_cert,
                }))
            }
            TlsPolicy::Verified(FluvioTlsConfig::Files(paths)) => {
                let FluvioTlsPaths {
                    domain,
                    key,
                    cert,
                    ca_cert,
                } = paths;
                FluvioTlsPolicy::Verified(TlsConfig::Files(TlsPaths {
                    domain,
                    key: PathBuf::from(key),
                    cert: PathBuf::from(cert),
                    ca_cert: PathBuf::from(ca_cert),
                }))
            }
        }
    }
}

/// Describes the TLS configuration either inline or via file paths
#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub enum FluvioTlsConfig {
    Inline(FluvioTlsCerts),
    Files(FluvioTlsPaths),
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub struct FluvioTlsCerts {
    /// Domain name
    pub domain: String,
    /// Client or Server private key
    pub key: String,
    /// Client or Server certificate
    pub cert: String,
    /// Certificate Authority cert
    pub ca_cert: String,
}

/// TLS config with paths to keys and certs
#[derive(Clone, Debug, Form, PartialEq, Eq)]
pub struct FluvioTlsPaths {
    /// Domain name
    pub domain: String,
    /// Path to client or server private key
    pub key: String,
    /// Path to client or server certificate
    pub cert: String,
    /// Path to Certificate Authority certificate
    pub ca_cert: String,
}

#[derive(Clone, Debug)]
pub struct FluvioIngressConfiguration {
    /// The topic to consume from.
    pub topic: String,
    /// Fluvio library configuration.
    pub fluvio: fluvio::FluvioConfig,
    /// The partition to consume from.
    pub partition: u32,
    /// The offset to start consuming from.
    pub offset: Offset,
    /// Specifications for the value lanes to define for the connector. This includes a pattern to
    /// define a selector that will pick out values to set to that lane, from a Fluvio message.
    pub value_lanes: Vec<IngressValueLaneSpec>,
    /// Specifications for the map lanes to define for the connector. This includes a pattern to
    /// define a selector that will pick out updates to apply to that lane, from a Fluvio message.
    pub map_lanes: Vec<IngressMapLaneSpec>,
    /// Deserialization format to use to interpret the contents of the keys of the Fluvio messages.
    pub key_deserializer: DataFormat,
    /// Deserialization format to use to interpret the contents of the payloads of the Fluvio
    /// messages.
    pub payload_deserializer: DataFormat,
    /// Collection of relays used for forwarding messages to lanes on agents.
    pub relays: Relays,
}
