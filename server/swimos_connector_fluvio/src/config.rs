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
use std::path::{Path, PathBuf};
use std::str::FromStr;
use swimos_connector::config::format::DataFormat;
use swimos_connector::config::{
    IngressMapLaneSpec, IngressValueLaneSpec, PubSubRelaySpecification,
};
use swimos_connector::selector::{PubSubSelector, Relays};
use swimos_form::Form;
use swimos_recon::parser::parse_recognize;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Configuration parameters for the Fluvio connector.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
#[form(tag = "fluvio")]
struct FluvioIngressSpecification {
    addr: String,
    topic: String,
    use_spu_local_address: Option<bool>,
    tls: Option<TlsPolicy>,
    client_id: Option<String>,
    partition: u32,
    #[form(name = "offset")]
    offset: OffsetSpecification,
    value_lanes: Vec<IngressValueLaneSpec>,
    map_lanes: Vec<IngressMapLaneSpec>,
    key_deserializer: DataFormat,
    payload_deserializer: DataFormat,
    relays: Vec<PubSubRelaySpecification>,
}

impl FluvioIngressSpecification {
    fn build(self) -> Result<FluvioIngressConfiguration, BoxError> {
        let FluvioIngressSpecification {
            topic,
            addr,
            use_spu_local_address,
            tls,
            client_id,
            partition,
            offset,
            value_lanes,
            map_lanes,
            key_deserializer,
            payload_deserializer,
            relays,
        } = self;

        let tls = match tls {
            Some(tls) => tls.build(),
            None => FluvioTlsPolicy::default(),
        };

        let mut fluvio = FluvioConfig::new(addr).with_tls(tls);
        fluvio.use_spu_local_address = use_spu_local_address.unwrap_or_default();
        fluvio.client_id = client_id;

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
enum OffsetSpecification {
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

/// Describes whether or not to use TLS and how.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
enum TlsPolicy {
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

/// Describes the TLS configuration either inline or via file paths.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
enum FluvioTlsConfig {
    Inline(FluvioTlsCerts),
    Files(FluvioTlsPaths),
}

#[derive(Clone, Debug, Form, PartialEq, Eq)]
struct FluvioTlsCerts {
    /// Domain name.
    domain: String,
    /// Client or Server private key.
    key: String,
    /// Client or Server certificate.
    cert: String,
    /// Certificate Authority cert.
    ca_cert: String,
}

/// TLS config with paths to keys and certs.
#[derive(Clone, Debug, Form, PartialEq, Eq)]
struct FluvioTlsPaths {
    /// Domain name.
    domain: String,
    /// Path to client or server private key.
    key: String,
    /// Path to client or server certificate.
    cert: String,
    /// Path to Certificate Authority certificate.
    ca_cert: String,
}

#[derive(Clone, Debug)]
pub struct FluvioIngressConfiguration {
    /// The topic to consume from.
    pub topic: String,
    /// Fluvio library configuration.
    pub fluvio: FluvioConfig,
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
    pub relays: Relays<PubSubSelector>,
}

impl FluvioIngressConfiguration {
    pub async fn from_file(path: impl AsRef<Path>) -> Result<FluvioIngressConfiguration, BoxError> {
        let content = tokio::fs::read_to_string(path).await?;
        FluvioIngressConfiguration::from_str(&content)
    }
}

impl FromStr for FluvioIngressConfiguration {
    type Err = BoxError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let config = parse_recognize::<FluvioIngressSpecification>(s, true)?.build()?;
        Ok(config)
    }
}
