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

use crate::ws::error::WebSocketError;
use native_tls::{Certificate, TlsConnector};
use tokio::net::TcpStream;
use tokio_tls::{TlsConnector as TokioTlsConnector, TlsStream};

use crate::ws::error::CertificateError;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

pub struct TlsError(pub String);

impl From<native_tls::Error> for TlsError {
    fn from(e: native_tls::Error) -> Self {
        TlsError(e.to_string())
    }
}

impl From<std::io::Error> for CertificateError {
    fn from(e: std::io::Error) -> Self {
        CertificateError::Io(e.to_string())
    }
}

impl From<CertificateError> for WebSocketError {
    fn from(e: CertificateError) -> Self {
        WebSocketError::CertificateError(e)
    }
}

/// Attempts to open TLS connection to the domain, adding the certificate to the set of roots that
/// the connector will trust.
pub async fn connect_tls(
    domain: &str,
    certificate: Certificate,
) -> Result<TlsStream<TcpStream>, TlsError> {
    let socket = TcpStream::connect(domain)
        .await
        .map_err(|e| TlsError(e.to_string()))?;

    let mut tls_conn_builder = TlsConnector::builder();
    tls_conn_builder.add_root_certificate(certificate);

    let connector = tls_conn_builder.build()?;
    let stream = TokioTlsConnector::from(connector);

    stream.connect(domain, socket).await.map_err(Into::into)
}

pub fn build_x509_certificate(path: impl AsRef<Path>) -> Result<Certificate, CertificateError> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut buf = vec![];
    reader.read_to_end(&mut buf)?;

    match Certificate::from_pem(&buf) {
        Ok(cert) => Ok(cert),
        Err(e) => Err(CertificateError::SSL(e.to_string())),
    }
}
