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

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use tokio_native_tls::{native_tls::Certificate, TlsConnector as TokioTlsConnector, TlsStream};

use crate::error::{TlsError, TlsErrorKind};
use tokio::net::TcpStream;
use tokio_native_tls::native_tls::TlsConnector;

/// Attempts to open TLS connection to the domain, adding the certificate to the set of roots that
/// the connector will trust.
pub async fn connect_tls(
    domain: &str,
    certificate: Certificate,
) -> Result<TlsStream<TcpStream>, TlsError> {
    let socket = TcpStream::connect(domain)
        .await
        .map_err(|e| TlsError::new(TlsErrorKind::InvalidCertificate, Some(e.to_string())))?;

    let mut tls_conn_builder = TlsConnector::builder();
    tls_conn_builder.add_root_certificate(certificate);

    let connector = tls_conn_builder.build()?;
    let stream = TokioTlsConnector::from(connector);

    stream.connect(domain, socket).await.map_err(Into::into)
}

pub fn build_x509_certificate(path: impl AsRef<Path>) -> Result<Certificate, TlsError> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut buf = vec![];
    reader.read_to_end(&mut buf)?;

    match Certificate::from_pem(&buf) {
        Ok(cert) => Ok(cert),
        Err(e) => Err(TlsError::new(
            TlsErrorKind::InvalidCertificate,
            Some(e.to_string()),
        )),
    }
}
