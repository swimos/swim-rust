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

use crate::net::Scheme;

use super::SchemeHostPort;

#[test]
fn parse_insecure_warp_url() {
    let SchemeHostPort(scheme, host, port) = "warp://localhost:8080"
        .parse::<SchemeHostPort>()
        .expect("Parse failed.");
    assert_eq!(scheme, Scheme::Ws);
    assert_eq!(host, "localhost");
    assert_eq!(port, 8080);

    let SchemeHostPort(scheme, host, port) = "ws://localhost:8080"
        .parse::<SchemeHostPort>()
        .expect("Parse failed.");
    assert_eq!(scheme, Scheme::Ws);
    assert_eq!(host, "localhost");
    assert_eq!(port, 8080);
}

#[test]
fn parse_secure_warp_url() {
    let SchemeHostPort(scheme, host, port) = "warps://localhost:8080"
        .parse::<SchemeHostPort>()
        .expect("Parse failed.");
    assert_eq!(scheme, Scheme::Wss);
    assert_eq!(host, "localhost");
    assert_eq!(port, 8080);

    let SchemeHostPort(scheme, host, port) = "wss://localhost:8080"
        .parse::<SchemeHostPort>()
        .expect("Parse failed.");
    assert_eq!(scheme, Scheme::Wss);
    assert_eq!(host, "localhost");
    assert_eq!(port, 8080);
}

#[test]
fn parse_unqualified_warp_url() {
    let SchemeHostPort(scheme, host, port) = "localhost:8080"
        .parse::<SchemeHostPort>()
        .expect("Parse failed.");
    assert_eq!(scheme, Scheme::Ws);
    assert_eq!(host, "localhost");
    assert_eq!(port, 8080);
}
