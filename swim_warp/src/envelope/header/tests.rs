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

use super::{peel_envelope_header, RawEnvelope};

#[test]
fn peel_auth() {
    let envelope = b"@auth@payload { name: bob }";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Auth(body)) => {
            assert_eq!(*body, "@payload { name: bob }");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }
}

#[test]
fn peel_deauth() {
    let envelope = b"@deauth@payload { name: bob }";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::DeAuth(body)) => {
            assert_eq!(*body, "@payload { name: bob }");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }
}

#[test]
fn peel_link() {
    let envelope = b"@link(node: \"/node\", lane: name)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Link {
            node_uri,
            lane_uri,
            rate,
            prio,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert!(rate.is_none());
            assert!(prio.is_none());
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }

    let envelope = b"@link(node: \"/node\", lane: name, rate: 0.5)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Link {
            node_uri,
            lane_uri,
            rate,
            prio,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert_eq!(rate, Some(0.5));
            assert!(prio.is_none());
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }

    let envelope = b"@link(node: \"/node\", lane: name, prio: 1)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Link {
            node_uri,
            lane_uri,
            rate,
            prio,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert!(rate.is_none());
            assert_eq!(prio, Some(1.0));
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }

    let envelope = b"@link(node: \"/node\", lane: name, rate: 0.1, prio: 1e-4)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Link {
            node_uri,
            lane_uri,
            rate,
            prio,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert_eq!(rate, Some(0.1));
            assert_eq!(prio, Some(1e-4));
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }
}

#[test]
fn peel_sync() {
    let envelope = b"@sync(node: \"/node\", lane: name)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Sync {
            node_uri,
            lane_uri,
            rate,
            prio,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert!(rate.is_none());
            assert!(prio.is_none());
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }

    let envelope = b"@sync(node: \"/node\", lane: name, rate: 0.5)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Sync {
            node_uri,
            lane_uri,
            rate,
            prio,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert_eq!(rate, Some(0.5));
            assert!(prio.is_none());
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }

    let envelope = b"@sync(node: \"/node\", lane: name, prio: 1)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Sync {
            node_uri,
            lane_uri,
            rate,
            prio,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert!(rate.is_none());
            assert_eq!(prio, Some(1.0));
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }

    let envelope = b"@sync(node: \"/node\", lane: name, rate: 0.1, prio: 1e-4)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Sync {
            node_uri,
            lane_uri,
            rate,
            prio,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert_eq!(rate, Some(0.1));
            assert_eq!(prio, Some(1e-4));
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }
}

#[test]
fn peel_linked() {
    let envelope = b"@linked(node: \"/node\", lane: name)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Linked {
            node_uri,
            lane_uri,
            rate,
            prio,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert!(rate.is_none());
            assert!(prio.is_none());
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }

    let envelope = b"@linked(node: \"/node\", lane: name, rate: 0.5)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Linked {
            node_uri,
            lane_uri,
            rate,
            prio,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert_eq!(rate, Some(0.5));
            assert!(prio.is_none());
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }

    let envelope = b"@linked(node: \"/node\", lane: name, prio: 1)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Linked {
            node_uri,
            lane_uri,
            rate,
            prio,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert!(rate.is_none());
            assert_eq!(prio, Some(1.0));
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }

    let envelope = b"@linked(node: \"/node\", lane: name, rate: 0.1, prio: 1e-4)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Linked {
            node_uri,
            lane_uri,
            rate,
            prio,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert_eq!(rate, Some(0.1));
            assert_eq!(prio, Some(1e-4));
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }
}

#[test]
fn peel_command() {
    let envelope = b"@command(node: \"/node\", lane: name)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Command {
            node_uri,
            lane_uri,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }
}

#[test]
fn peel_unlink() {
    let envelope = b"@unlink(node: \"/node\", lane: name)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Unlink {
            node_uri,
            lane_uri,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }
}

#[test]
fn peel_synced() {
    let envelope = b"@synced(node: \"/node\", lane: name)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Synced {
            node_uri,
            lane_uri,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }
}

#[test]
fn peel_unlinked() {
    let envelope = b"@unlinked(node: \"/node\", lane: name)@body {a: 1}";
    let result = peel_envelope_header(envelope);

    match result {
        Ok(RawEnvelope::Unlinked {
            node_uri,
            lane_uri,
            body,
        }) => {
            assert_eq!(node_uri, "/node");
            assert_eq!(lane_uri, "name");
            assert_eq!(*body, "@body {a: 1}");
        }
        Ok(ow) => panic!("Unexpected envelope: {:?}", ow),
        Err(e) => panic!("Peeling header failed: {}", e),
    }
}

#[test]
fn bad_enevelopes() {
    let envelopes: &[&[u8]] = &[
        b"{}",
        b"@unknown(node: \"/node\", lane: name)@body {a: 1}",
        b"@unlinked(node: 4, lane: name)@body {a: 1}",
        b"@unlinked(node: \"/node\", lane: 5.6)@body {a: 1}",
        b"@linked(node: \"/node\", lane: name, rate: half)@body {a: 1}",
        b"@linked(node: \"/node\", lane: name, prio: \"max\")@body {a: 1}",
        b"@linked@body {a: 1}",
        b"@linked(7, node: \"/node\", lane: name, rate: 0.5)@body {a: 1}",
    ];
    for envelope in envelopes {
        let result = peel_envelope_header(envelope);
        assert!(result.is_err());
    }
}
