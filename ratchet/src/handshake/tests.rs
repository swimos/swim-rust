use crate::handshake::{ProtocolError, ProtocolRegistry};
use http::header::SEC_WEBSOCKET_PROTOCOL;

#[test]
fn selects_protocol_ok() {
    let mut headers = [httparse::Header {
        name: SEC_WEBSOCKET_PROTOCOL.as_str(),
        value: b"warp, warps",
    }];
    let request = httparse::Request::new(&mut headers);

    let registry = ProtocolRegistry::new(vec!["warps", "warp"]);
    assert_eq!(
        registry.negotiate_request(&request),
        Ok(Some("warp".to_string()))
    );
}

#[test]
fn multiple_headers() {
    let mut headers = [
        httparse::Header {
            name: SEC_WEBSOCKET_PROTOCOL.as_str(),
            value: b"warp",
        },
        httparse::Header {
            name: SEC_WEBSOCKET_PROTOCOL.as_str(),
            value: b"warps",
        },
    ];
    let request = httparse::Request::new(&mut headers);

    let registry = ProtocolRegistry::new(vec!["warps", "warp"]);
    assert_eq!(
        registry.negotiate_request(&request),
        Ok(Some("warp".to_string()))
    );
}

#[test]
fn mixed_headers() {
    let mut headers = [
        httparse::Header {
            name: SEC_WEBSOCKET_PROTOCOL.as_str(),
            value: b"warp1.0",
        },
        httparse::Header {
            name: SEC_WEBSOCKET_PROTOCOL.as_str(),
            value: b"warps2.0,warp3.0",
        },
        httparse::Header {
            name: SEC_WEBSOCKET_PROTOCOL.as_str(),
            value: b"warps4.0",
        },
    ];
    let request = httparse::Request::new(&mut headers);

    let registry = ProtocolRegistry::new(vec!["warps", "warp", "warps2.0"]);
    assert_eq!(
        registry.negotiate_request(&request),
        Ok(Some("warps2.0".to_string()))
    );
}

#[test]
fn malformatted() {
    let mut headers = [httparse::Header {
        name: SEC_WEBSOCKET_PROTOCOL.as_str(),
        value: &[255, 255, 255, 255],
    }];
    let request = httparse::Request::new(&mut headers);

    let registry = ProtocolRegistry::new(vec!["warps", "warp", "warps2.0"]);
    assert_eq!(
        registry.negotiate_request(&request),
        Err(ProtocolError::Encoding)
    );
}

#[test]
fn no_match() {
    let mut headers = [httparse::Header {
        name: SEC_WEBSOCKET_PROTOCOL.as_str(),
        value: b"a,b,c",
    }];
    let request = httparse::Request::new(&mut headers);

    let registry = ProtocolRegistry::new(vec!["d"]);
    assert_eq!(registry.negotiate_request(&request), Ok(None));
}
