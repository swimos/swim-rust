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

use thiserror::Error;

/// Model for HTTP status codes. The representation is a u16 but only instances representing
/// valid status codes can be constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub struct StatusCode(u16);

#[derive(Debug, Error)]
#[error("{0} is not a valid HTTP status code.")]
pub struct InvalidStatusCode(u16);

impl StatusCode {
    pub fn as_u16(&self) -> u16 {
        self.0
    }

    pub const CONTINUE: StatusCode = StatusCode(100);
    pub const SWITCHING_PROTOCOLS: StatusCode = StatusCode(101);
    pub const OK: StatusCode = StatusCode(200);
    pub const CREATED: StatusCode = StatusCode(201);
    pub const ACCEPTED: StatusCode = StatusCode(202);
    pub const NON_AUTHORITATIVE_INFORMATION: StatusCode = StatusCode(203);
    pub const NO_CONTENT: StatusCode = StatusCode(204);
    pub const RESET_CONTENT: StatusCode = StatusCode(205);
    pub const PARTIAL_CONTENT: StatusCode = StatusCode(206);
    pub const MULTIPLE_CHOICES: StatusCode = StatusCode(300);
    pub const MOVED_PERMANENTLY: StatusCode = StatusCode(301);
    pub const FOUND: StatusCode = StatusCode(302);
    pub const SEE_OTHER: StatusCode = StatusCode(303);
    pub const NOT_MODIFIED: StatusCode = StatusCode(304);
    pub const USE_PROXY: StatusCode = StatusCode(305);
    pub const TEMPORARY_REDIRECT: StatusCode = StatusCode(307);
    pub const PERMANENT_REDIRECT: StatusCode = StatusCode(308);
    pub const BAD_REQUEST: StatusCode = StatusCode(400);
    pub const UNAUTHORIZED: StatusCode = StatusCode(401);
    pub const PAYMENT_REQUIRED: StatusCode = StatusCode(402);
    pub const FORBIDDEN: StatusCode = StatusCode(403);
    pub const NOT_FOUND: StatusCode = StatusCode(404);
    pub const METHOD_NOT_ALLOWED: StatusCode = StatusCode(405);
    pub const NOT_ACCEPTABLE: StatusCode = StatusCode(406);
    pub const PROXY_AUTHENTICATION_REQUIRED: StatusCode = StatusCode(407);
    pub const REQUEST_TIMEOUT: StatusCode = StatusCode(408);
    pub const CONFLICT: StatusCode = StatusCode(409);
    pub const GONE: StatusCode = StatusCode(410);
    pub const LENGTH_REQUIRED: StatusCode = StatusCode(411);
    pub const PRECONDITION_FAILED: StatusCode = StatusCode(412);
    pub const PAYLOAD_TOO_LARGE: StatusCode = StatusCode(413);
    pub const URI_TOO_LONG: StatusCode = StatusCode(414);
    pub const UNSUPPORTED_MEDIA_TYPE: StatusCode = StatusCode(415);
    pub const RANGE_NOT_SATISFIABLE: StatusCode = StatusCode(416);
    pub const EXPECTATION_FAILED: StatusCode = StatusCode(417);
    pub const IM_A_TEAPOT: StatusCode = StatusCode(418);
    pub const MISDIRECTED_REQUEST: StatusCode = StatusCode(421);
    pub const UNPROCESSABLE_ENTITY: StatusCode = StatusCode(422);
    pub const LOCKED: StatusCode = StatusCode(423);
    pub const FAILED_DEPENDENCY: StatusCode = StatusCode(424);
    pub const UPGRADE_REQUIRED: StatusCode = StatusCode(426);
    pub const PRECONDITION_REQUIRED: StatusCode = StatusCode(428);
    pub const TOO_MANY_REQUESTS: StatusCode = StatusCode(429);
    pub const REQUEST_HEADER_FIELDS_TOO_LARGE: StatusCode = StatusCode(431);
    pub const UNAVAILABLE_FOR_LEGAL_REASONS: StatusCode = StatusCode(451);
    pub const INTERNAL_SERVER_ERROR: StatusCode = StatusCode(500);
    pub const NOT_IMPLEMENTED: StatusCode = StatusCode(501);
    pub const BAD_GATEWAY: StatusCode = StatusCode(502);
    pub const SERVICE_UNAVAILABLE: StatusCode = StatusCode(503);
    pub const GATEWAY_TIMEOUT: StatusCode = StatusCode(504);
    pub const HTTP_VERSION_NOT_SUPPORTED: StatusCode = StatusCode(505);
    pub const VARIANT_ALSO_NEGOTIATES: StatusCode = StatusCode(506);
    pub const INSUFFICIENT_STORAGE: StatusCode = StatusCode(507);
    pub const LOOP_DETECTED: StatusCode = StatusCode(508);
    pub const NOT_EXTENDED: StatusCode = StatusCode(510);
    pub const NETWORK_AUTHENTICATION_REQUIRED: StatusCode = StatusCode(511);
}

impl From<http::StatusCode> for StatusCode {
    fn from(value: http::StatusCode) -> Self {
        StatusCode(value.as_u16())
    }
}

impl From<StatusCode> for http::StatusCode {
    fn from(value: StatusCode) -> Self {
        if let Ok(c) = http::StatusCode::from_u16(value.0) {
            c
        } else {
            unreachable!()
        }
    }
}

impl TryFrom<u16> for StatusCode {
    type Error = InvalidStatusCode;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        if http::StatusCode::from_u16(value).is_ok() {
            Ok(StatusCode(value))
        } else {
            Err(InvalidStatusCode(value))
        }
    }
}
