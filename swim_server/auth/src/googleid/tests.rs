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

use crate::googleid::{GoogleId, GoogleIdCredentials};
use biscuit::jwa::SignatureAlgorithm;
use biscuit::jws::{Compact, RegisteredHeader, Secret};
use biscuit::{ClaimsSet, Empty, RegisteredClaims, SingleOrMultiple};
use std::str::FromStr;

fn expected_biscuit() -> Compact<ClaimsSet<GoogleId>, Empty> {
    let expected_claims = ClaimsSet {
        registered: RegisteredClaims {
            issuer: Some(FromStr::from_str("accounts.google.com").unwrap()),
            subject: Some(FromStr::from_str("117614620700092979612").unwrap()),
            audience: Some(SingleOrMultiple::Single(
                FromStr::from_str(
                    "339656303991-hjc1rr2vv0lclnqg0jq76r4qar9c8p62.apps.googleusercontent.com",
                )
                .unwrap(),
            )),
            not_before: None,
            ..Default::default()
        },
        private: GoogleId {
            email_verified: false,
            authorised_party: "".to_string(),
            hosted_domain: Some("https://swim.ai/".parse().unwrap()),
            email: "tom@swim.ai".to_string(),
            name: "Tom".to_string(),
            picture: "https://www.swim.ai/images/marlin-swim-blue.svg"
                .parse()
                .unwrap(),
            given_name: "Tom".to_string(),
            family_name: "Tom".to_string(),
            locale: "EN".to_string(),
        },
    };

    Compact::new_decoded(
        From::from(RegisteredHeader {
            algorithm: SignatureAlgorithm::RS256,
            ..Default::default()
        }),
        expected_claims,
    )
}

fn expected_token() -> String {
    "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhY2NvdW50cy5nb29nbGUuY29tIiwic3ViIjoiMTE3N\
        jE0NjIwNzAwMDkyOTc5NjEyIiwiYXVkIjoiMzM5NjU2MzAzOTkxLWhqYzFycjJ2djBsY2xucWcwanE3NnI0cWFyOWM4\
        cDYyLmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJhenAiOiIiLCJoZCI\
        6Imh0dHBzOi8vc3dpbS5haS8iLCJlbWFpbCI6InRvbUBzd2ltLmFpIiwibmFtZSI6IlRvbSIsInBpY3R1cmUiOiJodH\
        RwczovL3d3dy5zd2ltLmFpL2ltYWdlcy9tYXJsaW4tc3dpbS1ibHVlLnN2ZyIsImdpdmVuX25hbWUiOiJUb20iLCJmY\
        W1pbHlfbmFtZSI6IlRvbSIsImxvY2FsZSI6IkVOIn0.USLIq07s92Ah4tMdJuxG-eiGwVxedFz8OBaI7r4GLLLzmJxq\
        FDdPbX_RuS7yejU9L22C45X7siqsWXcFwqenkNcwKcDArAxdyR5rjL8iOoC5i8s6i754zbLsxdqq5yuWBG18vqXFl7D\
        A0m2r8U60lUbdJh7kXDQNWn9hGfn4YjCnSEgCGkBVJQop_yIip-aTVzhahFEF4OFdMGBROHkUBeO5_FY_gV4PG5Qkc3\
        tNvt6GNZU6PkeSu0dhuivnmNkqBwyAjywnL4_nqNmRivvv2YYviKug80xR-n-jSO3Sm0Nj-8x44gLDzQAbxrYJwOzsu\
        MIe7Z7BTEC4lsNfpMfJWA"
        .into()
}

#[test]
fn test_signing() {
    let private_key = Secret::rsa_keypair_from_file("test/private_key.der").unwrap();
    let biscuit = expected_biscuit();
    let encoded_biscuit = biscuit.into_encoded(&private_key).expect("Encode error");
    let token = encoded_biscuit.unwrap_encoded().to_string();

    assert_eq!(token, expected_token())
}

#[tokio::test]
async fn test_decode() {
    let secret = Secret::public_key_from_file("test/public_key.der").unwrap();
    let credentials = GoogleIdCredentials(expected_token());
    let compact: Compact<ClaimsSet<GoogleId>, Empty> = Compact::new_encoded(&credentials.0);
    let algorithm = SignatureAlgorithm::RS256;
    let decoded_biscuit = compact.decode(&secret, algorithm).expect("Decode failure");

    assert_eq!(decoded_biscuit, expected_biscuit());
}
